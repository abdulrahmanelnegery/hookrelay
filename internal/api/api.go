// Package api is hookrelay's HTTP layer: a Gin router over the store. Handlers
// parse and validate input, call exactly one store method, and render JSON. No
// business logic lives here.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/abdulrahman-muhammed-07/hookrelay/internal/config"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/store"
)

const defaultDeliveryLimit = 100
const maxDeliveryLimit = 500

// New builds the router. gin runs in release mode; request logging is handled by
// the slog middleware so output stays structured.
func New(st *store.Store, cfg config.Config, log *slog.Logger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), requestLog(log))

	h := &handlers{store: st, cfg: cfg}

	r.GET("/healthz", h.health)
	r.POST("/subscriptions", h.createSubscription)
	r.GET("/subscriptions", h.listSubscriptions)
	r.POST("/events", h.createEvent)
	r.GET("/deliveries", h.listDeliveries)
	r.POST("/deliveries/:id/replay", h.replayDelivery)

	return r
}

type handlers struct {
	store *store.Store
	cfg   config.Config
}

func (h *handlers) health(c *gin.Context) {
	if err := h.store.Pool().Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, errorBody("database unreachable"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

type createSubscriptionRequest struct {
	TargetURL  string   `json:"target_url"`
	EventTypes []string `json:"event_types"`
	Secret     string   `json:"secret"`
}

func (h *handlers) createSubscription(c *gin.Context) {
	var req createSubscriptionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorBody("invalid JSON body"))
		return
	}
	if msg := validateSubscription(req); msg != "" {
		c.JSON(http.StatusUnprocessableEntity, errorBody(msg))
		return
	}

	sub, err := h.store.CreateSubscription(c.Request.Context(), req.TargetURL, dedupe(req.EventTypes), req.Secret)
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorBody("could not create subscription"))
		return
	}
	c.JSON(http.StatusCreated, sub)
}

func (h *handlers) listSubscriptions(c *gin.Context) {
	subs, err := h.store.ListSubscriptions(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorBody("could not list subscriptions"))
		return
	}
	c.JSON(http.StatusOK, subs)
}

type createEventRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (h *handlers) createEvent(c *gin.Context) {
	var req createEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorBody("invalid JSON body"))
		return
	}
	if req.Type == "" {
		c.JSON(http.StatusUnprocessableEntity, errorBody("type is required"))
		return
	}
	if len(req.Payload) == 0 || !json.Valid(req.Payload) {
		c.JSON(http.StatusUnprocessableEntity, errorBody("payload must be a JSON value"))
		return
	}

	ev, count, err := h.store.CreateEventWithDeliveries(c.Request.Context(), req.Type, req.Payload, h.cfg.MaxAttempts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorBody("could not accept event"))
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"event": ev, "deliveries_created": count})
}

func (h *handlers) listDeliveries(c *gin.Context) {
	status := c.Query("status")
	if status != "" && !store.DeliveryStatus(status).Valid() {
		c.JSON(http.StatusUnprocessableEntity, errorBody("unknown status filter"))
		return
	}

	limit := defaultDeliveryLimit
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			c.JSON(http.StatusUnprocessableEntity, errorBody("limit must be a positive integer"))
			return
		}
		if n > maxDeliveryLimit {
			n = maxDeliveryLimit
		}
		limit = n
	}

	items, err := h.store.ListDeliveries(c.Request.Context(), status, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorBody("could not list deliveries"))
		return
	}
	c.JSON(http.StatusOK, items)
}

func (h *handlers) replayDelivery(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, errorBody("id must be a UUID"))
		return
	}

	d, err := h.store.Replay(c.Request.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		c.JSON(http.StatusNotFound, errorBody("delivery not found"))
	case errors.Is(err, store.ErrNotReplayable):
		c.JSON(http.StatusConflict, errorBody("only dead or delivered deliveries can be replayed"))
	case err != nil:
		c.JSON(http.StatusInternalServerError, errorBody("could not replay delivery"))
	default:
		c.JSON(http.StatusOK, d)
	}
}

func validateSubscription(req createSubscriptionRequest) string {
	u, err := url.Parse(req.TargetURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "target_url must be an absolute http or https URL"
	}
	if len(req.EventTypes) == 0 {
		return "event_types must list at least one type"
	}
	for _, t := range req.EventTypes {
		if t == "" {
			return "event_types must not contain an empty string"
		}
	}
	if len(req.Secret) < 8 {
		return "secret must be at least 8 characters"
	}
	return ""
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func errorBody(msg string) gin.H {
	return gin.H{"error": msg}
}

// requestLog is a tiny structured access log so we do not pull in gin's default
// stdout logger.
func requestLog(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		log.Info("http",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
		)
	}
}
