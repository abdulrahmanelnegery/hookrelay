// Package signing produces and verifies the HMAC signatures hookrelay attaches
// to every webhook delivery so that a receiver can confirm the request really
// came from us and was not tampered with in transit.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// Header is the HTTP header hookrelay uses to carry the signature.
const Header = "X-Hookrelay-Signature"

// Sign returns the value for the signature header: the string "sha256=" followed
// by the hex-encoded HMAC-SHA256 of body keyed by secret.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify reports whether signature matches the HMAC of body under secret. The
// comparison is constant time so it does not leak how much of the signature was
// correct.
func Verify(secret string, body []byte, signature string) bool {
	expected := Sign(secret, body)
	return hmac.Equal([]byte(expected), []byte(signature))
}
