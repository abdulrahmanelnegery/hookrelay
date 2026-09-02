package signing

import "testing"

func TestSign(t *testing.T) {
	tests := []struct {
		name   string
		secret string
		body   string
		want   string
	}{
		{
			// Known vector: HMAC-SHA256("the-secret", `{"hello":"world"}`).
			// Regenerate with:
			//   printf '%s' '{"hello":"world"}' | openssl dgst -sha256 -hmac 'the-secret'
			name:   "known vector",
			secret: "the-secret",
			body:   `{"hello":"world"}`,
			want:   "sha256=52cf56620657f8d33389a43711016a17fa739db194853127b910efd13a22c16b",
		},
		{
			name:   "empty body",
			secret: "the-secret",
			body:   "",
			want:   "sha256=e872fd29a1974e0504506a0b5dc887db4ad97c1b6c75e5e13628223c2511f1dd",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sign(tt.secret, []byte(tt.body))
			if got != tt.want {
				t.Fatalf("Sign() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestVerify(t *testing.T) {
	secret := "s3cr3t"
	body := []byte(`{"event":"ping"}`)
	good := Sign(secret, body)

	if !Verify(secret, body, good) {
		t.Fatal("Verify() rejected a valid signature")
	}
	if Verify("wrong", body, good) {
		t.Fatal("Verify() accepted a signature under the wrong secret")
	}
	if Verify(secret, []byte("tampered"), good) {
		t.Fatal("Verify() accepted a signature for a tampered body")
	}
	if Verify(secret, body, "sha256=deadbeef") {
		t.Fatal("Verify() accepted a malformed signature")
	}
}
