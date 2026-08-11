package postgresstore

import (
	"testing"
	"time"
)

func TestValidateIdent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		ident   string
		wantErr bool
	}{
		{name: "ok", ident: "ratelimit_buckets", wantErr: false},
		{name: "empty", ident: "", wantErr: true},
		{name: "spaces", ident: "bad name", wantErr: true},
		{name: "inject", ident: "t;drop", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateIdent(tt.ident)
			if tt.wantErr && err == nil {
				t.Fatalf("got nil error, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		})
	}
}

func TestEncodeDecodeLogTimes(t *testing.T) {
	t.Parallel()
	in := []time.Time{
		time.Unix(0, 100).UTC(),
		time.Unix(0, 200).UTC(),
	}
	raw, err := encodeLogTimes(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decodeLogTimes(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("len: got %d, want %d", len(out), len(in))
	}
	for i := range in {
		if !out[i].Equal(in[i]) {
			t.Fatalf(
				"[%d]: got %v, want %v",
				i,
				out[i],
				in[i],
			)
		}
	}
}
