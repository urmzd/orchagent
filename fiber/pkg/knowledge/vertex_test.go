package knowledge

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"

	"google.golang.org/genai"
)

func fakeVertex(t *testing.T, calls *int, fail error) func(context.Context, string, []*genai.Content, *genai.EmbedContentConfig) (*genai.EmbedContentResponse, error) {
	t.Helper()
	return func(_ context.Context, model string, contents []*genai.Content, cfg *genai.EmbedContentConfig) (*genai.EmbedContentResponse, error) {
		*calls++
		if fail != nil {
			return nil, fail
		}
		if model != "m" || cfg.OutputDimensionality == nil || *cfg.OutputDimensionality != 3 {
			t.Fatalf("model %q dims %v", model, cfg.OutputDimensionality)
		}
		resp := &genai.EmbedContentResponse{}
		for i := range contents {
			resp.Embeddings = append(resp.Embeddings, &genai.ContentEmbedding{Values: []float32{float32(i + 1), 0, 0}})
		}
		return resp, nil
	}
}

func TestVertexBatchesAndNormalizes(t *testing.T) {
	calls := 0
	v, err := NewVertex(context.Background(), VertexConfig{Project: "p", Model: "m", Dimensions: 3, BatchSize: 2, embed: fakeVertex(t, &calls, nil)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := v.Embed(context.Background(), []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(got) != 3 {
		t.Fatalf("calls %d vectors %d", calls, len(got))
	}
	for _, vec := range got {
		var n float64
		for _, x := range vec {
			n += float64(x) * float64(x)
		}
		if math.Abs(n-1) > 1e-6 {
			t.Fatalf("not unit: %v", vec)
		}
	}
	if v.Model() != "vertex/m/3" || v.Dimensions() != 3 {
		t.Fatalf("model %q dims %d", v.Model(), v.Dimensions())
	}
}

func TestVertexReportsFailures(t *testing.T) {
	calls := 0
	v, _ := NewVertex(context.Background(), VertexConfig{Project: "p", Model: "m", Dimensions: 3, embed: fakeVertex(t, &calls, errors.New("quota"))})
	if _, err := v.Embed(context.Background(), []string{"a"}); err == nil {
		t.Fatal("no error")
	}
	if _, err := NewVertex(context.Background(), VertexConfig{}); err == nil {
		t.Fatal("accepted no project")
	}
}

// TestVertexLive calls Vertex AI for real. It runs only when
// HUMAGEN_VERTEX_LIVE_PROJECT names a project the caller's Application
// Default Credentials can use.
func TestVertexLive(t *testing.T) {
	project := os.Getenv("HUMAGEN_VERTEX_LIVE_PROJECT")
	if project == "" {
		t.Skip("set HUMAGEN_VERTEX_LIVE_PROJECT to call Vertex AI")
	}
	v, err := NewVertex(context.Background(), VertexConfig{Project: project})
	if err != nil {
		t.Fatal(err)
	}
	got, err := v.Embed(context.Background(), []string{"rotating keys", "key rotation runbook", "soup on Fridays"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || len(got[0]) != DefaultVertexDimensions {
		t.Fatalf("%d vectors of %d", len(got), len(got[0]))
	}
	cos := func(a, b []float32) (s float64) {
		for i := range a {
			s += float64(a[i]) * float64(b[i])
		}
		return s
	}
	near, far := cos(got[0], got[1]), cos(got[0], got[2])
	t.Logf("rotating keys ~ key rotation runbook = %.3f, ~ soup on Fridays = %.3f", near, far)
	if near <= far {
		t.Fatalf("not semantic: near %.3f far %.3f", near, far)
	}
}
