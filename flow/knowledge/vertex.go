package knowledge

import (
	"context"
	"errors"
	"fmt"
	"math"

	"google.golang.org/genai"
)

// Vertex defaults: gemini-embedding-001 accepts batches, and 768 dimensions
// keep a passage's vector at about 3 KB in float32.
const (
	DefaultVertexModel      = "gemini-embedding-001"
	DefaultVertexDimensions = 768
	DefaultVertexLocation   = "us-central1"
	// DefaultVertexBatchSize keeps a request of 800-byte passages well under
	// Vertex's per-request token limit.
	DefaultVertexBatchSize = 32
)

// VertexConfig configures a Vertex AI embedder.
type VertexConfig struct {
	// Project is the GCP project billed for the calls. Required.
	Project string
	// Location is the Vertex region. Empty takes DefaultVertexLocation.
	Location string
	// Model is the embedding model. Empty takes DefaultVertexModel.
	Model string
	// Dimensions is the width requested from the model. Zero takes
	// DefaultVertexDimensions.
	Dimensions int
	// BatchSize is how many texts go in one request. Zero takes
	// DefaultVertexBatchSize.
	BatchSize int
	// embed replaces the API call in tests.
	embed func(ctx context.Context, model string, contents []*genai.Content, config *genai.EmbedContentConfig) (*genai.EmbedContentResponse, error)
}

// Vertex embeds with Vertex AI under Application Default Credentials: the
// metadata server on GCE, gcloud's credentials on a workstation. No key is
// held by the process.
//
// Vectors are normalized to unit length: a model asked for fewer dimensions
// than it produces returns vectors that are not, and the dense arm compares
// them by dot product.
type Vertex struct {
	cfg   VertexConfig
	embed func(ctx context.Context, model string, contents []*genai.Content, config *genai.EmbedContentConfig) (*genai.EmbedContentResponse, error)
}

var _ Embedder = (*Vertex)(nil)

// NewVertex builds a Vertex embedder. It opens no connection: credentials are
// found on the first call.
func NewVertex(ctx context.Context, cfg VertexConfig) (*Vertex, error) {
	if cfg.Project == "" {
		return nil, errors.New("knowledge: vertex embedder needs a project")
	}
	if cfg.Location == "" {
		cfg.Location = DefaultVertexLocation
	}
	if cfg.Model == "" {
		cfg.Model = DefaultVertexModel
	}
	if cfg.Dimensions <= 0 {
		cfg.Dimensions = DefaultVertexDimensions
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultVertexBatchSize
	}
	v := &Vertex{cfg: cfg, embed: cfg.embed}
	if v.embed == nil {
		client, err := genai.NewClient(ctx, &genai.ClientConfig{Project: cfg.Project, Location: cfg.Location, Backend: genai.BackendVertexAI})
		if err != nil {
			return nil, fmt.Errorf("knowledge: vertex client: %w", err)
		}
		v.embed = client.Models.EmbedContent
	}
	return v, nil
}

func (v *Vertex) Dimensions() int { return v.cfg.Dimensions }

func (v *Vertex) Model() string { return fmt.Sprintf("vertex/%s/%d", v.cfg.Model, v.cfg.Dimensions) }

func (v *Vertex) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	dims := int32(v.cfg.Dimensions)
	for start := 0; start < len(texts); start += v.cfg.BatchSize {
		end := min(start+v.cfg.BatchSize, len(texts))
		contents := make([]*genai.Content, 0, end-start)
		for _, t := range texts[start:end] {
			contents = append(contents, genai.NewContentFromText(t, genai.RoleUser))
		}
		resp, err := v.embed(ctx, v.cfg.Model, contents, &genai.EmbedContentConfig{OutputDimensionality: &dims})
		if err != nil {
			return nil, fmt.Errorf("vertex embed: %w", err)
		}
		if resp == nil || len(resp.Embeddings) != end-start {
			got := 0
			if resp != nil {
				got = len(resp.Embeddings)
			}
			return nil, fmt.Errorf("vertex embed: %d vectors for %d texts", got, end-start)
		}
		for _, e := range resp.Embeddings {
			if e == nil {
				return nil, errors.New("vertex embed: an empty embedding")
			}
			out = append(out, unit(e.Values))
		}
	}
	return out, nil
}

// unit scales v to length one. A zero vector is returned as it is.
func unit(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return v
	}
	n := float32(1 / math.Sqrt(sum))
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * n
	}
	return out
}
