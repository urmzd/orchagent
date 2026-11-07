package github

import (
	"context"
	"fmt"
	"net/http"
)

type ReviewCommentRecord struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

// ReviewCommentsAPI recovers the actual native handles created by a review.
type ReviewCommentsAPI interface {
	ListReviewComments(context.Context, Ref, int64, int) ([]ReviewCommentRecord, error)
}

func (a *HTTPAPI) ListReviewComments(ctx context.Context, ref Ref, review int64, page int) ([]ReviewCommentRecord, error) {
	if err := validReadRef(ref, page); err != nil {
		return nil, err
	}
	if review <= 0 {
		return nil, fmt.Errorf("github: invalid review")
	}
	var out []ReviewCommentRecord
	err := a.client.Do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews/%d/comments?per_page=100&page=%d", ref.Owner, ref.Repo, ref.Number, review, page), nil, &out)
	return out, err
}
func (f *Fake) ListReviewComments(_ context.Context, ref Ref, review int64, page int) ([]ReviewCommentRecord, error) {
	if err := validReadRef(ref, page); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return nil, f.Err
	}
	for _, r := range f.submitted {
		if r.Ref == ref && r.Result.ID == review {
			var out []ReviewCommentRecord
			for i, c := range r.Review.Comments {
				if i/100+1 == page {
					out = append(out, ReviewCommentRecord{ID: review*1000 + int64(i) + 1, Body: c.Body})
				}
			}
			return out, nil
		}
	}
	return nil, nil
}
