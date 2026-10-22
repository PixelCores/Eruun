package jobexec

import (
	"context"
	"errors"
	"testing"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type fakeResource struct {
	value string
}

func TestCreateOrUpdateResource(t *testing.T) {
	t.Parallel()

	resource := schema.GroupResource{Resource: "fakes"}
	errNotFound := k8serrors.NewNotFound(resource, "demo")
	errAlreadyExists := k8serrors.NewAlreadyExists(resource, "demo")

	tests := []struct {
		name         string
		getFn        func() (*fakeResource, error)
		createFn     func() (*fakeResource, error)
		wantCreated  bool
		wantUpdated  bool
		wantErrMatch error
	}{
		{
			name: "existing resource updates",
			getFn: func() (*fakeResource, error) {
				return &fakeResource{value: "existing"}, nil
			},
			createFn:    func() (*fakeResource, error) { return &fakeResource{value: "created"}, nil },
			wantCreated: false,
			wantUpdated: true,
		},
		{
			name: "create when missing",
			getFn: func() (*fakeResource, error) {
				return nil, errNotFound
			},
			createFn:    func() (*fakeResource, error) { return &fakeResource{value: "created"}, nil },
			wantCreated: true,
			wantUpdated: false,
		},
		{
			name: "already exists after create retries update",
			getFn: func() func() (*fakeResource, error) {
				calls := 0
				return func() (*fakeResource, error) {
					calls++
					if calls == 1 {
						return nil, errNotFound
					}
					return &fakeResource{value: "existing"}, nil
				}
			}(),
			createFn:    func() (*fakeResource, error) { return nil, errAlreadyExists },
			wantCreated: false,
			wantUpdated: true,
		},
		{
			name: "get failure bubbles up",
			getFn: func() (*fakeResource, error) {
				return nil, errors.New("boom")
			},
			createFn:     func() (*fakeResource, error) { return &fakeResource{value: "created"}, nil },
			wantCreated:  false,
			wantUpdated:  false,
			wantErrMatch: errors.New("boom"),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			updated := 0
			onExisting := func(_ context.Context, _ *fakeResource) error {
				updated++
				return nil
			}

			got, created, err := CreateOrUpdateResource(context.Background(), func(context.Context) (*fakeResource, error) {
				return tt.getFn()
			}, func(context.Context) (*fakeResource, error) {
				return tt.createFn()
			}, onExisting)

			if tt.wantErrMatch != nil {
				if err == nil || err.Error() != tt.wantErrMatch.Error() {
					t.Fatalf("expected error %q, got %v", tt.wantErrMatch, err)
				}
				if got != nil {
					t.Fatalf("expected nil resource on error, got %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if created != tt.wantCreated {
				t.Fatalf("created = %v, want %v", created, tt.wantCreated)
			}
			if updated > 0 != tt.wantUpdated {
				t.Fatalf("updated = %v, want %v", updated > 0, tt.wantUpdated)
			}
		})
	}
}

func TestUpdateResourceWithRetry(t *testing.T) {
	t.Parallel()

	getCalls := 0
	updateCalls := 0
	err := UpdateResourceWithRetry(context.Background(), func(context.Context) (*fakeResource, error) {
		getCalls++
		return &fakeResource{value: "latest"}, nil
	}, func(_ context.Context, res *fakeResource) error {
		updateCalls++
		if res.value != "latest" {
			t.Fatalf("unexpected resource value: %s", res.value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if getCalls != 1 {
		t.Fatalf("get calls = %d, want 1", getCalls)
	}
	if updateCalls != 1 {
		t.Fatalf("update calls = %d, want 1", updateCalls)
	}
}
