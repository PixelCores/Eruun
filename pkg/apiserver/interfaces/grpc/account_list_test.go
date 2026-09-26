package grpcapi

import (
	"context"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/ptr"
	"testing"
)

type adminUserListStore struct {
	datastore.DataStore
	options *datastore.ListOptions
}

func (s *adminUserListStore) List(_ context.Context, _ datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	s.options = options
	return []datastore.Entity{&model.User{ID: "user", Name: "User"}}, nil
}

func TestAdminUsersGRPCPagination(t *testing.T) {
	for _, tc := range []struct {
		name       string
		req        *eruunv1.ListAdminUsersRequest
		code       codes.Code
		page, size int
	}{
		{"nil defaults", nil, codes.OK, 1, 20},
		{"empty defaults", &eruunv1.ListAdminUsersRequest{}, codes.OK, 1, 20},
		{"explicit bounds", &eruunv1.ListAdminUsersRequest{Page: ptr.To(int32(2)), PageSize: ptr.To(int32(100))}, codes.OK, 2, 100},
		{"zero page", &eruunv1.ListAdminUsersRequest{Page: ptr.To(int32(0))}, codes.InvalidArgument, 0, 0},
		{"large size", &eruunv1.ListAdminUsersRequest{PageSize: ptr.To(int32(101))}, codes.InvalidArgument, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &adminUserListStore{}
			server := &AccountServer{Accounts: &account.Service{Repo: repository.Accounts{Store: store}}}
			ctx := context.WithValue(context.Background(), principalContextKey{}, &account.Principal{User: &model.User{SystemAdmin: true}})
			response, err := server.ListAdminUsers(ctx, tc.req)
			require.Equal(t, tc.code, status.Code(err))
			if tc.code != codes.OK {
				require.Nil(t, store.options)
				return
			}
			require.Equal(t, tc.page, store.options.Page)
			require.Equal(t, tc.size, store.options.PageSize)
			require.Len(t, response.Users, 1)
			require.Equal(t, "user", response.Users[0].Id)
		})
	}
}
