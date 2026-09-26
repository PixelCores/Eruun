package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type grpcTestDelivery struct{ code string }

func TestRPCErrorPreservesDatastoreCause(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"canceled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"not found", datastore.ErrRecordNotExist, codes.NotFound},
		{"unexpected", errors.New("internal database failure"), codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := fmt.Errorf("query: %w", datastore.NewDBError(tc.err))
			require.Equal(t, tc.code, status.Code(rpcError(err)))
		})
	}
}

func (d *grpcTestDelivery) SendCode(_ context.Context, _, _, code string) error {
	d.code = code
	return nil
}
func (d *grpcTestDelivery) SendInvitation(context.Context, string, string) error { return nil }

func grpcTestAccounts(t *testing.T) (*account.Service, *grpcTestDelivery) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "grpc-accounts.db")), &gorm.Config{
		NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(
		&model.User{}, &model.Identity{}, &model.Session{}, &model.Workspace{},
		&model.WorkspaceMember{}, &model.WorkspaceInvitation{}, &model.SystemSetting{},
		&model.Applications{}, &model.WorkflowQueue{}, &model.JobInfo{},
		&model.JobArtifact{}, &model.ArtifactChunk{}, &model.JobDelivery{}, &model.JobSandbox{},
	))
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	delivery := &grpcTestDelivery{}
	accounts := account.New(&sqlstore.Driver{Client: *db}, &spec.AccountConfig{
		SMTP: spec.SMTPConfig{Host: "smtp.example.com"},
	}, client, delivery)
	return accounts, delivery
}

func grpcRegisterTestUser(t *testing.T, accounts *account.Service, delivery *grpcTestDelivery, address string) (*account.Login, *account.Principal) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, accounts.SendCode(ctx, "register", "email", address, "192.0.2.9"))
	login, err := accounts.Register(ctx, "email", address, delivery.code, "correct horse battery", "Test User")
	require.NoError(t, err)
	p, err := accounts.Authenticate(ctx, login.AccessToken)
	require.NoError(t, err)
	return login, p
}

func grpcTestClient(t *testing.T, accounts *account.Service) *grpc.ClientConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := NewServer(accounts, nil, &AdministrationServer{}, &JobsServer{}, &ApplicationsServer{}, nil)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); require.NoError(t, <-done) })
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func grpcBearer(ctx context.Context, token, workspaceID string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token, "x-eruun-workspace-id", workspaceID))
}

func TestGRPCNativeRegisterAndLoginReturnBothTokens(t *testing.T) {
	accounts, delivery := grpcTestAccounts(t)
	conn := grpcTestClient(t, accounts)
	client := eruunv1.NewAccountServiceClient(conn)
	identifier := "native-grpc@example.com"
	_, err := client.SendAuthCode(context.Background(), &eruunv1.AuthCodeRequest{
		Purpose: "register", Provider: "email", Identifier: identifier,
	})
	require.NoError(t, err)
	require.NotEmpty(t, delivery.code)
	registered, err := client.Register(context.Background(), &eruunv1.RegisterRequest{
		Provider: "email", Identifier: identifier, Code: delivery.code,
		Password: "correct horse battery", Name: "Native Client",
	})
	require.NoError(t, err)
	require.NotEmpty(t, registered.AccessToken)
	require.NotEmpty(t, registered.RefreshToken)

	loggedIn, err := client.Login(context.Background(), &eruunv1.LoginRequest{
		Provider: "email", Identifier: identifier, Password: "correct horse battery",
	})
	require.NoError(t, err)
	require.NotEmpty(t, loggedIn.AccessToken)
	require.NotEmpty(t, loggedIn.RefreshToken)
	require.NotEqual(t, registered.RefreshToken, loggedIn.RefreshToken)
}

func TestGRPCAuthenticationWorkspaceGuardAdminAndRotation(t *testing.T) {
	accounts, delivery := grpcTestAccounts(t)
	first, firstPrincipal := grpcRegisterTestUser(t, accounts, delivery, "grpc-first@example.com")
	second, secondPrincipal := grpcRegisterTestUser(t, accounts, delivery, "grpc-second@example.com")
	firstSpace, err := accounts.Workspace(context.Background(), firstPrincipal, "")
	require.NoError(t, err)
	secondSpace, err := accounts.Workspace(context.Background(), secondPrincipal, "")
	require.NoError(t, err)
	require.NoError(t, accounts.Repo.Store.Add(context.Background(), &model.Applications{
		ID: "app-of-second", WorkspaceID: secondSpace.Workspace.ID, Name: "private",
	}))
	conn := grpcTestClient(t, accounts)
	accountClient := eruunv1.NewAccountServiceClient(conn)
	jobsClient := eruunv1.NewJobsServiceClient(conn)
	applicationClient := eruunv1.NewApplicationServiceClient(conn)
	ctx := grpcBearer(context.Background(), first.AccessToken, firstSpace.Workspace.ID)
	me, err := accountClient.GetMe(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, firstPrincipal.User.ID, me.User.Id)
	_, err = accountClient.ListAdminUsers(ctx, &eruunv1.ListAdminUsersRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = jobsClient.GetJobStoragePolicy(grpcBearer(context.Background(), first.AccessToken, secondSpace.Workspace.ID), nil)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = applicationClient.GetApplicationStatus(ctx, &eruunv1.ApplicationIDRequest{AppId: "app-of-second"})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = accountClient.GetMe(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong-token")), nil)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = jobsClient.GetJobStoragePolicy(metadata.NewOutgoingContext(context.Background(), metadata.MD{
		"authorization":        {"Bearer " + first.AccessToken},
		"x-eruun-workspace-id": {firstSpace.Workspace.ID, secondSpace.Workspace.ID},
	}), nil)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = authorizeRPC(context.Background(), "/eruun.v1.Unknown/Call", accounts, nil)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	refreshed, err := accountClient.Refresh(context.Background(), &eruunv1.RefreshRequest{RefreshToken: first.RefreshToken})
	require.NoError(t, err)
	require.NotEmpty(t, refreshed.AccessToken)
	require.NotEmpty(t, refreshed.RefreshToken)
	require.NotEqual(t, first.RefreshToken, refreshed.RefreshToken)
	_, err = accountClient.GetMe(ctx, nil)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = accountClient.Refresh(context.Background(), &eruunv1.RefreshRequest{RefreshToken: first.RefreshToken})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = accountClient.GetMe(grpcBearer(context.Background(), refreshed.AccessToken, firstSpace.Workspace.ID), nil)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.NotEmpty(t, second.AccessToken)
}

func TestGRPCErrorDetailBusinessCodeAndRedaction(t *testing.T) {
	err := rpcError(errors.New("password=should-never-escape"))
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "should-never-escape")
	parsed := status.Convert(err)
	require.Len(t, parsed.Details(), 1)
	info, ok := parsed.Details()[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, "eruun.io", info.Domain)
	require.NotEmpty(t, info.Metadata["business_code"])

	err = rpcError(bcode.ErrApplicationNotExist)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.False(t, strings.Contains(err.Error(), "password="))
}
