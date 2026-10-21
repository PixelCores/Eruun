package grpcapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/nodepool"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	containerutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/container"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestSchedulingProfileHTTPAndGRPCParity(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "parity.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(&model.NodePool{}))
	store := &sqlstore.Driver{Client: *db}
	nodeRepo, err := repository.NewNodePoolRepositoryWithStore(store)
	require.NoError(t, err)
	nodes, err := nodepool.NewNodePoolServiceWithRepository(nodeRepo)
	require.NoError(t, err)
	nodeHandler := api.NewNodePools()
	beans := containerutil.NewContainer()
	require.NoError(t, beans.Provides(nodes, nodeHandler))
	require.NoError(t, beans.Populate())
	gin.SetMode(gin.TestMode)
	router := gin.New()
	group := router.Group("/api/v1")
	nodeHandler.RegisterRoutes(group)
	server := &AdministrationServer{NodePools: nodes}
	ctx := context.Background()
	request := func(t *testing.T, method, path, body string, want int) json.RawMessage {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, want, rec.Code, rec.Body.String())
		if want == 204 {
			require.Empty(t, rec.Body.String())
			return nil
		}
		var envelope struct {
			Code    int32           `json:"code"`
			Data    json.RawMessage `json:"data"`
			Message string          `json:"message"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope))
		if want >= 400 {
			return rec.Body.Bytes()
		}
		require.Zero(t, envelope.Code)
		return envelope.Data
	}
	for _, tc := range []struct {
		path, collection, create, update, invalid string
		invalidCode, existsCode, notFoundCode     string
		createRPC                                 func(string) (proto.Message, error)
		updateRPC                                 func(string) (proto.Message, error)
		getRPC                                    func(string) (proto.Message, error)
		listRPC                                   func() (proto.Message, error)
		optionsRPC                                func() (proto.Message, error)
		deleteRPC                                 func(string) error
	}{
		{path: "node-pools", collection: "nodePools",
			create: `{"name":"sample","description":"description","nodeSelector":{"eruun.io/pool":"cpu"},"tolerations":[{"operator":"Exists","effect":"NoExecute","tolerationSeconds":0}]}`,
			update: `{"nodeSelector":{"pool":"gpu"}}`, invalid: `{"name":"invalid","nodeSelector":{"pool":""}}`, invalidCode: "35000", existsCode: "35001", notFoundCode: "35002",
			createRPC: func(body string) (proto.Message, error) {
				req := &eruunv1.CreateNodePoolRequest{}
				require.NoError(t, protojson.Unmarshal([]byte(body), req))
				return server.CreateNodePool(ctx, req)
			},
			updateRPC: func(body string) (proto.Message, error) {
				req := &eruunv1.UpdateNodePoolRequest{}
				require.NoError(t, protojson.Unmarshal([]byte(body), req))
				return server.UpdateNodePool(ctx, req)
			},
			getRPC: func(name string) (proto.Message, error) {
				return server.GetNodePool(ctx, &eruunv1.NodePoolNameRequest{Name: name})
			},
			listRPC:    func() (proto.Message, error) { return server.ListNodePools(ctx, nil) },
			optionsRPC: func() (proto.Message, error) { return server.ListNodePoolOptions(ctx, nil) },
			deleteRPC: func(name string) error {
				_, err := server.DeleteNodePool(ctx, &eruunv1.NodePoolNameRequest{Name: name})
				return err
			},
		},
	} {
		t.Run(tc.path, func(t *testing.T) {
			path := "/api/v1/admin/" + tc.path
			compare := func(data json.RawMessage, message proto.Message) {
				t.Helper()
				expected := message.ProtoReflect().New().Interface()
				require.NoError(t, protojson.Unmarshal(data, expected))
				require.True(t, proto.Equal(expected, message), "HTTP %s; gRPC %s", data, message)
			}
			assertError := func(httpBody []byte, rpcErr error, code codes.Code, business string) {
				t.Helper()
				require.Equal(t, code, status.Code(rpcErr))
				var envelope struct {
					Code    json.Number `json:"code"`
					Message string      `json:"message"`
				}
				require.NoError(t, json.Unmarshal(httpBody, &envelope))
				require.Equal(t, business, envelope.Code.String())
				st := status.Convert(rpcErr)
				require.Equal(t, envelope.Message, st.Message())
				require.Len(t, st.Details(), 1)
				detail, ok := st.Details()[0].(*errdetails.ErrorInfo)
				require.True(t, ok)
				require.Equal(t, business, detail.Metadata["business_code"])
			}
			// Empty HTTP collections must be arrays, and have equivalent protobuf lists.
			empty := request(t, "GET", path, "", 200)
			require.JSONEq(t, `{"`+tc.collection+`":[]}`, string(empty))
			listed, err := tc.listRPC()
			require.NoError(t, err)
			compare(empty, listed)
			created := request(t, "POST", path, tc.create, 201)
			got, err := tc.getRPC("sample")
			require.NoError(t, err)
			compare(created, got)
			listed, err = tc.listRPC()
			require.NoError(t, err)
			compare(request(t, "GET", path, "", 200), listed)
			options := request(t, "GET", "/api/v1/"+tc.path, "", 200)
			require.JSONEq(t, `{"`+tc.collection+`":[{"name":"sample","description":"description"}]}`, string(options))
			rpcOptions, err := tc.optionsRPC()
			require.NoError(t, err)
			compare(options, rpcOptions)
			for _, secret := range []string{"nodeSelector", "tolerations", "rules"} {
				require.NotContains(t, string(options), secret)
			}
			duplicate := request(t, "POST", path, tc.create, 409)
			_, err = tc.createRPC(tc.create)
			assertError(duplicate, err, codes.FailedPrecondition, tc.existsCode)
			invalid := request(t, "POST", path, tc.invalid, 400)
			_, err = tc.createRPC(tc.invalid)
			assertError(invalid, err, codes.InvalidArgument, tc.invalidCode)
			// PUT replaces mutable fields and rejects attempts to rename the configuration.
			request(t, "PUT", path+"/sample", tc.create, 400)
			request(t, "POST", path, `{"name":"unknown","unexpected":true}`, 400)
			updated, err := tc.updateRPC(`{"name":"sample",` + strings.TrimPrefix(tc.update, "{"))
			require.NoError(t, err)
			compare(request(t, "GET", path+"/sample", "", 200), updated)
			request(t, "PUT", path+"/sample", tc.update, 200)
			got, err = tc.getRPC("sample")
			require.NoError(t, err)
			compare(request(t, "GET", path+"/sample", "", 200), got)
			// The opposite transport can create and delete the same stored objects.
			otherBody := strings.Replace(tc.create, `"sample"`, `"other"`, 1)
			other, err := tc.createRPC(otherBody)
			require.NoError(t, err)
			compare(request(t, "GET", path+"/other", "", 200), other)
			require.NoError(t, tc.deleteRPC("other"))
			request(t, "DELETE", path+"/sample", "", 204)
			missing := request(t, "GET", path+"/sample", "", 404)
			_, err = tc.getRPC("sample")
			assertError(missing, err, codes.NotFound, tc.notFoundCode)
			missing = request(t, "PUT", path+"/sample", tc.update, 404)
			_, err = tc.updateRPC(`{"name":"sample",` + strings.TrimPrefix(tc.update, "{"))
			assertError(missing, err, codes.NotFound, tc.notFoundCode)
			missing = request(t, "DELETE", path+"/sample", "", 404)
			assertError(missing, tc.deleteRPC("sample"), codes.NotFound, tc.notFoundCode)
		})
	}
}

func TestSchedulingProfileRPCRegistrationAndNilRequests(t *testing.T) {
	server := NewServer(nil, nil, &AdministrationServer{}, &JobsServer{}, &ApplicationsServer{}, nil)
	defer server.Stop()
	services := server.GetServiceInfo()
	require.Len(t, services["eruun.v1.NodePoolsService"].Methods, 6)
	adapter := &AdministrationServer{}
	for _, call := range []func() error{
		func() error { _, err := adapter.GetNodePool(context.Background(), nil); return err },
		func() error { _, err := adapter.CreateNodePool(context.Background(), nil); return err },
		func() error { _, err := adapter.UpdateNodePool(context.Background(), nil); return err },
		func() error { _, err := adapter.DeleteNodePool(context.Background(), nil); return err },
	} {
		require.Equal(t, codes.InvalidArgument, status.Code(call()))
	}
}
