package grpcapi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
)

// A gateway trait decodes to the same spec over gRPC as over HTTP and encodes
// back without losing a field, including through the wire format.
func TestGatewayTraitHTTPGRPCRoundTrip(t *testing.T) {
	input := &eruunv1.AppSpecTraits{Gateway: &eruunv1.AppSpecGatewayTraitSpec{
		Hosts: []string{"api-ws-a.apps.example.com", "chat-ws-a.apps.example.com"},
		Routes: []*eruunv1.AppSpecGatewayRouteSpec{
			{Path: "/v1", Match: spec.GatewayMatchPrefix, Rewrite: "/", Timeout: "600s", Backend: &eruunv1.AppSpecGatewayBackendSpec{Service: "api", Port: 8080}},
			{Path: "/healthz", Match: spec.GatewayMatchExact},
		},
	}}
	wire, err := proto.Marshal(input)
	require.NoError(t, err)
	decoded := &eruunv1.AppSpecTraits{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	require.True(t, proto.Equal(input, decoded))

	rpc, err := decodeTypedRequest[spec.Traits](decoded)
	require.NoError(t, err)
	var http spec.Traits
	require.NoError(t, json.Unmarshal([]byte(`{"gateway":{"hosts":["api-ws-a.apps.example.com","chat-ws-a.apps.example.com"],"routes":[{"path":"/v1","match":"prefix","rewrite":"/","timeout":"600s","backend":{"service":"api","port":8080}},{"path":"/healthz","match":"exact"}]}}`), &http))
	require.Equal(t, http, rpc)
	output, err := encodeTypedResponse(http, &eruunv1.AppSpecTraits{})
	require.NoError(t, err)
	require.True(t, proto.Equal(input, output))
}

// The component view's effective gateway and links reach gRPC clients intact.
func TestComponentGatewayViewEncodesForGRPC(t *testing.T) {
	view := apis.ListApplicationComponentsResponse{Components: []*apis.ApplicationComponent{{
		Name: "api", ComponentType: "webservice",
		Gateway: &apis.ComponentGatewayInfo{
			Hosts: []string{"api-ws-a.apps.example.com"},
			Routes: []apis.ComponentGatewayRouteInfo{
				{Path: "/v1", Match: spec.GatewayMatchPrefix, Timeout: "600s", Service: "api", Port: 8080},
				{Path: "/legacy", Match: spec.GatewayMatchExact},
			},
		},
		ExternalLinks: []apis.ExternalLink{{Type: "gateway", Value: "api-ws-a.apps.example.com/v1"}},
	}}}
	output, err := encodeTypedResponse(view, &eruunv1.AppDTOListApplicationComponentsResponse{})
	require.NoError(t, err)
	component := output.GetComponents()[0]
	require.True(t, proto.Equal(&eruunv1.AppDTOComponentGatewayInfo{
		Hosts: []string{"api-ws-a.apps.example.com"},
		Routes: []*eruunv1.AppDTOComponentGatewayRouteInfo{
			{Path: "/v1", Match: spec.GatewayMatchPrefix, Timeout: "600s", Service: "api", Port: 8080},
			{Path: "/legacy", Match: spec.GatewayMatchExact},
		},
	}, component.GetGateway()))
	require.Equal(t, "gateway", component.GetExternalLinks()[0].GetType())
	require.Equal(t, "api-ws-a.apps.example.com/v1", component.GetExternalLinks()[0].GetValue())

	back, err := decodeTypedRequest[apis.ListApplicationComponentsResponse](output)
	require.NoError(t, err)
	require.Equal(t, view.Components[0].Gateway, back.Components[0].Gateway)
}
