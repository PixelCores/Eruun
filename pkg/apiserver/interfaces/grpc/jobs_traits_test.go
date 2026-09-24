package grpcapi

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestJobSecurityPolicyTraitsRoundTrip(t *testing.T) {
	input := &eruunv1.JobTraits{
		SecurityPolicy: &eruunv1.AppKubeCoreSecurityContext{
			RunAsUser: ptr.To(int64(1000)), RunAsNonRoot: ptr.To(false),
			Capabilities:   &eruunv1.AppKubeCoreCapabilities{Drop: []string{"ALL"}},
			SeccompProfile: &eruunv1.AppKubeCoreSeccompProfile{Type: "RuntimeDefault"},
		},
		Resources: &eruunv1.JobResourceTrait{Cpu: "100m", Memory: "128Mi"},
	}
	traits, err := jobTraitsInput(input)
	require.NoError(t, err)
	require.NotNil(t, traits.SecurityPolicy)
	require.EqualValues(t, 1000, *traits.SecurityPolicy.RunAsUser)
	require.NotNil(t, traits.SecurityPolicy.RunAsNonRoot)
	require.False(t, *traits.SecurityPolicy.RunAsNonRoot)
	require.Equal(t, []corev1.Capability{"ALL"}, traits.SecurityPolicy.Capabilities.Drop)
	require.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, traits.SecurityPolicy.SeccompProfile.Type)
	require.Equal(t, "100m", traits.Resources.CPU)

	job, err := jobSpecOutput(spec.JobSpec{
		Name: "run", Type: "command", Spec: json.RawMessage(`{"image":"busybox:1.37.0"}`), Traits: traits,
	})
	require.NoError(t, err)
	require.NotNil(t, job.Traits.SecurityPolicy)
	require.NotNil(t, job.Traits.SecurityPolicy.RunAsNonRoot)
	require.False(t, *job.Traits.SecurityPolicy.RunAsNonRoot)
	require.Equal(t, input.SecurityPolicy.GetRunAsUser(), job.Traits.SecurityPolicy.GetRunAsUser())
	require.Equal(t, input.SecurityPolicy.Capabilities.Drop, job.Traits.SecurityPolicy.Capabilities.Drop)
	require.Equal(t, input.SecurityPolicy.SeccompProfile.Type, job.Traits.SecurityPolicy.SeccompProfile.Type)
	require.Equal(t, "100m", job.Traits.Resources.Cpu)
}

func TestEvaluationTraitSharedAcrossJobAndApplication(t *testing.T) {
	evaluation := &eruunv1.EvaluationTrait{
		Env: "ack", Model: "openai/model", Agent: "codex", TaskPackageId: "12345678-1234-1234-1234-123456789012",
		Attempts: 2, Concurrency: 3, TimeoutSeconds: 600,
		SandboxResources: &eruunv1.AppSpecResourceTraitsSpec{Cpu: "2", Memory: "4Gi", CpuLimit: "4", MemoryLimit: "8Gi"},
		ResultPolicy:     &eruunv1.JobResultPolicy{RetentionDays: 30, Targets: []*eruunv1.JobResultTarget{{Type: "database", Mode: "full"}}},
		Recovery:         &eruunv1.EvaluationRecovery{AgentVersion: spec.CodexRecoveryVersion, ReplaySafe: true, CheckpointIntervalSeconds: 300},
	}
	standalone, err := jobSubmitInput(&eruunv1.SubmitJobRequest{Name: "evaluate", Type: "job", Traits: &eruunv1.JobTraits{Eval: evaluation}})
	require.NoError(t, err)
	require.Empty(t, standalone.Spec)
	app, err := decodeTypedRequest[spec.Traits](&eruunv1.AppSpecTraits{Eval: evaluation})
	require.NoError(t, err)
	require.Equal(t, app.Evaluation, standalone.Traits.Evaluation)
	require.Equal(t, int64(600), app.Evaluation.TimeoutSeconds)
	require.Equal(t, "2", app.Evaluation.SandboxResources.CPU)
	require.Equal(t, 30, app.Evaluation.ResultPolicy.RetentionDays)
	require.Equal(t, spec.CodexRecoveryVersion, app.Evaluation.Recovery.AgentVersion)
	require.True(t, app.Evaluation.Recovery.ReplaySafe)
	require.EqualValues(t, 300, app.Evaluation.Recovery.CheckpointIntervalSeconds)

	output, err := jobSpecOutput(standalone.JobSpec)
	require.NoError(t, err)
	require.Nil(t, output.Spec)
	require.True(t, proto.Equal(evaluation, output.Traits.Eval))
	appOutput, err := encodeTypedResponse(app, &eruunv1.AppSpecTraits{})
	require.NoError(t, err)
	require.True(t, proto.Equal(output.Traits.Eval, appOutput.Eval))
	require.NoError(t, standalone.Normalize())
	for _, traits := range []proto.Message{output.Traits, appOutput} {
		encoded, err := protojson.Marshal(traits)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"eval":`)
		require.NotContains(t, string(encoded), `"evaluation":`)
	}
	for _, traits := range []proto.Message{&eruunv1.JobTraits{}, &eruunv1.AppSpecTraits{}} {
		require.Error(t, protojson.Unmarshal([]byte(`{"evaluation":{}}`), traits))
	}
}

func TestEvaluationRecoveryProtoRejectsUnapprovedReplayAndPreservesOmission(t *testing.T) {
	for _, tc := range []struct {
		name      string
		recovery  *eruunv1.EvaluationRecovery
		wantError string
	}{
		{"omitted", nil, ""},
		{"without replay consent", &eruunv1.EvaluationRecovery{AgentVersion: spec.CodexRecoveryVersion}, "replaySafe=true"},
		{"unsupported version", &eruunv1.EvaluationRecovery{AgentVersion: "0.1.0", ReplaySafe: true}, "agentVersion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evaluation := &eruunv1.EvaluationTrait{Env: "ack", Agent: "codex", Model: "openai/model", TaskPackageId: "12345678-1234-1234-1234-123456789012", Recovery: tc.recovery}
			input, err := jobSubmitInput(&eruunv1.SubmitJobRequest{Name: "evaluate", Type: "job", Traits: &eruunv1.JobTraits{Eval: evaluation}})
			require.NoError(t, err)
			app, err := decodeTypedRequest[spec.Traits](&eruunv1.AppSpecTraits{Eval: evaluation})
			require.NoError(t, err)
			for _, trait := range []*spec.EvaluationTraitSpec{input.Traits.Evaluation, app.Evaluation} {
				err = trait.Normalize()
				if tc.wantError != "" {
					require.ErrorContains(t, err, tc.wantError)
				} else {
					require.NoError(t, err)
					require.Nil(t, trait.Recovery)
				}
			}
		})
	}
}

func TestJobSubmitInputDeclarationContract(t *testing.T) {
	command, err := structpb.NewValue(map[string]any{"image": "busybox:1.37.0", "command": []any{"true"}})
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		request *eruunv1.SubmitJobRequest
		valid   bool
	}{
		{name: "command", request: &eruunv1.SubmitJobRequest{Name: "run", Type: "command", Spec: command}, valid: true},
		{name: "missing command spec", request: &eruunv1.SubmitJobRequest{Name: "run", Type: "command"}},
		{name: "removed eval type", request: &eruunv1.SubmitJobRequest{Name: "run", Type: "eval", Spec: command}},
		{name: "job requires evaluation trait", request: &eruunv1.SubmitJobRequest{Name: "run", Type: "job"}},
		{name: "job rejects spec", request: &eruunv1.SubmitJobRequest{Name: "run", Type: "job", Spec: command}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, err := jobSubmitInput(tc.request)
			require.NoError(t, err)
			err = input.Normalize()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestJobSubmitRejectsRemovedResultPolicyWireField(t *testing.T) {
	// Old binary clients can still transmit field 6 despite its reserved name.
	// Reject it instead of silently replacing the requested policy with defaults.
	wire := protowire.AppendTag(nil, 6, protowire.BytesType)
	wire = protowire.AppendBytes(wire, []byte{8, 30})
	request := &eruunv1.SubmitJobRequest{}
	require.NoError(t, proto.Unmarshal(wire, request))
	_, err := jobSubmitInput(request)
	require.Error(t, err)
}

func TestJobTraitsWithoutSecurityPolicy(t *testing.T) {
	input, err := jobTraitsInput(&eruunv1.JobTraits{Resources: &eruunv1.JobResourceTrait{Cpu: "1", Memory: "2Gi"}})
	require.NoError(t, err)
	require.Nil(t, input.SecurityPolicy)
	require.Equal(t, "1", input.Resources.CPU)

	output, err := jobTraitsOutput(input)
	require.NoError(t, err)
	require.Nil(t, output.SecurityPolicy)
	require.Equal(t, "1", output.Resources.Cpu)

	missing, err := jobTraitsInput(nil)
	require.NoError(t, err)
	require.Nil(t, missing.SecurityPolicy)
}
