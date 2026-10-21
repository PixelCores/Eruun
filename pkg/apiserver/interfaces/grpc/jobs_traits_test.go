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
	input := &eruunv1.AppSpecTraits{
		SecurityPolicy: &eruunv1.AppKubeCoreSecurityContext{
			RunAsUser: ptr.To(int64(1000)), RunAsNonRoot: ptr.To(false),
			Capabilities:   &eruunv1.AppKubeCoreCapabilities{Drop: []string{"ALL"}},
			SeccompProfile: &eruunv1.AppKubeCoreSeccompProfile{Type: "RuntimeDefault"},
		},
		Resources: &eruunv1.AppSpecResourceTraitsSpec{Cpu: "100m", Memory: "128Mi"},
	}
	traits, err := decodeTypedRequest[spec.Traits](input)
	require.NoError(t, err)
	require.NotNil(t, traits.SecurityPolicy)
	require.EqualValues(t, 1000, *traits.SecurityPolicy.RunAsUser)
	require.NotNil(t, traits.SecurityPolicy.RunAsNonRoot)
	require.False(t, *traits.SecurityPolicy.RunAsNonRoot)
	require.Equal(t, []corev1.Capability{"ALL"}, traits.SecurityPolicy.Capabilities.Drop)
	require.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, traits.SecurityPolicy.SeccompProfile.Type)
	require.Equal(t, "100m", traits.Resources.CPU)

	job, err := jobSpecOutput(spec.JobSpec{
		Name: "run", Stages: []spec.JobStage{{Name: "main", Steps: []spec.JobStep{{
			Name: "run", Type: "command", Spec: json.RawMessage(`{"image":"busybox:1.37.0"}`), Traits: traits,
		}}}},
	})
	require.NoError(t, err)
	require.NotNil(t, job.Stages[0].Steps[0].Traits.SecurityPolicy)
	require.NotNil(t, job.Stages[0].Steps[0].Traits.SecurityPolicy.RunAsNonRoot)
	require.False(t, *job.Stages[0].Steps[0].Traits.SecurityPolicy.RunAsNonRoot)
	require.Equal(t, input.SecurityPolicy.GetRunAsUser(), job.Stages[0].Steps[0].Traits.SecurityPolicy.GetRunAsUser())
	require.Equal(t, input.SecurityPolicy.Capabilities.Drop, job.Stages[0].Steps[0].Traits.SecurityPolicy.Capabilities.Drop)
	require.Equal(t, input.SecurityPolicy.SeccompProfile.Type, job.Stages[0].Steps[0].Traits.SecurityPolicy.SeccompProfile.Type)
	require.Equal(t, "100m", job.Stages[0].Steps[0].Traits.Resources.Cpu)
}

func TestEvaluationTraitSharedAcrossJobAndApplication(t *testing.T) {
	evaluation := &eruunv1.EvaluationTrait{
		Env: "ack", Model: "openai/model", Agent: "codex", TaskPackageId: "12345678-1234-1234-1234-123456789012",
		Attempts: 2, Concurrency: 3, TimeoutSeconds: 600,
		SandboxResources: &eruunv1.AppSpecResourceTraitsSpec{Cpu: "2", Memory: "4Gi", CpuLimit: "4", MemoryLimit: "8Gi"},
		ResultPolicy:     &eruunv1.JobResultPolicy{RetentionDays: 30, Targets: []*eruunv1.JobResultTarget{{Type: "database", Mode: "full"}}},
		Recovery:         &eruunv1.EvaluationRecovery{AgentVersion: spec.CodexRecoveryVersion, ReplaySafe: true, CheckpointIntervalSeconds: 300},
	}
	standalone, err := jobSubmitInput(&eruunv1.SubmitJobRequest{Name: "evaluate", Stages: []*eruunv1.JobStage{{Name: "main", Steps: []*eruunv1.JobStep{{Name: "evaluate", Type: "job", Traits: &eruunv1.AppSpecTraits{Eval: evaluation}}}}}})
	require.NoError(t, err)
	require.Empty(t, standalone.Stages[0].Steps[0].Spec)
	app, err := decodeTypedRequest[spec.Traits](&eruunv1.AppSpecTraits{Eval: evaluation})
	require.NoError(t, err)
	require.Equal(t, app.Evaluation, standalone.Stages[0].Steps[0].Traits.Evaluation)
	require.Equal(t, int64(600), app.Evaluation.TimeoutSeconds)
	require.Equal(t, "2", app.Evaluation.SandboxResources.CPU)
	require.Equal(t, 30, app.Evaluation.ResultPolicy.RetentionDays)
	require.Equal(t, spec.CodexRecoveryVersion, app.Evaluation.Recovery.AgentVersion)
	require.True(t, app.Evaluation.Recovery.ReplaySafe)
	require.EqualValues(t, 300, app.Evaluation.Recovery.CheckpointIntervalSeconds)

	output, err := jobSpecOutput(standalone.JobSpec)
	require.NoError(t, err)
	require.Nil(t, output.Stages[0].Steps[0].Spec)
	require.True(t, proto.Equal(evaluation, output.Stages[0].Steps[0].Traits.Eval))
	appOutput, err := encodeTypedResponse(app, &eruunv1.AppSpecTraits{})
	require.NoError(t, err)
	require.True(t, proto.Equal(output.Stages[0].Steps[0].Traits.Eval, appOutput.Eval))
	require.NoError(t, standalone.Normalize())
	for _, traits := range []proto.Message{output.Stages[0].Steps[0].Traits, appOutput} {
		encoded, err := protojson.Marshal(traits)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"eval":`)
		require.NotContains(t, string(encoded), `"evaluation":`)
	}
	require.Error(t, protojson.Unmarshal([]byte(`{"evaluation":{}}`), &eruunv1.AppSpecTraits{}))
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
			input, err := jobSubmitInput(&eruunv1.SubmitJobRequest{Name: "evaluate", Stages: []*eruunv1.JobStage{{Name: "main", Steps: []*eruunv1.JobStep{{Name: "evaluate", Type: "job", Traits: &eruunv1.AppSpecTraits{Eval: evaluation}}}}}})
			require.NoError(t, err)
			app, err := decodeTypedRequest[spec.Traits](&eruunv1.AppSpecTraits{Eval: evaluation})
			require.NoError(t, err)
			for _, trait := range []*spec.EvaluationTraitSpec{input.Stages[0].Steps[0].Traits.Evaluation, app.Evaluation} {
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
		name  string
		step  *eruunv1.JobStep
		valid bool
	}{
		{name: "command", step: &eruunv1.JobStep{Name: "run", Type: "command", Spec: command}, valid: true},
		{name: "missing command spec", step: &eruunv1.JobStep{Name: "run", Type: "command"}},
		{name: "removed eval type", step: &eruunv1.JobStep{Name: "run", Type: "eval", Spec: command}},
		{name: "job requires evaluation trait", step: &eruunv1.JobStep{Name: "run", Type: "job"}},
		{name: "job rejects spec", step: &eruunv1.JobStep{Name: "run", Type: "job", Spec: command}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, err := jobSubmitInput(&eruunv1.SubmitJobRequest{Name: "run", Stages: []*eruunv1.JobStage{{Name: "main", Steps: []*eruunv1.JobStep{tc.step}}}})
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

func TestJobSubmitRejectsRemovedTopLevelFields(t *testing.T) {
	for _, field := range []protowire.Number{3, 4, 5, 6} {
		wire := protowire.AppendTag(nil, field, protowire.BytesType)
		wire = protowire.AppendBytes(wire, []byte("legacy"))
		request := &eruunv1.SubmitJobRequest{}
		require.NoError(t, proto.Unmarshal(wire, request))
		_, err := jobSubmitInput(request)
		require.Error(t, err, "removed field %d must fail closed", field)
	}
	for _, raw := range []string{`{"name":"run","type":"command"}`, `{"name":"run","spec":{}}`, `{"name":"run","traits":{}}`} {
		require.Error(t, protojson.Unmarshal([]byte(raw), &eruunv1.SubmitJobRequest{}))
	}
}

func TestJobStagesVolumeAndRetryRoundTrip(t *testing.T) {
	command, err := structpb.NewValue(map[string]any{"image": "busybox:1.37.0", "command": []any{"true"}})
	require.NoError(t, err)
	request := &eruunv1.SubmitJobRequest{
		WorkspaceId: "workspace", Name: "pipeline",
		Volume: &eruunv1.JobVolume{Size: "1Gi", StorageClass: "shared", AccessMode: "ReadWriteMany"},
		Stages: []*eruunv1.JobStage{
			{Name: "prepare", Steps: []*eruunv1.JobStep{{Name: "first", Type: "command", Spec: command, Retry: &eruunv1.JobStepRetry{BackoffLimit: 0}}}},
			{Name: "run", Steps: []*eruunv1.JobStep{
				{Name: "second", Type: "command", Spec: command, Retry: &eruunv1.JobStepRetry{BackoffLimit: 10}},
				{Name: "third", Type: "command", Spec: command, Traits: &eruunv1.AppSpecTraits{Resources: &eruunv1.AppSpecResourceTraitsSpec{Cpu: "1", Memory: "2Gi"}}},
			}},
		},
	}
	input, err := jobSubmitInput(request)
	require.NoError(t, err)
	require.Equal(t, "workspace", input.WorkspaceID)
	require.NoError(t, input.Normalize())
	require.NotNil(t, input.Stages[0].Steps[0].Retry)
	require.Zero(t, input.Stages[0].Steps[0].Retry.BackoffLimit)
	require.Nil(t, input.Stages[1].Steps[1].Retry)
	output, err := jobSpecOutput(input.JobSpec)
	require.NoError(t, err)
	require.Equal(t, "ReadWriteMany", output.Volume.AccessMode)
	require.Equal(t, "shared", output.Volume.StorageClass)
	require.Len(t, output.Stages, 2)
	require.Len(t, output.Stages[1].Steps, 2)
	require.NotNil(t, output.Stages[0].Steps[0].Retry)
	require.EqualValues(t, 10, output.Stages[1].Steps[0].Retry.BackoffLimit)
	require.Nil(t, output.Stages[1].Steps[1].Retry)
	require.Equal(t, "1", output.Stages[1].Steps[1].Traits.Resources.Cpu)
	require.Nil(t, output.Stages[1].Steps[1].Traits.SecurityPolicy)
}

func TestNamedProfilesHTTPGRPCParity(t *testing.T) {
	input := &eruunv1.AppSpecTraits{NodePool: "gpu"}
	rpc, err := decodeTypedRequest[spec.Traits](input)
	require.NoError(t, err)
	var http spec.Traits
	require.NoError(t, json.Unmarshal([]byte(`{"nodePool":"gpu"}`), &http))
	require.Equal(t, http, rpc)
	output, err := encodeTypedResponse(http, &eruunv1.AppSpecTraits{})
	require.NoError(t, err)
	require.True(t, proto.Equal(input, output))
	descriptor := input.ProtoReflect().Descriptor()
	require.EqualValues(t, 16, descriptor.Fields().ByName("node_pool").Number())
	require.True(t, descriptor.ReservedRanges().Has(6))
	require.True(t, descriptor.ReservedRanges().Has(10))
	require.True(t, descriptor.ReservedNames().Has("rbac"))
	require.True(t, descriptor.ReservedNames().Has("target_work_env"))
}

func TestTraitsRejectRemovedComponentPermissions(t *testing.T) {
	for _, field := range []string{"rbac", "rbac" + "Profile"} {
		require.Error(t, protojson.Unmarshal([]byte(`{"`+field+`":null}`), &eruunv1.AppSpecTraits{}))
	}
	for _, number := range []protowire.Number{6, 17} {
		wire := protowire.AppendTag(nil, number, protowire.BytesType)
		wire = protowire.AppendBytes(wire, []byte("removed"))
		input := &eruunv1.AppSpecTraits{}
		require.NoError(t, proto.Unmarshal(wire, input))
		_, err := decodeTypedRequest[spec.Traits](input)
		require.Error(t, err, "removed field %d must fail closed", number)
		_, err = jobSubmitInput(&eruunv1.SubmitJobRequest{Stages: []*eruunv1.JobStage{{Steps: []*eruunv1.JobStep{{Traits: input}}}}})
		require.Error(t, err, "nested removed field %d must fail closed", number)
	}
}
