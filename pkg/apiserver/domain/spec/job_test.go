package spec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/stretchr/testify/require"
)

func TestJobContractNormalization(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"legacy top-level rejected", `{"name":"daily","type":"command","spec":{"image":"busybox:1.37.0","command":["true"]}}`, false},
		{"command", `{"name":"daily","stages":[{"name":"main","steps":[{"name":"run","type":"command","spec":{"image":"busybox:1.37.0","command":["echo","ok"]}}]}]}`, true},
		{"evaluation", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job","traits":{"eval":{"env":"ack","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"oracle"}}}]}]}`, true},
		{"old evaluation trait key", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job","traits":{"evaluation":{"env":"ack","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"oracle"}}}]}]}`, false},
		{"legacy eval type", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"eval","spec":{"datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}}}]}]}`, false},
		{"old ambiguous type", `{"name":"daily","stages":[{"name":"main","steps":[{"name":"run","type":"custom","spec":{}}]}]}`, false},
		{"application identity", `{"name":"daily","stages":[{"name":"main","steps":[{"name":"run","type":"command","appId":"app","spec":{}}]}]}`, false},
		{"unbounded image", `{"name":"daily","stages":[{"name":"main","steps":[{"name":"run","type":"command","spec":{"image":"busybox:latest","command":["true"]}}]}]}`, false},
		{"unknown command input", `{"name":"daily","stages":[{"name":"main","steps":[{"name":"run","type":"command","spec":{"image":"busybox:1","command":["true"],"cron":"* * * * *"}}]}]}`, false},
		{"missing evaluation trait", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job"}]}]}`, false},
		{"evaluation spec rejected", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job","spec":{},"traits":{"eval":{"env":"ack","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"oracle"}}}]}]}`, false},
		{"null evaluation spec rejected", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job","spec":null,"traits":{"eval":{"env":"ack","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"oracle"}}}]}]}`, false},
		{"framework rejected", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job","traits":{"eval":{"env":"ack","framework":"harbor","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"oracle"}}}]}]}`, false},
		{"framework version rejected", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job","traits":{"eval":{"env":"ack","frameworkVersion":"0.22.0","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"oracle"}}}]}]}`, false},
		{"agent object rejected", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job","traits":{"eval":{"env":"ack","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}}}}]}]}`, false},
		{"runtime env override", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job","traits":{"eval":{"env":"ack","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"oracle"},"envs":[{"name":"PATH","valueFrom":{"static":"evil"}}]}}]}]}`, false},
		{"top-level result policy rejected", `{"name":"model-eval","stages":[{"name":"main","steps":[{"name":"run","type":"job","resultPolicy":{"retentionDays":90,"targets":[{"type":"database","mode":"full"}]},"traits":{"eval":{"env":"ack","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"oracle"}}}]}]}`, false},
		{"command with evaluation rejected", `{"name":"daily","stages":[{"name":"main","steps":[{"name":"run","type":"command","spec":{"image":"busybox:1","command":["true"]},"traits":{"eval":{"env":"ack","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"oracle"}}}]}]}`, false},
		{"no input object", `{"name":"daily","stages":[{"name":"main","steps":[{"name":"run","type":"command","spec":null}]}]}`, false},
		{"concatenated JSON", `{"name":"daily","type":"command","spec":{}} {}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var job JobSpec
			err := DecodeJobJSON([]byte(tc.body), &job)
			if err == nil {
				err = job.Normalize()
			}
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, job.Stages[0].Steps[0].Traits.Resources)
			first, _ := json.Marshal(job)
			require.NoError(t, job.Normalize())
			second, _ := json.Marshal(job)
			require.JSONEq(t, string(first), string(second))
		})
	}
}

func TestJobStageValidation(t *testing.T) {
	command := func(name string) JobStep {
		return JobStep{Name: name, Type: "command", Spec: json.RawMessage(`{"image":"busybox:1.37.0","command":["true"]}`)}
	}
	for _, tc := range []struct {
		name   string
		change func(*JobSpec)
		want   string
	}{
		{"single command", func(*JobSpec) {}, ""},
		{"empty name", func(j *JobSpec) { j.Name = " " }, "name must contain"},
		{"empty stages", func(j *JobSpec) { j.Stages = nil }, "at least one stage"},
		{"empty stage name", func(j *JobSpec) { j.Stages[0].Name = " " }, "name must not be empty"},
		{"duplicate stage", func(j *JobSpec) { j.Stages = append(j.Stages, j.Stages[0]) }, "duplicate stage name"},
		{"empty steps", func(j *JobSpec) { j.Stages[0].Steps = nil }, "at least one step"},
		{"empty step name", func(j *JobSpec) { j.Stages[0].Steps[0].Name = " " }, "name must not be empty"},
		{"duplicate step in stage", func(j *JobSpec) { j.Stages[0].Steps = append(j.Stages[0].Steps, command("run")) }, "duplicate step name"},
		{"duplicate step across stages", func(j *JobSpec) {
			j.Stages = append(j.Stages, JobStage{Name: "second", Steps: []JobStep{command("run")}})
		}, "duplicate step name"},
		{"uppercase step", func(j *JobSpec) { j.Stages[0].Steps[0].Name = "Run" }, "generated Kubernetes Job name"},
		{"dot step", func(j *JobSpec) { j.Stages[0].Steps[0].Name = "run.one" }, "generated Kubernetes Job name"},
		{"step ending hyphen", func(j *JobSpec) { j.Stages[0].Steps[0].Name = "run-" }, "generated Kubernetes Job name"},
		{"step at length limit", func(j *JobSpec) { j.Stages[0].Steps[0].Name = strings.Repeat("a", 28) }, ""},
		{"step over length limit", func(j *JobSpec) { j.Stages[0].Steps[0].Name = strings.Repeat("a", 29) }, "generated Kubernetes Job name"},
		{"minimum retry", func(j *JobSpec) { j.Stages[0].Steps[0].Retry = &JobStepRetry{BackoffLimit: 0} }, ""},
		{"maximum retry", func(j *JobSpec) { j.Stages[0].Steps[0].Retry = &JobStepRetry{BackoffLimit: 10} }, ""},
		{"negative retry", func(j *JobSpec) { j.Stages[0].Steps[0].Retry = &JobStepRetry{BackoffLimit: -1} }, "retry.backoffLimit"},
		{"excessive retry", func(j *JobSpec) { j.Stages[0].Steps[0].Retry = &JobStepRetry{BackoffLimit: 11} }, "retry.backoffLimit"},
		{"volume default", func(j *JobSpec) { j.Volume = &JobVolume{Size: "1Gi"} }, ""},
		{"volume with class", func(j *JobSpec) { j.Volume = &JobVolume{Size: "1Gi", StorageClass: "fast-storage"} }, ""},
		{"empty volume size", func(j *JobSpec) { j.Volume = &JobVolume{} }, "volume.size"},
		{"invalid volume size", func(j *JobSpec) { j.Volume = &JobVolume{Size: "invalid"} }, "volume.size"},
		{"zero volume size", func(j *JobSpec) { j.Volume = &JobVolume{Size: "0"} }, "volume.size"},
		{"negative volume size", func(j *JobSpec) { j.Volume = &JobVolume{Size: "-1Gi"} }, "volume.size"},
		{"invalid storage class", func(j *JobSpec) { j.Volume = &JobVolume{Size: "1Gi", StorageClass: "../bad"} }, "volume.storageClass"},
		{"invalid access mode", func(j *JobSpec) { j.Volume = &JobVolume{Size: "1Gi", AccessMode: "ReadOnlyMany"} }, "volume.accessMode"},
		{"parallel without volume", func(j *JobSpec) { j.Stages[0].Steps = append(j.Stages[0].Steps, command("other")) }, ""},
		{"parallel read write once", func(j *JobSpec) {
			j.Volume = &JobVolume{Size: "1Gi"}
			j.Stages[0].Steps = append(j.Stages[0].Steps, command("other"))
		}, "multiple steps require volume.accessMode ReadWriteMany"},
		{"parallel read write many", func(j *JobSpec) {
			j.Volume = &JobVolume{Size: "1Gi", AccessMode: "ReadWriteMany"}
			j.Stages[0].Steps = append(j.Stages[0].Steps, command("other"))
		}, ""},
		{"sequential read write once", func(j *JobSpec) {
			j.Volume = &JobVolume{Size: "1Gi"}
			j.Stages = append(j.Stages, JobStage{Name: "second", Steps: []JobStep{command("other")}})
		}, ""},
		{"multiple evaluation steps", func(j *JobSpec) {
			j.Stages[0].Steps = []JobStep{
				{Name: "first", Type: "job", Traits: Traits{Evaluation: testEvaluationTrait()}},
				{Name: "second", Type: "job", Traits: Traits{Evaluation: testEvaluationTrait()}},
			}
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := JobSpec{Name: "pipeline", Stages: []JobStage{{Name: "main", Steps: []JobStep{command("run")}}}}
			tc.change(&job)
			err := job.Normalize()
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				return
			}
			require.NoError(t, err)
			if job.Volume != nil {
				require.NotEmpty(t, job.Volume.AccessMode)
			}
		})
	}
	var missing *JobSpec
	require.Error(t, missing.Normalize())
}

func TestJobEvaluationRejectsRetry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backoff int32
	}{
		{"negative", -1},
		{"zero", 0},
		{"positive", 2},
		{"maximum command retry", 10},
		{"excessive", 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := JobSpec{Name: "evaluation", Stages: []JobStage{{Name: "main", Steps: []JobStep{{
				Name: "evaluate", Type: "job", Traits: Traits{Evaluation: testEvaluationTrait()},
				Retry: &JobStepRetry{BackoffLimit: tc.backoff},
			}}}}}
			require.ErrorContains(t, job.Normalize(), "evaluation recovery is controlled by traits.eval.recovery")
		})
	}
}

func TestJobCommandTraitValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		traits Traits
		want   string
	}{
		{"existing PVC", Traits{Storage: []StorageTraitSpec{{Name: "data", Type: "persistent", ClaimName: "existing-data", MountPath: "/data"}}}, ""},
		{"existing PVC name fallback", Traits{Storage: []StorageTraitSpec{{Name: "data", Type: "persistent", MountPath: "/data"}}}, ""},
		{"ephemeral", Traits{Storage: []StorageTraitSpec{{Name: "temp", Type: "ephemeral", MountPath: "/tmp"}}}, ""},
		{"config", Traits{Storage: []StorageTraitSpec{{Name: "config", Type: "config", SourceName: "settings", MountPath: "/config"}}}, ""},
		{"ingress", Traits{Ingress: []IngressTraitsSpec{{}}}, "traits.ingress"},
		{"service", Traits{Service: []ServiceTraitSpec{{}}}, "traits.service"},
		{"rollout", Traits{Rollout: &RolloutTraitSpec{}}, "traits.rollout"},
		{"share", Traits{Share: &ShareTraitSpec{}}, "traits.share"},
		{"rbac", Traits{RBAC: []RBACPolicySpec{{}}}, "traits.rbac"},
		{"evaluation", Traits{Evaluation: testEvaluationTrait()}, "traits.eval"},
		{"PVC tmpCreate", Traits{Storage: []StorageTraitSpec{{Name: "data", Type: "persistent", TmpCreate: true}}}, "traits.storage[0]"},
		{"PVC size", Traits{Storage: []StorageTraitSpec{{Name: "data", Type: "persistent", Size: "1Gi"}}}, "traits.storage[0]"},
		{"PVC storage class", Traits{Storage: []StorageTraitSpec{{Name: "data", Type: "persistent", StorageClass: "fast"}}}, "traits.storage[0]"},
		{"PVC missing reference", Traits{Storage: []StorageTraitSpec{{Type: "persistent"}}}, "existing PVC"},
		{"PVC invalid reference", Traits{Storage: []StorageTraitSpec{{Name: "data", Type: "persistent", ClaimName: "../other"}}}, "existing PVC"},
	} {
		for _, location := range []string{"top", "init", "sidecar", "nested"} {
			t.Run(tc.name+"/"+location, func(t *testing.T) {
				traits := tc.traits
				switch location {
				case "init":
					traits = Traits{Init: []InitTraitSpec{{Name: "setup", Image: "busybox:1.37.0", Traits: traits}}}
				case "sidecar":
					traits = Traits{Sidecar: []SidecarTraitsSpec{{Name: "helper", Image: "busybox:1.37.0", Traits: traits}}}
				case "nested":
					traits = Traits{Init: []InitTraitSpec{{Name: "setup", Image: "busybox:1.37.0", Traits: Traits{Sidecar: []SidecarTraitsSpec{{Name: "helper", Image: "busybox:1.37.0", Traits: traits}}}}}}
				}
				job := JobSpec{Name: "command", Stages: []JobStage{{Name: "main", Steps: []JobStep{{
					Name: "run", Type: "command", Spec: json.RawMessage(`{"image":"busybox:1.37.0","command":["true"]}`), Traits: traits,
				}}}}}
				err := job.Normalize()
				if tc.want == "" {
					require.NoError(t, err)
					return
				}
				require.ErrorContains(t, err, tc.want)
				if location == "init" || location == "nested" {
					require.ErrorContains(t, err, "traits.init[0]")
				}
				if location == "sidecar" || location == "nested" {
					require.ErrorContains(t, err, "traits.sidecar[0]")
				}
			})
		}
	}
}

func TestJobVolumeRejectsConflictingTraitMounts(t *testing.T) {
	for _, storage := range []StorageTraitSpec{
		{Name: "workspace", Type: "ephemeral", MountPath: "/data"},
		{Name: "WorkSpace", Type: "ephemeral", MountPath: "/data"},
		{Name: "data", Type: "ephemeral", MountPath: "/workspace"},
		{Name: "data", Type: "ephemeral", MountPath: "/workspace/"},
	} {
		for _, nested := range []bool{false, true} {
			traits := Traits{Storage: []StorageTraitSpec{storage}}
			if nested {
				traits = Traits{Init: []InitTraitSpec{{Image: "busybox:1", Traits: traits}}}
			}
			job := JobSpec{Name: "command", Volume: &JobVolume{Size: "1Gi"}, Stages: []JobStage{{Name: "main", Steps: []JobStep{{
				Name: "run", Type: "command", Spec: json.RawMessage(`{"image":"busybox:1.37.0","command":["true"]}`), Traits: traits,
			}}}}}
			require.ErrorContains(t, job.Normalize(), "volume reserves")
			job.Volume = nil
			require.NoError(t, job.Normalize())
		}
	}
}

func TestJobEvaluationRetainsRestrictedTraits(t *testing.T) {
	for _, traits := range []Traits{
		{Init: []InitTraitSpec{{}}}, {Sidecar: []SidecarTraitsSpec{{}}},
		{Ingress: []IngressTraitsSpec{{}}}, {Service: []ServiceTraitSpec{{}}},
		{RBAC: []RBACPolicySpec{{}}}, {Probes: []ProbeTraitsSpec{{}}},
		{TargetWorkEnv: map[string]string{"zone": "a"}}, {Share: &ShareTraitSpec{}},
		{Rollout: &RolloutTraitSpec{}}, {SecurityPolicy: &SecurityPolicySpec{}},
		{Storage: []StorageTraitSpec{{Name: "data", Type: "persistent"}}},
		{EnvFrom: []EnvFromSourceSpec{{Type: "secret", SourceName: "credential"}}},
	} {
		traits.Evaluation = testEvaluationTrait()
		job := JobSpec{Name: "evaluation", Stages: []JobStage{{Name: "main", Steps: []JobStep{{Name: "evaluate", Type: "job", Traits: traits}}}}}
		require.ErrorContains(t, job.Normalize(), "evaluation supports resources and credential envs only")
	}
}

func testEvaluationTrait() *EvaluationTraitSpec {
	return &EvaluationTraitSpec{Env: "ack", TaskPackageID: "12345678-1234-1234-1234-123456789012", Agent: "oracle"}
}

func TestEvaluationDefaultsPersistInNormalizedTraits(t *testing.T) {
	job := JobSpec{Name: "model-eval", Stages: []JobStage{{Name: "main", Steps: []JobStep{{Name: "evaluate", Type: "job", Traits: Traits{Evaluation: testEvaluationTrait()}}}}}}
	require.NoError(t, job.Normalize())
	require.Equal(t, 1, job.Stages[0].Steps[0].Traits.Evaluation.Attempts)
	require.Equal(t, 1, job.Stages[0].Steps[0].Traits.Evaluation.Concurrency)
	require.EqualValues(t, 3600, job.Stages[0].Steps[0].Traits.Evaluation.TimeoutSeconds)
	require.Empty(t, job.Stages[0].Steps[0].Spec)
	require.NotSame(t, job.Stages[0].Steps[0].Traits.Resources, job.Stages[0].Steps[0].Traits.Evaluation.SandboxResources)
	require.Nil(t, job.Stages[0].Steps[0].Traits.Evaluation.ResultPolicy, "workspace policy is snapshotted by the submission service")
	require.Nil(t, job.Stages[0].Steps[0].Traits.Evaluation.Recovery, "recovery must remain opt-in")
}

func TestEvaluationRecoveryRequiresSupportedReplaySafeAgent(t *testing.T) {
	for _, tc := range []struct {
		name, agent, version string
		replaySafe           bool
		interval             int64
		wantError            string
	}{
		{"codex default", "codex", CodexRecoveryVersion, true, 0, ""},
		{"claude lower bound", "claude-code", ClaudeCodeRecoveryVersion, true, 60, ""},
		{"codex upper bound", "codex", CodexRecoveryVersion, true, 3600, ""},
		{"unsafe replay", "codex", CodexRecoveryVersion, false, 0, "replaySafe=true"},
		{"missing version", "codex", "", true, 0, "agentVersion"},
		{"unverified codex version", "codex", "0.155.0", true, 0, "agentVersion"},
		{"wrong agent version", "claude-code", CodexRecoveryVersion, true, 0, "agentVersion"},
		{"oracle unsupported", "oracle", CodexRecoveryVersion, true, 0, "does not support agent"},
		{"terminus unsupported", "terminus-2", CodexRecoveryVersion, true, 0, "does not support agent"},
		{"negative interval", "codex", CodexRecoveryVersion, true, -1, "checkpointIntervalSeconds"},
		{"interval too short", "codex", CodexRecoveryVersion, true, 59, "checkpointIntervalSeconds"},
		{"interval too long", "codex", CodexRecoveryVersion, true, 3601, "checkpointIntervalSeconds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testEvaluationTrait()
			e.Agent, e.Model = tc.agent, "provider/model"
			e.Recovery = &EvaluationRecoverySpec{AgentVersion: tc.version, ReplaySafe: tc.replaySafe, CheckpointIntervalSeconds: tc.interval}
			err := e.Normalize()
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			wantInterval := tc.interval
			if wantInterval == 0 {
				wantInterval = DefaultCheckpointIntervalSeconds
			}
			require.Equal(t, wantInterval, e.Recovery.CheckpointIntervalSeconds)
			encoded, err := json.Marshal(e)
			require.NoError(t, err)
			var roundTrip EvaluationTraitSpec
			require.NoError(t, DecodeJobJSON(encoded, &roundTrip))
			require.NoError(t, roundTrip.Normalize())
			require.Equal(t, *e, roundTrip)
		})
	}
}

func TestEvaluationTraitRejectsInvalidInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*EvaluationTraitSpec)
	}{
		{"missing env", func(e *EvaluationTraitSpec) { e.Env = "" }},
		{"unsupported env", func(e *EvaluationTraitSpec) { e.Env = "daytona" }},
		{"missing task package", func(e *EvaluationTraitSpec) { e.TaskPackageID = "" }},
		{"unsupported agent", func(e *EvaluationTraitSpec) { e.Agent = "custom" }},
		{"missing model", func(e *EvaluationTraitSpec) { e.Agent = "codex" }},
		{"model newline", func(e *EvaluationTraitSpec) { e.Model = "model\nvalue" }},
		{"attempts above limit", func(e *EvaluationTraitSpec) { e.Attempts = 11 }},
		{"negative attempts", func(e *EvaluationTraitSpec) { e.Attempts = -1 }},
		{"concurrency above limit", func(e *EvaluationTraitSpec) { e.Concurrency = 17 }},
		{"negative concurrency", func(e *EvaluationTraitSpec) { e.Concurrency = -1 }},
		{"timeout too short", func(e *EvaluationTraitSpec) { e.TimeoutSeconds = 59 }},
		{"timeout too long", func(e *EvaluationTraitSpec) { e.TimeoutSeconds = workflowconfig.MaxEvaluationTimeoutSeconds + 1 }},
		{"invalid sandbox resources", func(e *EvaluationTraitSpec) {
			e.SandboxResources = &ResourceTraitsSpec{CPU: "2", Memory: "1Gi", CPULimit: "1"}
		}},
		{"invalid result policy", func(e *EvaluationTraitSpec) { e.ResultPolicy = &JobResultPolicy{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testEvaluationTrait()
			tc.change(e)
			require.Error(t, e.Normalize())
		})
	}
}

func TestEvaluationSupportsBoundedLongRuntime(t *testing.T) {
	for _, seconds := range []int64{60, 86401, workflowconfig.MaxEvaluationTimeoutSeconds} {
		e := testEvaluationTrait()
		e.TimeoutSeconds = seconds
		require.NoError(t, e.Normalize())
		require.Equal(t, seconds, e.TimeoutSeconds)
	}
}

func TestResultPolicyPreservesFullData(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy JobResultPolicy
		valid  bool
	}{
		{"default", DefaultJobResultPolicy(), true},
		{"minio", JobResultPolicy{90, []JobResultTarget{{"minio", "full"}}}, true},
		{"metadata with files", JobResultPolicy{30, []JobResultTarget{{"database", "metadata"}, {"minio", "full"}}}, true},
		{"metadata only", JobResultPolicy{90, []JobResultTarget{{"database", "metadata"}}}, false},
		{"duplicate", JobResultPolicy{90, []JobResultTarget{{"database", "full"}, {"database", "full"}}}, false},
		{"zero retention", JobResultPolicy{0, []JobResultTarget{{"database", "full"}}}, false},
		{"unbounded retention", JobResultPolicy{3651, []JobResultTarget{{"database", "full"}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.policy.Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestEvaluationResourceRequestsNormalizeForRunner(t *testing.T) {
	job := JobSpec{Name: "evaluation", Stages: []JobStage{{Name: "main", Steps: []JobStep{{Name: "evaluate", Type: "job", Traits: Traits{Evaluation: testEvaluationTrait(), Resources: &ResourceTraitsSpec{CPU: "1", Memory: "2Gi"}}}}}}}
	require.NoError(t, job.Normalize())
	require.Equal(t, "1", job.Stages[0].Steps[0].Traits.Resources.CPULimit)
	require.Equal(t, "2Gi", job.Stages[0].Steps[0].Traits.Resources.MemoryLimit)
}

func TestEvaluationCredentialsUseUnifiedEnvTraits(t *testing.T) {
	secret := func(name string) JobTraits {
		return JobTraits{Envs: []SimplifiedEnvSpec{{Name: name, ValueFrom: ValueSource{Secret: &SecretSelectorSpec{Name: "model", Key: "api-key"}}}}}
	}
	literal := "sk-plaintext"
	for _, tc := range []struct {
		name   string
		traits JobTraits
		valid  bool
	}{
		{"secret credential", secret("OPENAI_API_KEY"), true},
		{"anthropic", secret("ANTHROPIC_API_KEY"), true},
		{"no credential", JobTraits{}, true},
		{"plaintext rejected", JobTraits{Envs: []SimplifiedEnvSpec{{Name: "OPENAI_API_KEY", ValueFrom: ValueSource{Static: &literal}}}}, false},
		{"non-whitelisted name", secret("MY_OWN_KEY"), false},
		{"platform name", secret("ERUUN_JOB_CONFIG"), false},
		{"pod name", secret("POD_NAME"), false},
		{"duplicate", JobTraits{Envs: []SimplifiedEnvSpec{
			{Name: "OPENAI_API_KEY", ValueFrom: ValueSource{Secret: &SecretSelectorSpec{Name: "a", Key: "k"}}},
			{Name: "OPENAI_API_KEY", ValueFrom: ValueSource{Secret: &SecretSelectorSpec{Name: "b", Key: "k"}}},
		}}, false},
		{"invalid secret reference", JobTraits{Envs: []SimplifiedEnvSpec{{Name: "OPENAI_API_KEY", ValueFrom: ValueSource{Secret: &SecretSelectorSpec{Name: "../other", Key: "k"}}}}}, false},
		{"envFrom rejected", JobTraits{EnvFrom: []EnvFromSourceSpec{{Type: "secret", SourceName: "model"}}}, false},
		{"storage rejected", JobTraits{Storage: []StorageTraitSpec{{Name: "d", Type: "persistent", MountPath: "/d"}}}, false},
		{"security policy rejected", JobTraits{SecurityPolicy: &SecurityPolicySpec{}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.traits.Evaluation = testEvaluationTrait()
			err := NormalizeEvaluationTraits(&tc.traits)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestEvaluationRejectsLegacyAgentCredentials(t *testing.T) {
	body := `{"env":"ack","taskPackageId":"12345678-1234-1234-1234-123456789012","agent":"codex","model":"openai/gpt-4","credentials":[{"name":"OPENAI_API_KEY","secretKeyRef":{"name":"model","key":"api-key"}}]}`
	var evaluation EvaluationTraitSpec
	err := DecodeJobJSON([]byte(body), &evaluation)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown field")
}

func TestJobRuntimeConfigurationFailsClosed(t *testing.T) {
	valid := `{"runnerImage":"registry.example.com/eruun-harbor:0.22.0","apiURL":"https://api.example.com","runnerEgress":[{"cidr":"192.0.2.1/32","port":443}]}`
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"valid", valid, true},
		{"bounded work storage", strings.Replace(valid, `"runnerImage"`, `"runnerWorkStorageMiB":4096,"runnerImage"`, 1), true},
		{"negative work storage", strings.Replace(valid, `"runnerImage"`, `"runnerWorkStorageMiB":-1,"runnerImage"`, 1), false},
		{"excessive work storage", strings.Replace(valid, `"runnerImage"`, `"runnerWorkStorageMiB":1048577,"runnerImage"`, 1), false},
		{"missing egress", `{"runnerImage":"runner:1","apiURL":"https://api.example.com"}`, false},
		{"network range", `{"runnerImage":"runner:1","apiURL":"https://api.example.com","runnerEgress":[{"cidr":"0.0.0.0/0","port":443}]}`, false},
		{"userinfo", `{"runnerImage":"runner:1","apiURL":"https://user:secret@api.example.com","runnerEgress":[{"cidr":"192.0.2.1/32","port":443}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.json")
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0600))
			cfg, err := LoadJobsRuntimeConfig(path)
			if tc.valid {
				require.NoError(t, err)
				require.NotNil(t, cfg)
				if tc.name == "valid" {
					require.EqualValues(t, DefaultRunnerWorkStorageMiB, cfg.RunnerWorkStorageMiB)
				} else if tc.name == "bounded work storage" {
					require.EqualValues(t, 4096, cfg.RunnerWorkStorageMiB)
				}
			} else {
				require.Error(t, err)
			}
		})
	}
}
