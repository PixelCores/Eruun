package spec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobContractNormalization(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"command", `{"name":"daily","type":"command","spec":{"image":"busybox:1.37.0","command":["echo","ok"]}}`, true},
		{"evaluation", `{"name":"agent","type":"eval","spec":{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}}}`, true},
		{"eval with defaults", `{"name":"agent","type":"eval","spec":{"datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}}}`, true},
		{"removed evaluation type", `{"name":"agent","type":"agent_evaluation","spec":{"datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}}}`, false},
		{"old ambiguous type", `{"name":"daily","type":"custom","spec":{}}`, false},
		{"application identity", `{"name":"daily","type":"command","appId":"app","spec":{}}`, false},
		{"unbounded image", `{"name":"daily","type":"command","spec":{"image":"busybox:latest","command":["true"]}}`, false},
		{"unknown command input", `{"name":"daily","type":"command","spec":{"image":"busybox:1","command":["true"],"cron":"* * * * *"}}`, false},
		{"foreign framework", `{"name":"agent","type":"eval","spec":{"framework":"other","frameworkVersion":"1","datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}}}`, false},
		{"unsupported explicit version", `{"name":"agent","type":"eval","spec":{"frameworkVersion":"0.21.0","datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}}}`, false},
		{"root code import", `{"name":"agent","type":"eval","spec":{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle","import_path":"evil.Agent"}}}`, false},
		{"credential interpreter injection", `{"name":"agent","type":"eval","spec":{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle","credentials":[{"name":"PYTHONPATH","secretKeyRef":{"name":"secret","key":"key"}}]}}}`, false},
		{"runtime env override", `{"name":"agent","type":"eval","spec":{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}},"traits":{"envs":[{"name":"PATH","valueFrom":{"static":"evil"}}]}}`, false},
		{"no input object", `{"name":"daily","type":"command","spec":null}`, false},
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
			require.NotNil(t, job.Traits.Resources)
			first, _ := json.Marshal(job)
			require.NoError(t, job.Normalize())
			second, _ := json.Marshal(job)
			require.JSONEq(t, string(first), string(second))
		})
	}
}

func TestEvalDefaultsPersistInNormalizedSpec(t *testing.T) {
	job := JobSpec{Name: "agent", Type: "eval", Spec: json.RawMessage(`{"datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}}`)}
	require.NoError(t, job.Normalize())
	var evaluation AgentEvaluationSpec
	require.NoError(t, DecodeJobJSON(job.Spec, &evaluation))
	require.Equal(t, "harbor", evaluation.Framework)
	require.Equal(t, HarborVersion, evaluation.FrameworkVersion)
	require.Equal(t, "eval", job.Type)
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
	job := JobSpec{Name: "evaluation", Type: "eval", Spec: json.RawMessage(`{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"oracle"}}`), Traits: JobTraits{Resources: &ResourceTraitsSpec{CPU: "1", Memory: "2Gi"}}}
	require.NoError(t, job.Normalize())
	require.Equal(t, "1", job.Traits.Resources.CPULimit)
	require.Equal(t, "2Gi", job.Traits.Resources.MemoryLimit)
}

// Application-only traits are absent from JobTraits, so DisallowUnknownFields
// rejects them at the decode boundary with no hand-written check.
func TestJobSpecRejectsApplicationOnlyTraits(t *testing.T) {
	for _, tc := range []struct{ name, trait string }{
		{"sidecar", `"sidecar":[{"name":"extra","image":"busybox:1.37.0"}]`},
		{"ingress", `"ingress":[{"host":"example.com"}]`},
		{"service", `"service":[{"port":80}]`},
		{"rbac", `"rbac":[{"name":"reader"}]`},
		{"probes", `"probes":[{"type":"liveness"}]`},
		{"init", `"init":[{"name":"setup","image":"busybox:1.37.0"}]`},
		{"rollout", `"rollout":{"strategy":"RollingUpdate"}`},
		{"share", `"share":{"enabled":true}`},
		{"targetWorkEnv", `"targetWorkEnv":{"zone":"a"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"name":"j","type":"command","spec":{"image":"busybox:1.37.0","command":["true"]},"traits":{` + tc.trait + `}}`
			var job JobSpec
			err := DecodeJobJSON([]byte(body), &job)
			require.Error(t, err)
			require.Contains(t, err.Error(), "unknown field")
		})
	}
}

func TestEvaluationCredentialsUseUnifiedEnvTraits(t *testing.T) {
	const evalSpec = `{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"terminus-2","model":"openai/gpt-4"}}`
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
			job := JobSpec{Name: "evaluation", Type: "eval", Spec: json.RawMessage(evalSpec), Traits: tc.traits}
			err := job.Normalize()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// The pre-unification spelling must fail loudly rather than be ignored.
func TestEvaluationRejectsLegacyAgentCredentials(t *testing.T) {
	body := `{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"12345678-1234-1234-1234-123456789012","agent":{"name":"terminus-2","model":"openai/gpt-4","credentials":[{"name":"OPENAI_API_KEY","secretKeyRef":{"name":"model","key":"api-key"}}]}}`
	job := JobSpec{Name: "evaluation", Type: "eval", Spec: json.RawMessage(body)}
	err := job.Normalize()
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
			} else {
				require.Error(t, err)
			}
		})
	}
}
