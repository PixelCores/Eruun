package deploy_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestLocalComposeContract(t *testing.T) {
	raw, err := os.ReadFile("compose.yaml")
	require.NoError(t, err)
	var compose struct {
		Name     string
		Services map[string]struct {
			Image         string
			ContainerName string `json:"container_name"`
			Ports         []string
			Volumes       []string
			Environment   map[string]string
			Command       []string
			Secrets       []string
			Healthcheck   struct{ Test []string }
			Deploy        struct{ Resources map[string]map[string]string }
		}
		Volumes map[string]interface{}
		Secrets map[string]struct{ Environment string }
	}
	require.NoError(t, yaml.Unmarshal(raw, &compose))
	require.Equal(t, "eruun-dev", compose.Name)
	require.Len(t, compose.Services, 3)
	require.Len(t, compose.Volumes, 3)
	for name, tc := range map[string]struct{ image, port, volume string }{
		"mysql": {"${MYSQL_IMAGE:-mysql:8.4.11}", "127.0.0.1:${MYSQL_PORT:-3306}:3306", "mysql-data:/var/lib/mysql"},
		"redis": {"${REDIS_IMAGE:-redis:7.2.16-alpine}", "127.0.0.1:${REDIS_PORT:-6379}:6379", "redis-data:/data"},
		"kafka": {"${KAFKA_IMAGE:-apache/kafka:3.9.1}", "127.0.0.1:${KAFKA_PORT:-9092}:9092", "kafka-data:/var/lib/kafka/data"},
	} {
		t.Run(name, func(t *testing.T) {
			service, ok := compose.Services[name]
			require.True(t, ok)
			require.Equal(t, tc.image, service.Image)
			require.Empty(t, service.ContainerName, "Compose must scope container names by project")
			require.Equal(t, []string{tc.port}, service.Ports)
			require.Equal(t, []string{tc.volume}, service.Volumes)
			require.Contains(t, compose.Volumes, strings.SplitN(tc.volume, ":", 2)[0])
			require.NotEmpty(t, service.Healthcheck.Test)
			for _, kind := range []string{"limits", "reservations"} {
				require.NotEmpty(t, service.Deploy.Resources[kind]["cpus"])
				require.NotEmpty(t, service.Deploy.Resources[kind]["memory"])
			}
		})
	}
	mysql := compose.Services["mysql"]
	require.Equal(t, "eruun", mysql.Environment["MYSQL_USER"])
	require.Equal(t, "${MYSQL_DATABASE:-eruun}", mysql.Environment["MYSQL_DATABASE"])
	for _, key := range []string{"MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD"} {
		require.NotContains(t, mysql.Environment, key)
		secret := strings.ToLower(key)
		require.Equal(t, "/run/secrets/"+secret, mysql.Environment[key+"_FILE"])
		require.Contains(t, mysql.Secrets, secret)
		require.Equal(t, key, compose.Secrets[secret].Environment)
	}
	require.Contains(t, strings.Join(mysql.Healthcheck.Test, " "), "--protocol=TCP")
	require.Contains(t, strings.Join(mysql.Healthcheck.Test, " "), "SELECT 1")
	redis := compose.Services["redis"]
	require.Equal(t, []string{"redis_password"}, redis.Secrets)
	require.Equal(t, "REDIS_PASSWORD", compose.Secrets["redis_password"].Environment)
	require.Contains(t, strings.Join(redis.Command, " "), "--appendonly yes --requirepass")
	require.Contains(t, strings.Join(redis.Healthcheck.Test, " "), "REDISCLI_AUTH=")
	kafka := compose.Services["kafka"]
	require.Equal(t, "broker,controller", kafka.Environment["KAFKA_PROCESS_ROLES"])
	require.Equal(t, "INTERNAL://kafka:19092,EXTERNAL://localhost:${KAFKA_PORT:-9092}", kafka.Environment["KAFKA_ADVERTISED_LISTENERS"])
	require.Equal(t, "1@kafka:9093", kafka.Environment["KAFKA_CONTROLLER_QUORUM_VOTERS"])
	require.Equal(t, "/var/lib/kafka/data", kafka.Environment["KAFKA_LOG_DIRS"])
	for _, key := range []string{"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"} {
		require.Equal(t, "1", kafka.Environment[key])
	}
}

func TestLocalStartDependencies(t *testing.T) {
	script, err := filepath.Abs("local_start_deps.sh")
	require.NoError(t, err)
	for _, tc := range []struct {
		name, variable, value, failCommand string
		wantError                          bool
	}{
		{name: "healthy dependencies"},
		{name: "missing mysql root password", variable: "MYSQL_ROOT_PASSWORD", wantError: true},
		{name: "missing mysql app password", variable: "MYSQL_PASSWORD", wantError: true},
		{name: "missing redis password", variable: "REDIS_PASSWORD", wantError: true},
		{name: "placeholder mysql root password", variable: "MYSQL_ROOT_PASSWORD", value: "__REPLACE_WITH_STRONG_PASSWORD__", wantError: true},
		{name: "placeholder mysql app password", variable: "MYSQL_PASSWORD", value: "__REPLACE_WITH_STRONG_PASSWORD__", wantError: true},
		{name: "placeholder redis password", variable: "REDIS_PASSWORD", value: "prefix__REPLACE_WITH_STRONG_PASSWORD__", wantError: true},
		{name: "compose unavailable", failCommand: "version", wantError: true},
		{name: "compose startup fails", failCommand: "up", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workdir := t.TempDir()
			logPath := filepath.Join(workdir, "docker.log")
			fakeDocker := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$TEST_DOCKER_LOG"
if [ "$*" = 'compose version' ]; then
  [ "$TEST_FAIL_COMMAND" != version ]
  exit
fi
[ "$1" = compose ] && [ "$2" = -f ] && [ -f "$3" ]
[ "$4" = up ] && [ "$5" = -d ] && [ "$6" = --wait ] && [ "$7" = --wait-timeout ] && [ "$8" = 180 ]
[ "$MYSQL_ROOT_PASSWORD" = test-only-root ]
[ "$MYSQL_PASSWORD" = test-only-mysql ]
[ "$REDIS_PASSWORD" = test-only-redis ]
[ "$TEST_FAIL_COMMAND" != up ]
`
			require.NoError(t, os.WriteFile(filepath.Join(workdir, "docker"), []byte(fakeDocker), 0700))
			cmd := exec.Command("bash", script)
			cmd.Dir = workdir // The script must locate Compose independently of the caller's cwd.
			cmd.Env = append(os.Environ(),
				"PATH="+workdir+string(os.PathListSeparator)+os.Getenv("PATH"),
				"TEST_DOCKER_LOG="+logPath, "TEST_FAIL_COMMAND="+tc.failCommand,
				"MYSQL_ROOT_PASSWORD=test-only-root", "MYSQL_PASSWORD=test-only-mysql", "REDIS_PASSWORD=test-only-redis",
				"MYSQL_PORT=13306", "REDIS_PORT=16379", "KAFKA_PORT=19092", "MYSQL_DATABASE=eruun_test",
			)
			if tc.variable != "" {
				cmd.Env = append(cmd.Env, tc.variable+"="+tc.value)
			}
			output, runErr := cmd.CombinedOutput()
			if tc.wantError {
				require.Error(t, runErr)
				require.NotContains(t, string(output), "Existing containers and data volumes are preserved")
			} else {
				require.NoError(t, runErr, "%s", output)
				require.Contains(t, string(output), "MySQL: 127.0.0.1:13306, database=eruun_test, user=eruun")
				require.Contains(t, string(output), "Redis: 127.0.0.1:16379")
				require.Contains(t, string(output), "Kafka: localhost:19092")
			}
			if tc.variable != "" {
				require.Contains(t, string(output), tc.variable)
				_, err := os.Stat(logPath)
				require.True(t, os.IsNotExist(err), "invalid credentials must fail before Docker is called")
			} else {
				log, err := os.ReadFile(logPath)
				require.NoError(t, err)
				require.NotContains(t, string(log), "test-only-")
				require.NotContains(t, string(log), "rm ")
				require.NotContains(t, string(log), "down")
				if tc.failCommand != "version" {
					require.Contains(t, string(log), "up -d --wait --wait-timeout 180")
				}
			}
			require.NotContains(t, string(output), "test-only-")
		})
	}
}
