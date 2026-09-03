package deploy_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// Opt-in: pulls images and creates an isolated project, then removes only that
// test project's containers, network and volumes. Existing dev projects are untouched.
func TestLocalDependenciesIntegration(t *testing.T) {
	if os.Getenv("ERUUN_TEST_LOCAL_DEPS") != "1" {
		t.Skip("set ERUUN_TEST_LOCAL_DEPS=1 to run the isolated Docker Compose smoke test")
	}
	compose, err := filepath.Abs("compose.yaml")
	require.NoError(t, err)
	script, err := filepath.Abs("local_start_deps.sh")
	require.NoError(t, err)
	project := "eruun-deps-test-" + strings.ToLower(rand.Text())
	password := rand.Text()
	ports := make([]string, 3)
	// Hold all three listeners until allocation is complete so ports are distinct.
	var listeners []net.Listener
	for i := range ports {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, listener)
		ports[i] = fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
		t.Cleanup(func() { _ = listener.Close() })
	}
	for _, listener := range listeners {
		require.NoError(t, listener.Close())
	}
	env := append(os.Environ(),
		"COMPOSE_PROJECT_NAME="+project,
		"MYSQL_ROOT_PASSWORD="+rand.Text(), "MYSQL_PASSWORD="+password, "REDIS_PASSWORD="+password,
		"MYSQL_DATABASE=eruun", "MYSQL_PORT="+ports[0], "REDIS_PORT="+ports[1], "KAFKA_PORT="+ports[2],
	)
	run := func(timeout time.Duration, command string, args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, command, args...)
		cmd.Env = env
		return cmd.CombinedOutput()
	}
	t.Cleanup(func() {
		output, err := run(time.Minute, "docker", "compose", "-p", project, "-f", compose, "down", "--volumes")
		if err != nil {
			t.Errorf("clean up test project %s: %v\n%s", project, err, output)
		}
	})
	output, err := run(10*time.Minute, "bash", script)
	require.NoError(t, err, "start test project: %s", output)

	db, err := sql.Open("mysql", fmt.Sprintf("eruun:%s@tcp(127.0.0.1:%s)/eruun?parseTime=true&timeout=5s&readTimeout=5s&writeTimeout=5s", password, ports[0]))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err = db.ExecContext(ctx, "CREATE TABLE dependency_smoke (id INT PRIMARY KEY, value VARCHAR(32))")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO dependency_smoke (id, value) VALUES (1, 'persistent')")
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + ports[1], Password: password})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Set(ctx, "dependency-smoke", "persistent", 0).Err())

	broker := "localhost:" + ports[2]
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	require.NoError(t, err)
	require.NoError(t, conn.SetDeadline(time.Now().Add(15*time.Second)))
	err = conn.CreateTopics(kafka.TopicConfig{Topic: "dependency-smoke", NumPartitions: 1, ReplicationFactor: 1})
	require.NoError(t, conn.Close())
	require.NoError(t, err)
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: "dependency-smoke", RequiredAcks: kafka.RequireAll}
	err = writer.WriteMessages(ctx, kafka.Message{Value: []byte("persistent")})
	require.NoError(t, writer.Close())
	require.NoError(t, err, "host client must reach the advertised Kafka listener")

	// A second start must retain all three stores and the original credentials.
	output, err = run(time.Minute, "docker", "compose", "-p", project, "-f", compose, "stop")
	require.NoError(t, err, "%s", output)
	output, err = run(4*time.Minute, "bash", script)
	require.NoError(t, err, "restart test project: %s", output)
	readCtx, readCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer readCancel()
	var value string
	require.NoError(t, db.QueryRowContext(readCtx, "SELECT value FROM dependency_smoke WHERE id = 1").Scan(&value))
	require.Equal(t, "persistent", value)
	value, err = rdb.Get(readCtx, "dependency-smoke").Result()
	require.NoError(t, err)
	require.Equal(t, "persistent", value)
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: "dependency-smoke", Partition: 0, MinBytes: 1, MaxBytes: 1024, MaxWait: time.Second})
	t.Cleanup(func() { _ = reader.Close() })
	message, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte("persistent"), message.Value))
}
