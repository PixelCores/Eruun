package messaging

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeRedis implements redisCommander for testing without a real Redis.
type fakeRedis struct {
	closed      bool
	xGroupErr   error
	groupStarts []string

	xAddID  string
	xAddErr error

	xReadStreams []redis.XStream
	xReadErr     error
	xReadErrs    []error
	xReadCalls   int
	lastReadArgs *redis.XReadGroupArgs

	xAckErr error

	xAutoClaimMessages []redis.XMessage
	xAutoClaimErr      error
	xAutoClaimErrs     []error
	xAutoClaimNext     []string
	xAutoClaimCalls    int
	lastAutoClaimArgs  *redis.XAutoClaimArgs
	autoClaimStarts    []string

	xLenVal int64
	xLenErr error

	xPendingVal *redis.XPending
	xPendingErr error
}

func (f *fakeRedis) Ping(ctx context.Context) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("PONG")
	return cmd
}

func (f *fakeRedis) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	f.groupStarts = append(f.groupStarts, start)
	cmd := redis.NewStatusCmd(ctx)
	if f.xGroupErr != nil {
		cmd.SetErr(f.xGroupErr)
		return cmd
	}
	cmd.SetVal("OK")
	return cmd
}

func (f *fakeRedis) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	if f.xAddErr != nil {
		cmd.SetErr(f.xAddErr)
		return cmd
	}
	id := f.xAddID
	if id == "" {
		id = "1-0"
	}
	cmd.SetVal(id)
	return cmd
}

func (f *fakeRedis) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	f.lastReadArgs = a
	cmd := redis.NewXStreamSliceCmd(ctx)
	if f.xReadCalls < len(f.xReadErrs) {
		err := f.xReadErrs[f.xReadCalls]
		f.xReadCalls++
		if err != nil {
			cmd.SetErr(err)
			return cmd
		}
	} else {
		f.xReadCalls++
	}
	if f.xReadErr != nil {
		cmd.SetErr(f.xReadErr)
		return cmd
	}
	cmd.SetVal(f.xReadStreams)
	return cmd
}

func (f *fakeRedis) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	if f.xAckErr != nil {
		cmd.SetErr(f.xAckErr)
		return cmd
	}
	cmd.SetVal(int64(len(ids)))
	return cmd
}

func (f *fakeRedis) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	f.lastAutoClaimArgs = a
	f.autoClaimStarts = append(f.autoClaimStarts, a.Start)
	cmd := redis.NewXAutoClaimCmd(ctx)
	call := f.xAutoClaimCalls
	f.xAutoClaimCalls++
	if call < len(f.xAutoClaimErrs) && f.xAutoClaimErrs[call] != nil {
		cmd.SetErr(f.xAutoClaimErrs[call])
		return cmd
	}
	if f.xAutoClaimErr != nil {
		cmd.SetErr(f.xAutoClaimErr)
		return cmd
	}
	next := "0-0"
	if call < len(f.xAutoClaimNext) {
		next = f.xAutoClaimNext[call]
	}
	cmd.SetVal(f.xAutoClaimMessages, next)
	return cmd
}

func (f *fakeRedis) XLen(ctx context.Context, stream string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	if f.xLenErr != nil {
		cmd.SetErr(f.xLenErr)
		return cmd
	}
	cmd.SetVal(f.xLenVal)
	return cmd
}

func (f *fakeRedis) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	cmd := redis.NewXPendingCmd(ctx)
	if f.xPendingErr != nil {
		cmd.SetErr(f.xPendingErr)
		return cmd
	}
	cmd.SetVal(f.xPendingVal)
	return cmd
}

func (f *fakeRedis) Close() error { f.closed = true; return nil }

func TestNewRedisStreamsWithClient_Validation(t *testing.T) {
	// nil client
	if _, err := NewRedisStreamsWithClient(nil, "k", 0); err == nil {
		t.Fatalf("expected error for nil client")
	}
	// empty key
	rcli := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	if _, err := NewRedisStreamsWithClient(rcli, "", 0); err == nil {
		t.Fatalf("expected error for empty key")
	}
}

func TestRedisStreams_WithCommanderBasic(t *testing.T) {
	f := &fakeRedis{}
	rs, err := NewRedisStreamsWithClient(f, "test-stream", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ctx := context.Background()
	// EnsureGroup
	if err := rs.EnsureGroup(ctx, "g"); err != nil {
		t.Fatalf("EnsureGroup error: %v", err)
	}
	// Enqueue
	if id, err := rs.Enqueue(ctx, []byte("hello")); err != nil || id == "" {
		t.Fatalf("Enqueue err=%v id=%q", err, id)
	}
	// Ack (no ids)
	if err := rs.Ack(ctx, "g"); err != nil {
		t.Fatalf("Ack empty ids should be nil, got %v", err)
	}
	// Close
	if err := rs.Close(ctx); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	if !f.closed {
		t.Fatalf("expected fake client to be closed")
	}
}

func TestRedisStreams_EnsureGroupIgnoresBusyGroup(t *testing.T) {
	f := &fakeRedis{
		xGroupErr: errors.New("BUSYGROUP Consumer Group name already exists"),
	}
	rs, err := NewRedisStreamsWithClient(f, "test-stream", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = rs.EnsureGroup(context.Background(), "g")
	if err != nil {
		t.Fatalf("expected BUSYGROUP to be ignored, got: %v", err)
	}
}

func TestRedisStreams_EnsureGroupIgnoresWrappedBusyGroup(t *testing.T) {
	f := &fakeRedis{
		xGroupErr: fmt.Errorf("create group failed: %w", errors.New("BUSYGROUP group exists")),
	}
	rs, err := NewRedisStreamsWithClient(f, "test-stream", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = rs.EnsureGroup(context.Background(), "g")
	if err != nil {
		t.Fatalf("expected wrapped BUSYGROUP to be ignored, got: %v", err)
	}
}

func TestRedisStreams_EnsureGroupPropagatesNonBusyGroupError(t *testing.T) {
	f := &fakeRedis{
		xGroupErr: errors.New("NOAUTH Authentication required"),
	}
	rs, err := NewRedisStreamsWithClient(f, "test-stream", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = rs.EnsureGroup(context.Background(), "g")
	if err == nil {
		t.Fatalf("expected non-BUSYGROUP error to propagate")
	}
	if err.Error() != "NOAUTH Authentication required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRedisStreams_ReadGroupDecodesPayloadAndSkipsMalformed(t *testing.T) {
	f := &fakeRedis{
		xReadStreams: []redis.XStream{
			{
				Stream: "test-stream",
				Messages: []redis.XMessage{
					{ID: "1-0", Values: map[string]interface{}{"p": "hello"}},
					{ID: "2-0", Values: map[string]interface{}{"p": []byte("world")}},
					{ID: "3-0", Values: map[string]interface{}{"p": 10}},
					{ID: "4-0", Values: map[string]interface{}{"other": "missing"}},
				},
			},
		},
	}
	rs, err := NewRedisStreamsWithClient(f, "test-stream", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs, err := rs.ReadGroup(context.Background(), "group-1", "consumer-1", 5, 2*time.Second)
	if err != nil {
		t.Fatalf("ReadGroup error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if string(msgs[0].Payload) != "hello" || string(msgs[1].Payload) != "world" {
		t.Fatalf("unexpected payloads: %+v", msgs)
	}
	if f.lastReadArgs == nil || len(f.lastReadArgs.Streams) != 2 || f.lastReadArgs.Streams[0] != "test-stream" {
		t.Fatalf("unexpected XREADGROUP args: %+v", f.lastReadArgs)
	}
}

func TestRedisStreams_ReadGroupHandlesNilAndError(t *testing.T) {
	rsNil, err := NewRedisStreamsWithClient(&fakeRedis{xReadErr: redis.Nil}, "test-stream", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msgs, err := rsNil.ReadGroup(context.Background(), "group", "consumer", 1, time.Second)
	if err != nil {
		t.Fatalf("expected redis.Nil to be ignored, got: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected empty messages on redis.Nil, got: %v", msgs)
	}

	rsErr, err := NewRedisStreamsWithClient(&fakeRedis{xReadErr: errors.New("read failed")}, "test-stream", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := rsErr.ReadGroup(context.Background(), "group", "consumer", 1, time.Second); err == nil {
		t.Fatalf("expected read error")
	}
}

func TestRedisStreams_ReadGroupRecreatesMissingGroupFromBacklog(t *testing.T) {
	f := &fakeRedis{
		xReadErrs: []error{errors.New("NOGROUP No such key or consumer group"), nil},
		xReadStreams: []redis.XStream{{Messages: []redis.XMessage{
			{ID: "1-0", Values: map[string]interface{}{"p": "backlog"}},
		}}},
	}
	rs, err := NewRedisStreamsWithClient(f, "test-stream", 0)
	if err != nil {
		t.Fatalf("new redis streams: %v", err)
	}

	messages, err := rs.ReadGroup(context.Background(), "group", "consumer", 1, time.Second)
	if err != nil {
		t.Fatalf("read after group recreation: %v", err)
	}
	if len(messages) != 1 || string(messages[0].Payload) != "backlog" {
		t.Fatalf("unexpected messages: %+v", messages)
	}
	if f.xReadCalls != 2 || len(f.groupStarts) != 1 || f.groupStarts[0] != "0" {
		t.Fatalf("expected one backlog group recreation and retry, calls=%d starts=%v", f.xReadCalls, f.groupStarts)
	}
}

func TestRedisStreams_AutoClaimDecodesPayloadAndHandlesErrors(t *testing.T) {
	f := &fakeRedis{
		xAutoClaimMessages: []redis.XMessage{
			{ID: "1-0", Values: map[string]interface{}{"p": "one"}},
			{ID: "2-0", Values: map[string]interface{}{"p": []byte("two")}},
			{ID: "3-0", Values: map[string]interface{}{"p": 12}},
		},
	}
	rs, err := NewRedisStreamsWithClient(f, "test-stream", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msgs, err := rs.AutoClaim(context.Background(), "group-1", "consumer-1", 3*time.Second, 8)
	if err != nil {
		t.Fatalf("AutoClaim error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if string(msgs[0].Payload) != "one" || string(msgs[1].Payload) != "two" {
		t.Fatalf("unexpected claimed payloads: %+v", msgs)
	}
	if f.lastAutoClaimArgs == nil || f.lastAutoClaimArgs.Stream != "test-stream" || f.lastAutoClaimArgs.Count != 8 {
		t.Fatalf("unexpected XAUTOCLAIM args: %+v", f.lastAutoClaimArgs)
	}

	rsNil, err := NewRedisStreamsWithClient(&fakeRedis{xAutoClaimErr: redis.Nil}, "test-stream", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msgs, err = rsNil.AutoClaim(context.Background(), "group", "consumer", time.Second, 1)
	if err != nil {
		t.Fatalf("expected redis.Nil to be ignored, got: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected empty messages on redis.Nil, got: %+v", msgs)
	}

	rsErr, err := NewRedisStreamsWithClient(&fakeRedis{xAutoClaimErr: errors.New("autoclaim failed")}, "test-stream", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := rsErr.AutoClaim(context.Background(), "group", "consumer", time.Second, 1); err == nil {
		t.Fatalf("expected autoclaim error")
	}
}

func TestRedisStreams_AutoClaimAdvancesCursorPerGroup(t *testing.T) {
	f := &fakeRedis{xAutoClaimNext: []string{"25-0", "0-0", "9-0"}}
	rs, err := NewRedisStreamsWithClient(f, "test-stream", 0)
	if err != nil {
		t.Fatalf("new redis streams: %v", err)
	}

	for _, group := range []string{"group-a", "group-a", "group-b"} {
		if _, err := rs.AutoClaim(context.Background(), group, "consumer", time.Second, 10); err != nil {
			t.Fatalf("auto claim %s: %v", group, err)
		}
	}
	want := []string{"0-0", "25-0", "0-0"}
	if fmt.Sprint(f.autoClaimStarts) != fmt.Sprint(want) {
		t.Fatalf("unexpected cursor sequence: got %v want %v", f.autoClaimStarts, want)
	}
}

func TestRedisStreams_AutoClaimRecreatesMissingGroupOnce(t *testing.T) {
	f := &fakeRedis{
		xAutoClaimErrs: []error{errors.New("NOGROUP group was deleted"), nil},
		xAutoClaimNext: []string{"", "4-0"},
	}
	rs, err := NewRedisStreamsWithClient(f, "test-stream", 0)
	if err != nil {
		t.Fatalf("new redis streams: %v", err)
	}

	if _, err := rs.AutoClaim(context.Background(), "group", "consumer", time.Second, 10); err != nil {
		t.Fatalf("auto claim after group recreation: %v", err)
	}
	if f.xAutoClaimCalls != 2 || len(f.groupStarts) != 1 || f.groupStarts[0] != "0" {
		t.Fatalf("expected one backlog group recreation and retry, calls=%d starts=%v", f.xAutoClaimCalls, f.groupStarts)
	}
	if len(f.autoClaimStarts) != 2 || f.autoClaimStarts[1] != "0-0" {
		t.Fatalf("retry must restart scan, got %v", f.autoClaimStarts)
	}
}

func TestRedisStreams_StatsBranches(t *testing.T) {
	tests := []struct {
		name        string
		fake        *fakeRedis
		wantBacklog int64
		wantPending int64
		wantErr     bool
	}{
		{
			name: "success",
			fake: &fakeRedis{
				xLenVal:     10,
				xPendingVal: &redis.XPending{Count: 3},
			},
			wantBacklog: 10,
			wantPending: 3,
		},
		{
			name: "xlen error keeps pending count",
			fake: &fakeRedis{
				xLenErr:     errors.New("xlen failed"),
				xPendingVal: &redis.XPending{Count: 4},
			},
			wantBacklog: 0,
			wantPending: 4,
			wantErr:     true,
		},
		{
			name: "xpending redis nil ignored",
			fake: &fakeRedis{
				xLenVal:     9,
				xPendingErr: redis.Nil,
			},
			wantBacklog: 9,
			wantPending: 0,
		},
		{
			name: "xpending error",
			fake: &fakeRedis{
				xLenVal:     9,
				xPendingErr: errors.New("xpending failed"),
			},
			wantBacklog: 9,
			wantPending: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, err := NewRedisStreamsWithClient(tt.fake, "test-stream", 0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			backlog, pending, err := rs.Stats(context.Background(), "group-1")
			if tt.wantErr && err == nil {
				t.Fatalf("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if backlog != tt.wantBacklog || pending != tt.wantPending {
				t.Fatalf("unexpected stats: backlog=%d pending=%d", backlog, pending)
			}
		})
	}
}
