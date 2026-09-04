package account

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type testDelivery struct {
	code, invitation string
	fail             bool
}

func (d *testDelivery) SendCode(_ context.Context, _, _, code string) error {
	d.code = code
	if d.fail {
		return errors.New("provider credential must not escape")
	}
	return nil
}
func (d *testDelivery) SendInvitation(_ context.Context, _, link string) error {
	d.invitation = link
	if d.fail {
		return errors.New("provider failure")
	}
	return nil
}

func testAccounts(t *testing.T) (*Service, *miniredis.Miniredis, *testDelivery) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "accounts.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, db.AutoMigrate(&model.User{}, &model.Identity{}, &model.Session{}, &model.Workspace{}, &model.WorkspaceMember{}, &model.WorkspaceInvitation{}, &model.SystemSetting{}, &model.Applications{}))
	r := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: r.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cfg := &spec.AccountConfig{Origins: []string{"https://console.example.com"}, FrontendURL: "https://console.example.com", SMTP: spec.SMTPConfig{Host: "smtp.example.com"}, SMS: spec.SMSConfig{AccessKeyID: "test-key"}, Google: spec.OAuthProviderConfig{Enabled: true}, GitHub: spec.OAuthProviderConfig{Enabled: true}}
	d := &testDelivery{}
	return New(&sqlstore.Driver{Client: *db}, cfg, client, d), r, d
}
func registerTestUser(t *testing.T, s *Service, d *testDelivery, provider, id string) (*Login, *Principal) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.SendCode(ctx, "register", provider, id, "192.0.2.1"))
	l, err := s.Register(ctx, provider, id, d.code, "correct horse battery", "Test user")
	require.NoError(t, err)
	p, err := s.Authenticate(ctx, l.AccessToken)
	require.NoError(t, err)
	return l, p
}

func TestContactRegistrationPasswordAndCodes(t *testing.T) {
	for _, tc := range []struct{ provider, input, normalized string }{{"email", "Case.Sensitive+tag@example.com", "case.sensitive+tag@example.com"}, {"phone", "13800138000", "+8613800138000"}} {
		t.Run(tc.provider, func(t *testing.T) {
			s, r, d := testAccounts(t)
			ctx := context.Background()
			login, p := registerTestUser(t, s, d, tc.provider, tc.input)
			identities, err := s.Identities(ctx, p)
			require.NoError(t, err)
			require.Equal(t, tc.normalized, identities[0].(*model.Identity).Subject)
			space, err := s.Workspace(ctx, p, "")
			require.NoError(t, err)
			require.Equal(t, "owner", space.Role)
			require.Equal(t, "personal", space.Workspace.Kind)
			require.NotContains(t, space.Workspace.Namespace, tc.normalized)
			_, err = s.Register(ctx, tc.provider, tc.input, d.code, "correct horse battery", "Replay")
			require.ErrorIs(t, err, bcode.ErrAccountCode)
			_, err = s.Login(ctx, tc.provider, tc.input, "wrong password value", "")
			require.ErrorIs(t, err, bcode.ErrUnauthorized)
			_, err = s.Login(ctx, tc.provider, tc.input, "correct horse battery", "")
			require.NoError(t, err)
			r.FastForward(time.Minute)
			require.NoError(t, s.SendCode(ctx, "login", tc.provider, tc.input, "192.0.2.1"))
			_, err = s.Login(ctx, tc.provider, tc.input, "", d.code)
			require.NoError(t, err)
			_, err = s.Login(ctx, tc.provider, tc.input, "", d.code)
			require.ErrorIs(t, err, bcode.ErrAccountCode)
			r.FastForward(time.Minute)
			require.NoError(t, s.SendCode(ctx, "register", tc.provider, tc.input, "192.0.2.1"))
			_, err = s.Register(ctx, tc.provider, tc.input, d.code, "correct horse battery", "Duplicate")
			require.ErrorIs(t, err, bcode.ErrAccountConflict)
			n, err := s.Repo.Store.Count(ctx, &model.User{}, nil)
			require.NoError(t, err)
			require.EqualValues(t, 1, n)
			row := &model.Session{AccessHash: tokenHash(login.AccessToken)}
			require.NoError(t, s.Repo.One(ctx, row))
			require.NotEqual(t, login.AccessToken, row.AccessHash)
			require.NotEqual(t, login.RefreshToken, row.RefreshHash)
		})
	}
}

func TestCodeExpiryAttemptsConcurrencyAndDelivery(t *testing.T) {
	s, r, d := testAccounts(t)
	ctx := context.Background()
	id := "verify@example.com"
	require.NoError(t, s.SendCode(ctx, "register", "email", id, "192.0.2.1"))
	code := d.code
	require.ErrorIs(t, s.SendCode(ctx, "login", "email", id, "192.0.2.1"), bcode.ErrAccountRateLimit)
	wrong := "111111"
	if code == wrong {
		wrong = "222222"
	}
	for range 5 {
		require.ErrorIs(t, s.consumeCode(ctx, "register", "email", id, wrong), bcode.ErrAccountCode)
	}
	require.ErrorIs(t, s.consumeCode(ctx, "register", "email", id, code), bcode.ErrAccountCode)
	r.FastForward(time.Minute)
	require.NoError(t, s.SendCode(ctx, "register", "email", id, "192.0.2.1"))
	code = d.code
	var won atomic.Int32
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if s.consumeCode(ctx, "register", "email", id, code) == nil {
				won.Add(1)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, won.Load())
	r.FastForward(time.Minute)
	require.NoError(t, s.SendCode(ctx, "register", "email", id, "192.0.2.1"))
	r.FastForward(5 * time.Minute)
	require.ErrorIs(t, s.consumeCode(ctx, "register", "email", id, d.code), bcode.ErrAccountCode)
	d.fail = true
	for _, provider := range []string{"email", "phone"} {
		target := "failed@example.com"
		if provider == "phone" {
			target = "13900139000"
		}
		err := s.SendCode(ctx, "register", provider, target, "192.0.2.1")
		require.ErrorIs(t, err, bcode.ErrAccountDelivery)
		require.NotContains(t, err.Error(), "credential")
		normalized, _ := NormalizeIdentity(provider, target)
		require.False(t, r.Exists(codeKey("register", provider, normalized)))
	}
}

func TestSessionRotationResetAndDisable(t *testing.T) {
	s, r, d := testAccounts(t)
	ctx := context.Background()
	l, p := registerTestUser(t, s, d, "email", "session@example.com")
	otherLogin, other := registerTestUser(t, s, d, "email", "other@example.com")
	spaces, err := s.Workspaces(ctx, p)
	require.NoError(t, err)
	require.Len(t, spaces, 1)
	require.Equal(t, p.User.ID, spaces[0].Workspace.OwnerID)
	newLogin, err := s.Refresh(ctx, l.RefreshToken)
	require.NoError(t, err)
	_, err = s.Refresh(ctx, l.RefreshToken)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	_, err = s.Authenticate(ctx, l.AccessToken)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	_, err = s.Authenticate(ctx, newLogin.AccessToken)
	require.NoError(t, err)
	r.FastForward(time.Minute)
	require.NoError(t, s.SendCode(ctx, "reset", "email", "session@example.com", "192.0.2.1"))
	require.NoError(t, s.ResetPassword(ctx, "email", "session@example.com", d.code, "new correct password"))
	_, err = s.Authenticate(ctx, newLogin.AccessToken)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	_, err = s.Refresh(ctx, newLogin.RefreshToken)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	l, err = s.Login(ctx, "email", "session@example.com", "new correct password", "")
	require.NoError(t, err)
	p, err = s.Authenticate(ctx, l.AccessToken)
	require.NoError(t, err)
	require.NoError(t, s.Logout(ctx, p))
	_, err = s.Authenticate(ctx, l.AccessToken)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	l, err = s.Login(ctx, "email", "session@example.com", "new correct password", "")
	require.NoError(t, err)
	admin := &Principal{User: &model.User{ID: "test-admin", SystemAdmin: true}}
	require.NoError(t, s.SetDisabled(ctx, admin, p.User.ID, true))
	_, err = s.Authenticate(ctx, l.AccessToken)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	_, err = s.Login(ctx, "email", "session@example.com", "new correct password", "")
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	_, err = s.Refresh(ctx, l.RefreshToken)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	remaining, err := s.Authenticate(ctx, otherLogin.AccessToken)
	require.NoError(t, err)
	require.Equal(t, other.User.ID, remaining.User.ID, "revoking one user's sessions must preserve another user's sessions")
}

func TestPasswordsAndIdentityBinding(t *testing.T) {
	for _, value := range []string{"too short", strings.Repeat("a", 129), string([]byte{255})} {
		_, err := hashPassword(value)
		require.ErrorIs(t, err, bcode.ErrAccountInput)
	}
	password := "  exact password  "
	hash, err := hashPassword(password)
	require.NoError(t, err)
	require.True(t, verifyPassword(hash, password))
	require.False(t, verifyPassword(hash, strings.TrimSpace(password)))
	s, r, d := testAccounts(t)
	ctx := context.Background()
	_, p := registerTestUser(t, s, d, "email", "bound@example.com")
	ids, err := s.Identities(ctx, p)
	require.NoError(t, err)
	require.ErrorIs(t, s.Unbind(ctx, p, ids[0].(*model.Identity).ID), bcode.ErrAccountConflict)
	require.NoError(t, s.SendCode(ctx, "bind", "phone", "13800138001", "192.0.2.1"))
	require.NoError(t, s.Bind(ctx, p, "phone", "13800138001", d.code))
	require.NoError(t, s.Unbind(ctx, p, ids[0].(*model.Identity).ID))
	_, err = s.Login(ctx, "phone", "13800138001", "correct horse battery", "")
	require.NoError(t, err)
	r.FastForward(time.Minute)
	require.NoError(t, s.SendCode(ctx, "bind", "email", "bound@example.com", "192.0.2.1"))
	p.Session.AuthenticatedAt = time.Now().Add(-6 * time.Minute)
	require.ErrorIs(t, s.Bind(ctx, p, "email", "bound@example.com", d.code), bcode.ErrAccountRecentAuth)
}

func TestBootstrapDoesNotOverwriteAccounts(t *testing.T) {
	s, _, _ := testAccounts(t)
	ctx := context.Background()
	s.Config.BootstrapAdmin.Email = "admin@example.com"
	s.Config.BootstrapAdmin.Password = "initial admin password"
	require.NoError(t, s.Bootstrap(ctx))
	first, err := s.Login(ctx, "email", "admin@example.com", "initial admin password", "")
	require.NoError(t, err)
	require.True(t, first.User.SystemAdmin)
	require.True(t, first.User.MustChangePassword)
	p, err := s.Authenticate(ctx, first.AccessToken)
	require.NoError(t, err)
	require.NoError(t, s.ChangePassword(ctx, p, "changed admin password"))
	_, err = s.Authenticate(ctx, first.AccessToken)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	_, err = s.Refresh(ctx, first.RefreshToken)
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	s.Config.BootstrapAdmin.Password = "another deployment password"
	require.NoError(t, s.Bootstrap(ctx))
	changed, err := s.Login(ctx, "email", "admin@example.com", "changed admin password", "")
	require.NoError(t, err)
	require.False(t, changed.User.MustChangePassword)
	require.Equal(t, first.User.SecurityVersion+1, changed.User.SecurityVersion)
	n, err := s.Repo.Store.Count(ctx, &model.User{}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}

func TestRegistrationTransactionRollsBackOnConflict(t *testing.T) {
	s, r, d := testAccounts(t)
	ctx := context.Background()
	_, _ = registerTestUser(t, s, d, "email", "duplicate@example.com")
	r.FastForward(time.Minute)
	require.NoError(t, s.SendCode(ctx, "register", "email", "duplicate@example.com", "192.0.2.1"))
	_, err := s.Register(ctx, "email", "duplicate@example.com", d.code, "correct horse battery", "Duplicate")
	require.ErrorIs(t, err, bcode.ErrAccountConflict)
	for _, entity := range []datastore.Entity{&model.User{}, &model.Identity{}, &model.Workspace{}, &model.WorkspaceMember{}} {
		n, e := s.Repo.Store.Count(ctx, entity, nil)
		require.NoError(t, e)
		require.EqualValues(t, 1, n)
	}
}
