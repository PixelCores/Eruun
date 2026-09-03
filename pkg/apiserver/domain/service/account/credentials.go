package account

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/argon2"
)

const (
	accessTTL     = 15 * time.Minute
	sessionTTL    = 30 * 24 * time.Hour
	recentAuthTTL = 5 * time.Minute
)

var mainlandPhone = regexp.MustCompile(`^1[3-9][0-9]{9}$`)

func NormalizeIdentity(provider, subject string) (string, error) {
	subject = strings.TrimSpace(subject)
	switch provider {
	case "email":
		a, e := mail.ParseAddress(subject)
		if e != nil || a.Address != subject || len(subject) > 254 {
			return "", bcode.ErrAccountInput
		}
		return strings.ToLower(subject), nil
	case "phone":
		subject = strings.TrimPrefix(subject, "+86")
		if !mainlandPhone.MatchString(subject) {
			return "", bcode.ErrAccountInput
		}
		return "+86" + subject, nil
	default:
		return "", bcode.ErrAccountInput
	}
}

func hashPassword(password string) (string, error) {
	if !utf8.ValidString(password) || utf8.RuneCountInString(password) < 12 || utf8.RuneCountInString(password) > 128 {
		return "", bcode.ErrAccountInput
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	digest := argon2.IDKey([]byte(password), salt, 2, 19*1024, 1, 32)
	return "$argon2id$v=19$m=19456,t=2,p=1$" + base64.RawStdEncoding.EncodeToString(salt) + "$" + base64.RawStdEncoding.EncodeToString(digest), nil
}

func verifyPassword(encoded, password string) bool {
	if len(password) > 512 {
		return false
	}
	p := strings.Split(encoded, "$")
	if len(p) != 6 || p[1] != "argon2id" || p[2] != "v=19" || p[3] != "m=19456,t=2,p=1" {
		return false
	}
	salt, e := base64.RawStdEncoding.DecodeString(p[4])
	if e != nil || len(salt) != 16 {
		return false
	}
	expected, e := base64.RawStdEncoding.DecodeString(p[5])
	if e != nil || len(expected) != 32 {
		return false
	}
	actual := argon2.IDKey([]byte(password), salt, 2, 19*1024, 1, 32)
	return subtle.ConstantTimeCompare(actual, expected) == 1
}

func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate credential: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
func tokenHash(token string) string {
	v := sha256.Sum256([]byte(token))
	return hex.EncodeToString(v[:])
}

// The script keeps failed attempts and successful consumption atomic across API replicas.
var checkCodeScript = redis.NewScript(`
local value=redis.call('HGET',KEYS[1],'hash')
if not value then return 0 end
local attempts=redis.call('HINCRBY',KEYS[1],'attempts',1)
if attempts > 5 then redis.call('DEL',KEYS[1]); return 0 end
if value == ARGV[1] then redis.call('DEL',KEYS[1]); return 1 end
if attempts == 5 then redis.call('DEL',KEYS[1]) end
return 0`)

var rateScript = redis.NewScript(`local n=redis.call('INCR',KEYS[1]);if n==1 then redis.call('EXPIRE',KEYS[1],ARGV[2]) end;return n<=tonumber(ARGV[1]) and 1 or 0`)

func (s *Service) RateLimit(ctx context.Context, bucket string, limit int, window time.Duration) error {
	if s.Redis == nil {
		return bcode.ErrServiceUnavailable
	}
	ok, err := rateScript.Run(ctx, s.Redis, []string{"eruun:auth:rate:" + tokenHash(bucket)}, limit, int(window.Seconds())).Int()
	if err != nil {
		return fmt.Errorf("authentication rate limit: %w", err)
	}
	if ok != 1 {
		return bcode.ErrAccountRateLimit
	}
	return nil
}

func codeKey(purpose, provider, subject string) string {
	return "eruun:auth:code:" + tokenHash(purpose+"\x00"+provider+"\x00"+subject)
}

func (s *Service) SendCode(ctx context.Context, purpose, provider, subject, ip string) error {
	if purpose != "register" && purpose != "login" && purpose != "reset" && purpose != "bind" {
		return bcode.ErrAccountInput
	}
	id, err := NormalizeIdentity(provider, subject)
	if err != nil {
		return err
	}
	if (provider == "email" && s.Config.SMTP.Host == "") || (provider == "phone" && s.Config.SMS.AccessKeyID == "") {
		return bcode.ErrServiceUnavailable
	}
	if err = s.RateLimit(ctx, "code-ip:"+ip, 20, time.Hour); err != nil {
		return err
	}
	if err = s.RateLimit(ctx, "code-target:"+id, 10, time.Hour); err != nil {
		return err
	}
	if err = s.RateLimit(ctx, "code-cooldown:"+id, 1, time.Minute); err != nil {
		return err
	}
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return fmt.Errorf("generate verification code: %w", err)
	}
	code := fmt.Sprintf("%06d", n.Int64())
	key := codeKey(purpose, provider, id)
	_, err = s.Redis.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, key, "hash", tokenHash(code), "attempts", 0)
		p.Expire(ctx, key, 5*time.Minute)
		return nil
	})
	if err != nil {
		return fmt.Errorf("store verification challenge: %w", err)
	}
	if err = s.Delivery.SendCode(ctx, provider, id, code); err != nil {
		if delErr := s.Redis.Del(ctx, key).Err(); delErr != nil {
			return fmt.Errorf("discard failed delivery challenge: %w", delErr)
		}
		return bcode.ErrAccountDelivery
	}
	return nil
}

func (s *Service) consumeCode(ctx context.Context, purpose, provider, id, code string) error {
	if len(code) != 6 {
		return bcode.ErrAccountCode
	}
	if s.Redis == nil {
		return bcode.ErrServiceUnavailable
	}
	ok, err := checkCodeScript.Run(ctx, s.Redis, []string{codeKey(purpose, provider, id)}, tokenHash(code)).Int()
	if err != nil {
		return fmt.Errorf("consume verification code: %w", err)
	}
	if ok != 1 {
		return bcode.ErrAccountCode
	}
	return nil
}
