package identity

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/stretchr/testify/require"
)

type deliveryTransport func(*http.Request) (*http.Response, error)

func (f deliveryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAliyunSMSContractAndFailures(t *testing.T) {
	cfg := &spec.AccountConfig{SMS: spec.SMSConfig{AccessKeyID: "test-key", AccessKeySecret: "test-secret", SignName: "测试签名", TemplateCode: "SMS_TEST"}}
	for _, tc := range []struct {
		body     string
		status   int
		succeeds bool
	}{
		{`{"Code":"OK"}`, 200, true}, {`{"Code":"isv.BUSINESS_LIMIT_CONTROL"}`, 200, false}, {`{"Code":"OK"}`, 500, false}, {`not json`, 200, false},
	} {
		d := &Delivery{Config: cfg, HTTPClient: &http.Client{Transport: deliveryTransport(func(r *http.Request) (*http.Response, error) {
			require.Equal(t, "https://dysmsapi.aliyuncs.com/", r.URL.String())
			require.Equal(t, http.MethodPost, r.Method)
			require.NoError(t, r.ParseForm())
			p := r.PostForm
			require.Equal(t, "SendSms", p.Get("Action"))
			require.Equal(t, "2017-05-25", p.Get("Version"))
			require.Equal(t, "13800138000", p.Get("PhoneNumbers"))
			require.Equal(t, `{"code":"012345"}`, p.Get("TemplateParam"))
			require.Equal(t, cfg.SMS.SignName, p.Get("SignName"))
			require.NotEmpty(t, p.Get("SignatureNonce"))
			_, err := time.Parse(time.RFC3339, p.Get("Timestamp"))
			require.NoError(t, err)
			signature := p.Get("Signature")
			p.Del("Signature")
			canonical := strings.ReplaceAll(p.Encode(), "+", "%20")
			// Check the RPC wire contract: POST, absolute root and percent-escaped
			// sorted parameters, signed with the secret plus '&'.
			var escaped strings.Builder
			for _, b := range []byte(canonical) {
				if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.ContainsRune("-_.~", rune(b)) {
					escaped.WriteByte(b)
				} else {
					fmt.Fprintf(&escaped, "%%%02X", b)
				}
			}
			mac := hmac.New(sha1.New, []byte(cfg.SMS.AccessKeySecret+"&"))
			mac.Write([]byte("POST&%2F&" + escaped.String()))
			require.Equal(t, base64.StdEncoding.EncodeToString(mac.Sum(nil)), signature)
			return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body)), Header: make(http.Header)}, nil
		})}}
		err := d.SendCode(context.Background(), "phone", "+8613800138000", "012345")
		if tc.succeeds {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.NotContains(t, err.Error(), "test-secret")
		}
	}
}

func TestSMTPEncryptedDeliveryAndTLSRequired(t *testing.T) {
	certServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificates := certServer.TLS.Certificates
	roots := x509.NewCertPool()
	roots.AddCert(certServer.Certificate())
	certServer.Close()
	for _, encrypted := range []bool{true, false} {
		t.Run(strconv.FormatBool(encrypted), func(t *testing.T) {
			var listener net.Listener
			var err error
			if encrypted {
				listener, err = tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: certificates, MinVersion: tls.VersionTLS12})
			} else {
				listener, err = net.Listen("tcp", "127.0.0.1:0")
			}
			require.NoError(t, err)
			defer listener.Close()
			received := make(chan string, 1)
			go func() {
				conn, e := listener.Accept()
				if e != nil {
					received <- ""
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				fmt.Fprint(conn, "220 localhost ESMTP\r\n")
				reader := bufio.NewReader(conn)
				var body strings.Builder
				defer func() { received <- body.String() }()
				for {
					line, e := reader.ReadString('\n')
					if e != nil {
						return
					}
					switch {
					case strings.HasPrefix(line, "EHLO"):
						fmt.Fprint(conn, "250 localhost\r\n")
					case strings.HasPrefix(line, "STARTTLS"):
						fmt.Fprint(conn, "454 TLS unavailable\r\n")
					case strings.HasPrefix(line, "DATA"):
						fmt.Fprint(conn, "354 send data\r\n")
						for {
							part, e := reader.ReadString('\n')
							if e != nil {
								return
							}
							if part == ".\r\n" {
								break
							}
							body.WriteString(part)
						}
						fmt.Fprint(conn, "250 accepted\r\n")
					case strings.HasPrefix(line, "QUIT"):
						fmt.Fprint(conn, "221 goodbye\r\n")
						return
					default:
						fmt.Fprint(conn, "250 ok\r\n")
					}
				}
			}()
			host, port, err := net.SplitHostPort(listener.Addr().String())
			require.NoError(t, err)
			portNumber, _ := strconv.Atoi(port)
			mode := "implicit"
			if !encrypted {
				mode = "starttls"
			}
			d := &Delivery{Config: &spec.AccountConfig{SMTP: spec.SMTPConfig{Host: host, Port: portNumber, From: "accounts@example.com", TLS: mode}}, tlsConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
			err = d.SendCode(context.Background(), "email", "recipient@example.com", "012345")
			message := <-received
			if encrypted {
				require.NoError(t, err)
				require.Contains(t, message, "To: recipient@example.com")
				require.Contains(t, message, "012345")
			} else {
				require.Error(t, err)
				require.Empty(t, message)
			}
		})
	}
}
