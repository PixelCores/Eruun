package identity

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/google/uuid"
)

type Delivery struct {
	Config     *spec.AccountConfig
	HTTPClient *http.Client
	tlsConfig  *tls.Config
}

func (d *Delivery) SendCode(ctx context.Context, provider, address, code string) error {
	if provider == "email" {
		return d.mail(ctx, address, "Eruun verification code", "Your verification code is "+code+". It expires in 5 minutes.\r\n")
	}
	if provider == "phone" {
		return d.sms(ctx, address, code)
	}
	return fmt.Errorf("unsupported verification channel")
}
func (d *Delivery) SendInvitation(ctx context.Context, address, link string) error {
	return d.mail(ctx, address, "Eruun workspace invitation", "Sign in with this email address to accept your workspace invitation:\r\n"+link+"\r\nThis invitation expires in 7 days.\r\n")
}

func (d *Delivery) mail(ctx context.Context, to, subject, body string) error {
	c := d.Config.SMTP
	if c.Host == "" {
		return fmt.Errorf("SMTP is not configured")
	}
	address := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	conn, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("connect SMTP: %w", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(10 * time.Second)
	if v, ok := ctx.Deadline(); ok && v.Before(deadline) {
		deadline = v
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return err
	}
	tlsConfig := &tls.Config{ServerName: c.Host, MinVersion: tls.VersionTLS12}
	if d.tlsConfig != nil {
		tlsConfig = d.tlsConfig.Clone()
		tlsConfig.ServerName = c.Host
		tlsConfig.MinVersion = tls.VersionTLS12
	}
	if c.TLS == "implicit" {
		secured := tls.Client(conn, tlsConfig)
		if err = secured.HandshakeContext(ctx); err != nil {
			return fmt.Errorf("SMTP TLS: %w", err)
		}
		conn = secured
	}
	client, err := smtp.NewClient(conn, c.Host)
	if err != nil {
		return fmt.Errorf("SMTP greeting: %w", err)
	}
	defer client.Close()
	if c.TLS == "starttls" {
		if err = client.StartTLS(tlsConfig); err != nil {
			return fmt.Errorf("SMTP STARTTLS: %w", err)
		}
	} else if c.TLS != "implicit" {
		return fmt.Errorf("SMTP requires TLS")
	}
	if c.Username != "" {
		if err = client.Auth(smtp.PlainAuth("", c.Username, c.Password, c.Host)); err != nil {
			return fmt.Errorf("SMTP authenticate: %w", err)
		}
	}
	if err = client.Mail(c.From); err != nil {
		return err
	}
	if err = client.Rcpt(to); err != nil {
		return err
	}
	w, err := client.Data()
	if err != nil {
		return err
	}
	message := "From: " + c.From + "\r\nTo: " + to + "\r\nSubject: " + subject + "\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n" + body
	if _, err = io.WriteString(w, message); err != nil {
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	return client.Quit()
}

// Aliyun's RPC signature covers the complete sorted request, including the SMS
// template parameter. The destination is fixed and never supplied by a caller.
func (d *Delivery) sms(ctx context.Context, phone, code string) error {
	c := d.Config.SMS
	if c.AccessKeyID == "" {
		return fmt.Errorf("SMS is not configured")
	}
	params := url.Values{"Action": {"SendSms"}, "Version": {"2017-05-25"}, "Format": {"JSON"}, "AccessKeyId": {c.AccessKeyID}, "SignatureMethod": {"HMAC-SHA1"}, "SignatureVersion": {"1.0"}, "SignatureNonce": {uuid.NewString()}, "Timestamp": {time.Now().UTC().Format("2006-01-02T15:04:05Z")}, "RegionId": {"cn-hangzhou"}, "PhoneNumbers": {strings.TrimPrefix(phone, "+86")}, "SignName": {c.SignName}, "TemplateCode": {c.TemplateCode}, "TemplateParam": {`{"code":"` + code + `"}`}}
	canonical := strings.ReplaceAll(params.Encode(), "+", "%20")
	mac := hmac.New(sha1.New, []byte(c.AccessKeySecret+"&"))
	mac.Write([]byte("POST&%2F&" + strings.ReplaceAll(url.QueryEscape(canonical), "+", "%20")))
	params.Set("Signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://dysmsapi.aliyuncs.com/", strings.NewReader(params.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := d.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send SMS: %w", err)
	}
	defer resp.Body.Close()
	var result struct {
		Code string `json:"Code"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&result); err != nil {
		return fmt.Errorf("decode SMS result: %w", err)
	}
	if resp.StatusCode != http.StatusOK || result.Code != "OK" {
		return fmt.Errorf("SMS provider rejected delivery")
	}
	return nil
}
