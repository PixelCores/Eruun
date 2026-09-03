package spec

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/mail"
	"net/url"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kvalidation "k8s.io/apimachinery/pkg/util/validation"
)

type AccountConfig struct {
	Origins           []string `json:"origins"`
	FrontendURL       string   `json:"frontendURL"`
	TrustedProxyCIDRs []string `json:"trustedProxyCIDRs"`
	BootstrapAdmin    struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	} `json:"bootstrapAdmin"`
	Google    OAuthProviderConfig `json:"google"`
	GitHub    OAuthProviderConfig `json:"github"`
	SMTP      SMTPConfig          `json:"smtp"`
	SMS       SMSConfig           `json:"sms"`
	Workspace WorkspaceConfig     `json:"workspace"`
}

type OAuthProviderConfig struct {
	Enabled      bool   `json:"enabled"`
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	RedirectURI  string `json:"redirectURI"`
}

type SMTPConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	From     string `json:"from"`
	// TLS is either implicit TLS or STARTTLS, never plaintext fallback.
	TLS string `json:"tls"`
}

type SMSConfig struct {
	AccessKeyID     string `json:"accessKeyId"`
	AccessKeySecret string `json:"accessKeySecret"`
	SignName        string `json:"signName"`
	TemplateCode    string `json:"templateCode"`
}

type WorkspaceConfig struct {
	ClusterCIDRs     []string            `json:"clusterCIDRs"`
	StorageClasses   []string            `json:"storageClasses"`
	IngressDomain    string              `json:"ingressDomain"`
	IngressClass     string              `json:"ingressClass"`
	IngressNamespace string              `json:"ingressNamespace"`
	DNSNamespace     string              `json:"dnsNamespace"`
	Quota            corev1.ResourceList `json:"quota"`
}

func LoadAccountConfig(path string) (*AccountConfig, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("auth-config-file is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open auth config: %w", err)
	}
	defer f.Close()
	var cfg AccountConfig
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if err = d.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode auth config: %w", err)
	}
	var extra interface{}
	if d.Decode(&extra) != io.EOF {
		return nil, fmt.Errorf("auth config must contain one JSON object")
	}
	if err = cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *AccountConfig) Validate() error {
	for _, cidr := range c.TrustedProxyCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("trustedProxyCIDRs must contain explicit proxy CIDRs")
		}
	}
	if len(c.Origins) == 0 {
		return fmt.Errorf("auth origins are required")
	}
	for _, origin := range c.Origins {
		u, e := url.Parse(origin)
		if e != nil || u.Scheme != "https" || u.Host == "" || strings.Contains(u.Host, "*") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("auth origins must be exact HTTPS origins")
		}
	}
	if !c.AllowedURL(c.FrontendURL) {
		return fmt.Errorf("frontendURL must use an allowed HTTPS origin")
	}
	for name, p := range map[string]OAuthProviderConfig{"google": c.Google, "github": c.GitHub} {
		if p.Enabled && (invalidAccountSecret(p.ClientID) || invalidAccountSecret(p.ClientSecret) || !c.AllowedURL(p.RedirectURI)) {
			return fmt.Errorf("%s OAuth credentials and allowed redirectURI are required", name)
		}
	}
	if c.BootstrapAdmin.Email != "" {
		address, err := mail.ParseAddress(c.BootstrapAdmin.Email)
		if err != nil || address.Address != c.BootstrapAdmin.Email {
			return fmt.Errorf("bootstrap administrator email is invalid")
		}
		if len([]rune(c.BootstrapAdmin.Password)) < 12 || len([]rune(c.BootstrapAdmin.Password)) > 128 || invalidAccountSecret(c.BootstrapAdmin.Password) {
			return fmt.Errorf("bootstrap password must contain 12–128 characters")
		}
	}
	if c.BootstrapAdmin.Email == "" && c.BootstrapAdmin.Password != "" {
		return fmt.Errorf("bootstrap administrator email is required with password")
	}
	if c.SMTP.Host == "" && (c.SMTP.Port != 0 || c.SMTP.Username != "" || c.SMTP.Password != "" || c.SMTP.From != "" || c.SMTP.TLS != "") {
		return fmt.Errorf("SMTP host is required with SMTP configuration")
	}
	if c.SMTP.Host != "" {
		if c.SMTP.Port < 1 || c.SMTP.Port > 65535 || (c.SMTP.TLS != "implicit" && c.SMTP.TLS != "starttls") {
			return fmt.Errorf("SMTP requires a valid port and tls=implicit or starttls")
		}
		a, e := mail.ParseAddress(c.SMTP.From)
		if e != nil || a.Address != c.SMTP.From {
			return fmt.Errorf("SMTP from must be an email address")
		}
		if c.SMTP.Username != "" && invalidAccountSecret(c.SMTP.Password) {
			return fmt.Errorf("SMTP password is required")
		}
	}
	if c.SMS.AccessKeyID != "" && (invalidAccountSecret(c.SMS.AccessKeyID) || invalidAccountSecret(c.SMS.AccessKeySecret) || invalidAccountSecret(c.SMS.SignName) || invalidAccountSecret(c.SMS.TemplateCode)) {
		return fmt.Errorf("SMS credentials, signName and templateCode are required")
	}
	if c.SMS.AccessKeyID == "" && (c.SMS.AccessKeySecret != "" || c.SMS.SignName != "" || c.SMS.TemplateCode != "") {
		return fmt.Errorf("SMS accessKeyId is required with SMS configuration")
	}
	if len(c.Workspace.ClusterCIDRs) == 0 {
		return fmt.Errorf("workspace.clusterCIDRs must include cluster pod, service, node and control-plane networks")
	}
	for _, cidr := range c.Workspace.ClusterCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("invalid workspace cluster CIDR")
		}
	}
	if c.Workspace.DNSNamespace == "" {
		c.Workspace.DNSNamespace = "kube-system"
	}
	if c.Workspace.IngressDomain != "" && (c.Workspace.IngressClass == "" || c.Workspace.IngressNamespace == "") {
		return fmt.Errorf("ingress domain requires ingressClass and ingressNamespace")
	}
	for _, name := range c.Workspace.StorageClasses {
		if len(kvalidation.IsDNS1123Subdomain(name)) != 0 || invalidAccountSecret(name) {
			return fmt.Errorf("workspace storageClasses must contain valid storage class names")
		}
	}
	for _, name := range []string{c.Workspace.DNSNamespace, c.Workspace.IngressNamespace, c.Workspace.IngressClass, c.Workspace.IngressDomain} {
		if name != "" && len(kvalidation.IsDNS1123Subdomain(name)) != 0 {
			return fmt.Errorf("workspace DNS and ingress configuration contains an invalid name")
		}
	}
	defaults := map[corev1.ResourceName]string{"pods": "20", "requests.cpu": "2", "limits.cpu": "4", "requests.memory": "4Gi", "limits.memory": "8Gi", "persistentvolumeclaims": "10", "requests.storage": "20Gi", "requests.nvidia.com/gpu": "0", "services.nodeports": "0", "services.loadbalancers": "0"}
	if c.Workspace.Quota == nil {
		c.Workspace.Quota = corev1.ResourceList{}
	}
	for k, v := range defaults {
		if _, ok := c.Workspace.Quota[k]; !ok {
			c.Workspace.Quota[k] = resource.MustParse(v)
		}
	}
	for _, v := range c.Workspace.Quota {
		if v.Sign() < 0 {
			return fmt.Errorf("workspace quota cannot be negative")
		}
	}
	return nil
}

func (c *AccountConfig) AllowedOrigin(origin string) bool {
	for _, v := range c.Origins {
		if v == origin {
			return true
		}
	}
	return false
}
func (c *AccountConfig) AllowedURL(raw string) bool {
	u, e := url.Parse(raw)
	return e == nil && u.User == nil && u.Fragment == "" && u.Scheme == "https" && c.AllowedOrigin(u.Scheme+"://"+u.Host)
}
func invalidAccountSecret(s string) bool {
	return strings.TrimSpace(s) == "" || s == "******" || strings.Contains(strings.ToLower(s), "replace") || strings.HasPrefix(s, "<")
}
