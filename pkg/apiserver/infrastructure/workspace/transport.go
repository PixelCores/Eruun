package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// The transport validates the final generated payload, including apply patches
// and nested pod templates, after trait expansion and before Kubernetes IO.
type tenantTransport struct {
	next      http.RoundTripper
	namespace string
	config    spec.WorkspaceConfig
}

func (t *tenantTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	nsIndex := -1
	for i, p := range parts {
		if p == "namespaces" {
			nsIndex = i
			break
		}
	}
	if nsIndex < 0 || nsIndex+2 >= len(parts) || parts[nsIndex+1] != t.namespace {
		return nil, bcode.ErrForbidden
	}
	resource := parts[nsIndex+2]
	if resource == "serviceaccounts" || resource == "roles" || resource == "rolebindings" || resource == "networkpolicies" || resource == "resourcequotas" || resource == "limitranges" {
		return nil, bcode.ErrForbidden
	}
	if (req.Method == http.MethodPost || req.Method == http.MethodPut || req.Method == http.MethodPatch) && req.Body != nil && req.Header.Get("Content-Type") != "application/json-patch+json" && !(resource == "pods" && strings.HasSuffix(req.URL.Path, "/exec")) {
		raw, err := io.ReadAll(io.LimitReader(req.Body, (8<<20)+1))
		req.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(raw) > 8<<20 {
			return nil, fmt.Errorf("workspace resource payload is too large")
		}
		if len(raw) == 0 {
			req = req.Clone(req.Context())
			req.Body = http.NoBody
		}
		if len(raw) > 0 {
			if !json.Valid(raw) {
				raw, err = yaml.YAMLToJSON(raw)
				if err != nil {
					return nil, fmt.Errorf("decode workspace resource: %w", err)
				}
			}
			var obj map[string]interface{}
			if json.Unmarshal(raw, &obj) != nil || obj == nil {
				return nil, bcode.ErrForbidden
			}
			if err = t.prepare(resource, obj); err != nil {
				return nil, err
			}
			raw, err = json.Marshal(obj)
			if err != nil {
				return nil, err
			}
			copy := req.Clone(req.Context())
			copy.Header = req.Header.Clone()
			copy.Body = io.NopCloser(bytes.NewReader(raw))
			copy.ContentLength = int64(len(raw))
			copy.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(raw)), nil }
			req = copy
		}
	} else if req.Header.Get("Content-Type") == "application/json-patch+json" {
		return nil, bcode.ErrForbidden
	}
	return t.next.RoundTrip(req)
}

func mapAt(object map[string]interface{}, key string) map[string]interface{} {
	m, _ := object[key].(map[string]interface{})
	return m
}

func (t *tenantTransport) prepare(kind string, obj map[string]interface{}) error {
	if meta := mapAt(obj, "metadata"); meta != nil {
		if ns, ok := meta["namespace"].(string); ok && ns != "" && ns != t.namespace {
			return bcode.ErrForbidden
		}
	}
	specification := mapAt(obj, "spec")
	if specification == nil {
		return nil
	}
	switch kind {
	case "deployments", "statefulsets", "replicasets", "jobs":
		if template := mapAt(specification, "template"); template != nil {
			if ps := mapAt(template, "spec"); ps != nil {
				if err := securePodMap(ps); err != nil {
					return err
				}
			}
		}
		if templates, ok := specification["volumeClaimTemplates"].([]interface{}); ok {
			for _, item := range templates {
				v, _ := item.(map[string]interface{})
				if v == nil {
					return bcode.ErrForbidden
				}
				if err := t.pvc(mapAt(v, "spec")); err != nil {
					return err
				}
			}
		}
	case "cronjobs":
		if jt := mapAt(specification, "jobTemplate"); jt != nil {
			return t.prepare("jobs", jt)
		}
	case "pods":
		return securePodMap(specification)
	case "services":
		if v, _ := specification["type"].(string); v != "" && v != "ClusterIP" {
			return bcode.ErrForbidden
		}
		if lenValue(specification["externalIPs"]) > 0 || specification["externalName"] != nil {
			return bcode.ErrForbidden
		}
	case "persistentvolumeclaims":
		return t.pvc(specification)
	case "ingresses":
		if t.config.IngressDomain == "" {
			return bcode.ErrForbidden
		}
		if meta := mapAt(obj, "metadata"); meta != nil && len(mapAt(meta, "annotations")) != 0 {
			return bcode.ErrForbidden
		}
		if v, _ := specification["ingressClassName"].(string); v != t.config.IngressClass || specification["defaultBackend"] != nil {
			return bcode.ErrForbidden
		}
		if lenValue(specification["rules"]) == 0 {
			return bcode.ErrForbidden
		}
		for _, field := range []string{"rules", "tls"} {
			items, _ := specification[field].([]interface{})
			for _, item := range items {
				v, _ := item.(map[string]interface{})
				if field == "rules" {
					host, _ := v["host"].(string)
					if !t.allowedHost(host) {
						return bcode.ErrForbidden
					}
				}
				hosts, _ := v["hosts"].([]interface{})
				for _, host := range hosts {
					h, _ := host.(string)
					if !t.allowedHost(h) {
						return bcode.ErrForbidden
					}
				}
			}
		}
	}
	return nil
}
func (t *tenantTransport) allowedHost(host string) bool {
	return host != "" && !strings.Contains(host, "*") && strings.HasSuffix(host, "."+t.namespace+"."+t.config.IngressDomain)
}
func lenValue(v interface{}) int { a, _ := v.([]interface{}); return len(a) }
func (t *tenantTransport) pvc(s map[string]interface{}) error {
	if s == nil {
		return nil
	}
	if v, _ := s["volumeName"].(string); v != "" {
		return bcode.ErrForbidden
	}
	if s["selector"] != nil || s["dataSourceRef"] != nil || s["dataSource"] != nil {
		return bcode.ErrForbidden
	}
	class, _ := s["storageClassName"].(string)
	for _, allowed := range t.config.StorageClasses {
		if class == allowed {
			return nil
		}
	}
	return bcode.ErrForbidden
}

func securePodMap(object map[string]interface{}) error {
	// Decode the final pod spec to check every container, not just the primary one.
	raw, err := json.Marshal(object)
	if err != nil {
		return err
	}
	var pod corev1.PodSpec
	if json.Unmarshal(raw, &pod) != nil {
		return bcode.ErrForbidden
	}
	if pod.HostNetwork || pod.HostPID || pod.HostIPC || (pod.ServiceAccountName != "" && pod.ServiceAccountName != "default") || pod.NodeName != "" {
		return bcode.ErrForbidden
	}
	if pod.AutomountServiceAccountToken != nil && *pod.AutomountServiceAccountToken {
		return bcode.ErrForbidden
	}
	for _, v := range pod.Volumes {
		if v.HostPath != nil || v.NFS != nil || v.CSI != nil {
			return bcode.ErrForbidden
		}
		if v.Projected != nil {
			for _, s := range v.Projected.Sources {
				if s.ServiceAccountToken != nil {
					return bcode.ErrForbidden
				}
			}
		}
	}
	if pod.SecurityContext != nil {
		if pod.SecurityContext.SeccompProfile != nil && pod.SecurityContext.SeccompProfile.Type == corev1.SeccompProfileTypeUnconfined {
			return bcode.ErrForbidden
		}
		if pod.SecurityContext.RunAsUser != nil && *pod.SecurityContext.RunAsUser == 0 {
			return bcode.ErrForbidden
		}
		if pod.SecurityContext.RunAsNonRoot != nil && !*pod.SecurityContext.RunAsNonRoot {
			return bcode.ErrForbidden
		}
	}
	for _, containers := range [][]corev1.Container{pod.Containers, pod.InitContainers} {
		for _, c := range containers {
			if err = validateContainerSecurity(c.SecurityContext); err != nil {
				return err
			}
			for _, p := range c.Ports {
				if p.HostPort != 0 {
					return bcode.ErrForbidden
				}
			}
		}
	}
	if len(pod.EphemeralContainers) != 0 {
		return bcode.ErrForbidden
	}
	object["automountServiceAccountToken"] = false
	// Only populate present containers. Strategic merge patches that update a
	// field such as replicas must not manufacture or replace container arrays.
	for _, key := range []string{"containers", "initContainers"} {
		items, _ := object[key].([]interface{})
		for _, item := range items {
			c, _ := item.(map[string]interface{})
			if c == nil {
				return bcode.ErrForbidden
			}
			security := mapAt(c, "securityContext")
			if security == nil {
				security = map[string]interface{}{}
				c["securityContext"] = security
			}
			security["allowPrivilegeEscalation"] = false
			security["runAsNonRoot"] = true
			security["capabilities"] = map[string]interface{}{"drop": []string{"ALL"}}
			security["seccompProfile"] = map[string]interface{}{"type": "RuntimeDefault"}
		}
	}
	return nil
}
func validateContainerSecurity(s *corev1.SecurityContext) error {
	if s == nil {
		return nil
	}
	if (s.Privileged != nil && *s.Privileged) || (s.AllowPrivilegeEscalation != nil && *s.AllowPrivilegeEscalation) || (s.RunAsUser != nil && *s.RunAsUser == 0) || (s.RunAsNonRoot != nil && !*s.RunAsNonRoot) || s.ProcMount != nil && *s.ProcMount != corev1.DefaultProcMount || (s.Capabilities != nil && len(s.Capabilities.Add) > 0) || (s.SeccompProfile != nil && s.SeccompProfile.Type == corev1.SeccompProfileTypeUnconfined) {
		return bcode.ErrForbidden
	}
	return nil
}

func (m *Manager) checkEmptyResource(ctx context.Context, w *model.Workspace, groupVersion, resource string) error {
	if resource == "events" {
		return nil
	}
	prefix := "/apis/" + groupVersion
	if groupVersion == "v1" {
		prefix = "/api/v1"
	}
	cursor := ""
	for {
		raw, err := m.Client.Discovery().RESTClient().Get().AbsPath(prefix+"/namespaces/"+w.Namespace+"/"+resource).Param("limit", "100").Param("continue", cursor).DoRaw(ctx)
		if err != nil {
			return fmt.Errorf("check empty namespace %s: %w", resource, err)
		}
		var list struct {
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
			Items []struct {
				Metadata struct {
					Name   string            `json:"name"`
					Labels map[string]string `json:"labels"`
				} `json:"metadata"`
			} `json:"items"`
		}
		if json.Unmarshal(raw, &list) != nil {
			return fmt.Errorf("decode namespace inventory")
		}
		for _, item := range list.Items {
			name := item.Metadata.Name
			baseline := (resource == "serviceaccounts" && (name == "default" || name == runnerName)) || (resource == "configmaps" && name == "kube-root-ca.crt") || ((resource == "roles" || resource == "rolebindings") && name == runnerName) || ((resource == "networkpolicies" || resource == "resourcequotas" || resource == "limitranges") && name == baselineName)
			if !baseline {
				return bcode.ErrWorkspaceNotEmpty
			}
		}
		cursor = list.Metadata.Continue
		if cursor == "" {
			return nil
		}
	}
}

// ValidateTraits rejects unsafe input before persistence. Final-payload checks
// in tenantTransport remain authoritative after templates and traits expand.
func ValidateTraits(namespace, name string, traits *spec.Traits, properties *spec.Properties, cfg spec.WorkspaceConfig) error {
	if properties != nil {
		if properties.Cloud != nil {
			return bcode.ErrForbidden
		}
		for key := range properties.Labels {
			if strings.HasPrefix(key, "eruun.io/") {
				return bcode.ErrForbidden
			}
		}
	}
	if traits == nil {
		return nil
	}
	if len(traits.RBAC) > 0 {
		return bcode.ErrForbidden
	}
	if err := validateContainerSecurity(traits.SecurityPolicy); err != nil {
		return err
	}
	for i := range traits.Storage {
		v := &traits.Storage[i]
		if v.Type == "persistent" {
			if v.StorageClass == "" && len(cfg.StorageClasses) > 0 {
				v.StorageClass = cfg.StorageClasses[0]
			}
			allowed := false
			for _, class := range cfg.StorageClasses {
				if v.StorageClass == class {
					allowed = true
				}
			}
			if !allowed {
				return bcode.ErrForbidden
			}
		}
	}
	for _, v := range traits.Service {
		if v.Type != "" && v.Type != "internal" && v.Type != "ClusterIP" {
			return bcode.ErrForbidden
		}
	}
	for i := range traits.Ingress {
		v := &traits.Ingress[i]
		if cfg.IngressDomain == "" || len(v.Annotations) > 0 || (v.Namespace != "" && v.Namespace != namespace) || (v.IngressClassName != "" && v.IngressClassName != cfg.IngressClass) {
			return bcode.ErrForbidden
		}
		v.Namespace = namespace
		v.IngressClassName = cfg.IngressClass
		check := func(h string) bool {
			return h != "" && !strings.Contains(h, "*") && strings.HasSuffix(h, "."+namespace+"."+cfg.IngressDomain)
		}
		if len(v.Hosts) == 0 {
			v.Hosts = []string{name + "." + namespace + "." + cfg.IngressDomain}
		}
		for _, h := range v.Hosts {
			if !check(h) {
				return bcode.ErrForbidden
			}
		}
		for j := range v.Routes {
			if v.Routes[j].Host == "" {
				v.Routes[j].Host = v.Hosts[0]
			}
			if !check(v.Routes[j].Host) {
				return bcode.ErrForbidden
			}
		}
		for _, tls := range v.TLS {
			for _, h := range tls.Hosts {
				if !check(h) {
					return bcode.ErrForbidden
				}
			}
		}
	}
	for i := range traits.Init {
		c := &traits.Init[i]
		if err := ValidateTraits(namespace, name, &c.Traits, &c.Properties, cfg); err != nil {
			return err
		}
	}
	for i := range traits.Sidecar {
		c := &traits.Sidecar[i]
		if err := ValidateTraits(namespace, name, &c.Traits, nil, cfg); err != nil {
			return err
		}
	}
	return nil
}
