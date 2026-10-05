package v1

import (
	"fmt"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func buildComponentCredentials(component *apisv1.ApplicationComponent, secrets componentSecretIndex) []apisv1.ComponentCredentialInfo {
	if component == nil {
		return nil
	}
	var credentials []apisv1.ComponentCredentialInfo
	credentials = appendTraitCredentials(credentials, component.Namespace, "component", component.Traits, secrets)
	if len(credentials) == 0 {
		return nil
	}
	return credentials
}

func appendTraitCredentials(credentials []apisv1.ComponentCredentialInfo, namespace, source string, traits model.Traits, secrets componentSecretIndex) []apisv1.ComponentCredentialInfo {
	for _, env := range traits.Envs {
		if env.ValueFrom.Secret == nil {
			continue
		}
		ref := env.ValueFrom.Secret
		credentials = appendSecretKeyCredential(credentials, namespace, source+".envs", env.Name, ref.Name, ref.Key, secrets)
	}
	for _, envFrom := range traits.EnvFrom {
		if !strings.EqualFold(strings.TrimSpace(envFrom.Type), domainspec.StorageTypeSecret) {
			continue
		}
		credentials = appendWholeSecretCredentials(credentials, namespace, source+".envFrom", "", envFrom.SourceName, secrets)
	}
	for _, storage := range traits.Storage {
		if !strings.EqualFold(strings.TrimSpace(storage.Type), domainspec.StorageTypeSecret) {
			continue
		}
		secretName := strings.TrimSpace(storage.SourceName)
		if secretName == "" {
			secretName = strings.TrimSpace(storage.Name)
		}
		credentials = appendWholeSecretCredentials(credentials, namespace, source+".storage", "", secretName, secrets)
	}
	for _, init := range traits.Init {
		childSource := fmt.Sprintf("%s.init[%s]", source, strings.TrimSpace(init.Name))
		credentials = appendTraitCredentials(credentials, namespace, childSource, init.Traits, secrets)
	}
	for _, sidecar := range traits.Sidecar {
		childSource := fmt.Sprintf("%s.sidecar[%s]", source, strings.TrimSpace(sidecar.Name))
		credentials = appendTraitCredentials(credentials, namespace, childSource, sidecar.Traits, secrets)
	}
	return credentials
}

func appendSecretKeyCredential(credentials []apisv1.ComponentCredentialInfo, namespace, source, envName, secretName, key string, secrets componentSecretIndex) []apisv1.ComponentCredentialInfo {
	secretName = strings.TrimSpace(secretName)
	key = strings.TrimSpace(key)
	if secretName == "" {
		return credentials
	}
	value, resolved := lookupSecretValue(secrets, namespace, secretName, key)
	return append(credentials, apisv1.ComponentCredentialInfo{
		Source:     source,
		EnvName:    strings.TrimSpace(envName),
		SecretName: secretName,
		Key:        key,
		Value:      value,
		Resolved:   resolved,
	})
}

func appendWholeSecretCredentials(credentials []apisv1.ComponentCredentialInfo, namespace, source, envName, secretName string, secrets componentSecretIndex) []apisv1.ComponentCredentialInfo {
	secretName = strings.TrimSpace(secretName)
	if secretName == "" {
		return credentials
	}
	values, ok := secrets[componentSecretKey(namespace, secretName)]
	if !ok {
		return append(credentials, apisv1.ComponentCredentialInfo{
			Source:     source,
			EnvName:    strings.TrimSpace(envName),
			SecretName: secretName,
			Resolved:   false,
		})
	}
	keys := sortedSecretEntryKeys(values)
	if len(keys) == 0 {
		return credentials
	}
	for _, key := range keys {
		value := values[key]
		credentials = append(credentials, apisv1.ComponentCredentialInfo{
			Source:     source,
			EnvName:    strings.TrimSpace(envName),
			SecretName: secretName,
			Key:        key,
			Value:      value,
			Resolved:   value != "",
		})
	}
	return credentials
}

func lookupSecretValue(secrets componentSecretIndex, namespace, secretName, key string) (string, bool) {
	value := secrets[componentSecretKey(namespace, secretName)][key]
	return value, value != ""
}

func componentSecretKey(namespace, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	return pickComponentNamespace(namespace) + "/" + name
}

func pickComponentNamespace(namespace string) string {
	if namespace = strings.TrimSpace(namespace); namespace != "" {
		return namespace
	}
	return config.DefaultNamespace
}

func copyStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copied := make(map[string]string, len(values))
	for key, value := range values {
		copied[key] = value
	}
	return copied
}

func sortedSecretEntryKeys(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
