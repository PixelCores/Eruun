package application

import (
	"fmt"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/traitvalidation"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type templateRewriteMap struct {
	exact            map[string]string
	serviceExact     map[string]string
	serviceAmbiguous map[string][]string
	text             map[string]string
	exactKey         []string
	textKey          []string
}

type templateServiceRewrite struct {
	oldName         string
	newName         string
	sourceNamespace string
}

func newTemplateRewriteMap() *templateRewriteMap {
	return &templateRewriteMap{
		exact:            make(map[string]string),
		serviceExact:     make(map[string]string),
		serviceAmbiguous: make(map[string][]string),
		text:             make(map[string]string),
	}
}

func (m *templateRewriteMap) clone() *templateRewriteMap {
	cloned := newTemplateRewriteMap()
	if m == nil {
		return cloned
	}
	for key, value := range m.exact {
		cloned.exact[key] = value
	}
	for key, value := range m.serviceExact {
		cloned.serviceExact[key] = value
	}
	for key, candidates := range m.serviceAmbiguous {
		cloned.serviceAmbiguous[key] = append([]string(nil), candidates...)
	}
	for key, value := range m.text {
		cloned.text[key] = value
	}
	cloned.refresh()
	return cloned
}

func buildTemplateRewriteMaps(plans []templateComponentPlan, targetNamespace, baseName string) error {
	globalRewriteMap := newTemplateRewriteMap()
	for oldName, newName := range uniqueTemplateComponentTargets(plans) {
		if err := addTemplateComponentNameRewrite(globalRewriteMap, oldName, newName); err != nil {
			return err
		}
	}

	rewritesByPlan := make(map[int][]templateServiceRewrite)
	serviceCandidates := make(map[string][]string)
	serviceDNSCandidates := make(map[string]map[string][]string)
	type targetService struct {
		component    string
		serviceIndex int
	}
	targetServiceTargets := make(map[string]targetService)
	targetNamespaceName := strings.TrimSpace(targetNamespace)
	if targetNamespaceName == "" {
		targetNamespaceName = config.DefaultNamespace
	}
	duplicatedTemplateComponents := duplicatedTemplateComponentNames(plans)
	for planIndex, plan := range plans {
		if plan.templateComp == nil {
			continue
		}
		var traits apisv1.Traits
		if err := decodeJSONStruct(plan.templateComp.Traits, &traits); err != nil {
			return fmt.Errorf("convert template component %s traits: %w", plan.templateComp.Name, err)
		}
		sourceNamespace := strings.TrimSpace(plan.templateComp.Namespace)
		if sourceNamespace == "" {
			sourceNamespace = config.DefaultNamespace
		}
		serviceTargets := make(map[string]int, len(traits.Service))
		for serviceIndex, serviceTrait := range traits.Service {
			oldServiceName := strings.TrimSpace(serviceTrait.Name)
			if oldServiceName == "" {
				continue
			}
			newServiceName := rewriteTemplateServiceNameForTrait(oldServiceName, plan.templateComp.Name, plan.targetName, baseName, serviceTrait.Type)
			if duplicatedTemplateComponents[plan.templateComp.Name] {
				newServiceName = rewriteTemplateServiceName(oldServiceName, plan.templateComp.Name, plan.targetName)
			}
			if err := validateTemplateServiceName(newServiceName, fmt.Sprintf("template component %s traits.service[%d].name", plan.templateComp.Name, serviceIndex)); err != nil {
				return err
			}
			if previousIndex, ok := serviceTargets[newServiceName]; ok {
				return fmt.Errorf("%w: template component %s traits.service[%d].name rewrites to duplicate service name %q already used by traits.service[%d].name",
					bcode.ErrApplicationConfig, plan.templateComp.Name, serviceIndex, newServiceName, previousIndex)
			}
			serviceTargets[newServiceName] = serviceIndex
			if previous, ok := targetServiceTargets[newServiceName]; ok {
				return fmt.Errorf("%w: template component %s traits.service[%d].name rewrites to duplicate service name %q already used by template component %s traits.service[%d].name in namespace %s",
					bcode.ErrApplicationConfig, plan.templateComp.Name, serviceIndex, newServiceName, previous.component, previous.serviceIndex, targetNamespaceName)
			}
			targetServiceTargets[newServiceName] = targetService{component: plan.templateComp.Name, serviceIndex: serviceIndex}
			rewritesByPlan[planIndex] = append(rewritesByPlan[planIndex], templateServiceRewrite{
				oldName:         oldServiceName,
				newName:         newServiceName,
				sourceNamespace: sourceNamespace,
			})
			serviceCandidates[oldServiceName] = appendUniqueString(serviceCandidates[oldServiceName], newServiceName)
			if serviceDNSCandidates[oldServiceName] == nil {
				serviceDNSCandidates[oldServiceName] = make(map[string][]string)
			}
			serviceDNSCandidates[oldServiceName][sourceNamespace] = appendUniqueString(serviceDNSCandidates[oldServiceName][sourceNamespace], newServiceName)
		}
	}

	ambiguousServiceNames := make(map[string]bool)
	for oldName, candidates := range serviceCandidates {
		if len(candidates) <= 1 {
			continue
		}
		ambiguousServiceNames[oldName] = true
		globalRewriteMap.addAmbiguousService(oldName, candidates)
	}

	for _, rewrites := range rewritesByPlan {
		for _, rewrite := range rewrites {
			if ambiguousServiceNames[rewrite.oldName] {
				if len(serviceDNSCandidates[rewrite.oldName][rewrite.sourceNamespace]) == 1 {
					if err := addTemplateServiceDNSRewrite(globalRewriteMap, rewrite, targetNamespace, false); err != nil {
						return err
					}
				}
				continue
			}
			if err := addTemplateServiceRewrite(globalRewriteMap, rewrite, targetNamespace, true); err != nil {
				return err
			}
		}
	}
	globalRewriteMap.refresh()

	for i := range plans {
		rewriteMap := globalRewriteMap.clone()
		if plans[i].templateComp != nil {
			if err := addTemplateComponentNameRewrite(rewriteMap, plans[i].templateComp.Name, plans[i].targetName); err != nil {
				return err
			}
		}
		for _, rewrite := range rewritesByPlan[i] {
			if err := addTemplateServiceRewrite(rewriteMap, rewrite, targetNamespace, !ambiguousServiceNames[rewrite.oldName]); err != nil {
				return err
			}
		}
		rewriteMap.refresh()
		plans[i].rewriteMap = rewriteMap
	}
	return nil
}

func uniqueTemplateComponentTargets(plans []templateComponentPlan) map[string]string {
	targets := make(map[string][]string)
	for _, plan := range plans {
		if plan.templateComp == nil {
			continue
		}
		templateName := strings.TrimSpace(plan.templateComp.Name)
		targetName := strings.TrimSpace(plan.targetName)
		if templateName == "" || targetName == "" {
			continue
		}
		targets[templateName] = appendUniqueString(targets[templateName], targetName)
	}
	unique := make(map[string]string)
	for templateName, targetNames := range targets {
		if len(targetNames) == 1 {
			unique[templateName] = targetNames[0]
		}
	}
	return unique
}

func duplicatedTemplateComponentNames(plans []templateComponentPlan) map[string]bool {
	targets := make(map[string][]string)
	for _, plan := range plans {
		if plan.templateComp == nil {
			continue
		}
		templateName := strings.TrimSpace(plan.templateComp.Name)
		targetName := strings.TrimSpace(plan.targetName)
		if templateName == "" || targetName == "" {
			continue
		}
		targets[templateName] = appendUniqueString(targets[templateName], targetName)
	}
	duplicated := make(map[string]bool)
	for templateName, targetNames := range targets {
		if len(targetNames) > 1 {
			duplicated[templateName] = true
		}
	}
	return duplicated
}

func addTemplateComponentNameRewrite(rewriteMap *templateRewriteMap, oldName, newName string) error {
	if rewriteMap == nil {
		return nil
	}
	if err := rewriteMap.addExact(oldName, newName); err != nil {
		return err
	}
	return rewriteMap.addExact(fmt.Sprintf("tem-%s", oldName), newName)
}

func addTemplateServiceRewrite(rewriteMap *templateRewriteMap, rewrite templateServiceRewrite, targetNamespace string, includeTargetNamespaceAlias bool) error {
	if err := rewriteMap.addServiceExact(rewrite.oldName, rewrite.newName); err != nil {
		return err
	}
	return addTemplateServiceDNSRewrite(rewriteMap, rewrite, targetNamespace, includeTargetNamespaceAlias)
}

func addTemplateServiceDNSRewrite(rewriteMap *templateRewriteMap, rewrite templateServiceRewrite, targetNamespace string, includeTargetNamespaceAlias bool) error {
	if err := rewriteMap.addServiceDNS(rewrite.oldName, rewrite.newName, rewrite.sourceNamespace, targetNamespace); err != nil {
		return err
	}
	if includeTargetNamespaceAlias && targetNamespace != "" && rewrite.sourceNamespace != targetNamespace {
		if err := rewriteMap.addServiceDNS(rewrite.oldName, rewrite.newName, targetNamespace, targetNamespace); err != nil {
			return err
		}
	}
	return nil
}

func rewriteTemplateServiceName(oldServiceName, oldComponentName, newComponentName string) string {
	oldServiceName = strings.TrimSpace(oldServiceName)
	if oldServiceName == "" {
		return ""
	}
	oldComponentName = strings.TrimSpace(oldComponentName)
	newComponentName = strings.TrimSpace(newComponentName)
	if newComponentName == "" {
		return oldServiceName
	}
	if isAlreadyTargetServiceName(oldServiceName, newComponentName) && hasHyphenDelimitedToken(oldServiceName, oldComponentName) {
		return oldServiceName
	}
	if rewritten, ok := rewriteHyphenDelimitedName(oldServiceName, oldComponentName, newComponentName); ok {
		return rewritten
	}
	return fmt.Sprintf("%s-%s", newComponentName, oldServiceName)
}

func rewriteTemplateServiceNameForTrait(oldServiceName, oldComponentName, newComponentName, baseName, serviceType string) string {
	if isLiteralInternalServiceType(serviceType) {
		return rewriteInternalTemplateServiceName(oldServiceName, baseName)
	}
	return rewriteTemplateServiceName(oldServiceName, oldComponentName, newComponentName)
}

func rewriteInternalTemplateServiceName(oldServiceName, baseName string) string {
	oldServiceName = strings.TrimSpace(oldServiceName)
	if oldServiceName == "" {
		return ""
	}
	baseName = strings.TrimSpace(baseName)
	if baseName == "" || isAlreadyTargetServiceName(oldServiceName, baseName) {
		return oldServiceName
	}
	return fmt.Sprintf("%s-%s", baseName, oldServiceName)
}

func isLiteralInternalServiceType(serviceType string) bool {
	return strings.TrimSpace(serviceType) == string(config.ServiceAccessInternal)
}

func isAlreadyTargetServiceName(serviceName, targetName string) bool {
	return serviceName == targetName || strings.HasPrefix(serviceName, targetName+"-")
}

func hasHyphenDelimitedToken(name, token string) bool {
	name = strings.TrimSpace(name)
	token = strings.TrimSpace(token)
	if name == "" || token == "" {
		return false
	}
	for start := 0; start < len(name); {
		match := strings.Index(name[start:], token)
		if match < 0 {
			return false
		}
		match += start
		if hasHyphenRewriteBoundary(name, match, len(token)) {
			return true
		}
		start = match + 1
	}
	return false
}

func validateTemplateServiceName(name, field string) error {
	validationErrors := traitvalidation.ValidateKubeResourceName(name, field)
	if len(validationErrors) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s", bcode.ErrApplicationConfig, validationErrors[0].Message)
}

func (m *templateRewriteMap) addExact(oldValue, newValue string) error {
	return m.add(m.exact, oldValue, newValue)
}

func (m *templateRewriteMap) addServiceExact(oldValue, newValue string) error {
	return m.add(m.serviceExact, oldValue, newValue)
}

func (m *templateRewriteMap) addAmbiguousService(oldValue string, candidates []string) {
	oldValue = strings.TrimSpace(oldValue)
	if oldValue == "" || len(candidates) <= 1 {
		return
	}
	normalized := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		normalized = appendUniqueString(normalized, strings.TrimSpace(candidate))
	}
	if len(normalized) <= 1 {
		return
	}
	sort.Strings(normalized)
	m.serviceAmbiguous[oldValue] = normalized
}

func (m *templateRewriteMap) addText(oldValue, newValue string) error {
	return m.add(m.text, oldValue, newValue)
}

func (m *templateRewriteMap) addServiceDNS(oldServiceName, newServiceName, sourceNamespace, targetNamespace string) error {
	sourceNamespace = strings.TrimSpace(sourceNamespace)
	if sourceNamespace == "" {
		sourceNamespace = config.DefaultNamespace
	}
	targetNamespace = strings.TrimSpace(targetNamespace)
	if targetNamespace == "" {
		targetNamespace = config.DefaultNamespace
	}
	oldPrefixes := []string{
		fmt.Sprintf("%s.%s", oldServiceName, sourceNamespace),
		fmt.Sprintf("%s.%s.svc", oldServiceName, sourceNamespace),
		fmt.Sprintf("%s.%s.svc.cluster.local", oldServiceName, sourceNamespace),
	}
	newPrefixes := []string{
		fmt.Sprintf("%s.%s", newServiceName, targetNamespace),
		fmt.Sprintf("%s.%s.svc", newServiceName, targetNamespace),
		fmt.Sprintf("%s.%s.svc.cluster.local", newServiceName, targetNamespace),
	}
	for i := range oldPrefixes {
		if err := m.addServiceExact(oldPrefixes[i], newPrefixes[i]); err != nil {
			return err
		}
		if err := m.addText(oldPrefixes[i], newPrefixes[i]); err != nil {
			return err
		}
	}
	return nil
}

func (m *templateRewriteMap) add(target map[string]string, oldValue, newValue string) error {
	oldValue = strings.TrimSpace(oldValue)
	newValue = strings.TrimSpace(newValue)
	if oldValue == "" || newValue == "" || oldValue == newValue {
		return nil
	}
	if existing, ok := target[oldValue]; ok && existing != newValue {
		return fmt.Errorf("%w: template reference %q maps to both %q and %q", bcode.ErrApplicationConfig, oldValue, existing, newValue)
	}
	target[oldValue] = newValue
	return nil
}

func (m *templateRewriteMap) refresh() {
	if m == nil {
		return
	}
	m.exactKey = sortedRewriteKeys(m.exact)
	m.textKey = sortedRewriteKeys(m.text)
}

func sortedRewriteKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) == len(keys[j]) {
			return keys[i] < keys[j]
		}
		return len(keys[i]) > len(keys[j])
	})
	return keys
}

func appendUniqueString(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func (m *templateRewriteMap) exactValue(value string) (string, bool) {
	if m == nil || len(m.exact) == 0 {
		return "", false
	}
	replacement, ok := m.exact[value]
	return replacement, ok
}

func (m *templateRewriteMap) serviceValue(value string) (string, bool) {
	if m == nil || len(m.serviceExact) == 0 {
		return "", false
	}
	replacement, ok := m.serviceExact[value]
	return replacement, ok
}

func (m *templateRewriteMap) ambiguousServiceCandidates(value string) ([]string, bool) {
	if m == nil || len(m.serviceAmbiguous) == 0 {
		return nil, false
	}
	candidates, ok := m.serviceAmbiguous[value]
	return candidates, ok
}

func (m *templateRewriteMap) rewriteText(value string) string {
	if m == nil {
		return value
	}
	return rewriteStringByKeys(value, m.text, m.textKey)
}

func (m *templateRewriteMap) rewriteValue(value string) string {
	if m == nil {
		return value
	}
	if replacement, ok := m.serviceValue(value); ok {
		return replacement
	}
	if replacement, ok := m.exactValue(value); ok {
		return replacement
	}
	return value
}

func rewriteStringByKeys(value string, replacements map[string]string, keys []string) string {
	if value == "" || len(keys) == 0 {
		return value
	}
	var builder strings.Builder
	changed := false
	for i := 0; i < len(value); {
		matched := false
		for _, key := range keys {
			if strings.HasPrefix(value[i:], key) && hasTextRewriteBoundary(value, i, len(key)) {
				builder.WriteString(replacements[key])
				i += len(key)
				matched = true
				changed = true
				break
			}
		}
		if matched {
			continue
		}
		builder.WriteByte(value[i])
		i++
	}
	if !changed {
		return value
	}
	return builder.String()
}

func hasTextRewriteBoundary(value string, start, length int) bool {
	before := start == 0 || !isTemplateReferenceChar(value[start-1])
	end := start + length
	after := end == len(value) || !isTemplateReferenceChar(value[end])
	return before && after
}

func isTemplateReferenceChar(ch byte) bool {
	return ch >= 'a' && ch <= 'z' ||
		ch >= 'A' && ch <= 'Z' ||
		ch >= '0' && ch <= '9' ||
		ch == '-' ||
		ch == '.'
}

func rewriteHyphenDelimitedName(value, oldToken, newToken string) (string, bool) {
	value = strings.TrimSpace(value)
	oldToken = strings.TrimSpace(oldToken)
	newToken = strings.TrimSpace(newToken)
	if value == "" || oldToken == "" || newToken == "" {
		return value, false
	}

	var builder strings.Builder
	changed := false
	for i := 0; i < len(value); {
		match := strings.Index(value[i:], oldToken)
		if match < 0 {
			if changed {
				builder.WriteString(value[i:])
			}
			break
		}
		match += i
		if !hasHyphenRewriteBoundary(value, match, len(oldToken)) {
			if changed {
				builder.WriteString(value[i : match+1])
			}
			i = match + 1
			continue
		}
		if !changed {
			builder.Grow(len(value) + len(newToken))
			builder.WriteString(value[:match])
		} else {
			builder.WriteString(value[i:match])
		}
		builder.WriteString(newToken)
		i = match + len(oldToken)
		changed = true
	}
	if !changed {
		return value, false
	}
	return builder.String(), true
}

func hasHyphenRewriteBoundary(value string, start, length int) bool {
	before := start == 0 || value[start-1] == '-'
	end := start + length
	after := end == len(value) || value[end] == '-'
	return before && after
}
