{{- define "eruun.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "eruun.labels" -}}
app.kubernetes.io/name: {{ include "eruun.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
app.kubernetes.io/part-of: eruun
app.kubernetes.io/managed-by: eruun
{{- end -}}

{{- define "eruun.clusterRBACName" -}}
{{- printf "%s-%s" (include "eruun.fullname" .) .Release.Namespace -}}
{{- end -}}

{{- define "eruun.suffixedName" -}}
{{- $root := index . "root" -}}
{{- $suffix := required "suffix is required for a suffixed Eruun resource name" (index . "suffix") | toString -}}
{{- $maxBaseLength := int (sub 62 (len $suffix)) -}}
{{- $base := include "eruun.fullname" $root | trunc $maxBaseLength | trimSuffix "-" -}}
{{- printf "%s-%s" $base $suffix | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "eruun.persistentWorkloadName" -}}
{{- $root := index . "root" -}}
{{- $component := required "component is required for a persistent Eruun workload" (index . "component") | toString -}}
{{- $currentName := required "currentName is required for a persistent Eruun workload" (index . "currentName") | toString -}}
{{- $identities := dict -}}
{{- $statefulSets := lookup "apps/v1" "StatefulSet" $root.Release.Namespace "" -}}
{{- range $candidate := default (list) (get $statefulSets "items") -}}
{{- $labels := default dict $candidate.metadata.labels -}}
{{- $selectorLabels := default dict $candidate.spec.selector.matchLabels -}}
{{- if and (eq (get $labels "app.kubernetes.io/instance") $root.Release.Name) (eq (get $selectorLabels "app.kubernetes.io/component") $component) -}}
{{- $_ := set $identities (get $candidate.metadata "name") true -}}
{{- end -}}
{{- end -}}
{{- $secrets := lookup "v1" "Secret" $root.Release.Namespace "" -}}
{{- range $candidate := default (list) (get $secrets "items") -}}
{{- $labels := default dict $candidate.metadata.labels -}}
{{- $name := default "" (get $candidate.metadata "name") -}}
{{- if and (eq (get $labels "app.kubernetes.io/instance") $root.Release.Name) (hasSuffix (printf "-%s" $component) $name) -}}
{{- $_ := set $identities $name true -}}
{{- end -}}
{{- end -}}
{{- $claims := lookup "v1" "PersistentVolumeClaim" $root.Release.Namespace "" -}}
{{- range $claim := default (list) (get $claims "items") -}}
{{- $labels := default dict $claim.metadata.labels -}}
{{- if and (eq (get $labels "app.kubernetes.io/instance") $root.Release.Name) (eq (get $labels "app.kubernetes.io/component") $component) -}}
{{- $_ := set $identities (trimSuffix "-0" (trimPrefix "data-" (get $claim.metadata "name"))) true -}}
{{- end -}}
{{- end -}}
{{- $currentClaim := lookup "v1" "PersistentVolumeClaim" $root.Release.Namespace (printf "data-%s-0" $currentName) -}}
{{- if $currentClaim -}}
{{- $_ := set $identities $currentName true -}}
{{- end -}}
{{- if gt (len $identities) 1 -}}
{{- fail (printf "multiple bundled %s persistent workload identities exist; use a separate data migration procedure" $component) -}}
{{- end -}}
{{- if eq (len $identities) 1 -}}
{{- first (keys $identities) -}}
{{- end -}}
{{- end -}}

{{- define "eruun.runtimeLockName" -}}
{{- default (include "eruun.suffixedName" (dict "root" . "suffix" "runtime")) .Values.runtime.leaderLockName -}}
{{- end -}}

{{- define "eruun.datastoreEnv" -}}
- name: ERUUN_DATASTORE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "eruun.suffixedName" (dict "root" . "suffix" "mysql") }}
      key: datastore-url
- name: MYSQL_HOST
  value: {{ include "eruun.suffixedName" (dict "root" . "suffix" "mysql") }}
- name: MYSQL_PORT
  value: {{ .Values.mysql.servicePort | quote }}
- name: MYSQL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "eruun.suffixedName" (dict "root" . "suffix" "mysql") }}
      key: password
- name: MYSQL_DATABASE
  value: {{ .Values.mysql.database | quote }}
{{- end -}}

{{- define "eruun.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "eruun.suffixedName" (dict "root" . "suffix" "runtime")) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "eruun.validateRuntime" -}}
{{- if eq (int .Values.service.port) (int .Values.service.grpcPort) -}}
{{- fail "service.grpcPort must differ from service.port" -}}
{{- end -}}
{{- if or (empty .Values.auth.existingSecret) (contains "REPLACE" .Values.auth.existingSecret) (eq .Values.auth.existingSecret "******") -}}
{{- fail "auth.existingSecret is required and must reference a configured account Secret" -}}
{{- end -}}
{{- if empty .Values.auth.key -}}{{- fail "auth.key is required" -}}{{- end -}}
{{- if le (int .Values.runtime.terminationGracePeriodSeconds) (int .Values.runtime.workerDrainTimeoutSeconds) -}}
{{- fail "runtime.terminationGracePeriodSeconds must be greater than runtime.workerDrainTimeoutSeconds" -}}
{{- end -}}
{{- range $env := .Values.env -}}
{{- $name := trim (default "" $env.name) -}}
{{- if or (eq $name "ERUUN_AUTH_CONFIG_FILE") (eq $name "ERUUN_ROLE") (eq $name "ERUUN_ID") (eq $name "ERUUN_POD_NAME") (eq $name "ERUUN_EXIT_ON_LOST_LEADER") (eq $name "ERUUN_WORKFLOW_WORKER_DRAIN_TIMEOUT") (eq $name "ERUUN_DATASTORE_SCHEMA_MODE") (eq $name "ERUUN_GRPC_BIND_ADDR") (eq $name "ERUUN_BIND_ADDR") (eq $name "ERUUN_LEADER_LOCK_NAME") (eq $name "ERUUN_LEADER_NAMESPACE") (eq $name "ERUUN_LEADER_SERVICE_NAME") (eq $name "ERUUN_CONTROLLER_LOCK_NAME") (eq $name "ERUUN_SCHEDULER_LOCK_NAME") -}}
{{- fail (printf "env must not override Chart-managed variable %s" $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
