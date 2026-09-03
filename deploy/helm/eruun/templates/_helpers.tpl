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

{{- define "eruun.controllerObserverRBACName" -}}
{{- printf "%s-controller-observer" (include "eruun.clusterRBACName" .) -}}
{{- end -}}

{{- define "eruun.suffixedName" -}}
{{- $root := index . "root" -}}
{{- $suffix := required "suffix is required for a suffixed Eruun resource name" (index . "suffix") | toString -}}
{{- $maxBaseLength := int (sub 62 (len $suffix)) -}}
{{- $base := include "eruun.fullname" $root | trunc $maxBaseLength | trimSuffix "-" -}}
{{- printf "%s-%s" $base $suffix | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "eruun.runtimeLockName" -}}
{{- $root := index . "root" -}}
{{- $role := index . "role" -}}
{{- $override := index $root.Values.runtime (printf "%sLockName" $role) -}}
{{- default (include "eruun.suffixedName" (dict "root" $root "suffix" $role)) $override -}}
{{- end -}}

{{- define "eruun.datastoreEnv" -}}
- name: ERUUN_DATASTORE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "eruun.fullname" . }}-mysql
      key: datastore-url
- name: MYSQL_HOST
  value: {{ include "eruun.fullname" . }}-mysql
- name: MYSQL_PORT
  value: {{ .Values.mysql.servicePort | quote }}
- name: MYSQL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "eruun.fullname" . }}-mysql
      key: password
- name: MYSQL_DATABASE
  value: {{ .Values.mysql.database | quote }}
{{- end -}}

{{- define "eruun.roleServiceAccountName" -}}
{{- $root := index . "root" -}}
{{- $role := index . "role" -}}
{{- if $root.Values.serviceAccount.create -}}
{{- include "eruun.suffixedName" (dict "root" $root "suffix" $role) -}}
{{- else -}}
{{- $roleNames := default dict $root.Values.serviceAccount.roleNames -}}
{{- required (printf "serviceAccount.roleNames.%s is required when serviceAccount.create=false" $role) (index $roleNames $role) -}}
{{- end -}}
{{- end -}}

{{- define "eruun.validateRuntime" -}}
{{- if or (empty .Values.auth.existingSecret) (contains "REPLACE" .Values.auth.existingSecret) (eq .Values.auth.existingSecret "******") -}}
{{- fail "auth.existingSecret is required and must reference a configured account Secret" -}}
{{- end -}}
{{- if empty .Values.auth.key -}}{{- fail "auth.key is required" -}}{{- end -}}
{{- if not .Values.serviceAccount.create -}}
{{- $roleNames := default dict .Values.serviceAccount.roleNames -}}
{{- $apiName := default "" (index $roleNames "api") -}}
{{- $controllerName := default "" (index $roleNames "controller") -}}
{{- $schedulerName := default "" (index $roleNames "scheduler") -}}
{{- $workerName := default "" (index $roleNames "worker") -}}
{{- if and $controllerName $apiName (eq $controllerName $apiName) -}}
{{- fail "serviceAccount.roleNames.controller must differ from serviceAccount.roleNames.api" -}}
{{- end -}}
{{- if and $controllerName $workerName (eq $controllerName $workerName) -}}
{{- fail "serviceAccount.roleNames.controller must differ from serviceAccount.roleNames.worker" -}}
{{- end -}}
{{- range $role := list "api" "controller" "worker" -}}
{{- $roleName := default "" (index $roleNames $role) -}}
{{- if and $schedulerName $roleName (eq $schedulerName $roleName) -}}
{{- fail (printf "serviceAccount.roleNames.scheduler must differ from serviceAccount.roleNames.%s" $role) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if le (int .Values.runtime.terminationGracePeriodSeconds) (int .Values.runtime.workerDrainTimeoutSeconds) -}}
{{- fail "runtime.terminationGracePeriodSeconds must be greater than runtime.workerDrainTimeoutSeconds" -}}
{{- end -}}
{{- range $env := .Values.env -}}
{{- $name := trim (default "" $env.name) -}}
{{- if or (eq $name "ERUUN_AUTH_CONFIG_FILE") (eq $name "ERUUN_ROLE") (eq $name "ERUUN_ID") (eq $name "ERUUN_EXIT_ON_LOST_LEADER") (eq $name "ERUUN_WORKFLOW_WORKER_DRAIN_TIMEOUT") (eq $name "ERUUN_DATASTORE_SCHEMA_MODE") -}}
{{- fail (printf "env must not override Chart-managed variable %s" $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
