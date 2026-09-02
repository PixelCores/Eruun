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

{{- define "eruun.runtimeLockName" -}}
{{- $root := index . "root" -}}
{{- $role := index . "role" -}}
{{- $override := index $root.Values.runtime (printf "%sLockName" $role) -}}
{{- default (include "eruun.suffixedName" (dict "root" $root "suffix" $role)) $override -}}
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
{{- if not .Values.serviceAccount.create -}}
{{- $roleNames := default dict .Values.serviceAccount.roleNames -}}
{{- $schedulerName := default "" (index $roleNames "scheduler") -}}
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
{{- if or (eq $name "ERUUN_ROLE") (eq $name "ERUUN_ID") (eq $name "ERUUN_EXIT_ON_LOST_LEADER") (eq $name "ERUUN_WORKFLOW_WORKER_DRAIN_TIMEOUT") -}}
{{- fail (printf "env must not override Chart-managed variable %s" $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
