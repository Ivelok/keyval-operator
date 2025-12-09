{{- define "keyval-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "keyval-operator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "keyval-operator.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "keyval-operator.labels" -}}
app.kubernetes.io/name: {{ include "keyval-operator.name" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/instance: {{ .Release.Name | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.labels }}
{{ toYaml . | indent 0 }}
{{- end }}
{{- end -}}

{{- define "keyval-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "keyval-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name | trunc 63 | trimSuffix "-" }}
control-plane: controller-manager
{{- end -}}

{{- define "keyval-operator.controllerName" -}}
{{- printf "%s-controller-manager" (include "keyval-operator.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "keyval-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "keyval-operator.controllerName" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "keyval-operator-controller-manager" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
