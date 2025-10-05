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
{{- end -}}

{{- define "keyval-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (printf "%s-controller" (include "keyval-operator.fullname" .)) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "keyval-operator-controller" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
