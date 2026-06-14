{{/*
Expand the name of the chart.
*/}}
{{- define "hf-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hf-csi.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "hf-csi.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "hf-csi.labels" -}}
helm.sh/chart: {{ include "hf-csi.chart" . }}
{{ include "hf-csi.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "hf-csi.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hf-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "hf-csi.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "hf-csi.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
CSI driver name
*/}}
{{- define "hf-csi.driverName" -}}
{{- default .Values.csiDriverName "csi.huggingface.co" }}
{{- end }}

{{/*
Storage class name
*/}}
{{- define "hf-csi.storageClassName" -}}
{{- default .Values.storageClass.name (include "hf-csi.fullname" .) }}
{{- end }}

{{/*
Token secret name
*/}}
{{- define "hf-csi.tokenSecretName" -}}
{{- if .Values.token.existingSecret }}
{{- .Values.token.existingSecret }}
{{- else }}
{{- printf "%s-token" (include "hf-csi.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Controller selector labels
*/}}
{{- define "hf-csi.controllerSelectorLabels" -}}
{{ include "hf-csi.selectorLabels" . }}
app: hf-csi-controller
{{- end }}

{{/*
Node selector labels
*/}}
{{- define "hf-csi.nodeSelectorLabels" -}}
{{ include "hf-csi.selectorLabels" . }}
app: hf-csi-node
{{- end }}
