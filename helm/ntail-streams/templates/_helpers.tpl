{{/*
Expand the name of the chart.
*/}}
{{- define "ntail-streams.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 56 | trimSuffix "-" -}}
{{- end }}
{{- define "ntail-streams.gwName" -}}
{{- printf "%s-%s" (include "ntail-streams.name" .) "gw" }}
{{- end }}
{{- define "ntail-streams.bufferName" -}}
{{- printf "%s-%s" (include "ntail-streams.name" .) "buffer" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}

{{- define "ntail-streams.fullname" -}}
{{- $name := (include "ntail-streams.name" .) }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 56 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 56 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "ntail-streams.gwFullname" -}}
{{- printf "%s-%s" (include "ntail-streams.fullname" .) "gw" -}}
{{- end }}

{{- define "ntail-streams.bufferFullname" -}}
{{- printf "%s-%s" (include "ntail-streams.fullname" .) "buffer" -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ntail-streams.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ntail-streams.labels" -}}
helm.sh/chart: {{ include "ntail-streams.chart" . }}
{{ include "ntail-streams.gwSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ntail-streams.gwSelectorLabels" -}}
app.kubernetes.io/name: {{ include "ntail-streams.gwName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
{{- define "ntail-streams.bufferSelectorLabels" -}}
app.kubernetes.io/name: {{ include "ntail-streams.bufferName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "ntail-streams.gwServiceAccountName" -}}
{{- if .Values.gwServiceAccount.create }}
{{- default (include "ntail-streams.gwFullname" .) .Values.gwServiceAccount.name }}
{{- else }}
{{- default "default" .Values.gwServiceAccount.name }}
{{- end }}
{{- end }}
{{- define "ntail-streams.bufferServiceAccountName" -}}
{{- if .Values.bufferServiceAccount.create }}
{{- default (include "ntail-streams.bufferFullname" .) .Values.bufferServiceAccount.name }}
{{- else }}
{{- default "default" .Values.bufferServiceAccount.name }}
{{- end }}
{{- end }}
