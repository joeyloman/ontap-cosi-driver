{{/*
Expand the name of the chart.
*/}}
{{- define "ontap-cosi-driver.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this.
*/}}
{{- define "ontap-cosi-driver.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains .Release.Name $name }}
{{- $name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ontap-cosi-driver.labels" -}}
helm.sh/chart: {{ include "ontap-cosi-driver.name" . }}-{{ .Chart.Version | replace "+" "_" }}
{{ include "ontap-cosi-driver.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: {{ include "ontap-cosi-driver.name" . }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ontap-cosi-driver.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ontap-cosi-driver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: cosi-driver
{{- end }}

{{/*
Driver name used in COSI resources
*/}}
{{- define "ontap-cosi-driver.driverName" -}}
{{- .Values.driverName | default "ontap.objectstorage.k8s.io" }}
{{- end }}
