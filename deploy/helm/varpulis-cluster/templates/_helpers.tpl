{{/*
Common labels
*/}}
{{- define "varpulis-cluster.labels" -}}
app.kubernetes.io/name: varpulis
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/*
Coordinator selector labels
*/}}
{{- define "varpulis-cluster.coordinator.selectorLabels" -}}
app.kubernetes.io/name: varpulis
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: coordinator
{{- end }}

{{/*
Worker selector labels
*/}}
{{- define "varpulis-cluster.worker.selectorLabels" -}}
app.kubernetes.io/name: varpulis
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: worker
{{- end }}

{{/*
Coordinator service name
*/}}
{{- define "varpulis-cluster.coordinator.fullname" -}}
{{ .Release.Name }}-coordinator
{{- end }}

{{/*
Worker service name
*/}}
{{- define "varpulis-cluster.worker.fullname" -}}
{{ .Release.Name }}-worker
{{- end }}

{{/*
MQTT service name
*/}}
{{- define "varpulis-cluster.mqtt.fullname" -}}
{{ .Release.Name }}-mqtt
{{- end }}
