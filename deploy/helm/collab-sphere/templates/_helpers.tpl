{{- define "collab-sphere.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "collab-sphere.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "collab-sphere.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
app.kubernetes.io/name: {{ include "collab-sphere.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "collab-sphere.selectorLabels" -}}
app.kubernetes.io/name: {{ include "collab-sphere.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "collab-sphere.pgHost" -}}
{{- if .Values.postgresql.enabled -}}
{{- printf "%s-postgresql" (include "collab-sphere.fullname" .) -}}
{{- else -}}
{{- required "postgresql.host is required when postgresql.enabled is false" .Values.postgresql.host -}}
{{- end -}}
{{- end -}}

{{- define "collab-sphere.databaseURL" -}}
{{- $root := index . 0 -}}
{{- $db := index . 1 -}}
{{- printf "postgres://%s:%s@%s:%v/%s?sslmode=disable" $db.user $db.password (include "collab-sphere.pgHost" $root) $root.Values.postgresql.port $db.name -}}
{{- end -}}

{{- define "collab-sphere.redisURL" -}}
{{- if .Values.redis.enabled -}}
{{- printf "redis://%s-redis:6379/0" (include "collab-sphere.fullname" .) -}}
{{- else -}}
{{- required "redis.url is required when redis.enabled is false" .Values.redis.url -}}
{{- end -}}
{{- end -}}

{{- define "collab-sphere.minioEndpoint" -}}
{{- if .Values.minio.enabled -}}
{{- printf "http://%s-minio:9000" (include "collab-sphere.fullname" .) -}}
{{- else -}}
{{- required "minio.endpoint is required when minio.enabled is false" .Values.minio.endpoint -}}
{{- end -}}
{{- end -}}

{{- define "collab-sphere.addr" -}}
{{- $root := index . 0 -}}
{{- $name := index . 1 -}}
{{- $port := index . 2 -}}
{{- printf "%s-%s:%v" (include "collab-sphere.fullname" $root) $name $port -}}
{{- end -}}

{{- define "collab-sphere.secretEnv" -}}
- name: {{ .name }}
  valueFrom:
    secretKeyRef:
      name: {{ include "collab-sphere.fullname" .root }}-secrets
      key: {{ .key }}
{{- end -}}

{{- define "collab-sphere.extraEnv" -}}
- name: METRICS_ADDR
  value: {{ .metrics | quote }}
- name: GC_INTERVAL
  value: {{ .root.Values.gc.interval | quote }}
- name: RESTORE_HORIZON
  value: {{ .root.Values.gc.restoreHorizon | quote }}
- name: REPLICA_REPORT_TTL
  value: {{ .root.Values.gc.replicaReportTTL | quote }}
{{- end -}}

{{- define "collab-sphere.appResources" -}}
resources:
  requests:
    cpu: {{ .Values.resources.requests.cpu }}
    memory: {{ .Values.resources.requests.memory }}
{{- end -}}
