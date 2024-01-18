{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}


{{- define "charts.host.app" -}}
{{- .Values.global.gateway.default.protocol -}}://{{- .Values.global.gateway.default.appHost | default .Values.global.gateway.default.host }}
{{- end }}

{{- define "charts.host.api" -}}
{{- .Values.global.gateway.default.protocol -}}://{{- .Values.global.gateway.default.apiHost | default .Values.global.gateway.default.host }}
{{- end }}

{{- define "charts.name" -}}
{{- default .Chart.Name .Values.global.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "charts.s3.url" -}} https://s3-{{.Values.global.region}}.amazonaws.com {{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "charts.fullname" -}}
{{- if .Values.global.fullnameOverride -}}
{{- .Values.global.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.global.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "charts.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create environment variables used by all platform services.
*/}}
}

{{ define "charts.platform.env.clients" }}
        - name: FORWARD_AUTH_CLIENT_ADDRESS
          value: http://auth:{{.Values.global.ports.auth}}
{{ end }}

{{ define "charts.tracing.common" }}
        - name: POD_NAME
          valueFrom:
              fieldRef:
                fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
              fieldRef:
                fieldPath: metadata.namespace
        - name: POD_IP
          valueFrom:
              fieldRef:
                fieldPath: status.podIP
        - name: OC_AGENT_HOST
          value: "oc-collector.tracing:55678"
        - name: OTEL_COLLECTOR_HOST
          value: "otel-collector.observability:55680"
{{ end }}

{{ define "charts.platform.env.misc" }}
{{ include "charts.tracing.common" . }}
        - name: AWS_REGION
          value: {{ .Values.global.region }}
        - name: STATEMACHINE2_ENV
          value: local
        - name: STATEMACHINE2_LOGGER_LEVEL
          value: {{ .Values.global.logLevel }}
        - name: STATEMACHINE2_SERVER_TLS
          value: "false"
        - name: STATEMACHINE2_AUTH_SERVICE_SECRET
          valueFrom:
            secretKeyRef:
              name: auth
              key: ServiceAuth
{{ end }}

{{- define "charts.routing.opts.shadowing" -}}
      shadowing:
        upstream:
          name: {{ .Values.shadowing.upstreamName | quote }}
          namespace: {{ .Values.shadowing.namespace | quote }}
        percentage: {{ .Values.shadowing.percentage }}
{{- end }}

{{/*
Create liveness and readiness probes for platform services.
*/}}
{{- define "charts.platform.probes" -}}
        readinessProbe:
          httpGet:
            path: /status
            port: {{.}}
          initialDelaySeconds: 5
          periodSeconds: 10
          timeoutSeconds: 5
{{- end -}}
{{- define "charts.platform.grpc_probes" -}}
        readinessProbe:
          exec:
            command: ["/bin/grpc_health_probe", "-addr=:{{.}}"]
          initialDelaySeconds: 5
          periodSeconds: 10
          timeoutSeconds: 5
{{- end -}}
{{- define "charts.init.shoreline" -}}
      - name: init-shoreline
        image: busybox:1.31.1
        command: ['sh', '-c', 'until nc -zvv shoreline {{.Values.global.ports.shoreline}}; do echo waiting for shoreline; sleep 2; done;']
{{- end -}}

{{- define "charts.labels.standard" }}
    helm.sh/chart: {{ include "charts.chart" . }}
    app.kubernetes.io/managed-by: {{ .Release.Service }}
    app.kubernetes.io/name: {{ include "charts.name" . }}
    app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "charts.service.https.port" -}}
{{ .Values.gloo.gatewayProxies.gatewayProxyV2.service.httpsPort }}
{{ end }}

{{- define "charts.service.http.port" -}}
{{ .Values.gloo.gatewayProxies.gatewayProxyV2.service.httpPort }}
{{ end }}

{{- define "charts.service.type" -}}
{{ .Values.gloo.gatewayProxies.gatewayProxyV2.service.type }}
{{ end }}

{{- define "charts.kafka.common" -}}
        - name: KAFKA_BROKERS
          valueFrom:
            configMapKeyRef:
              name: {{ .Values.kafka.configmapName }}
              key: Brokers
              optional: true
        - name: KAFKA_TOPIC_PREFIX
          valueFrom:
            configMapKeyRef:
              name: {{ .Values.kafka.configmapName }}
              key: TopicPrefix
              optional: true
        - name: KAFKA_REQUIRE_SSL
          valueFrom:
            configMapKeyRef:
              name: {{ .Values.kafka.configmapName }}
              key: RequireSSL
              optional: true
        - name: KAFKA_USERNAME
          valueFrom:
            configMapKeyRef:
              name: {{ .Values.kafka.configmapName }}
              key: Username
              optional: true
        - name: KAFKA_PASSWORD
          valueFrom:
            secretKeyRef:
              name: {{ .Values.kafka.secretName }}
              key: {{ .Values.global.kafka.passwordKeyName | default "Password" }}
        - name: KAFKA_VERSION
          valueFrom:
            configMapKeyRef:
              name: {{ .Values.kafka.configmapName }}
              key: Version
              optional: true
{{ end }}

{{- define "charts.kafka.cloudevents.client" -}}
        - name: CLOUD_EVENTS_SOURCE
          value: {{ .client | quote }}
        - name: KAFKA_CONSUMER_GROUP
          value: {{ printf "%s-%s" .Release.Namespace .client | quote }}
        - name: KAFKA_TOPIC
          valueFrom:
            configMapKeyRef:
              name: {{ .Values.kafka.configmapName }}
              key: UserEventsTopic
              optional: true
        - name: KAFKA_DEAD_LETTERS_TOPIC
          valueFrom:
            configMapKeyRef:
              name: {{ .Values.kafka.configmapName }}
              key: UserEvents{{ .client | title }}DeadLettersTopic
              optional: true
{{ end }}