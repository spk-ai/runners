{{- /*
  Renders the caller-authorization environment from .Values.rpcAuth into
  .Values.env (see internal/config loadRPCAuth and internal/rpcauth.Policy).
*/ -}}
{{- define "runners.configureRPCAuth" -}}
{{- $rpc := .Values.rpcAuth -}}
{{- if not (has $rpc.mode (list "enforce" "permissive")) -}}
{{- fail "rpcAuth.mode must be enforce or permissive" -}}
{{- end -}}
{{- $callers := list -}}
{{- range $rpc.callers -}}
{{- $callers = append $callers (dict "namespace" (default $.Release.Namespace .namespace) "serviceAccount" .serviceAccount "grants" .grants) -}}
{{- end -}}
{{- $policy := dict "version" 1 "audience" $rpc.audience "cacheTTLSeconds" (int $rpc.cacheTTLSeconds) "callers" $callers -}}
{{- if $rpc.tokenless -}}
{{- $_ := set $policy "tokenless" $rpc.tokenless -}}
{{- end -}}
{{- $env := list (dict "name" "RUNNERS_RPC_AUTH_MODE" "value" $rpc.mode) (dict "name" "RUNNERS_RPC_POLICY" "value" (toJson $policy)) -}}
{{- $_ := set .Values "env" (concat (.Values.env | default (list)) $env) -}}
{{- end -}}
