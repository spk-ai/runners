#!/usr/bin/env bash
# Runs the caller-authorization matrix against enforcing runners in the
# disposable VM, through both the Service address and the Pod IP.
# Usage: run.sh PROBE_BINARY TOKENREVIEW_BINDING RUNNERS_SERVICE_ACCOUNT
set -euo pipefail

probe="$1"
binding="$2"
runners_sa="$3"
ns=agyn-platform
dir="$(cd "$(dirname "$0")" && pwd)"

for sa in agents-orchestrator gateway; do
  if ! kubectl -n "$ns" get serviceaccount "$sa" >/dev/null 2>&1; then
    echo "::error::caller ServiceAccount $ns/$sa is missing from the VM" >&2
    exit 1
  fi
done

kubectl apply -f "$dir/probes.yaml"
kubectl -n "$ns" wait pod -l app.kubernetes.io/managed-by=rpcauth-e2e \
  --for=condition=Ready --timeout=300s

# The Pod IP behind the Service, so both targets reach the same source pod.
read -r -a endpoint_ips <<< "$(kubectl -n "$ns" get endpoints runners -o jsonpath='{.subsets[*].addresses[*].ip}')"
if [ "${#endpoint_ips[@]}" -ne 1 ]; then
  echo "::error::expected exactly one runners endpoint, got: ${endpoint_ips[*]:-none}" >&2
  exit 1
fi
pod_ip="${endpoint_ips[0]}"
kubectl -n "$ns" get pods -o wide --field-selector="status.podIP=${pod_ip}"
targets="runners.${ns}.svc.cluster.local:50051,${pod_ip}:50051"
echo "Probing ${targets}"

for p in orchestrator gateway intruder; do
  kubectl -n "$ns" exec -i "rpcauth-$p" -- \
    sh -c 'cat > /probe/rpcauthprobe && chmod 0755 /probe/rpcauthprobe' < "$probe"
done

run_matrix() {
  kubectl -n "$ns" exec -i "rpcauth-$1" -- /probe/rpcauthprobe -targets "$targets" < "$dir/matrix-$2.json"
}

for p in orchestrator gateway intruder; do
  echo "== $p =="
  run_matrix "$p" "$p"
done

# Fail closed: without its TokenReview permission runners must answer
# Unavailable, not allow. The E2E policy caches results for 5s.
echo "== fail-closed: removing ${binding} =="
kubectl delete clusterrolebinding "$binding"
sleep 20
fail_closed_status=0
run_matrix orchestrator fail-closed || fail_closed_status=$?
kubectl create clusterrolebinding "$binding" --clusterrole="$binding" \
  --serviceaccount="${ns}:${runners_sa}"
if [ "$fail_closed_status" -ne 0 ]; then
  echo "::error::runners did not fail closed without TokenReview permission" >&2
  exit 1
fi

echo "== recovery after restoring ${binding} =="
for attempt in $(seq 1 12); do
  if run_matrix orchestrator recovered; then
    break
  fi
  if [ "$attempt" -eq 12 ]; then
    echo "::error::runners did not recover after the binding was restored" >&2
    exit 1
  fi
  sleep 5
done

echo "Runners denial log sample:"
kubectl -n "$ns" logs -l app.kubernetes.io/name=runners --tail=400 | grep 'rpcauth:' | tail -40 || true
