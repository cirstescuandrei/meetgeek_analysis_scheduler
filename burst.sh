#!/usr/bin/env bash
COUNT="${1:?usage: $0 <count> [temporal|celery]}"
TARGET="${2:-temporal}"
URL="http://$(minikube ip):$(kubectl get svc "${TARGET}-client" -n meetgeek -o jsonpath='{.spec.ports[0].nodePort}')"
for i in $(seq 1 "$COUNT"); do
  body=$(printf '{"title":"burst-%d","size":52428800,"should_fail":false}' "$i")
  curl -s -o /dev/null -X POST "$URL/analyze" -H "Content-Type: application/json" -d "$body" &
done
wait
echo "$COUNT $TARGET workflows started"
