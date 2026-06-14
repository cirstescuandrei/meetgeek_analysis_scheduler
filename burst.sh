#!/usr/bin/env bash
COUNT="${1:?usage: $0 <count>}"
URL="http://$(minikube ip):$(kubectl get svc temporal-client -n meetgeek -o jsonpath='{.spec.ports[0].nodePort}')"
for i in $(seq 1 "$COUNT"); do
  curl -s -o /dev/null -X POST "$URL/analyze" -H "Content-Type: application/json" -d '{}' &
done
wait
echo "$COUNT workflows started"
