#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$(readlink -f "$0")")"

NAMESPACE=meetgeek

SERVICES=(
  "meetgeek-temporal-worker|implementations/temporal/worker/Dockerfile|temporal-worker"
  "meetgeek-temporal-client|implementations/temporal/client/Dockerfile|temporal-client"
  "meetgeek-celery-worker|implementations/celery/worker/Dockerfile|celery-worker"
  "meetgeek-celery-client|implementations/celery/client/Dockerfile|celery-client"
)

selected=()
if [ "$#" -eq 0 ]; then
  selected=("${SERVICES[@]}")
else
  for arg in "$@"; do
    match=""
    for entry in "${SERVICES[@]}"; do
      IFS='|' read -r image dockerfile deployment <<< "$entry"
      if [ "$arg" = "$deployment" ] || [ "$arg" = "$image" ]; then
        match="$entry"
        break
      fi
    done
    if [ -z "$match" ]; then
      echo "unknown service: $arg" >&2
      exit 1
    fi
    selected+=("$match")
  done
fi

for entry in "${selected[@]}"; do
  IFS='|' read -r image dockerfile deployment <<< "$entry"
  echo ">> building $image:latest"
  docker build -f "$dockerfile" -t "$image:latest" .
  echo ">> loading $image:latest into minikube"
  docker save "$image:latest" | docker exec -i minikube docker load
  echo ">> restarting deployment/$deployment"
  kubectl rollout restart "deployment/$deployment" -n "$NAMESPACE"
done

for entry in "${selected[@]}"; do
  IFS='|' read -r image dockerfile deployment <<< "$entry"
  kubectl rollout status "deployment/$deployment" -n "$NAMESPACE" --timeout=180s
done
