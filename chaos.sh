#!/usr/bin/env bash
TARGET="${1:?usage: $0 <app-label> [interval_sec]}"
INTERVAL="${2:-15}"
while true; do
  pods=($(kubectl get pods -n meetgeek -l "app=$TARGET" -o name 2>/dev/null | cut -d/ -f2))
  if [ "${#pods[@]}" -gt 0 ]; then
    victim="${pods[RANDOM % ${#pods[@]}]}"
    echo "[$(date +%T)] killing $victim"
    kubectl delete pod "$victim" -n meetgeek --grace-period=0 --force >/dev/null 2>&1
  fi
  sleep "$INTERVAL"
done
