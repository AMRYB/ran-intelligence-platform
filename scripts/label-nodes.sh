#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 5 ]]; then
  echo "Usage: $0 <node-1> [node-2] [node-3] [node-4] [node-5]" >&2
  echo "Example: $0 zzzz-vm Victus sharawy-VMware-Virtual-Platform" >&2
  exit 1
fi

for ((index = 1; index <= $#; index++)); do
  node="${!index}"
  if ! docker node inspect "$node" >/dev/null 2>&1; then
    echo "Swarm node not found: $node" >&2
    echo "Use 'docker node ls' and pass the exact case-sensitive HOSTNAME or node ID." >&2
    exit 1
  fi
  docker node update --label-add "node_id=$index" "$node"
done
