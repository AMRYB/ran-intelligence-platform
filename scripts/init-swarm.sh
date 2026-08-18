#!/usr/bin/env bash
set -euo pipefail

docker swarm init --advertise-addr 100.94.25.100 || true
