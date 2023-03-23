#!/bin/bash

# DESCRIPTION
# ===========
# This script removes all containers which haven't been stopped and removed
# properly during execution of integrations tests

# USAGE
# =====
#   docker-cleanup.sh

docker --version
docker ps -a | grep bmq-broker-java-it | awk '{print $NF}' | xargs --no-run-if-empty docker rm -f
