#!/bin/bash

# DESCRIPTION
# ===========
# This script removes all tmp folders generated during ITs launch with native brokers

# USAGE
# =====
#   broker-cleanup.sh broker_dir

broker_dir=""

if [ -n "$1" ]; then
  broker_dir=$1
  echo "Using broker dir '$broker_dir' from the script argument"
else
  echo "No broker_dir was provided"

  cmd=`basename "$0"`
  echo "Script usage:"
  echo -e "\t$cmd broker_dir"

  exit 1
fi

cd "$broker_dir"

rm -rf localBMQ_*
# With a static broker config we are not able to specify different dir for logs/storage
# That's why it's being stores in a default dir
rm -rf localBMQ/logs/*
rm -rf localBMQ/storage/local/bmq*
