#!/usr/bin/env bash
set -euo pipefail

role="${1:?usage: entrypoint.sh namenode|datanode|resourcemanager|nodemanager}"

case "${role}" in
  namenode)
    # Format only on first start so a restart keeps existing HDFS data.
    if [ ! -d /data/hdfs/name/current ]; then
      hdfs namenode -format -nonInteractive
    fi
    exec hdfs namenode
    ;;
  datanode) exec hdfs datanode ;;
  resourcemanager) exec yarn resourcemanager ;;
  nodemanager) exec yarn nodemanager ;;
  *)
    echo "unknown role: ${role}" >&2
    exit 1
    ;;
esac
