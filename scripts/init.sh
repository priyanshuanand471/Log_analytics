#!/bin/sh
set -eu
docker compose up -d kafka cassandra namenode datanode
printf 'Wait until Cassandra is healthy, then run: docker compose exec cassandra cqlsh -f /schema.cql\n'
printf 'Prepare HDFS: docker compose exec namenode hdfs dfs -mkdir -p /data/logs /checkpoints\n'
printf 'Then: docker compose exec namenode hdfs dfs -chmod -R 777 /data /checkpoints\n'
