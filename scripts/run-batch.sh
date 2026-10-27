#!/bin/sh
set -eu
docker compose exec spark-stream /opt/spark/bin/spark-submit --master local[2] /app/spark/batch.py
