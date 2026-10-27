# Complete Log Analytics — Lambda Architecture (VS Code project)

This project follows your screenshot's architecture: **log ingestion (NiFi) → Kafka → Spark speed layer → Cassandra serving layer → Plotly Dash analytics**, plus **HDFS + Spark batch (cold path)**, Docker and optional AWS CLI/Terraform exercise.

**Important scope:** This is an independently written educational implementation of the diagram, not the original course's code. The Kafka-to-Spark-to-Cassandra dashboard path is wired in code; NiFi uses a separate optional profile with a best-effort REST setup script. A manual NiFi UI check/start is required. The AWS Terraform example only creates a private S3 bucket, not a full AWS deployment.

## Prerequisites
- VS Code, Docker Desktop with Compose, Python 3.11+ for optional local producer
- **12–16 GB RAM recommended**; Kafka + Spark + NiFi + HDFS + Cassandra are resource-heavy
- Internet on first launch for Docker images, Python packages and Spark Kafka connector

## Folder structure
```
complete_log_analytics_lambda_project/
├── docker-compose.yml
├── cassandra/schema.cql
├── producer/{generate.py,Dockerfile,requirements.txt}
├── spark/{common.py,streaming.py,batch.py,Dockerfile}
├── dashboard/{app.py,Dockerfile}
├── nifi/bootstrap.py
├── terraform/aws/{main.tf,README.md}
├── scripts/{init.sh,run-batch.sh}
└── logs/
```

## QUICK START (PowerShell or VS Code terminal)

### Step 1: Start core infrastructure
```bash
docker compose up -d kafka cassandra namenode datanode
```
Wait 1–3 minutes for Cassandra. Verify:
```bash
docker compose ps
```

### Step 2: Create Cassandra schema
```bash
docker compose exec cassandra cqlsh -f /schema.cql
```
If Cassandra is still starting, retry after it becomes healthy.

### Step 3: Prepare HDFS directories
```bash
docker compose exec namenode hdfs dfs -mkdir -p /data/logs /checkpoints
docker compose exec namenode hdfs dfs -chmod -R 777 /data /checkpoints
```
`777` is for **local demo only**, never production. Check HDFS at http://localhost:9870.

### Step 4: Start Spark and Dashboard
```bash
docker compose up -d --build spark-stream dashboard
```
Spark downloads the Kafka connector package on first run. Monitor:
```bash
docker compose logs -f spark-stream
```

### Step 5A: Easy demo — send generated logs directly to Kafka
```bash
docker compose --profile demo up -d --build producer
```
This path validates the streaming architecture without NiFi setup.

### Step 5B: Full NiFi path — ingest actual log file
**Do not run both producers for the same test unless you want duplicate events.**

Start NiFi:
```bash
docker compose --profile nifi up -d nifi
```
Open http://localhost:8080/nifi. Create a `TailFile` processor reading `/shared/application.log`, connect to a `PublishKafka_2_6` processor (or the available Kafka publish processor), configure broker `kafka:9092`, topic `application-logs`, connect success relationship and start both processors. The optional helper tries to create these processors:
```bash
docker compose --profile nifi run --rm nifi-setup
```
NiFi versions may expose different property names. Inspect any invalid processors in the UI and correct their settings before starting. To generate the log file, install producer requirements on your host and run:
```bash
python -m pip install -r producer/requirements.txt
python producer/generate.py --mode file --count 100
```
NiFi reads JSONL lines from the shared `logs/application.log` file. The file must exist before starting the TailFile processor.

### Step 6: View dashboard
Visit **http://localhost:8050** for Plotly Dash charts. Cassandra tables: `events_by_service`, `metrics_by_window`, `batch_summary`. The live metrics represent **per-microbatch counts**, not true event-time windows; dashboard sums are a demonstration, not production alerting metrics.

### Step 7: Run historical Spark batch (cold path)
Once the streaming job has archived data to HDFS:
```bash
docker compose exec spark-stream /opt/spark/bin/spark-submit --master local[2] /app/spark/batch.py
```
Reload dashboard to view historical daily summary. Cold-path aggregation is manually triggered; schedule it externally if needed.

### Step 8: Inspect data
```bash
docker compose exec cassandra cqlsh
```
```sql
USE log_analytics;
SELECT * FROM metrics_by_window LIMIT 20;
SELECT * FROM batch_summary LIMIT 20;
SELECT * FROM events_by_service LIMIT 20;
```

### Step 9: Stop
```bash
docker compose --profile demo --profile nifi down
```
To **erase all stored data**, optionally run `docker compose --profile demo --profile nifi down -v` (destructive).

## Technology mapping
| Screenshot component | This project |
|---|---|
| Log collection | `producer/generate.py` (JSONL) |
| NiFi ingestion | NiFi Docker profile, UI flow and optional bootstrap |
| Kafka | `application-logs` topic |
| Speed/hot path | `spark/streaming.py` |
| Cold/batch path | HDFS Parquet + `spark/batch.py` |
| Serving layer | Cassandra |
| Analytics | Plotly Dash at localhost:8050 |
| Docker | `docker-compose.yml` |
| AWS CLI / Terraform | Optional S3 example in `terraform/aws/` |

## Known limitations
- **Not end-to-end tested here:** Docker is not available in the build environment; container images and runtime compatibility require verification on your machine.
- NiFi REST bootstrap is best-effort; manual flow setup may be needed.
- Spark writes each micro-batch to HDFS and Cassandra; HDFS is not an independent Kafka batch consumer. This is a simplified Lambda-style dual path.
- Spark's Python Cassandra sink is an educational example; it is not optimized for massive throughput. Cassandra writes are keyed for repeatability, but end-to-end exactly-once delivery is not guaranteed.
- Dashboard table scans are limited to 1000 rows; use purpose-built query tables and pagination for production.
- Kafka and NiFi are configured for **local development**, without TLS or access control.
- The Terraform folder is an optional AWS exercise and can create billable resources.

## Debugging
```bash
docker compose logs kafka
docker compose logs cassandra
docker compose logs spark-stream
docker compose logs dashboard
docker compose ps
```
If Spark reports HDFS permission denied, repeat Step 3. If Spark cannot connect to Kafka, wait for broker health. If the dashboard says schema missing, repeat Step 2. If a connector dependency fails to download, check network access and Spark package version.
