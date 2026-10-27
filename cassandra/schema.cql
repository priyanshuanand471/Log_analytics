CREATE KEYSPACE IF NOT EXISTS log_analytics WITH replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS log_analytics.events_by_service (
 service text, day date, event_time timestamp, event_id text,
 level text, message text, host text,
 PRIMARY KEY ((service,day),event_time,event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);
CREATE TABLE IF NOT EXISTS log_analytics.metrics_by_window (
 bucket text, service text, level text, count bigint,
 PRIMARY KEY ((bucket),service,level)
);
CREATE TABLE IF NOT EXISTS log_analytics.batch_summary (
 day text, service text, level text, count bigint,
 PRIMARY KEY ((day),service,level)
);
