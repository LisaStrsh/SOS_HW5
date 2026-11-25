CREATE DATABASE IF NOT EXISTS cinema;
CREATE TABLE IF NOT EXISTS cinema.raw_events (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED'=1,'VIEW_FINISHED'=2,'VIEW_PAUSED'=3,'VIEW_RESUMED'=4,'LIKED'=5,'SEARCHED'=6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE'=1,'DESKTOP'=2,'TV'=3,'TABLET'=4),
    session_id String,
    progress_seconds Int32
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), event_id);
CREATE TABLE IF NOT EXISTS cinema.kafka_events (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED'=1,'VIEW_FINISHED'=2,'VIEW_PAUSED'=3,'VIEW_RESUMED'=4,'LIKED'=5,'SEARCHED'=6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE'=1,'DESKTOP'=2,'TV'=3,'TABLET'=4),
    session_id String,
    progress_seconds Int32
) ENGINE = Kafka
SETTINGS kafka_broker_list='kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list='movie-events', kafka_group_name='clickhouse-movie-events-v1',
    kafka_format='AvroConfluent', kafka_num_consumers=3, kafka_thread_per_consumer=1,
    kafka_flush_interval_ms=1000,
    format_avro_schema_registry_url='http://schema-registry:8081';
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.ingest_events TO cinema.raw_events AS
SELECT * FROM cinema.kafka_events;
CREATE TABLE IF NOT EXISTS cinema.daily_metrics (
    date Date,
    metric_name LowCardinality(String),
    dimension String,
    value Nullable(Float64),
    calculated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (date, metric_name, dimension);
CREATE TABLE IF NOT EXISTS cinema.retention_cohorts (
    cohort_date Date,
    day UInt8,
    cohort_size UInt64,
    retained_users Nullable(UInt64),
    retention Nullable(Float64),
    calculated_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (cohort_date, day);
