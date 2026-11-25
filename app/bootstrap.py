"""Idempotent infrastructure initialization; services wait for successful completion."""
import json
import os
from pathlib import Path

import psycopg
import requests
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from confluent_kafka import KafkaException

from app.common import PG_DSN, ch, log, retry


@retry(attempts=12)
def initialize():
    admin = AdminClient({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:9092,kafka-2:9092,kafka-3:9092')})
    futures = admin.create_topics([NewTopic('movie-events', num_partitions=3, replication_factor=2,
                                           config={'min.insync.replicas': '1', 'retention.ms': '604800000'})])
    try:
        futures['movie-events'].result(30)
    except KafkaException as exc:
        if exc.args[0].name() != 'TOPIC_ALREADY_EXISTS':
            raise
    topic = admin.list_topics('movie-events', timeout=30).topics['movie-events']
    assert len(topic.partitions) >= 3
    assert all(len(p.replicas) >= 2 for p in topic.partitions.values())
    configs = admin.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, 'movie-events')])
    assert next(iter(configs.values())).result(30)['min.insync.replicas'].value == '1'
    registry = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
    r = requests.put(f'{registry}/config/movie-events-value', json={'compatibility': 'BACKWARD'}, timeout=20)
    r.raise_for_status()
    schema = Path('schemas/movie-event-v1.avsc').read_text()
    r = requests.post(f'{registry}/subjects/movie-events-value/versions', json={'schemaType': 'AVRO', 'schema': schema}, timeout=20)
    r.raise_for_status()
    version = requests.post(f'{registry}/subjects/movie-events-value', json={'schema': schema}, timeout=20)
    version.raise_for_status()
    log.info('Schema registered: subject=movie-events-value version=%s id=%s', version.json()['version'], r.json()['id'])
    with psycopg.connect(PG_DSN) as conn:
        conn.execute(Path('migrations/postgres/001.sql').read_text())
    for statement in Path('migrations/clickhouse/001.sql').read_text().split(';'):
        if statement.strip():
            ch(statement)
    Path('/tmp/initialized').touch()
    log.info('Initialization completed')


if __name__ == '__main__':
    initialize()
