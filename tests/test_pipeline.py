"""Run inside compose test profile; UUIDs + an unused historical date isolate each run."""
import json
import random
import time
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import psycopg
import pytest
import requests
from confluent_kafka.admin import AdminClient, ConfigResource

from app.aggregation import s3_client
from app.common import PG_DSN, ch_rows

PRODUCER = 'http://producer:8000'
AGGREGATION = 'http://aggregation:8001'


def wait_for(fn, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(0.5)
    raise AssertionError('Timed out waiting for Kafka → ClickHouse ingestion')


def post_event(event):
    r = requests.post(f'{PRODUCER}/events', json=event, timeout=90)
    assert r.status_code == 201, r.text
    assert r.json()['event_id'] == event['event_id']


@pytest.fixture(scope='module')
def dataset():
    rng = random.SystemRandom()
    for _ in range(100):
        day = date(1971, 1, 1) + timedelta(days=rng.randrange(18000))
        rows = ch_rows('SELECT count() AS n FROM cinema.raw_events WHERE toDate(timestamp) BETWEEN {day:Date} AND addDays({day:Date}, 7)', {'param_day': str(day)})
        if int(rows[0]['n']) == 0:
            break
    else:
        raise AssertionError('No isolated fixture date')
    prefix = str(uuid4())
    events = []
    def session(user, offset, finished, duration):
        sid = str(uuid4())
        start = datetime.combine(day + timedelta(days=offset), datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=10)
        for kind, progress in [('VIEW_STARTED', 0)] + ([('VIEW_FINISHED', duration)] if finished else [('VIEW_PAUSED', 300), ('VIEW_RESUMED', 300)]):
            event = dict(event_id=str(uuid4()), user_id=f'{prefix}-{user}', movie_id=f'{prefix}-{user}',
                         event_type=kind, timestamp=(start + timedelta(seconds=progress)).isoformat(),
                         device_type='TV', session_id=sid, progress_seconds=progress)
            events.append(event)
            post_event(event)
    session('a', 0, True, 1800)
    session('b', 0, False, 0)
    session('a', 1, True, 2400)
    session('b', 7, True, 3000)
    ids = [event['event_id'] for event in events]
    params = {f'param_id{i}': value for i, value in enumerate(ids)}
    placeholders = ','.join('{id'+str(i)+':UUID}' for i in range(len(ids)))
    wait_for(lambda: len(ch_rows(f'SELECT event_id FROM cinema.raw_events FINAL WHERE event_id IN ({placeholders})', params)) == len(ids))
    return day, events


def test_schema_topic_and_replication():
    versions = requests.get('http://schema-registry:8081/subjects/movie-events-value/versions', timeout=10)
    versions.raise_for_status()
    assert 1 in versions.json()
    schema = requests.get('http://schema-registry:8081/subjects/movie-events-value/versions/1', timeout=10).json()
    assert json.loads(schema['schema'])['name'] == 'MovieEvent'
    admin = AdminClient({'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092'})
    partitions = admin.list_topics('movie-events', timeout=15).topics['movie-events'].partitions
    assert len(partitions) >= 3
    assert all(len(p.replicas) >= 2 for p in partitions.values())
    result = admin.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, 'movie-events')])
    assert next(iter(result.values())).result(15)['min.insync.replicas'].value == '1'


def test_validation_api():
    response = requests.post(f'{PRODUCER}/events', json={'event_id': 'invalid'}, timeout=10)
    assert response.status_code == 422


def test_pipeline_fields_and_deduplication(dataset):
    _, events = dataset
    event = events[0]
    rows = ch_rows('SELECT *, toUnixTimestamp64Milli(timestamp) AS millis FROM cinema.raw_events FINAL WHERE event_id={id:UUID}', {'param_id': event['event_id']})
    assert len(rows) == 1
    for field in ('event_id', 'user_id', 'movie_id', 'event_type', 'device_type', 'session_id', 'progress_seconds'):
        assert rows[0][field] == event[field]
    assert int(rows[0]['millis']) == int(datetime.fromisoformat(event['timestamp']).timestamp() * 1000)
    before = int(ch_rows('SELECT count() AS n FROM cinema.raw_events WHERE event_id={id:UUID}', {'param_id': event['event_id']})[0]['n'])
    post_event(event)
    # A later marker with the same user key proves the preceding duplicate was consumed.
    marker = dict(event, event_id=str(uuid4()), event_type='SEARCHED',
                  timestamp=datetime.now(timezone.utc).isoformat())
    post_event(marker)
    wait_for(lambda: ch_rows('SELECT event_id FROM cinema.raw_events FINAL WHERE event_id={id:UUID}', {'param_id': marker['event_id']}))
    assert int(ch_rows('SELECT count() AS n FROM cinema.raw_events FINAL WHERE event_id={id:UUID}', {'param_id': event['event_id']})[0]['n']) == 1
    assert before >= 1


def test_metrics_cohorts_and_idempotence(dataset):
    day, events = dataset
    for _ in range(2):
        result = requests.post(f'{AGGREGATION}/aggregate', params={'date': str(day)}, timeout=120)
        assert result.status_code == 200, result.text
        assert result.json()['processed_records'] == 5
    with psycopg.connect(PG_DSN, row_factory=psycopg.rows.dict_row) as conn:
        rows = conn.execute('SELECT * FROM daily_metrics WHERE date=%s', (day,)).fetchall()
        cohorts = conn.execute('SELECT * FROM retention_cohorts WHERE cohort_date=%s ORDER BY day', (day,)).fetchall()
    metrics = {r['metric_name']: r['value'] for r in rows if not r['dimension']}
    assert metrics == {'dau': 2, 'avg_watch_seconds': 1800, 'view_started': 2, 'view_finished': 1, 'conversion': .5, 'retention_d1': .5, 'retention_d7': .5}
    assert len(rows) == 9
    assert len(cohorts) == 8
    assert [r['retention'] for r in cohorts] == [1, .5, 0, 0, 0, 0, 0, .5]
    assert all(r['cohort_size'] == 2 for r in cohorts)
    materialized = ch_rows('SELECT metric_name, dimension, value FROM cinema.daily_metrics FINAL WHERE date={day:Date}', {'param_day': str(day)})
    assert len(materialized) == 9
    assert {r['metric_name']: r['value'] for r in materialized if not r['dimension']} == metrics


def test_export_overwrites_object(dataset):
    day, _ = dataset
    r = requests.post(f'{AGGREGATION}/aggregate', params={'date': str(day)}, timeout=120)
    assert r.status_code == 200, r.text
    for _ in range(2):
        r = requests.post(f'{AGGREGATION}/export', params={'date': str(day)}, timeout=90)
        assert r.status_code == 200, r.text
    client = s3_client()
    prefix = f'daily/{day}/'
    objects = client.list_objects_v2(Bucket='movie-analytics', Prefix=prefix)['Contents']
    assert len(objects) == 1
    assert objects[0]['Key'] == prefix + 'aggregates.json'
    body = json.loads(client.get_object(Bucket='movie-analytics', Key=objects[0]['Key'])['Body'].read())
    assert body['date'] == str(day)
    assert len(body['metrics']) == 9
    assert len(body['retention_cohorts']) == 8
    assert all(row['calculated_at'] for row in body['metrics'])


def test_grafana_provisioning_and_queries():
    session = requests.Session()
    session.auth = ('admin', 'cinema-admin')
    r = session.get('http://grafana:3000/api/dashboards/uid/movie-analytics', timeout=20)
    assert r.status_code == 200, r.text
    panels = r.json()['dashboard']['panels']
    assert len(panels) >= 3
    for panel in panels:
        target = dict(panel['targets'][0])
        target.update(datasource={'uid': 'clickhouse', 'type': 'grafana-clickhouse-datasource'}, intervalMs=60000, maxDataPoints=1000)
        r = session.post('http://grafana:3000/api/ds/query', json={'queries': [target], 'from': 'now-14d', 'to': 'now'}, timeout=60)
        assert r.status_code == 200, (panel['title'], r.text)
        result = r.json()['results']['A']
        assert not result.get('error'), (panel['title'], result)
