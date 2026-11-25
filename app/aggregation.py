import json
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

import boto3
import psycopg
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException

from app.common import PG_DSN, ch, log, retry
from app.metrics import calculate

lock = threading.Lock()


@retry()
def save_postgres(metrics, cohorts):
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cursor:
            cursor.executemany('''
                INSERT INTO daily_metrics(date, metric_name, dimension, value, calculated_at)
                VALUES (%(date)s, %(metric_name)s, %(dimension)s, %(value)s, %(calculated_at)s)
                ON CONFLICT(date, metric_name, dimension) DO UPDATE
                SET value=EXCLUDED.value, calculated_at=EXCLUDED.calculated_at
            ''', metrics)
            cursor.executemany('''
                INSERT INTO retention_cohorts(cohort_date, day, cohort_size, retained_users, retention, calculated_at)
                VALUES (%(cohort_date)s, %(day)s, %(cohort_size)s, %(retained_users)s, %(retention)s, %(calculated_at)s)
                ON CONFLICT(cohort_date, day) DO UPDATE SET
                    cohort_size=EXCLUDED.cohort_size, retained_users=EXCLUDED.retained_users,
                    retention=EXCLUDED.retention, calculated_at=EXCLUDED.calculated_at
            ''', cohorts)


@retry()
def materialize(metrics, cohorts):
    ch('INSERT INTO cinema.daily_metrics', rows=metrics)
    ch('INSERT INTO cinema.retention_cohorts', rows=cohorts)


def aggregate(day):
    with lock:
        started = time.monotonic()
        log.info('Цикл начат: date=%s', day)
        metrics, cohorts, processed = calculate(day)
        calculated_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        for row in metrics + cohorts:
            row['calculated_at'] = calculated_at
        materialize(metrics, cohorts)
        save_postgres(metrics, cohorts)
        duration = time.monotonic() - started
        log.info('Цикл завершён, обработано %s записей за %.3f сек; date=%s', processed, duration, day)
        return {'date': day, 'processed_records': processed, 'duration_seconds': duration, 'metrics': len(metrics)}


def s3_client():
    return boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://minio:9000'),
                        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'cinema-minio-admin-2026'),
                        region_name='us-east-1')


@retry(attempts=3)
def export(day):
    with psycopg.connect(PG_DSN, row_factory=psycopg.rows.dict_row) as conn:
        # One consistent snapshot for both tables.
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        metrics = conn.execute('SELECT * FROM daily_metrics WHERE date=%s ORDER BY metric_name, dimension', (day,)).fetchall()
        cohorts = conn.execute('SELECT * FROM retention_cohorts WHERE cohort_date=%s ORDER BY day', (day,)).fetchall()
    if not metrics:
        raise ValueError(f'No aggregates for {day}; recalculate first')
    client = s3_client()
    bucket = os.getenv('S3_BUCKET', 'movie-analytics')
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('404', 'NoSuchBucket', 'NotFound'):
            raise
        client.create_bucket(Bucket=bucket)
    key = f'daily/{day.isoformat()}/aggregates.json'
    payload = json.dumps({'date': day.isoformat(), 'metrics': metrics, 'retention_cohorts': cohorts}, default=str, ensure_ascii=False, allow_nan=False)
    client.put_object(Bucket=bucket, Key=key, Body=payload.encode(), ContentType='application/json')
    log.info('Exported date=%s uri=s3://%s/%s', day, bucket, key)
    return {'date': day, 'uri': f's3://{bucket}/{key}', 'metrics': len(metrics)}


def scheduled_cycle():
    today = datetime.now(timezone.utc).date()
    # Revisit cohorts for late arrivals and maturation of D7.
    for offset in range(int(os.getenv('RECALCULATE_DAYS', '9'))):
        day = today - timedelta(days=offset)
        try:
            aggregate(day)
        except Exception:
            log.exception('Aggregation failed date=%s; retry next cycle', day)
            continue
        if day < today:
            try:
                export(day)
            except Exception:
                log.exception('Export failed date=%s; retry next cycle', day)


def schedule(stop):
    interval = float(os.getenv('AGGREGATION_INTERVAL_SECONDS', '60'))
    if stop.wait(10):
        return
    while not stop.is_set():
        try:
            scheduled_cycle()
        except Exception:
            log.exception('Scheduled cycle failed; retry next cycle')
        stop.wait(interval)


@asynccontextmanager
async def lifespan(app):
    stop = threading.Event()
    thread = threading.Thread(target=schedule, args=(stop,), daemon=True)
    thread.start()
    yield
    stop.set()
    thread.join(timeout=5)


app = FastAPI(title='Aggregation Service', lifespan=lifespan)


@app.get('/health')
def health():
    try:
        ch('SELECT 1')
        with psycopg.connect(PG_DSN, connect_timeout=3) as conn:
            conn.execute('SELECT 1')
        return {'status': 'ok'}
    except Exception as exc:
        raise HTTPException(503, 'Database unavailable') from exc


@app.post('/aggregate')
def manual_aggregate(date: date):
    try:
        return aggregate(date)
    except Exception as exc:
        log.exception('Manual aggregation failed date=%s', date)
        raise HTTPException(503, 'Aggregation failed; see logs') from exc


@app.post('/export')
def manual_export(date: date):
    try:
        return export(date)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        log.exception('Manual export failed date=%s', date)
        raise HTTPException(503, 'Export failed; see logs') from exc
