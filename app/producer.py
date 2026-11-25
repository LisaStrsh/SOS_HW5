import io
import json
import os
import random
import struct
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from uuid import UUID, uuid4

import requests
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from fastavro import parse_schema, schemaless_writer
from fastavro.validation import validate
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.common import log, retry


class EventType(str, Enum):
    VIEW_STARTED = 'VIEW_STARTED'
    VIEW_FINISHED = 'VIEW_FINISHED'
    VIEW_PAUSED = 'VIEW_PAUSED'
    VIEW_RESUMED = 'VIEW_RESUMED'
    LIKED = 'LIKED'
    SEARCHED = 'SEARCHED'


class DeviceType(str, Enum):
    MOBILE = 'MOBILE'
    DESKTOP = 'DESKTOP'
    TV = 'TV'
    TABLET = 'TABLET'


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra='forbid')
    event_id: UUID
    user_id: str = Field(min_length=1, max_length=200)
    movie_id: str = Field(min_length=1, max_length=200)
    event_type: EventType
    timestamp: datetime
    device_type: DeviceType
    session_id: str = Field(min_length=1, max_length=200)
    progress_seconds: int = Field(ge=0, le=2147483647, strict=True)

    @field_validator('timestamp')
    @classmethod
    def utc_timestamp(cls, value):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError('timestamp must include a timezone')
        return value.astimezone(timezone.utc)


class Publisher:
    def __init__(self):
        self.schema = parse_schema(json.loads(Path('schemas/movie-event-v1.avsc').read_text()))
        registry = os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
        # Lookup exact schema, so a later registry version cannot silently change the wire format.
        r = requests.post(f'{registry}/subjects/movie-events-value', json={
            'schema': Path('schemas/movie-event-v1.avsc').read_text()}, timeout=20)
        r.raise_for_status()
        self.schema_id = r.json()['id']
        self.producer = Producer({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:9092,kafka-2:9092,kafka-3:9092'),
            'enable.idempotence': True, 'acks': 'all', 'retries': 2147483647,
            'retry.backoff.ms': 200, 'retry.backoff.max.ms': 5000,
            'delivery.timeout.ms': 20000,
        })

    @retry(attempts=3)
    def publish(self, event):
        record = event.model_dump(mode='json')
        record['timestamp'] = int(event.timestamp.timestamp() * 1000)
        validate(record, self.schema)
        buffer = io.BytesIO()
        buffer.write(struct.pack('>bI', 0, self.schema_id))
        schemaless_writer(buffer, self.schema, record)
        completed = threading.Event()
        errors = []

        def delivered(error, message):
            if error:
                errors.append(str(error))
            completed.set()

        self.producer.produce('movie-events', key=event.user_id.encode(), value=buffer.getvalue(), on_delivery=delivered)
        while not completed.is_set():
            self.producer.poll(0.1)
        if errors:
            raise RuntimeError(errors[0])
        log.info('Published event_id=%s event_type=%s timestamp=%s', event.event_id, event.event_type.value, event.timestamp.isoformat())
        return str(event.event_id)


def session_events(user, movie, start, rng, complete=True):
    session = str(uuid4())
    device = rng.choice(list(DeviceType))
    sequence = [('SEARCHED', 0, 0), ('VIEW_STARTED', 0, 5)]
    if rng.random() < 0.6:
        sequence += [('VIEW_PAUSED', 300, 305), ('VIEW_RESUMED', 300, 365)]
    if complete:
        sequence += [('VIEW_FINISHED', 3600, 3665)]
        if rng.random() < 0.5:
            sequence += [('LIKED', 3600, 3670)]
    return [MovieEvent(event_id=uuid4(), user_id=user, movie_id=movie, event_type=kind,
                       timestamp=start + timedelta(seconds=offset), device_type=device,
                       session_id=session, progress_seconds=progress) for kind, progress, offset in sequence]


def generate(publisher, stop):
    rng = random.Random(42)
    try:
        if os.getenv('DEMO_SEED', 'true').lower() == 'true':
            # Fixed user IDs make first-view cohorts meaningful across generator restarts.
            today = datetime.now(timezone.utc).replace(hour=9, minute=0, second=0, microsecond=0)
            for ago in range(8, 0, -1):
                for i in range(12):
                    if ago == 8 or rng.random() < 0.65:
                        for event in session_events(f'demo-{i}', f'movie-{i % 5}', today - timedelta(days=ago), rng, rng.random() < 0.8):
                            if stop.is_set():
                                return
                            publisher.publish(event)
        while not stop.is_set():
            # Emit in real time; a compressed demo minute still preserves timestamp/progress ordering.
            start = datetime.now(timezone.utc) - timedelta(seconds=3670)
            for event in session_events(f'demo-{rng.randrange(30)}', f'movie-{rng.randrange(8)}', start, rng, rng.random() < 0.8):
                if stop.is_set():
                    return
                publisher.publish(event)
            stop.wait(float(os.getenv('GENERATOR_INTERVAL_SECONDS', '5')))
    except Exception:
        log.exception('Generator failed; container will be restarted')
        os._exit(1)


publisher = None


@asynccontextmanager
async def lifespan(app):
    global publisher
    publisher = Publisher()
    stop = threading.Event()
    thread = threading.Thread(target=generate, args=(publisher, stop), daemon=True)
    if os.getenv('GENERATE_EVENTS', 'true').lower() == 'true':
        thread.start()
    yield
    stop.set()
    if thread.is_alive():
        thread.join(timeout=25)
    publisher.producer.flush(5)


app = FastAPI(title='Movie Service', lifespan=lifespan)


@app.get('/health')
def health():
    try:
        metadata = publisher.producer.list_topics('movie-events', timeout=5)
        if metadata.topics['movie-events'].error:
            raise RuntimeError('movie-events unavailable')
        return {'status': 'ok'}
    except Exception as exc:
        raise HTTPException(503, 'Kafka unavailable') from exc


@app.post('/events', status_code=201)
def events(event: MovieEvent):
    try:
        return {'event_id': publisher.publish(event)}
    except Exception as exc:
        log.exception('Publication failed event_id=%s', event.event_id)
        raise HTTPException(503, 'Kafka publication failed') from exc
