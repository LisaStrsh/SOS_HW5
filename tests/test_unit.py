import io
import json
import random
import struct
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from pydantic import ValidationError
from app.producer import MovieEvent, Publisher, session_events


def sample():
    return dict(event_id=str(uuid4()), user_id='user', movie_id='movie', event_type='VIEW_STARTED',
                timestamp='2026-01-01T13:00:00+03:00', device_type='TV', session_id='session', progress_seconds=0)


@pytest.mark.parametrize('field,value', [('progress_seconds', -1), ('progress_seconds', 1.5),
    ('event_type', 'UNKNOWN'), ('device_type', 'WATCH'), ('event_id', 'bad'),
    ('timestamp', '2026-01-01T00:00:00'), ('user_id', '')])
def test_validation(field, value):
    data = sample()
    data[field] = value
    with pytest.raises(ValidationError):
        MovieEvent(**data)


def test_utc_and_avro_roundtrip():
    event = MovieEvent(**sample())
    assert event.timestamp.hour == 10
    schema = parse_schema(json.loads(Path('schemas/movie-event-v1.avsc').read_text()))
    record = event.model_dump(mode='json')
    record['timestamp'] = int(event.timestamp.timestamp() * 1000)
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    buf.seek(0)
    restored = schemaless_reader(buf, schema)
    assert str(restored['event_id']) == str(event.event_id)
    assert restored['timestamp'] == event.timestamp


def test_generator_sequences():
    rng = random.Random(7)
    for complete in (True, False):
        for _ in range(30):
            events = session_events('u', 'm', datetime.now(timezone.utc), rng, complete)
            kinds = [event.event_type.value for event in events]
            assert len({e.session_id for e in events}) == 1
            assert kinds.index('VIEW_STARTED') < kinds.index('VIEW_FINISHED') if complete else 'VIEW_FINISHED' not in kinds
            assert [e.timestamp for e in events] == sorted(e.timestamp for e in events)
            assert [e.progress_seconds for e in events] == sorted(e.progress_seconds for e in events)
            if 'VIEW_PAUSED' in kinds:
                assert kinds.index('VIEW_PAUSED') < kinds.index('VIEW_RESUMED')


def test_publisher_waits_for_ack_and_uses_user_key():
    event = MovieEvent(**sample())
    class FakeProducer:
        def produce(self, topic, **kwargs):
            assert topic == 'movie-events'
            assert kwargs['key'] == b'user'
            assert kwargs['value'][:5] == struct.pack('>bI', 0, 42)
            self.callback = kwargs['on_delivery']
        def poll(self, timeout):
            self.callback(None, object())
    publisher = Publisher.__new__(Publisher)
    publisher.schema = parse_schema(json.loads(Path('schemas/movie-event-v1.avsc').read_text()))
    publisher.schema_id = 42
    publisher.producer = FakeProducer()
    assert publisher.publish(event) == str(event.event_id)
