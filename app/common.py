import logging
import os
import time
from functools import wraps

import requests

logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s %(levelname)s %(name)s %(message)s')
log = logging.getLogger('cinema')
CH_URL = os.getenv('CLICKHOUSE_URL', 'http://clickhouse:8123')
PG_DSN = os.getenv('PG_DSN', 'postgresql://cinema:cinema@postgres:5432/cinema')


def retry(attempts=5, initial=0.5):
    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            for attempt in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except Exception:
                    log.exception('%s failed, attempt=%s/%s', fn.__name__, attempt + 1, attempts)
                    if attempt == attempts - 1:
                        raise
                    time.sleep(min(initial * 2 ** attempt, 8))
        return wrapped
    return decorate


def ch(sql, params=None, rows=None):
    query = dict(params or {})
    query.update(user=os.getenv('CLICKHOUSE_USER', 'cinema'), password=os.getenv('CLICKHOUSE_PASSWORD', 'cinema'))
    if rows is None:
        data = sql.encode()
    else:
        import json
        query['query'] = sql + ' FORMAT JSONEachRow'
        data = '\n'.join(json.dumps(row) for row in rows).encode()
    response = requests.post(CH_URL, params=query, data=data, timeout=120)
    response.raise_for_status()
    return response.text


def ch_rows(sql, params=None):
    import json
    return [json.loads(line) for line in ch(sql + ' FORMAT JSONEachRow', params).splitlines() if line]
