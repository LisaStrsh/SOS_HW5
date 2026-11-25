"""ClickHouse aggregate queries. FINAL removes at-least-once delivery duplicates."""
from datetime import datetime, timezone
from app.common import ch_rows

DAILY_SQL = """
SELECT uniqExact(user_id) AS dau,
       avgIfOrNull(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_seconds,
       countIf(event_type = 'VIEW_STARTED') AS started,
       countIf(event_type = 'VIEW_FINISHED') AS finished,
       count() AS processed_records
FROM cinema.raw_events FINAL
WHERE toDate(timestamp) = {date:Date}
"""

COHORT_SQL = """
WITH first_views AS (
    SELECT user_id, min(toDate(timestamp)) AS cohort_date
    FROM cinema.raw_events FINAL
    WHERE event_type = 'VIEW_STARTED'
    GROUP BY user_id
), cohort AS (
    SELECT user_id FROM first_views WHERE cohort_date = {date:Date}
), activity AS (
    SELECT DISTINCT user_id, toDate(timestamp) AS active_date
    FROM cinema.raw_events FINAL
    WHERE toDate(timestamp) BETWEEN {date:Date} AND addDays({date:Date}, 7)
      AND event_type IN ('VIEW_STARTED', 'VIEW_PAUSED', 'VIEW_RESUMED', 'VIEW_FINISHED')
)
SELECT day, (SELECT count() FROM cohort) AS cohort_size,
       uniqExactIf(a.user_id, a.active_date = addDays({date:Date}, toInt32(day))) AS retained
FROM (SELECT number AS day FROM numbers(8)) AS offsets
CROSS JOIN cohort c
LEFT JOIN activity a ON c.user_id = a.user_id
GROUP BY day ORDER BY day
"""


def calculate(day):
    params = {'param_date': day.isoformat()}
    result = ch_rows(DAILY_SQL, params)[0]
    started, finished = int(result['started']), int(result['finished'])
    values = {'dau': float(result['dau']), 'avg_watch_seconds': result['avg_watch_seconds'],
              'view_started': float(started), 'view_finished': float(finished),
              'conversion': finished / started if started else None}
    today = datetime.now(timezone.utc).date()
    cohort_rows = ch_rows(COHORT_SQL, params)
    by_day = {int(row['day']): row for row in cohort_rows}
    cohorts = []
    for offset in range(8):
        row = by_day.get(offset, {'cohort_size': 0, 'retained': 0})
        size = int(row['cohort_size'])
        # Day is complete only after midnight UTC; future/incomplete cells stay NULL.
        mature = (today - day).days > offset
        retained = int(row['retained']) if mature else None
        rate = retained / size if size and mature else None
        cohorts.append({'cohort_date': day.isoformat(), 'day': offset, 'cohort_size': size,
                        'retained_users': retained, 'retention': rate})
    values['retention_d1'] = cohorts[1]['retention']
    values['retention_d7'] = cohorts[7]['retention']
    metrics = [{'date': day.isoformat(), 'metric_name': key, 'dimension': '', 'value': value} for key, value in values.items()]
    movies = ch_rows("""
        SELECT movie_id, countIf(event_type = 'VIEW_STARTED') AS views
        FROM cinema.raw_events FINAL WHERE toDate(timestamp) = {date:Date}
        GROUP BY movie_id HAVING views > 0 ORDER BY views DESC, movie_id
    """, params)
    metrics += [{'date': day.isoformat(), 'metric_name': 'movie_views', 'dimension': r['movie_id'],
                 'value': float(r['views'])} for r in movies]
    return metrics, cohorts, int(result['processed_records'])
