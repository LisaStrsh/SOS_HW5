CREATE TABLE IF NOT EXISTS daily_metrics (
    date date NOT NULL,
    metric_name text NOT NULL,
    dimension text NOT NULL DEFAULT '',
    value double precision,
    calculated_at timestamptz NOT NULL,
    PRIMARY KEY (date, metric_name, dimension)
);
CREATE TABLE IF NOT EXISTS retention_cohorts (
    cohort_date date NOT NULL,
    day integer NOT NULL CHECK(day BETWEEN 0 AND 7),
    cohort_size bigint NOT NULL,
    retained_users bigint,
    retention double precision,
    calculated_at timestamptz NOT NULL,
    PRIMARY KEY(cohort_date, day)
);
