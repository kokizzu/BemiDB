-- Usage: psql postgres://127.0.0.1:5432/dbname -P pager=off -v ON_ERROR_STOP=on -f ./scripts/test-partitioned-tables.sql

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (
    id SERIAL,
    created_at TIMESTAMP NOT NULL
) PARTITION BY RANGE (created_at);

CREATE TABLE test_table_q1 PARTITION OF test_table FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE test_table_q2 PARTITION OF test_table FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
CREATE TABLE test_table_q3 PARTITION OF test_table FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
CREATE TABLE test_table_q4 PARTITION OF test_table FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');

INSERT INTO test_table (created_at) VALUES
  ('2024-02-15 10:00:00'),
  ('2024-09-01 12:00:30'),
  ('2024-10-12 08:00:00'),
  ('2024-05-20 14:30:00');
