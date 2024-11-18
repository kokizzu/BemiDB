-- Usage: psql postgres://127.0.0.1:5432/dbname -P pager=off -v ON_ERROR_STOP=on -f ./scripts/test-json.sql

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    data JSONB
);

INSERT INTO test_table (data)
SELECT jsonb_build_object('uuid', gen_random_uuid())
FROM generate_series(1, 1000000);
