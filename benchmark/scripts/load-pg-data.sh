#!/bin/bash

cd ./benchmark/data

psql postgres://127.0.0.1:5432/tpch -f ./create-tables.ddl

for i in `ls *.tbl`; do
  table=${i/.tbl/}
  echo "Loading $table..."
  sed 's/|$//' $i > /tmp/$i
  psql postgres://127.0.0.1:5432/tpch -q -c "TRUNCATE $table"
  psql postgres://127.0.0.1:5432/tpch -c "\\copy $table FROM '/tmp/$i' CSV DELIMITER '|'"
done
