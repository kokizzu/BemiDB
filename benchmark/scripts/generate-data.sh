#!/bin/bash

cd benchmark

# Structure
cp ./tpch-kit/dbgen/dss.ddl ./data/create-tables.ddl

# Data
cd ./tpch-kit/dbgen
export DSS_PATH=../../data
export DSS_CONFIG=./
./dbgen -vf -s $SCALE_FACTOR # 1 = 1GB

# Queries
cd -
rm -rf /tmp/query-templates
mkdir /tmp/query-templates
for i in `ls query-templates/*.sql`; do
  tac $i | sed '2s/;//' | tac > /tmp/$i # Remove ";"
done
cd ./tpch-kit/dbgen
export DSS_QUERY=/tmp/query-templates
./qgen -v -s 0.1 | sed 's/limit -1//' > ../../queries.sql
