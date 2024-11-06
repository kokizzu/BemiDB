sh:
	devbox shell

install:
	devbox run "cd src && go mod tidy"

up:
	devbox run --env-file .env "cd src && go run ."

.PHONY: build
build:
	rm -rf build/bemidb-* && \
		devbox run "go build -C src -o ../build/bemidb-darwin-arm64" && \
		./scripts/build.sh

sync:
	devbox run --env-file .env "cd src && go run . sync"

test:
	devbox run "cd src && go test ./..."

debug:
	devbox run "cd src && dlv test github.com/BemiHQ/BemiDB"

lint:
	devbox run "cd src && go fmt"

outdated:
	devbox run "cd src && go list -u -m -f '{{if and .Update (not .Indirect)}}{{.}}{{end}}' all"

.PHONY: benchmark
benchmark:
	devbox run "time psql postgres://127.0.0.1:54321/bemidb < ./benchmark/queries.sql"

pg-init:
	devbox run initdb

pg-up:
	devbox services start postgresql

pg-create:
	devbox run "(dropdb tpch || true) && \
		createdb tpch && \
		./benchmark/scripts/load-pg-data.sh"

pg-index:
	devbox run "psql postgres://127.0.0.1:5432/tpch -f ./benchmark/data/create-indexes.ddl"

pg-benchmark:
	devbox run "psql postgres://127.0.0.1:5432/tpch -c 'ANALYZE VERBOSE' && \
		time psql postgres://127.0.0.1:5432/tpch < ./benchmark/queries.sql"

pg-down:
	devbox services stop postgresql

tpch-install:
	devbox run "cd benchmark && \
		git clone https://github.com/gregrahn/tpch-kit.git
		cd tpch-kit/dbgen
		make MACHINE=$$MACHINE DATABASE=POSTGRESQL"

tpch-generate:
	devbox run "./benchmark/scripts/generate-data.sh"
