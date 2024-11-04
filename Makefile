sh:
	devbox shell

install:
	devbox run "cd src && go mod tidy"

up:
	devbox run --env-file .env "cd src && go run ."

build:
	devbox run "go build -C src -o ../bemidb"

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
