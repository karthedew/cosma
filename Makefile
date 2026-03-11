

build:
	go build -o cosma-dev ./cmd/cosma-dev

run:
	make build && ./cosma-dev

unit-tests:
	go test ./...

lint:
	golangci-lint run
