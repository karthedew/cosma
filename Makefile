

build:
	go build -o cosma-dev ./cmd/cosma-dev

run:
	./cosma-dev

unit-tests:
	go test ./...

lint:
	golangci-lint run
