CONTAINER_ENGINE ?= $(shell command -v podman 2>/dev/null || echo docker)
COMPOSE          ?= $(CONTAINER_ENGINE) compose

.PHONY: build test lint up down tidy run-simple run-yaml image

build:
	go build ./...

test:
	go test -race -count=1 ./...

lint:
	golangci-lint run

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down -v

image:
	$(CONTAINER_ENGINE) build -t go-dcp-pg-redis .

tidy:
	go mod tidy

run-simple:
	go run example/simple/main.go

run-yaml:
	cd example/yaml && go run main.go
