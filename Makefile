PROTO_FILES := $(shell find proto -name '*.proto')
TEST_DATABASE_URL ?= postgres://collab:collab@localhost:55432/collab?sslmode=disable
TEST_REDIS_URL ?= redis://localhost:56379/0

.PHONY: proto test test-integration up down run-docsvc run-hub

proto:
	protoc -I proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

test:
	go test -race ./...

up:
	docker compose up -d --wait postgres redis

down:
	docker compose down -v

test-integration: up
	COLLAB_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' COLLAB_TEST_REDIS_URL='$(TEST_REDIS_URL)' go test -race -count=1 ./...

run-docsvc:
	DATABASE_URL='$(TEST_DATABASE_URL)' go run ./services/document-service/cmd/document-service

run-hub:
	go run ./services/collab-hub/cmd/collab-hub
