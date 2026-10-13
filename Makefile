PROTO_FILES := $(shell find proto -name '*.proto')
TEST_DATABASE_URL ?= postgres://collab:collab@localhost:55432/collab?sslmode=disable

.PHONY: proto test test-integration up down run-docsvc run-hub

proto:
	protoc -I proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

test:
	go test -race ./...

up:
	docker compose up -d --wait postgres

down:
	docker compose down -v

test-integration: up
	COLLAB_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -count=1 ./...

run-docsvc:
	DATABASE_URL='$(TEST_DATABASE_URL)' go run ./services/document-service/cmd/document-service

run-hub:
	go run ./services/collab-hub/cmd/collab-hub
