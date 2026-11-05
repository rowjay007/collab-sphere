PROTO_FILES := $(shell find proto -name '*.proto')
TEST_DATABASE_URL ?= postgres://collab:collab@localhost:55432/collab?sslmode=disable
TEST_REDIS_URL ?= redis://localhost:56379/0

.PHONY: proto test test-integration up down run-docsvc run-hub run-presence run-permission run-export helm-check images

DEV_AUTH_PUBLIC_KEY ?= wBiwn96X59Q0XaGgZIFY1UDnR55L2agldgCbneQFQvM=
DEV_SHARE_PRIVATE_KEY ?= 1/Phydme/tVCmEdnYd3iDBlvCBEtVvanqFUW10vwMg0=
DEV_INTERNAL_TOKEN ?= 99555b306395f2318808fdf9d3c6e1262e809d072790eeb91871cdc6c7214ee8

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

run-presence:
	REDIS_URL='$(TEST_REDIS_URL)' go run ./services/presence-service/cmd/presence-service

run-permission:
	DATABASE_URL='postgres://permission:permission@localhost:55433/permission?sslmode=disable' \
	GRPC_ADDR=:9094 \
	AUTH_PUBLIC_KEY='$(DEV_AUTH_PUBLIC_KEY)' \
	SHARE_PRIVATE_KEY='$(DEV_SHARE_PRIVATE_KEY)' \
	INTERNAL_TOKEN='$(DEV_INTERNAL_TOKEN)' \
	DOCUMENT_ADDR=localhost:9090 \
	go run ./services/permission-service/cmd/permission-service

run-export:
	DATABASE_URL='postgres://export:export@localhost:55434/export?sslmode=disable' \
	GRPC_ADDR=:9095 \
	AUTH_PUBLIC_KEY='$(DEV_AUTH_PUBLIC_KEY)' \
	INTERNAL_TOKEN='$(DEV_INTERNAL_TOKEN)' \
	DOCUMENT_ADDR=localhost:9090 \
	VERSION_ADDR=localhost:9093 \
	PERMISSION_ADDR=localhost:9094 \
	S3_ENDPOINT=http://localhost:9000 \
	S3_BUCKET=exports \
	S3_ACCESS_KEY=collab \
	S3_SECRET_KEY=collabsecret \
	S3_REGION=us-east-1 \
	go run ./services/export-service/cmd/export-service

helm-check:
	@command -v kubeconform >/dev/null 2>&1 || go install github.com/yannh/kubeconform/cmd/kubeconform@latest
	helm lint deploy/helm/collab-sphere
	helm template collab deploy/helm/collab-sphere > /tmp/collab-helm-incluster.yaml
	helm template collab deploy/helm/collab-sphere \
		--set postgresql.enabled=false \
		--set redis.enabled=false \
		--set minio.enabled=false \
		--set postgresql.host=postgres.example \
		--set redis.url=redis://redis.example:6379/0 \
		--set minio.endpoint=http://minio.example:9000 \
		> /tmp/collab-helm-external.yaml
	PATH="$(PATH):$(shell go env GOPATH)/bin" kubeconform -strict -kubernetes-version 1.31.0 -summary /tmp/collab-helm-incluster.yaml /tmp/collab-helm-external.yaml

SERVICES := document-service presence-service comment-service version-service permission-service export-service collab-hub
IMAGE_TAG ?= latest

images:
	@for s in $(SERVICES); do docker build --build-arg SERVICE=$$s -t collab-sphere/$$s:$(IMAGE_TAG) . || exit 1; done
