GO_TEST_FLAGS?=
GO_TEST_PKGS?=$(shell go list ./...)
SRC_FILES:=$(shell find . -type f -name '*.go')

.PHONY: all
all: $(SRC_FILES)
	@echo "BUILD $@"
	@env CGO_ENABLED=1 go build ./...

.PHONY: test
test:
	@go test ${GO_TEST_FLAGS} -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | grep total

.PHONY: fmt
fmt:
	go fmt  ./...