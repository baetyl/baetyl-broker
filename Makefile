GO_TEST_FLAGS?=-race
GO_TEST_PKGS?=$(shell go list ./...)
SRC_FILES:=$(shell find . -type f -name '*.go')

.PHONY: all
all: $(SRC_FILES)
	@echo "BUILD $@"
	@env CGO_ENABLED=1 go build -race -o baetyl-broker

.PHONY: test
test: fmt
	@go test ${GO_TEST_FLAGS} -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | grep total

.PHONY: fmt
fmt:
	go fmt  ./...