.PHONY: build-linux
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./release/copier-linux main.go

.PHONY: build-mac
build-mac:
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o ./release/copier-arm64 main.go

.PHONY: build
build: build-linux build-mac
