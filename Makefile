BUILD_BIN_PATH := $(shell pwd)/bin

default: build

build: opt-ctl

# Tools
opt-ctl: export GO111MODULE=on
opt-ctl: export GOPROXY=https://proxy.golang.org
opt-ctl:
	CGO_ENABLED=0 go build -o $(BUILD_BIN_PATH)/optimizer-tester main.go

clean-build:
	# Cleaning building files...
	rm -rf $(BUILD_BIN_PATH)

clean: clean-build

.PHONY: all ci vendor tidy clean-test clean-build clean