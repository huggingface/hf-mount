# Targets
.PHONY: all build check clean doc fmt release test

BUILD_TOOL := cargo

all: clean fmt build

build:
	@$(BUILD_TOOL) build

check:
	@$(BUILD_TOOL) check

clean:
	@$(BUILD_TOOL) clean

doc:
	@$(BUILD_TOOL) doc --no-deps

fmt:
	@$(BUILD_TOOL) fmt

release: clean
	@$(BUILD_TOOL) build --release

test:
	@$(BUILD_TOOL) test
