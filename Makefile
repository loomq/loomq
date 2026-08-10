# LoomQ Makefile
# Version: 0.9.2

.PHONY: help build test clean format check-format check

# Variables
SPOTLESS_PLUGIN := com.diffplug.spotless:spotless-maven-plugin:3.0.0

# Default target
help:
	@echo "LoomQ Build System v0.9.2"
	@echo ""
	@echo "Available targets:"
	@echo "  build                - Build the project (mvn clean package)"
	@echo "  build-fast           - Build without tests"
	@echo "  test                 - Run all fast tests"
	@echo "  test-slow            - Run slow tests"
	@echo "  test-full            - Run all tests including slow/benchmark"
	@echo "  clean                - Clean build artifacts"
	@echo "  format               - Apply Spotless formatting"
	@echo "  check-format         - Verify Spotless formatting"
	@echo "  check                - Run formatting checks + tests"

# Build targets
build:
	mvn clean package

build-fast:
	mvn clean package -DskipTests

# Test targets
test:
	mvn test

test-slow:
	mvn test -Pslow-tests

test-full:
	mvn test -Pfull-tests

benchmark:
	powershell -ExecutionPolicy Bypass -File benchmark/scripts/benchmark.ps1 -NoPause

benchmark-quick:
	powershell -ExecutionPolicy Bypass -File benchmark/scripts/benchmark.ps1 -Quick -NoPause

# Clean
clean:
	mvn clean
	rm -rf logs/
	rm -rf data/

format:
	mvn -B -ntp $(SPOTLESS_PLUGIN):apply

check-format:
	mvn -B -ntp $(SPOTLESS_PLUGIN):check

check: check-format test
	@echo "All checks passed!"
