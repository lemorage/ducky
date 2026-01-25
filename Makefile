# Ducky Makefile
# Build, test, and release automation

.POSIX:
.SUFFIXES:

# Variables
PROJECT_NAME := ducky
RUST_NIF_DIR := priv/ducky_nif
RUST_MANIFEST := $(RUST_NIF_DIR)/Cargo.toml
RUST_TARGET_DIR := $(RUST_NIF_DIR)/target/release
NIF_OUTPUT_DIR := priv/native
VERSION := $(shell grep '^version' gleam.toml | cut -d'"' -f2)

# Detect OS for lib extension
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    LIB_EXT := dylib
    LIB_PREFIX := lib
else ifeq ($(UNAME_S),Linux)
    LIB_EXT := so
    LIB_PREFIX := lib
else
    LIB_EXT := dll
    LIB_PREFIX :=
endif

NIF_SOURCE := $(RUST_TARGET_DIR)/$(LIB_PREFIX)ducky_nif.$(LIB_EXT)
NIF_TARGET := $(NIF_OUTPUT_DIR)/ducky_nif.so

# Colors and formatting
CYAN := \033[0;36m
GREEN := \033[0;32m
BOLD := \033[1m
DIM := \033[2m
RESET := \033[0m

# Default target
.DEFAULT_GOAL := help

# Phony targets
.PHONY: help build build-nif test clean format check publish release version dev

#-----------------------------------------------------------------------------
# Public targets
#-----------------------------------------------------------------------------

## help: Show this help message
help:
	@echo ""
	@echo "  $(BOLD)$(PROJECT_NAME)$(RESET) $(DIM)v$(VERSION)$(RESET)"
	@echo "  ─────────────────────"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## //' | awk -F': ' '{ \
		printf "  $(CYAN)%-10s$(RESET) %s\n", $$1, $$2 \
	}'
	@echo ""

## clean: Clean all build artifacts
clean:
	@echo "$(CYAN)Cleaning build artifacts...$(RESET)"
	rm -rf build/
	rm -f $(NIF_TARGET)
	cargo clean --manifest-path=$(RUST_MANIFEST)
	@echo "$(GREEN)✓ Clean complete$(RESET)"

## docs: Generate and open documentation
docs:
	@echo "$(CYAN)Generating documentation...$(RESET)"
	gleam docs build
	@echo "$(GREEN)✓ Documentation generated$(RESET)"
	@echo "Open: build/dev/docs/$(PROJECT_NAME)/index.html"

## dev: Development workflow (check + format + build + test)
dev: check format build test
	@echo "$(GREEN)✓ Development cycle complete$(RESET)"

## release: Full release workflow (version, commit, tag, publish)
release:
	@if [ -z "$(NEXT)" ]; then \
		echo "Error: NEXT not set"; \
		echo "Usage: make release NEXT=0.2.0"; \
		exit 1; \
	fi
	@echo "$(CYAN)Starting release process for v$(NEXT)...$(RESET)"
	@$(MAKE) --no-print-directory version NEXT=$(NEXT)
	@$(MAKE) --no-print-directory test
	@echo "$(CYAN)Committing changes...$(RESET)"
	git add gleam.toml mix.exs $(RUST_MANIFEST) priv/ducky_nif/Cargo.lock priv/VERSION examples/gleam.toml examples/manifest.toml CHANGELOG.md
	git commit -m "Release v$(NEXT)"
	@echo "$(CYAN)Creating git tag...$(RESET)"
	git tag -a "v$(NEXT)" -m "Ducky Release $(NEXT)"
	@echo "$(CYAN)Ready to push to remote...$(RESET)"
	@read -p "Push to origin? [y/N] " answer; \
	if [ "$$answer" = "y" ] || [ "$$answer" = "Y" ]; then \
		git push origin master --tags; \
		echo "$(GREEN)✓ Pushed to remote$(RESET)"; \
	else \
		echo "Push cancelled. Run manually: git push origin master --tags"; \
		exit 1; \
	fi
	@$(MAKE) --no-print-directory publish
	@echo "$(GREEN)✓ Release v$(NEXT) complete!$(RESET)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Visit https://hex.pm/packages/$(PROJECT_NAME)"
	@echo "  2. Create GitHub release at https://github.com/lemorage/$(PROJECT_NAME)/releases/new"

#-----------------------------------------------------------------------------
# Internal targets
#-----------------------------------------------------------------------------

version:
	@if [ -z "$(NEXT)" ]; then \
		echo "Current version: $(VERSION)"; \
		echo "Usage: make version NEXT=0.2.0"; \
		exit 1; \
	fi
	@echo "$(CYAN)Updating version: $(VERSION) -> $(NEXT)$(RESET)"
	@sed -i.bak 's/^version = ".*"/version = "$(NEXT)"/' gleam.toml && rm gleam.toml.bak
	@echo "  ✓ gleam.toml"
	@sed -i.bak 's/version: ".*"/version: "$(NEXT)"/' mix.exs && rm mix.exs.bak
	@echo "  ✓ mix.exs"
	@sed -i.bak 's/^version = ".*"/version = "$(NEXT)"/' $(RUST_MANIFEST) && rm $(RUST_MANIFEST).bak
	@echo "  ✓ priv/ducky_nif/Cargo.toml"
	@echo "$(NEXT)" > priv/VERSION
	@echo "  ✓ priv/VERSION"
	@sed -i.bak 's/^version = ".*"/version = "$(NEXT)"/' examples/gleam.toml && rm examples/gleam.toml.bak
	@echo "  ✓ examples/gleam.toml"
	@echo "$(CYAN)Rebuilding Rust to update Cargo.lock...$(RESET)"
	@cargo build --manifest-path=$(RUST_MANIFEST) --quiet 2>&1 | head -5 || true
	@echo "  ✓ Cargo.lock updated"
	@echo "$(CYAN)Regenerating examples/manifest.toml...$(RESET)"
	@cd examples && gleam update >/dev/null 2>&1
	@echo "  ✓ manifest.toml regenerated"
	@echo "$(GREEN)✓ Version updated to $(NEXT)$(RESET)"
	@echo ""
	@echo "$(CYAN)Don't forget to update CHANGELOG.md!$(RESET)"

publish: clean
	@echo "$(CYAN)Publishing to Hex...$(RESET)"
	@echo "Current version: $(VERSION)"
	@read -p "Proceed with publish? [y/N] " answer; \
	if [ "$$answer" = "y" ] || [ "$$answer" = "Y" ]; then \
		gleam publish; \
		echo "$(GREEN)✓ Published successfully$(RESET)"; \
	else \
		echo "Publish cancelled"; \
	fi

build-nif:
	@echo "$(CYAN)Building Rust NIF...$(RESET)"
	@cargo build --manifest-path=$(RUST_MANIFEST) --release
	@mkdir -p $(NIF_OUTPUT_DIR)
	@cp $(NIF_SOURCE) $(NIF_TARGET)
	@echo "$(GREEN)✓ NIF built and copied to $(NIF_TARGET)$(RESET)"

build: build-nif
	@echo "$(CYAN)Building Gleam project...$(RESET)"
	@gleam build
	@echo "$(GREEN)✓ Build complete$(RESET)"

test: build-nif
	@echo "$(CYAN)Running tests...$(RESET)"
	@gleam test
	@echo "$(GREEN)✓ Tests passed$(RESET)"

format:
	@echo "$(CYAN)Formatting code...$(RESET)"
	gleam format
	@echo "$(GREEN)✓ Code formatted$(RESET)"

check:
	@echo "$(CYAN)Checking code format...$(RESET)"
	gleam format --check
	@echo "$(GREEN)✓ Format check passed$(RESET)"
