# aws-messaging-webhook -- developer commands
# Run `make help` (or just `make`) to list available targets.

.PHONY: help fmt fmt-check clippy test test-handlers test-verifier deny lint check watch build deploy-dev deploy-prod validate coverage clean install-tools doc prek

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

fmt: ## Format all code
	cargo fmt --all

fmt-check: ## Check formatting (no changes)
	cargo fmt --all -- --check

clippy: ## Run clippy with strict warnings
	cargo clippy --all-targets --all-features -- -D warnings

test: ## Run all tests (no AWS needed)
	cargo test --workspace

test-handlers: ## Run handler integration tests only
	cargo test -p aws-messaging-webhook --test handlers

test-verifier: ## Run SNS verifier tests only
	cargo test -p sns-message-verifier

deny: ## Check dependencies (advisories, licenses, bans)
	cargo deny check

lint: fmt-check clippy deny ## Full lint suite (fmt + clippy + deny)

check: lint test ## Everything CI runs (lint + test)

watch: ## Start local Lambda runtime (cargo-lambda)
	cargo lambda watch

build: ## Build for deployment (SAM + cargo-lambda)
	sam build --config-env dev

deploy-dev: ## Build and deploy to dev
	sam build --config-env dev
	sam deploy --config-env dev

deploy-prod: ## Build and deploy to prod
	sam build --config-env prod
	sam deploy --config-env prod

validate: ## Lint the SAM template
	sam validate --lint

coverage: ## Generate code coverage report (HTML)
	cargo llvm-cov --workspace --html
	@echo "Report: target/llvm-cov/html/index.html"

clean: ## Remove build artifacts
	cargo clean

install-tools: ## Install dev toolchain (cargo-lambda, cargo-deny, cargo-llvm-cov, prek)
	@echo "Installing cargo-lambda..."
	cargo install cargo-lambda
	@echo "Installing cargo-deny..."
	cargo install cargo-deny
	@echo "Installing cargo-llvm-cov..."
	cargo install cargo-llvm-cov
	@echo "Installing prek..."
	cargo install prek
	@echo "Checking for actionlint..."
	@command -v actionlint >/dev/null 2>&1 || echo "  ⚠ actionlint not found -- install via: brew install actionlint"
	@echo "Checking for zizmor..."
	@command -v zizmor >/dev/null 2>&1 || echo "  ⚠ zizmor not found -- install via: cargo install zizmor"
	@echo "Done. Run 'prek install' to set up git hooks."

doc: ## Generate and open rustdoc
	cargo doc --workspace --no-deps --open

prek: ## Run pre-commit hooks
	prek run
