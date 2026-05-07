# Varpulis — top-level developer entry-points.
#
# Thin wrappers over scripts in `scripts/`. The script remains the source
# of truth — make targets exist so `make <target>` works without remembering
# the path. CI invokes the same scripts directly.

.PHONY: verify verify-fmt verify-clippy verify-audit verify-deny help

# Run the full local verification suite (fmt + clippy + audit + deny).
# Mirrors the gates that CI enforces.
verify:
	@bash scripts/verify.sh

# Convenience subsets — useful while iterating on a single gate.
verify-fmt:
	@SKIP_CLIPPY=1 SKIP_AUDIT=1 SKIP_DENY=1 bash scripts/verify.sh

verify-clippy:
	@SKIP_FMT=1 SKIP_AUDIT=1 SKIP_DENY=1 bash scripts/verify.sh

verify-audit:
	@SKIP_FMT=1 SKIP_CLIPPY=1 SKIP_DENY=1 bash scripts/verify.sh

verify-deny:
	@SKIP_FMT=1 SKIP_CLIPPY=1 SKIP_AUDIT=1 bash scripts/verify.sh

help:
	@echo "Targets:"
	@echo "  make verify          Run fmt + clippy + audit + deny (full local suite)"
	@echo "  make verify-fmt      Run only nightly rustfmt --check"
	@echo "  make verify-clippy   Run only per-crate clippy with -D warnings"
	@echo "  make verify-audit    Run only cargo audit"
	@echo "  make verify-deny     Run only cargo deny check"
