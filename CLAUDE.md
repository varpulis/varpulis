# CEP (Varpulis) Project

Rust-based Complex Event Processing engine.

## Build & Test

```bash
cargo build
cargo test
cargo clippy

# Local pre-push verification (fmt + clippy + audit + deny — same gates as CI):
make verify
```

`make verify` is a thin wrapper over `scripts/verify.sh`. Subsets:
`make verify-fmt`, `make verify-clippy`, `make verify-audit`, `make verify-deny`.
Equivalent skip toggles work directly on the script:
`SKIP_FMT=1 SKIP_CLIPPY=1 SKIP_AUDIT=1 SKIP_DENY=1 bash scripts/verify.sh`.
