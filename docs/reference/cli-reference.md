# CLI Reference

The `varpulis` command is the engine's command line. Three things, all
offline; running a program on a bus is a host's job — the reference host is
[Vejas](https://github.com/cpoder/vejas), where a `.vpl` file under
`detects/` is a unit ([ADR-008](../adr/008-engine-only-platform-retired.md)).

```bash
cargo install --path crates/varpulis-cli
```

## `varpulis check <file>`

Parses the program and loads it into the engine. Prints `ok`, or the error
on stderr with exit code 1 — the same verdict a host gives before it starts
the program.

```bash
varpulis check examples/security-demo/detect_lateral_movement.vpl
```

## `varpulis parse <file>`

Prints the program's syntax tree, for when the error message is not enough.

```bash
varpulis parse examples/hvac_quickstart.vpl
```

## `varpulis simulate -p <program.vpl> -e <events.evt>`

Runs the program over an event file and prints every emit as one JSON
object per line on stdout; a count of events in and emits out goes to
stderr. `-v` prints each input event on stderr as it is fed.

```bash
varpulis simulate -p examples/transaction_monitoring.vpl -e examples/transaction_monitoring.evt
varpulis simulate -p examples/hvac_quickstart.vpl -e examples/hvac_quickstart.evt -v
```

An `.evt` file is one event per line — `Type { field: value, ... }` — with
`BATCH <ms>` lines advancing the clock and `#` comments; the examples
directory is full of them. Time is the events' own: `.within()` and windows
are judged against the event time, never against the wall clock, so the
same file gives the same emits every run.
