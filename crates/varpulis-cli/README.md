# varpulis

The engine's command line. Three things, all offline:

```bash
varpulis check program.vpl                     # parse it, load it into the engine: "ok" or the error
varpulis parse program.vpl                     # the syntax tree
varpulis simulate -p program.vpl -e events.evt # run it over an event file; one JSON object per emit
```

Running a program on a bus is a host's job. The reference host is
[Vejas](https://github.com/cpoder/vejas), where a `.vpl` file under
`detects/` is a unit with a durable consumer, snapshots and replay.
