# Troubleshooting Guide

Diagnostic procedures and solutions for common Varpulis issues.

## Quick Diagnostics Checklist

Before diving into specific issues, run through this checklist:

```bash
# 1. Check Varpulis version
varpulis --version

# 2. Validate syntax
varpulis check program.vpl

# 3. Test with verbose output
varpulis simulate -p program.vpl -e test.evt --verbose

# 4. Check logs
RUST_LOG=debug varpulis run --file program.vpl

# 5. Verify MQTT connectivity (if applicable)
mosquitto_sub -h localhost -t '#' -v  # In separate terminal
```

---

## Startup Issues

### "Parse error: unexpected token"

**Symptom:** Program fails to load with syntax error.

**Common causes:**
1. Typo in keyword (e.g., `form` instead of `from`)
2. Missing or extra brackets
3. Unquoted strings where quotes are required

**Solution:**
```bash
# Get detailed error location
varpulis check program.vpl
```

**Example fix:**
```vpl
// Wrong
stream Readings form SensorReading

// Correct
stream Readings from SensorReading
```

### "Failed to bind to address"

**Symptom:** Server won't start, address already in use.

**Solution:**
```bash
# Find what's using the port
lsof -i :9000

# Kill the process or use different port
varpulis server --port 9001
```

### "MQTT connection failed"

**Symptom:** Can't connect to MQTT broker.

**Debug steps:**
```bash
# Test broker connectivity
nc -zv localhost 1883

# Test with mosquitto client
mosquitto_pub -h localhost -t test -m "hello"

# Check broker logs
sudo journalctl -u mosquitto -f
```

**Common fixes:**
- Verify broker is running
- Check firewall rules
- Verify credentials if authentication enabled
- For TLS: check certificate paths and validity

### "Permission denied" for files

**Symptom:** Can't read program file or write output.

**Solution:**
```bash
# Check file permissions
ls -la program.vpl

# Fix permissions
chmod 644 program.vpl

# For workdir
chmod 755 /var/lib/varpulis
```

---

## Runtime Issues

### No Events Being Processed

**Symptom:** Events processed counter stays at 0.

**Debug steps:**

1. **Verify event source is sending:**
```bash
# For MQTT
mosquitto_sub -h localhost -t 'events/#' -v

# For file simulation
head -5 events.evt
```

2. **Check event type matches:**
```vpl
// If events come as "temperature_reading" (snake_case)
stream Temps from temperature_reading  // Must match exactly!
```

3. **Verify topic subscription:**
```vpl
connector Sensors = mqtt (
    host: "localhost",
    port: 1883,
    client_id: "my-app"
)

stream Events = SensorReading
    .from(Sensors, topic: "sensors/#")  // Make sure wildcard is correct
```

### Events Processed but No Alerts

**Symptom:** Events are counted but alerts are 0.

**Debug steps:**

1. **Check filter conditions:**
```vpl
// Too restrictive?
.where(temperature > 1000)  // Maybe threshold is too high

// Debug: remove filters temporarily
stream Debug from TemperatureReading
    .print("Got event: {temperature}")
```

2. **Verify field names:**
```vpl
// Field names are case-sensitive
.where(Temperature > 100)  // Wrong if field is "temperature"
.where(temperature > 100)  // Correct
```

3. **Check data types:**
```vpl
// String comparison vs numeric
.where(value > 100)      // Works if value is numeric
.where(value > "100")    // String comparison - different!
```

### Memory Growing Unbounded

**Symptom:** Memory usage keeps increasing.

**Common causes:**

1. **Unbounded windows:**
```vpl
// Bad: no time limit
stream Bad from Event
    .window(1h, sliding: 1s)  // 3600 overlapping windows!

// Better: reasonable window
stream Better from Event
    .window(1m)
```

2. **Too many partitions:**
```vpl
// Bad: high-cardinality partition
.partition_by(request_id)  // Every request = new partition!

// Better: use low-cardinality
.partition_by(customer_id)  // Bounded number of customers
```

3. **Pattern state accumulation:**
```vpl
// Bad: long timeout, lots of partial matches
pattern Bad = A -> B -> C within 24h

// Better: shorter timeout
pattern Better = A -> B -> C within 5m
```

**Solution:**
```bash
# Monitor memory
watch -n 1 'ps aux | grep varpulis'

# Enable memory limits
ulimit -v 4000000  # 4GB limit
```

### High Latency

**Symptom:** Event processing takes too long.

**Debug steps:**

1. **Enable metrics:**
```bash
varpulis server --metrics --metrics-port 9090
curl localhost:9090/metrics | grep duration
```

2. **Profile with trace logging:**
```bash
RUST_LOG=varpulis_runtime::engine=trace varpulis run ...
```

3. **Check for blocking operations:**
- HTTP webhooks with slow endpoints
- Large aggregation windows
- Complex pattern matching

**Solutions:**
- Increase workers: `--workers 8`
- Use partitioning to parallelize
- Reduce window sizes
- Simplify patterns

---

## Pattern Matching Issues

### Pattern Never Matches

**Debug approach:**

1. **Simplify the pattern:**
```vpl
// Start with just the first event type
stream Debug1 from A
    .print("Matched A")

// Then test with a sequence
stream Debug2 = A as a -> B
    .within(1h)
    .print("Matched A -> B")
```

2. **Check event types exactly:**
```vpl
// Event types are case-sensitive
pattern Wrong = LoginEvent -> LogoutEvent
pattern Right = login_event -> logout_event  // If that's the actual type
```

3. **Verify timing:**
```bash
# Check event timestamps
varpulis simulate -p rules.vpl -e events.evt --verbose
# Look at timestamps in output
```

### Pattern Matches Too Often

**Common causes:**

1. **Missing partition-by:**
```vpl
// Bad: matches across all users
pattern AllUsers = Login -> Logout

// Better: per-user matching
pattern PerUser = Login -> Logout
    partition by user_id
```

2. **Predicates too loose:**
```vpl
// Bad: any transaction
pattern Fraud = Transaction+

// Better: constrained
pattern Fraud = Transaction[amount > 1000]+
```

### Negation Not Working

**Debug steps:**

1. **Check timeout is sufficient:**
```vpl
// If events are slow, timeout might expire before negated event
pattern TooShort = A -> NOT(B) within 1s

// Try longer timeout
pattern Longer = A -> NOT(B) within 1m
```

2. **Verify event type in NOT:**
```vpl
// Must match exactly
pattern Check = Order -> NOT(OrderConfirm) within 1h
// ^^ Make sure "OrderConfirm" is the exact event type
```

---

## Error Message Reference

### Parse Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `unexpected token 'xyz'` | Typo or wrong syntax | Check keyword spelling |
| `expected '}'` | Missing closing brace | Count opening/closing braces |
| `invalid duration` | Wrong duration format | Use `5s`, `1m`, `1h`, not `5 seconds` |

### Runtime Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `field 'x' not found` | Event doesn't have field | Check field name spelling |
| `type mismatch` | Comparing incompatible types | Ensure consistent types |
| `timeout exceeded` | Operation took too long | Simplify query or increase resources |

### Connection Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `connection refused` | Service not running | Start the service (MQTT, etc.) |
| `connection reset` | Network issue or service crashed | Check service logs |
| `TLS handshake failed` | Certificate issue | Verify cert paths and validity |
| `authentication failed` | Wrong credentials | Check API key or password |

---

## Debug Tools

### Verbose Mode

```bash
# Show all events and processing
varpulis simulate -p rules.vpl -e events.evt --verbose
```

### Trace Logging

```bash
# Full trace output
RUST_LOG=trace varpulis run --file rules.vpl

# Specific module tracing
RUST_LOG=varpulis_runtime::sase=trace varpulis run --file rules.vpl
RUST_LOG=varpulis_runtime::engine=debug varpulis run --file rules.vpl
```

### AST Inspection

```bash
# View parsed structure
varpulis parse program.vpl
```

### Metrics Inspection

```bash
# Start with metrics
varpulis server --metrics --metrics-port 9090

# Query metrics
curl -s localhost:9090/metrics | grep varpulis
```

### Event File Debugging

Create a minimal test case:
```
# test.evt - Minimal reproduction
@0 EventA { id: "1", value: 100 }
@100 EventB { id: "1", value: 200 }
```

```bash
varpulis simulate -p rules.vpl -e test.evt --verbose
```

---

## Performance Profiling

### CPU Profiling

```bash
# With perf (Linux)
perf record -g varpulis simulate -p rules.vpl -e large.evt --immediate
perf report

# With flamegraph
cargo flamegraph -- simulate -p rules.vpl -e large.evt --immediate
```

### Memory Profiling

```bash
# With heaptrack
heaptrack varpulis simulate -p rules.vpl -e large.evt --immediate
heaptrack_gui heaptrack.varpulis.*.gz

# With valgrind massif
valgrind --tool=massif varpulis simulate -p rules.vpl -e test.evt
ms_print massif.out.*
```

---

## Getting Help

### Logs to Collect

When reporting issues, include:

1. **Varpulis version:**
```bash
varpulis --version
```

2. **Full error output:**
```bash
RUST_LOG=debug varpulis ... 2>&1 | tee varpulis.log
```

3. **Minimal reproduction:**
- Simplified VPL file
- Sample event file
- Steps to reproduce

4. **System information:**
```bash
uname -a
cat /etc/os-release
```

### Resources

- [GitHub Issues](https://github.com/varpulis/varpulis/issues) - Report bugs
- [Documentation](../README.md) - Full documentation index

---

## Common Fixes Summary

| Problem | Quick Fix |
|---------|-----------|
| Syntax error | `varpulis check file.vpl` to see details |
| No events processed | Check event type names match exactly |
| No alerts | Check filter conditions, try removing `where` |
| Memory growth | Add timeouts, use partitioning wisely |
| High latency | Increase workers, simplify patterns |
| Pattern won't match | Simplify to debug, check timing |
| MQTT won't connect | Test with `mosquitto_sub` first |
| Server won't start | Check port availability with `lsof` |
