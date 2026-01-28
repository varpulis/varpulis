#!/bin/bash
#
# Unified Benchmark Script: Varpulis vs Flink CEP
# Runs all 4 comparable scenarios (1, 2, 3, 4)
#
# Prerequisites:
#   - MQTT broker running (mosquitto)
#   - Varpulis built: cargo build --release
#   - Flink test-harness built: cd test-harness && mvn package
#
# Usage:
#   ./run_all_scenarios.sh [processing|event] [scenario_num]
#
#   Examples:
#     ./run_all_scenarios.sh              # Run all scenarios with processing time
#     ./run_all_scenarios.sh event        # Run all scenarios with event time
#     ./run_all_scenarios.sh processing 2 # Run only scenario 2 with processing time
#

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VARPULIS_BIN="/home/cpo/cep/target/release/varpulis"
FLINK_JAR="$SCRIPT_DIR/test-harness/target/flink-varpulis-comparison-1.0-SNAPSHOT.jar"
RESULTS_DIR="$SCRIPT_DIR/results"
TIME_MODE="${1:-processing}"
SPECIFIC_SCENARIO="${2:-all}"

MQTT_BROKER="localhost"
MQTT_PORT=1883

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE} $1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_info() {
    echo -e "[INFO] $1"
}

check_prerequisites() {
    print_header "Checking Prerequisites"

    # Check MQTT broker
    if ! nc -z $MQTT_BROKER $MQTT_PORT 2>/dev/null; then
        print_error "MQTT broker not running at $MQTT_BROKER:$MQTT_PORT"
        echo "Start with: docker-compose up -d"
        exit 1
    fi
    print_success "MQTT broker at $MQTT_BROKER:$MQTT_PORT"

    # Check Varpulis
    if [[ ! -x "$VARPULIS_BIN" ]]; then
        print_error "Varpulis binary not found: $VARPULIS_BIN"
        echo "Build with: cd /home/cpo/cep && cargo build --release"
        exit 1
    fi
    print_success "Varpulis binary found"

    # Check Flink JAR
    if [[ ! -f "$FLINK_JAR" ]]; then
        print_warning "Flink JAR not found. Building..."
        cd "$SCRIPT_DIR/test-harness"
        mvn package -DskipTests -q
        cd "$SCRIPT_DIR"
    fi
    print_success "Flink JAR found"

    # Create results directory
    mkdir -p "$RESULTS_DIR"
}

cleanup_processes() {
    print_info "Cleaning up background processes..."
    pkill -f "varpulis.*scenario" 2>/dev/null || true
    pkill -f "flink-varpulis-comparison" 2>/dev/null || true
    sleep 1
}

# Trap to cleanup on exit
trap cleanup_processes EXIT

# Test event generators
generate_scenario1_events() {
    # Scenario 1: PageView aggregation events
    local base_ts=$(($(date +%s) * 1000))

    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/PageView" -m "{\"type\":\"PageView\",\"user_id\":\"user1\",\"page\":\"/home\",\"category\":\"news\",\"duration_ms\":1500,\"ts\":$((base_ts))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/PageView" -m "{\"type\":\"PageView\",\"user_id\":\"user2\",\"page\":\"/sports\",\"category\":\"news\",\"duration_ms\":2000,\"ts\":$((base_ts + 1000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/PageView" -m "{\"type\":\"PageView\",\"user_id\":\"user3\",\"page\":\"/tech\",\"category\":\"tech\",\"duration_ms\":3000,\"ts\":$((base_ts + 2000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/PageView" -m "{\"type\":\"PageView\",\"user_id\":\"user1\",\"page\":\"/weather\",\"category\":\"news\",\"duration_ms\":1000,\"ts\":$((base_ts + 3000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/PageView" -m "{\"type\":\"PageView\",\"user_id\":\"user4\",\"page\":\"/gadgets\",\"category\":\"tech\",\"duration_ms\":2500,\"ts\":$((base_ts + 4000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/PageView" -m "{\"type\":\"PageView\",\"user_id\":\"user2\",\"page\":\"/reviews\",\"category\":\"tech\",\"duration_ms\":4000,\"ts\":$((base_ts + 5000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/PageView" -m "{\"type\":\"PageView\",\"user_id\":\"user5\",\"page\":\"/politics\",\"category\":\"news\",\"duration_ms\":1800,\"ts\":$((base_ts + 6000))}"
}

generate_scenario2_events() {
    # Scenario 2: Login -> FailedTransaction sequence events
    local base_ts=$(($(date +%s) * 1000))

    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Login" -m "{\"type\":\"Login\",\"user_id\":\"user1\",\"ip_address\":\"192.168.1.1\",\"device\":\"mobile\",\"ts\":$((base_ts))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Login" -m "{\"type\":\"Login\",\"user_id\":\"user2\",\"ip_address\":\"192.168.1.2\",\"device\":\"desktop\",\"ts\":$((base_ts + 1000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user1\",\"amount\":500.0,\"status\":\"failed\",\"merchant\":\"store_a\",\"ts\":$((base_ts + 2000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user3\",\"amount\":100.0,\"status\":\"success\",\"merchant\":\"store_b\",\"ts\":$((base_ts + 3000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Login" -m "{\"type\":\"Login\",\"user_id\":\"user3\",\"ip_address\":\"192.168.1.3\",\"device\":\"tablet\",\"ts\":$((base_ts + 4000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user2\",\"amount\":1500.0,\"status\":\"failed\",\"merchant\":\"store_c\",\"ts\":$((base_ts + 5000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user3\",\"amount\":200.0,\"status\":\"failed\",\"merchant\":\"store_d\",\"ts\":$((base_ts + 6000))}"
}

generate_scenario3_events() {
    # Scenario 3: Fraud pattern events
    # Suspicious -> 3 small purchases -> large withdrawal
    local base_ts=$(($(date +%s) * 1000))

    # User1: suspicious transaction (high risk)
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user1\",\"amount\":6000.0,\"type\":\"transfer\",\"merchant\":\"foreign_bank\",\"location\":\"unknown\",\"risk_score\":0.85,\"ts\":$((base_ts))}"

    # User1: 3 small purchases
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user1\",\"amount\":25.0,\"type\":\"purchase\",\"merchant\":\"coffee_shop\",\"location\":\"city_a\",\"risk_score\":0.1,\"ts\":$((base_ts + 5000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user1\",\"amount\":50.0,\"type\":\"purchase\",\"merchant\":\"gas_station\",\"location\":\"city_a\",\"risk_score\":0.1,\"ts\":$((base_ts + 10000))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user1\",\"amount\":35.0,\"type\":\"purchase\",\"merchant\":\"grocery\",\"location\":\"city_a\",\"risk_score\":0.1,\"ts\":$((base_ts + 15000))}"

    # User1: large withdrawal
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user1\",\"amount\":5000.0,\"type\":\"withdrawal\",\"merchant\":\"atm\",\"location\":\"city_b\",\"risk_score\":0.5,\"ts\":$((base_ts + 20000))}"

    # User2: normal activity (no fraud pattern)
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/Transaction" -m "{\"type\":\"Transaction\",\"user_id\":\"user2\",\"amount\":200.0,\"type\":\"purchase\",\"merchant\":\"store\",\"location\":\"city_a\",\"risk_score\":0.2,\"ts\":$((base_ts + 3000))}"
}

generate_scenario4_events() {
    # Scenario 4: Market ticks for arbitrage detection
    local base_ts=$(($(date +%s) * 1000))

    # AAPL prices with >1% spread
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/MarketATick" -m "{\"symbol\":\"AAPL\",\"price\":150.00,\"volume\":1000,\"exchange\":\"NYSE\",\"ts\":$((base_ts))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/MarketBTick" -m "{\"symbol\":\"AAPL\",\"price\":152.50,\"volume\":800,\"exchange\":\"NASDAQ\",\"ts\":$((base_ts + 100))}"

    # GOOG prices with small spread (no alert expected)
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/MarketATick" -m "{\"symbol\":\"GOOG\",\"price\":2800.00,\"volume\":500,\"exchange\":\"NYSE\",\"ts\":$((base_ts + 200))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/MarketBTick" -m "{\"symbol\":\"GOOG\",\"price\":2805.00,\"volume\":600,\"exchange\":\"NASDAQ\",\"ts\":$((base_ts + 300))}"

    # MSFT prices with >1% spread
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/MarketATick" -m "{\"symbol\":\"MSFT\",\"price\":380.00,\"volume\":1200,\"exchange\":\"NYSE\",\"ts\":$((base_ts + 400))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/MarketBTick" -m "{\"symbol\":\"MSFT\",\"price\":386.00,\"volume\":1000,\"exchange\":\"NASDAQ\",\"ts\":$((base_ts + 500))}"

    # TSLA prices with >1% spread
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/MarketATick" -m "{\"symbol\":\"TSLA\",\"price\":250.00,\"volume\":2000,\"exchange\":\"NYSE\",\"ts\":$((base_ts + 600))}"
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/input/MarketBTick" -m "{\"symbol\":\"TSLA\",\"price\":255.00,\"volume\":1800,\"exchange\":\"NASDAQ\",\"ts\":$((base_ts + 700))}"
}

run_scenario() {
    local scenario_num=$1
    local scenario_name=$2
    local vpl_file=$3
    local flink_class=$4
    local generate_func=$5
    local wait_time=${6:-10}

    print_header "Scenario $scenario_num: $scenario_name"

    local log_file="$RESULTS_DIR/scenario${scenario_num}_$(date +%Y%m%d_%H%M%S).log"

    # Start output collector
    print_info "Starting output collectors..."
    local varpulis_alerts=0
    local flink_alerts=0

    # Clear any retained messages
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/output/varpulis" -n -r 2>/dev/null || true
    mosquitto_pub -h $MQTT_BROKER -t "benchmark/output/flink" -n -r 2>/dev/null || true

    # Start Varpulis
    print_info "Starting Varpulis..."
    $VARPULIS_BIN run -f "$vpl_file" > "$RESULTS_DIR/varpulis_s${scenario_num}.out" 2>&1 &
    local varpulis_pid=$!

    # Start Flink
    print_info "Starting Flink (${TIME_MODE} time)..."
    java -cp "$FLINK_JAR" "com.benchmark.flink.${flink_class}" "$TIME_MODE" > "$RESULTS_DIR/flink_s${scenario_num}.out" 2>&1 &
    local flink_pid=$!

    # Wait for systems to initialize
    print_info "Waiting for systems to initialize (5s)..."
    sleep 5

    # Check if processes are running
    if ! kill -0 $varpulis_pid 2>/dev/null; then
        print_error "Varpulis failed to start. Check $RESULTS_DIR/varpulis_s${scenario_num}.out"
        kill $flink_pid 2>/dev/null || true
        return 1
    fi

    if ! kill -0 $flink_pid 2>/dev/null; then
        print_error "Flink failed to start. Check $RESULTS_DIR/flink_s${scenario_num}.out"
        kill $varpulis_pid 2>/dev/null || true
        return 1
    fi

    print_success "Both systems running"

    # Generate test events
    print_info "Generating test events..."
    $generate_func

    # Wait for processing
    print_info "Waiting for results (${wait_time}s)..."
    sleep $wait_time

    # Collect results from output files
    if [[ -f "$RESULTS_DIR/varpulis_s${scenario_num}.out" ]]; then
        varpulis_alerts=$(grep -c "emit\|alert\|ALERT\|published" "$RESULTS_DIR/varpulis_s${scenario_num}.out" 2>/dev/null || echo "0")
    fi

    if [[ -f "$RESULTS_DIR/flink_s${scenario_num}.out" ]]; then
        flink_alerts=$(grep -c "Published\|ALERT\|alert_type" "$RESULTS_DIR/flink_s${scenario_num}.out" 2>/dev/null || echo "0")
    fi

    # Stop processes
    print_info "Stopping processes..."
    kill $varpulis_pid 2>/dev/null || true
    kill $flink_pid 2>/dev/null || true
    wait $varpulis_pid 2>/dev/null || true
    wait $flink_pid 2>/dev/null || true

    # Report results
    echo ""
    echo "Results for Scenario $scenario_num ($scenario_name):"
    echo "  Varpulis: Check $RESULTS_DIR/varpulis_s${scenario_num}.out"
    echo "  Flink:    Check $RESULTS_DIR/flink_s${scenario_num}.out"
    echo ""

    return 0
}

run_all_scenarios() {
    print_header "Running All Scenarios ($TIME_MODE time)"

    local timestamp=$(date +%Y%m%d_%H%M%S)

    # Scenario 1: Aggregation
    if [[ "$SPECIFIC_SCENARIO" == "all" || "$SPECIFIC_SCENARIO" == "1" ]]; then
        run_scenario 1 "PageView Aggregation" \
            "$SCRIPT_DIR/scenario1-aggregation/varpulis.vpl" \
            "Scenario1FlinkMqtt" \
            "generate_scenario1_events" \
            35  # Need to wait for window to close (30s slide)
    fi

    # Scenario 2: Sequence Pattern
    if [[ "$SPECIFIC_SCENARIO" == "all" || "$SPECIFIC_SCENARIO" == "2" ]]; then
        run_scenario 2 "Login -> FailedTransaction Sequence" \
            "$SCRIPT_DIR/scenario2-sequence/varpulis.vpl" \
            "Scenario2FlinkMqtt" \
            "generate_scenario2_events" \
            10
    fi

    # Scenario 3: Fraud Detection
    if [[ "$SPECIFIC_SCENARIO" == "all" || "$SPECIFIC_SCENARIO" == "3" ]]; then
        run_scenario 3 "Multi-Event Fraud Pattern" \
            "$SCRIPT_DIR/scenario3-fraud/varpulis.vpl" \
            "Scenario3FlinkMqtt" \
            "generate_scenario3_events" \
            30  # Fraud pattern needs more time
    fi

    # Scenario 4: Stream Join (Arbitrage)
    if [[ "$SPECIFIC_SCENARIO" == "all" || "$SPECIFIC_SCENARIO" == "4" ]]; then
        run_scenario 4 "Market Data Join (Arbitrage)" \
            "$SCRIPT_DIR/scenario4-join/varpulis.vpl" \
            "Scenario4FlinkMqtt" \
            "generate_scenario4_events" \
            5
    fi
}

generate_summary() {
    print_header "Benchmark Summary"

    echo "Results saved to: $RESULTS_DIR/"
    echo ""
    echo "Output files:"
    ls -la "$RESULTS_DIR"/*.out 2>/dev/null || echo "  No output files yet"
    echo ""
    echo "To view Varpulis output: cat $RESULTS_DIR/varpulis_s*.out"
    echo "To view Flink output:    cat $RESULTS_DIR/flink_s*.out"
}

main() {
    print_header "Varpulis vs Flink CEP Benchmark"
    echo "Time Mode: $TIME_MODE"
    echo "Scenarios: $SPECIFIC_SCENARIO"
    echo ""

    check_prerequisites
    cleanup_processes
    run_all_scenarios
    generate_summary

    print_header "Benchmark Complete"
}

main
