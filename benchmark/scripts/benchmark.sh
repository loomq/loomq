#!/bin/bash
# LoomQ 性能基准测试 (Linux/macOS)
#
# 用法:
#   ./benchmark.sh                       # 全量 (Create + Delivery 4 档 + Precision)
#   ./benchmark.sh --quick               # 快速 (跳过耗时最长的 Precision)
#   ./benchmark.sh --scenario=create     # 仅 CreateIntentBenchmark
#   ./benchmark.sh --scenario=delivery   # 仅 DeliveryPathBenchmark
#   ./benchmark.sh --scenario=precision  # 仅 PrecisionLatencyBenchmark
#   ./benchmark.sh --compare             # 查看最近一次报告
#
# 输出:
#   benchmark/results/reports/benchmark-report-{timestamp}.md
#
# 依赖: JDK 25+ / Maven 3.9+。无 Python/Excel 依赖。

set -euo pipefail

QUICK=false
SCENARIO="all"
NO_COMPILE=false
COMPARE=false
SWEEP_CONSUMERS=""

for arg in "$@"; do
    case $arg in
        --quick) QUICK=true ;;
        --scenario=*) SCENARIO="${arg#*=}" ;;
        --no-compile) NO_COMPILE=true ;;
        --compare) COMPARE=true ;;
        --sweep-consumers=*) SWEEP_CONSUMERS="${arg#*=}" ;;
        --help|-h)
            echo "LoomQ 性能基准测试"
            echo ""
            echo "用法: ./benchmark.sh [选项]"
            echo "  --quick              快速模式 (跳过 Precision 基准)"
            echo "  --scenario=NAME      场景: all / create / delivery / precision"
            echo "  --no-compile         跳过编译"
            echo "  --compare            查看最近一次报告"
            echo "  --sweep-consumers=N  消费者数扫参 (追加 -Dsweep.consumers=N)"
            echo "  --help               显示帮助"
            exit 0
            ;;
        *)
            echo "未知选项: $arg"
            echo "使用 --help 查看帮助。"
            exit 1
            ;;
    esac
done

# ---- pin JDK (25.0.4) ----
LOOMQ_JAVA_HOME="${LOOMQ_JAVA_HOME:-/usr/lib/jvm/temurin-25.0.4}"
if [ -x "$LOOMQ_JAVA_HOME/bin/java" ]; then
    JAVA_HOME="$LOOMQ_JAVA_HOME"
    export JAVA_HOME
    export PATH="$JAVA_HOME/bin:$PATH"
    echo ">>> 使用 JDK: $JAVA_HOME"
else
    echo "WARN: JDK 路径未找到: $LOOMQ_JAVA_HOME —— 退回使用 PATH 上的 java/mvn" >&2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/benchmark/results"
REPORTS_DIR="$RESULTS_DIR/reports"
LOGS_DIR="$RESULTS_DIR/logs"
CONFIG="$PROJECT_ROOT/benchmark/config.json"
mkdir -p "$REPORTS_DIR" "$LOGS_DIR"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
DATE_ISO=$(date +%Y-%m-%dT%H:%M:%S)
COMMIT=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")
BRANCH=$(git -C "$PROJECT_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
JAVA_VER=$(java -version 2>&1 | head -1 || echo "unknown")
OS_NAME=$(uname -srm)
CPU_CORES=$(nproc 2>/dev/null || echo "?")

# scenario -> test class selection
case "$SCENARIO" in
    all) TEST_SELECT="*Benchmark" ;;
    create) TEST_SELECT="CreateIntentBenchmark" ;;
    delivery) TEST_SELECT="DeliveryPathBenchmark" ;;
    precision) TEST_SELECT="PrecisionLatencyBenchmark" ;;
    *) echo "未知场景: $SCENARIO (all/create/delivery/precision)"; exit 1 ;;
esac
if [ "$QUICK" = true ]; then
    TEST_SELECT="CreateIntentBenchmark,DeliveryPathBenchmark"
fi

# ---- compare mode ----
if [ "$COMPARE" = true ]; then
    LATEST=$(ls -1t "$REPORTS_DIR"/benchmark-report-*.md 2>/dev/null | head -1)
    if [ -n "$LATEST" ]; then
        echo "=== 最新报告: $LATEST ==="
        echo ""
        cat "$LATEST"
    else
        echo "未找到报告。请先运行一次基准测试。"
    fi
    exit 0
fi

# ---- compile ----
if [ "$NO_COMPILE" = false ]; then
    echo ">>> 编译项目..."
    (cd "$PROJECT_ROOT" && mvn test-compile -q)
fi

# ---- run ----
echo ">>> 运行基准测试 (场景: $SCENARIO, 测试: $TEST_SELECT)"
LOG_FILE="$LOGS_DIR/benchmark-$TIMESTAMP.log"
set +e
SWEEP_ARGS=()
[ -n "$SWEEP_CONSUMERS" ] && SWEEP_ARGS=("-Dsweep.consumers=$SWEEP_CONSUMERS")
(cd "$PROJECT_ROOT" && mvn test -pl loomq-core "-Dtest=$TEST_SELECT" "-Dtest.excludedGroups=" "${SWEEP_ARGS[@]}" > "$LOG_FILE" 2>&1)
MVN_EXIT=$?
set -e

# ---- extract RESULT markers ----
RESULT_FILE="$LOGS_DIR/raw-$TIMESTAMP.txt"
grep -E "^RESULT\|" "$LOG_FILE" > "$RESULT_FILE" || true

# helper: extract key=value from a RESULT line (split by |)
kv() {
    local line="$1" key="$2"
    echo "$line" | tr '|' '\n' | sed -n "s/^$key=//p" | head -1
}

# helper: read an SLO value for a tier from config.json
slo_get() {
    local tier="$1" key="$2"
    grep "\"$tier\"" "$CONFIG" 2>/dev/null |
        sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p" | head -1 || true
}

# ---- generate Markdown report ----
MD_FILE="$REPORTS_DIR/benchmark-report-$TIMESTAMP.md"
{
    echo "# LoomQ Benchmark 报告"
    echo ""
    echo "**时间**: $DATE_ISO"
    echo "**Commit**: \`$COMMIT\` | **分支**: $BRANCH"
    echo "**Java**: $JAVA_VER | **OS**: $OS_NAME | **CPU**: $CPU_CORES cores"
    echo ""

    echo "## 创建吞吐"
    echo ""
    echo "| 模式 | QPS(median) | QPS(IQR) | samples |"
    echo "|------|-------------|----------|---------|"
    while IFS= read -r line; do
        case "$line" in
            RESULT\|create\|*)
                echo "| $(kv "$line" batch) | $(kv "$line" qps_median) | $(kv "$line" qps_iqr) | $(kv "$line" samples) |"
                ;;
        esac
    done < "$RESULT_FILE"
    echo ""

    echo "## 投递吞吐"
    echo ""
    echo "| 档位 | QPS(median) | QPS(IQR) | wake p50/p99 | e2e p50/p99 | overhead p99 | SLO(wake p99) | SLO(e2e p99) |"
    echo "|------|-------------|----------|--------------|-------------|--------------|----------------|---------------|"
    while IFS= read -r line; do
        case "$line" in
            RESULT\|delivery\|*)
                tier=$(kv "$line" tier)
                w9=$(kv "$line" wake_p99_ms)
                e9=$(kv "$line" e2e_p99_ms)
                wt=$(slo_get "$tier" p99_wakeup_ms)
                et=$(slo_get "$tier" p99_e2e_ms)
                wpass="PASS"; epass="PASS"
                [ -n "$wt" ] && [ "$w9" -gt "$wt" ] && wpass="FAIL"
                [ -n "$et" ] && [ "$e9" -gt "$et" ] && epass="FAIL"
                echo "| $tier | $(kv "$line" qps_median) | $(kv "$line" qps_iqr) | $(kv "$line" wake_p50_ms)/$w9 | $(kv "$line" e2e_p50_ms)/$e9 | $(kv "$line" overhead_p99_ms) | $wpass | $epass |"
                ;;
        esac
    done < "$RESULT_FILE"
    echo ""

    echo "## 档位资源盘点"
    echo ""
    echo "| 档位 | 扫描模式 | 消费者数 | 队列容量 | 最大并发 | 窗口(ms) | 批量大小 |"
    echo "|------|----------|----------|----------|----------|----------|----------|"
    while IFS= read -r line; do
        case "$line" in
            RESULT\|tier_config\|*)
                echo "| $(kv "$line" tier) | $(kv "$line" scanner) | $(kv "$line" consumers) | $(kv "$line" queue_capacity) | $(kv "$line" max_concurrency) | $(kv "$line" window_ms) | $(kv "$line" batch_size) |"
                ;;
        esac
    done < "$RESULT_FILE"
    echo ""

    echo "## 消费者数扫参"
    echo ""
    echo "| 档位 | consumers | QPS(median) | QPS(IQR) | e2e p99 |"
    echo "|------|-----------|-------------|----------|---------|"
    while IFS= read -r line; do
        case "$line" in
            RESULT\|delivery\|*consumers=*)
                echo "| $(kv "$line" tier) | $(kv "$line" consumers) | $(kv "$line" qps_median) | $(kv "$line" qps_iqr) | $(kv "$line" e2e_p99_ms) |"
                ;;
        esac
    done < "$RESULT_FILE"
    echo ""

    echo "## 触发精度"
    echo ""
    echo "| 档位 | p50(ms) | p99(ms) | p999(ms) |"
    echo "|------|---------|---------|----------|"
    while IFS= read -r line; do
        case "$line" in
            RESULT\|precision\|*)
                echo "| $(kv "$line" tier) | $(kv "$line" p50_ms) | $(kv "$line" p99_ms) | $(kv "$line" p999_ms) |"
                ;;
        esac
    done < "$RESULT_FILE"
    echo ""
} > "$MD_FILE"

# ---- rotate reports (keep recent N) ----
KEEP=10
SC=$(grep -o '"keep_recent"[[:space:]]*:[[:space:]]*[0-9]*' "$CONFIG" 2>/dev/null | grep -o '[0-9]*' | head -1)
if [ -n "$SC" ]; then KEEP=$SC; fi
ls -1t "$REPORTS_DIR"/benchmark-report-*.md 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f --

echo ""
echo "报告已生成: $MD_FILE"
echo "使用 --compare 查看最近报告。"

exit "$MVN_EXIT"
