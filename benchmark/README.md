# LoomQ Benchmark Suite

性能基准测试套件，覆盖 `loomq-core` 的创建吞吐、投递吞吐、触发精度三个维度。

基准以 JUnit 测试形式位于 `loomq-core`（`@Tag("benchmark")`），由薄 runner 脚本驱动并汇总为 Markdown 报告。

## 快速开始

```bash
# Windows
benchmark\scripts\benchmark.ps1

# Linux/macOS
./benchmark/scripts/benchmark.sh

# 快速验证（跳过耗时最长的 Precision 基准）
benchmark\scripts\benchmark.ps1 -Quick
./benchmark/scripts/benchmark.sh --quick
```

输出：
- **MD 报告**: `benchmark/results/reports/benchmark-report-{timestamp}.md`
- **原始日志**: `benchmark/results/logs/benchmark-{timestamp}.log`

---

## 目录结构

```
benchmark/
├── README.md
├── config.json                  # SLO 阈值配置
├── scripts/
│   ├── benchmark.ps1            # Windows 薄 runner
│   └── benchmark.sh             # Linux/macOS 薄 runner
└── results/                     # 运行产物（gitignore）
    ├── reports/                 # Markdown 报告
    └── logs/                    # 原始日志 + RESULT 标记
```

---

## 基准测试类

| 类 | 测试 | 输出 RESULT 标记 |
|----|------|------------------|
| `CreateIntentBenchmark` | createIntent / createIntents 批量 DURABLE 吞吐 | `RESULT\|create\|` |
| `DeliveryPathBenchmark` | 4 档（ULTRA/FAST/STANDARD/MILLI）schedule→deliver→ACKED 吞吐与延迟 + 档位配置盘点 | `RESULT\|delivery\|` / `RESULT\|tier_config\|` |
| `PrecisionLatencyBenchmark` | 全 4 档（MILLI/ULTRA/FAST/STANDARD）触发精度 p50/p99/p999 + 空闲 CPU | `RESULT\|precision\|` |

MILLI 档为 1ms 事件驱动直插桶，吞吐语义与批量档不同（单发、信号驱动），但同一套投递测量逻辑适用。

---

## 用法

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-Quick` / `--quick` | 快速模式（跳过 Precision 基准） | false |
| `-Scenario <name>` / `--scenario=<name>` | 场景: all / create / delivery / precision | all |
| `-NoCompile` / `--no-compile` | 跳过编译 | false |
| `-Compare` / `--compare` | 查看最近一次报告 | false |
| `-SweepConsumers` / `--sweep-consumers` | 消费者数扫参 | false |
| `-JavaHome <path>` | JDK 路径 (ps1; sh 用 LOOMQ_JAVA_HOME 环境变量) | 见各脚本默认 |

### 示例

```powershell
# 全量测试
.\benchmark\scripts\benchmark.ps1

# 快速验证
.\benchmark\scripts\benchmark.ps1 -Quick

# 仅投递吞吐
.\benchmark\scripts\benchmark.ps1 -Scenario delivery

# 仅触发精度
.\benchmark\scripts\benchmark.ps1 -Scenario precision

# 查看上次报告
.\benchmark\scripts\benchmark.ps1 -Compare
```

---

## 报告格式

MD 报告包含五部分：

| 章节 | 内容 |
|------|------|
| 环境 | 时间、Commit、分支、Java、OS、CPU |
| 创建吞吐 | 单发/批量 QPS、耗时 |
| 投递吞吐 | 每档 QPS、create/delivery 耗时、wake/E2E p50/p95/p99、overhead p99、SLO 通过/失败 |
| 档位资源盘点 | 每档扫描模式、消费者数、队列容量、最大并发、窗口、批量大小（精简决策的收益侧） |
| 触发精度 | 每档（全 4 档）p50/p99/p999 |

投递的 SLO 通过/失败对照 `config.json` 中该档的 `p99_wakeup_ms` 与 `p99_e2e_ms`。

### 测量有效性说明（2026-08-05 迭代）

- **E2E/drain 计时**：`DeliveryPathBenchmark` 使用 `System.nanoTime()` 相对计时，
  免疫 Windows `currentTimeMillis` ~15.6ms 量化（此前 MILLI e2e 测量在 Windows 无效）。
- **fire delay 抖动**：每轮 4.7–5.3s 随机。固定 5s 与所有 fixed-rate 档的
  scanInterval 整除相位锁定，wake 退化为单相位样本；抖动后 wake 呈真实均匀分布。

---

## 配置

`config.json` 定义各档位 SLO（毫秒）：

```json
{
  "slo": {
    "ULTRA":    { "p95_wakeup_ms": 15,  "p99_wakeup_ms": 25,  "p95_e2e_ms": 50,  "p99_e2e_ms": 100 },
    "MILLI":    { "p95_wakeup_ms": 1,   "p99_wakeup_ms": 5,   "p95_e2e_ms": 5,   "p99_e2e_ms": 20 }
  },
  "rotation": { "keep_recent": 10 }
}
```

**wake SLO 校准规则**：fixed-rate 档（FAST/STANDARD）的 wake 延迟
结构上呈 uniform[0, window]（桶下取整 + 固定轮询拾取），任何低于 window 的
wake SLO 都会在某个分位上必然失败。因此 fixed-rate 档按
`p95_wakeup = 1.0 × window`、`p99_wakeup = 1.2 × window` 校准——其判别目标是
扫描线程饥饿等病态回归（wake 超出窗口），而非窗口内量化。adaptive 档
（MILLI/ULTRA）使用绝对小值阈值。e2e SLO 不受此影响。

---

## 依赖

| 工具 | 用途 | 必需 |
|------|------|------|
| JDK 25+ | 编译和运行 | 是 |
| Maven 3.9+ | 构建系统 | 是 |

无 Python / Excel / 服务器依赖。

---

## 常见问题

**Q: 如何只运行调度器/投递测试？**
```powershell
.\benchmark\scripts\benchmark.ps1 -Scenario delivery
```

**Q: 如何查看历史对比？**
```powershell
.\benchmark\scripts\benchmark.ps1 -Compare
```

**Q: 如何清理旧报告？**
报告自动轮转（保留最近 10 份）。手动清理：
```bash
rm -rf benchmark/results/reports/*
rm -rf benchmark/results/logs/*
```
