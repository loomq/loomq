<#
.SYNOPSIS
    LoomQ 性能基准测试 (Windows)

.DESCRIPTION
    运行 loomq-core 的 JUnit 基准测试 (CreateIntent / DeliveryPath / PrecisionLatency)，
    解析 RESULT 标记并生成 Markdown 报告。无 Python/Excel 依赖。

.PARAMETER Quick
    快速模式 (跳过 Precision 基准)

.PARAMETER Scenario
    场景: all / create / delivery / precision

.PARAMETER NoCompile
    跳过编译

.PARAMETER Compare
    查看最近一次报告

.EXAMPLE
    .\benchmark.ps1
    .\benchmark.ps1 -Quick
    .\benchmark.ps1 -Scenario delivery
#>

param(
    [switch]$Quick,
    [string]$Scenario = "all",
    [switch]$NoCompile,
    [switch]$Compare,
    [string]$SweepConsumers = "",
    [string]$JavaHome = "D:\Development\JDKs\jdk-25.0.4"
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# ---- paths ----
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
$ResultsDir = Join-Path $ProjectRoot "benchmark\results"
$ReportsDir = Join-Path $ResultsDir "reports"
$LogsDir = Join-Path $ResultsDir "logs"
$ConfigPath = Join-Path $ProjectRoot "benchmark\config.json"
New-Item -ItemType Directory -Force -Path $ReportsDir, $LogsDir | Out-Null

# ---- pin JDK (25.0.4) ----
if (Test-Path "$JavaHome\bin\java.exe") {
    $env:JAVA_HOME = $JavaHome
    $env:Path = "$JavaHome\bin;" + $env:Path
    Write-Host ">>> 使用 JDK: $JavaHome"
} else {
    Write-Warning "JDK 路径未找到: $JavaHome —— 退回使用 PATH 上的 java/mvn。"
}

$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$DateIso = Get-Date -Format "yyyy-MM-ddTHH:mm:ss"
$Commit = try { (git -C $ProjectRoot rev-parse --short HEAD 2>$null).Trim() } catch { "unknown" }
$Branch = try { (git -C $ProjectRoot rev-parse --abbrev-ref HEAD 2>$null).Trim() } catch { "unknown" }
$JavaVer = (& java -version 2>&1 | Select-Object -First 1)
$OsName = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
$CpuCores = [System.Environment]::ProcessorCount

# ---- scenario -> test select ----
switch ($Scenario) {
    "all" { $TestSelect = "*Benchmark" }
    "create" { $TestSelect = "CreateIntentBenchmark" }
    "delivery" { $TestSelect = "DeliveryPathBenchmark" }
    "precision" { $TestSelect = "PrecisionLatencyBenchmark" }
    default { Write-Error "未知场景: $Scenario (all/create/delivery/precision)" }
}
if ($Quick) { $TestSelect = "CreateIntentBenchmark,DeliveryPathBenchmark" }

# ---- compare mode ----
if ($Compare) {
    $LatestMd = Get-ChildItem -Path $ReportsDir -Filter "benchmark-report-*.md" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($LatestMd) {
        Write-Host "=== 最新报告: $($LatestMd.FullName) ==="
        Write-Host ""
        Get-Content $LatestMd.FullName -Encoding UTF8
    } else {
        Write-Host "未找到报告。请先运行一次基准测试。"
    }
    exit 0
}

# ---- compile ----
if (-not $NoCompile) {
    Write-Host ">>> 编译项目..."
    Push-Location $ProjectRoot
    try { & mvn test-compile -q } finally { Pop-Location }
    if ($LASTEXITCODE -ne 0) { Write-Error "编译失败" }
}

# ---- run ----
Write-Host ">>> 运行基准测试 (场景: $Scenario, 测试: $TestSelect)"
$LogFile = Join-Path $LogsDir "benchmark-$Timestamp.log"
Push-Location $ProjectRoot
try {
    & mvn test -pl loomq-core "-Dtest=$TestSelect" "-Dtest.excludedGroups=" "-Dsweep.consumers=$SweepConsumers" *> $LogFile
} finally {
    Pop-Location
}
$MvnExit = $LASTEXITCODE

# ---- extract RESULT markers ----
$ResultFile = Join-Path $LogsDir "raw-$Timestamp.txt"
Select-String -Path $LogFile -Pattern '^RESULT\|' | ForEach-Object { $_.Line } | Set-Content -Path $ResultFile -Encoding UTF8

# helper: extract key=value from a RESULT line
function Get-Kv {
    param([string]$Line, [string]$Key)
    $m = [regex]::Match($Line, "\|$Key=([^|]*)")
    if ($m.Success) { return $m.Groups[1].Value } else { return "" }
}

# helper: read an SLO value for a tier from config.json
function Get-Slo {
    param([string]$Tier, [string]$Key)
    $cfg = Get-Content $ConfigPath -Raw | ConvertFrom-Json
    $t = $cfg.slo.$Tier
    if ($null -ne $t) { return "$($t.$Key)" } else { return "" }
}

# ---- generate Markdown report ----
$MdFile = Join-Path $ReportsDir "benchmark-report-$Timestamp.md"
$report = @()
$report += "# LoomQ Benchmark 报告"
$report += ""
$report += "**时间**: $DateIso"
$report += "**Commit**: ``$Commit`` | **分支**: $Branch"
$report += "**Java**: $JavaVer | **OS**: $OsName | **CPU**: $CpuCores cores"
$report += ""
$report += "## 创建吞吐"
$report += ""
$report += "| 模式 | QPS(median) | QPS(IQR) | samples |"
$report += "|------|-------------|----------|---------|"
foreach ($line in Get-Content $ResultFile -Encoding UTF8) {
    if ($line -match '^RESULT\|create\|') {
        $report += "| $(Get-Kv $line batch) | $(Get-Kv $line qps_median) | $(Get-Kv $line qps_iqr) | $(Get-Kv $line samples) |"
    }
}
$report += ""
$report += "## 投递吞吐"
$report += ""
$report += "| 档位 | QPS(median) | QPS(IQR) | wake p50/p99 | e2e p50/p99 | overhead p99 | SLO(wake p99) | SLO(e2e p99) |"
$report += "|------|-------------|----------|--------------|-------------|--------------|----------------|---------------|"
foreach ($line in Get-Content $ResultFile -Encoding UTF8) {
    if ($line -match '^RESULT\|delivery\|') {
        $tier = Get-Kv $line "tier"
        $w9 = Get-Kv $line "wake_p99_ms"
        $e9 = Get-Kv $line "e2e_p99_ms"
        $wt = Get-Slo $tier "p99_wakeup_ms"
        $et = Get-Slo $tier "p99_e2e_ms"
        $wpass = "PASS"; $epass = "PASS"
        if ($wt -and $w9 -and [int]$w9 -gt [int]$wt) { $wpass = "FAIL" }
        if ($et -and $e9 -and [int]$e9 -gt [int]$et) { $epass = "FAIL" }
        $report += "| $tier | $(Get-Kv $line qps_median) | $(Get-Kv $line qps_iqr) | $(Get-Kv $line wake_p50_ms)/$w9 | $(Get-Kv $line e2e_p50_ms)/$e9 | $(Get-Kv $line overhead_p99_ms) | $wpass | $epass |"
    }
}
$report += ""
$report += "## 档位资源盘点"
$report += ""
$report += "| 档位 | 扫描模式 | 消费者数 | 队列容量 | 最大并发 | 窗口(ms) | 批量大小 |"
$report += "|------|----------|----------|----------|----------|----------|----------|"
foreach ($line in Get-Content $ResultFile -Encoding UTF8) {
    if ($line -match '^RESULT\|tier_config\|') {
        $report += "| $(Get-Kv $line tier) | $(Get-Kv $line scanner) | $(Get-Kv $line consumers) | $(Get-Kv $line queue_capacity) | $(Get-Kv $line max_concurrency) | $(Get-Kv $line window_ms) | $(Get-Kv $line batch_size) |"
    }
}
$report += ""
$report += "## 消费者数扫参"
$report += ""
$report += "| 档位 | consumers | QPS(median) | QPS(IQR) | e2e p99 |"
$report += "|------|-----------|-------------|----------|---------|"
foreach ($line in Get-Content $ResultFile -Encoding UTF8) {
    if ($line -match '^RESULT\|delivery\|' -and $line -match 'consumers=') {
        $report += "| $(Get-Kv $line tier) | $(Get-Kv $line consumers) | $(Get-Kv $line qps_median) | $(Get-Kv $line qps_iqr) | $(Get-Kv $line e2e_p99_ms) |"
    }
}
$report += ""
$report += "## 触发精度"
$report += ""
$report += "| 档位 | p50(ms) | p99(ms) | p999(ms) |"
$report += "|------|---------|---------|----------|"
foreach ($line in Get-Content $ResultFile -Encoding UTF8) {
    if ($line -match '^RESULT\|precision\|') {
        $report += "| $(Get-Kv $line tier) | $(Get-Kv $line p50_ms) | $(Get-Kv $line p99_ms) | $(Get-Kv $line p999_ms) |"
    }
}
$report += ""
$report | Set-Content -Path $MdFile -Encoding UTF8

# ---- rotate reports (keep recent N) ----
$Keep = 10
$rc = Get-Content $ConfigPath -Raw | ConvertFrom-Json
if ($rc.rotation.keep_recent) { $Keep = [int]$rc.rotation.keep_recent }
$old = Get-ChildItem -Path $ReportsDir -Filter "benchmark-report-*.md" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -Skip $Keep
foreach ($f in $old) { Remove-Item $f.FullName -Force -ErrorAction SilentlyContinue }

Write-Host ""
Write-Host "报告已生成: $MdFile"
Write-Host "使用 -Compare 查看最近报告。"

exit $MvnExit
