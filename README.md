# 📈 股票换手率异动策略分析系统

> A-股量化选股框架，专注"黄金坑"形态挖掘：行业龙头 × 换手率异动 × 底部均线突破

---

## 目录

- [项目简介](#项目简介)
- [核心策略逻辑](#核心策略逻辑)
- [项目结构](#项目结构)
- [数据库架构](#数据库架构)
- [快速开始](#快速开始)
- [模块详解](#模块详解)
- [分析与回测](#分析与回测)
- [环境依赖](#环境依赖)
- [注意事项](#注意事项)

---

## 项目简介

本项目是一套完整的 A 股量化分析与策略回测框架，核心思路是捕捉**底部放量异动**形态——当行业龙头股在均线下方累积筹码，并伴随换手率的突然放大（1.8x–2.6x）且当日出现强势阳线突破时，识别并记录该信号，跟踪后续 10 个交易日的价格走势以评估策略效果。

**数据来源**：[理杏仁 API](https://www.lixinger.com/)（需申请 Token）  
**行业分类**：申万 2021 三级行业（`sw_2021`）  
**数据存储**：SQLite（本地无服务器依赖）

---

## 核心策略逻辑

策略筛选包含四个维度的联合过滤：

```
换手率异动 (1.5x ≤ trigger_ratio ≤ 4.0x)
    ×
启动日强势 (Day0 涨幅 ≥ 4%)
    ×
底部均线确认 (收盘价 < MA20 & MA60 & MA200)
    ×
行业龙头地位 (sw_2021 三级行业市值 Top-3)
```

> **新增：基准期均线位置分析**  
> 统计基准期（30个交易日）内收盘价高于/低于 MA20、MA60、MA200 的天数（`bl_above_ma20` / `bl_below_ma20` 等），用于后续区分「持续下跌」、「横盘整理」等不同趋势模式，进一步提升筛选精度。

### 时间窗口定义（均为交易日）

| 窗口名称 | 范围 | 长度 | 用途 |
|----------|------|------|------|
| 基准期 | Day(-35) ~ Day(-6) | 30 天 | 计算正常换手率基线；同时统计各均线上方/下方天数 |
| 观察期 | Day(-5) ~ Day(-1) | 5 天 | 计算近期异动换手率 |
| **启动日 Day0** | Day0 | 1 天 | 触发日：换手率异动倍数 1.5x~4x 且涨幅 ≥ 4% |
| 跟踪期 | Day(1) ~ Day(9) | 9 天 | 跟踪策略触发后的表现 |

触发比例 = `recent_to_r / baseline_to_r`，当前阈值区间为 **[1.5, 4.0]**（宽幅覆盖，可结合基准期均线位置进一步精细筛选）。

---

## 项目结构

```
stock/
│
├── 📥 数据获取层
│   ├── fetch_stocks.py          # 获取全量 A 股基础信息
│   ├── fetch_klines.py          # 批量下载历史 K 线（支持断点续传 & 增量更新）
│   ├── fetch_fundamentals.py    # 获取财务基本面数据（市值等）
│   ├── fetch_fs.py              # 获取财务报表数据
│   └── fetch_industries.py      # 获取股票行业分类（申万 2021）
│
├── 🔧 数据处理层
│   ├── build_calendar.py        # 生成 A 股交易日历（基于 600519 茅台）
│   └── compute_ma.py            # 计算均线（MA5 / MA20 / MA60 / MA200）
│
├── 🔍 策略分析层
│   ├── turnover_surge.py        # 换手率异动核心分析引擎（支持单日/批量）
│   ├── strategy.py              # 生产级选股扫描器（每日信号落库）
│   └── analyze_v1.py ~ v4.py   # 策略参数探索与回测分析脚本（迭代实验）
│
├── 💾 数据存储
│   ├── stock_data.db            # 主数据库（K线、基本面、行业、均线）
│   ├── turnover_surge.db        # 换手率异动分析结果库
│   └── strategy_results.db      # 每日最终选股信号库
│
├── 📊 分析报告
│   ├── analysis_summary_v1.md   # 实验一结果摘要
│   ├── analysis_summary_v2.md   # 实验二结果摘要
│   └── analysis_summary_v3.md   # 实验三结果摘要（次日涨幅 × 基准换手率交叉分析）
│
├── 📁 output/                   # 每日 CSV 分析输出目录
├── trade_calendar.csv           # 交易日历文件（build_calendar.py 生成）
└── venv/                        # Python 虚拟环境
```

---

## 数据库架构

### `stock_data.db`（主数据库）

| 表名 | 主键 | 说明 |
|------|------|------|
| `stocks` | `stock_code` | A 股基础信息（名称、交易所、上市状态等） |
| `daily_kline` | `(stock_code, date)` | 日 K 线（开高低收、成交量、换手率 `to_r`、涨跌幅） |
| `fundamentals` | `(stock_code, date)` | 财务基本面（市值 `mc` 等） |
| `stock_industries` | — | 股票行业归属（`sw_2021` 三级） |
| `moving_averages` | `(stock_code, date)` | MA5 / MA20 / MA60 / MA200 |
| `trade_calendar` | `date` | A 股交易日历 |

### `turnover_surge.db`

| 表名 | 主键 | 说明 |
|------|------|------|
| `turnover_surge` | `(day1, stock_code, day_offset)` | 每次触发信号 + 10 日跟踪期完整数据 |

### `strategy_results.db`

| 表名 | 主键 | 说明 |
|------|------|------|
| `strategy_signals` | `(date, stock_code)` | 每日生产选股输出，含均线快照 |

---

## 快速开始

### 1. 环境准备

```bash
python -m venv venv
source venv/bin/activate
pip install pandas numpy requests sqlite3
```

### 2. 配置 API Token

```bash
export LIXINGER_TOKEN="你的理杏仁API Token"
```

### 3. 数据初始化（首次运行）

按顺序执行以下脚本：

```bash
# Step 1: 获取股票列表
python fetch_stocks.py

# Step 2: 获取行业分类
python fetch_industries.py

# Step 3: 下载K线数据（约 5600 只股票，耗时较长）
python fetch_klines.py

# Step 4: 获取财务基本面（市值数据）
python fetch_fundamentals.py

# Step 5: 构建交易日历
python build_calendar.py

# Step 6: 计算均线（MA5/MA20/MA60/MA200）
python compute_ma.py
```

### 4. 日常运行

```bash
# 扫描今日选股信号（默认最新交易日）
python strategy.py

# 指定日期扫描
python strategy.py --date 2026-03-28

# 批量回溯扫描
python strategy.py --start 2025-01-01 --end 2026-03-28
```

### 5. 换手率异动深度分析

```bash
# 分析单日触发信号并跟踪后10日
python turnover_surge.py --day1 2026-03-28

# 批量分析（结果写入 turnover_surge.db）
python turnover_surge.py --start 2025-01-01 --end 2026-03-28

# 仅输出CSV，不写数据库
python turnover_surge.py --day1 2026-03-28 --no-db

# 查看行业代码样例（调试用）
python turnover_surge.py --show-industries
```

---

## 模块详解

### `fetch_klines.py` — K 线下载

- 支持**断点续传**：已是最新的股票自动跳过
- 支持**增量更新**：每只股票只拉取上次最新日期之后的新数据
- 速率控制：每次 API 调用间隔 ≥ 0.25 秒（≤ 4 次/秒）
- 复权类型：理杏仁前复权 `lxr_fc_rights`

### `compute_ma.py` — 均线计算

- 计算 MA5 / MA20 / MA60 / MA200
- 按批次（300只/批）处理，控制内存占用
- 支持断点续传，已计算的股票自动跳过

### `turnover_surge.py` — 核心分析引擎

输出字段包括：

| 字段组 | 字段 | 说明 |
|--------|------|------|
| 触发信息 | `baseline_to_r`, `recent_to_r`, `trigger_ratio` | 基准/近期换手率及倍数（阈值 1.5x~4.0x）|
| 基准期均线位置 | `bl_above_ma20/60/200`, `bl_below_ma20/60/200` | 基准期30日内收盘价高于/低于各均线的天数（新增）|
| Day0 快照 | `d0_close`, `d0_change_pct`, `d0_ma5/20/60/200` | 启动日收盘价、涨幅（≥4%）及各均线 |
| Day0 位置 | `d0_vs_ma20/60`, `d0_above_ma20/60` | 价格对均线比例及方向 |
| 跟踪期 | `day_offset`, `close`, `to_r`, `to_r_ratio` | 后9日逐日价格及换手率 |

### `strategy.py` — 生产选股扫描

- 联合过滤：换手率异动 + 日内涨幅 + 底部均线 + 行业龙头
- 结果去重后写入 `strategy_results.db`
- 支持单日、批量两种扫描模式

> `strategy.py` 的参数与 `turnover_surge.py` 独立，如需同步请手动更新 `TRIGGER_MIN/MAX` 和 `MIN_D1_CHANGE`。

---

## 分析与回测

`analyze_v1.py ~ v4.py` 是策略参数探索的迭代实验脚本，各版本分析侧重：

| 版本 | 分析重点 |
|------|----------|
| `v1` | 基础胜率统计（信号数量、未破发率、最大涨幅） |
| `v2` | 加入基准换手率分层，交叉分析胜率变化 |
| `v3` | 次日涨幅 × 基准换手率二维交叉验证 |
| `v4` | "先涨后跌" vs "先跌后涨"时序分析（最大涨幅与最大回撤的先后顺序） |

分析结果摘要见：
- [`analysis_summary_v1.md`](analysis_summary_v1.md)
- [`analysis_summary_v2.md`](analysis_summary_v2.md)
- [`analysis_summary_v3.md`](analysis_summary_v3.md)

---

## 环境依赖

- **Python**: 3.10+（使用了 `X | Y` 类型注解语法）
- **核心库**：

```
pandas >= 1.5
numpy >= 1.23
requests >= 2.28
sqlite3 (内置)
```

- **数据 API**：[理杏仁开放平台](https://www.lixinger.com/) Token（免费版支持前复权K线）

---

## 注意事项

1. **首次运行**：`fetch_klines.py` 全量下载约 5600+ 只股票，按 0.25s/请求计算，耗时约 **25-40 分钟**，请确保网络稳定。
2. **数据库大小**：`stock_data.db` 存储全量 K 线数据，大小约 **3+ GB**，请预留充足磁盘空间。
3. **Token 安全**：请勿将 `LIXINGER_TOKEN` 硬编码到脚本中，始终通过环境变量传入。
4. **均线依赖**：`strategy.py` 和 `turnover_surge.py` 依赖 `moving_averages` 表，务必在运行策略前先执行 `compute_ma.py`。
5. **行业数据**：策略使用申万 2021 (`sw_2021`) 三级行业，请确保 `fetch_industries.py` 已成功运行。
