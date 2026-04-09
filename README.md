# 📈 Stock Analysis Pipeline

> 基于理杏仁 API 的 A 股数据采集与量化分析系统，聚焦于「换手率异动 + 均线位置」信号的多维度回测研究。

---

## 目录

- [项目概览](#项目概览)
- [目录结构](#目录结构)
- [数据流](#数据流)
- [快速开始](#快速开始)
- [模块说明](#模块说明)
  - [fetchers — 数据抓取](#fetchers--数据抓取)
  - [tools — 工具脚本](#tools--工具脚本)
  - [strategy — 信号扫描](#strategy--信号扫描)
  - [analyze — 回测分析](#analyze--回测分析)
- [Docker 部署](#docker-部署)
- [数据库结构](#数据库结构)
- [分析维度说明](#分析维度说明)

---

## 项目概览

本项目完整实现了以下流程：

```
理杏仁 API → 数据入库 → 均线计算 → 信号扫描 → 多维度回测分析 → Markdown / Excel 报告
```

核心策略：在申万 2021 三级行业龙头股（市值 Top-3）中，筛选满足以下条件的「换手率异动启动日」：

| 条件 | 参数 |
|------|------|
| 近 5 日均换手率 / 基准 30 日均换手率 | 1.5x ～ 4.0x |
| 启动日（Day0）涨幅 | ≥ 4% |

随后跟踪 Day1 ～ Day9 的价格与换手率表现，并按 4 个维度（均线位置 × 基准期强弱 × 触发倍数 × 启动涨幅）进行分组统计。

---

## 目录结构

```
/data/stock/
├── fetchers/                   # 数据抓取模块（理杏仁 API）
│   ├── fetch_stocks.py         # 获取 A 股股票列表
│   ├── fetch_industries.py     # 获取申万行业归属
│   ├── fetch_fundamentals.py   # 获取市值等基本面数据
│   ├── fetch_fs.py             # 获取财务报表数据
│   └── fetch_klines.py         # 获取日 K 线数据（含断点续传）
│
├── tools/                      # 工具脚本
│   ├── compute_ma.py           # 计算 MA5/20/60/200，写入 moving_averages 表
│   ├── build_calendar.py       # 构建交易日历（trade_calendar 表 + CSV）
│   └── check_daily_sentiment.py  # 查看每日市场情绪分布（辅助分析用）
│
├── strategy/                   # 信号扫描策略
│   └── turnover_surge.py       # 换手率异动信号扫描 → 写入 turnover_surge.db
│
├── analyze/                    # 回测分析报告
│   ├── analyze_v5_1.py         # 4D 分组回测（全局，4320 种组合）
│   ├── analyze_v5_2.py         # 极端情绪日专项分析（Top/Bottom 15%）
│   └── analyze_v5_3.py         # 峰值/谷值到达时间分析（止盈止损辅助）
│
├── output/                     # 分析输出（.md 报告 + .xlsx 表格）
│   ├── analysis_v5_1.md / .xlsx
│   ├── analysis_v5_2.md / .xlsx
│   └── analysis_v5_3.md / .xlsx
│
├── fetch_all.py                # 批量调度入口（依次运行所有 fetcher）
├── scheduler.py                # Docker 定时调度器（每日 01:00 触发）
├── Dockerfile                  # 容器构建文件
├── docker-compose.yml          # Portainer / docker compose 部署配置
├── requirements.txt            # Python 依赖
├── stock_data.db               # 主数据库（K线、基本面、行业等）
└── turnover_surge.db           # 策略信号数据库
```

---

## 数据流

```
                    ┌─────────────────────────┐
                    │    理杏仁 Open API        │
                    └────────────┬────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
     fetch_stocks.py   fetch_klines.py    fetch_fundamentals.py ...
              │                  │                  │
              └──────────────────┴──────────────────┘
                                 │
                         stock_data.db
                                 │
              ┌──────────────────┼──────────────────┐
              │                                     │
     compute_ma.py                        build_calendar.py
     (moving_averages 表)                 (trade_calendar 表)
              │                                     │
              └──────────────────┬──────────────────┘
                                 │
                      strategy/turnover_surge.py
                                 │
                         turnover_surge.db
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
     analyze_v5_1.py   analyze_v5_2.py   analyze_v5_3.py
              │                  │                  │
              └──────────────────┴──────────────────┘
                                 │
                           output/*.md
                           output/*.xlsx
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
# 分析模块还需要：
pip install pandas numpy openpyxl
```

### 2. 配置 API Token

```bash
export LIXINGER_TOKEN="你的理杏仁Token"
```

### 3. 初次数据采集（全量）

```bash
# 一键运行全部抓取脚本
python fetch_all.py

# 或分步运行
python fetchers/fetch_stocks.py
python fetchers/fetch_industries.py
python fetchers/fetch_fundamentals.py
python fetchers/fetch_fs.py
python fetchers/fetch_klines.py
```

### 4. 构建交易日历 & 计算均线

```bash
python tools/build_calendar.py
python tools/compute_ma.py
```

### 5. 运行信号扫描

```bash
# 扫描全部历史日期
python strategy/turnover_surge.py --start 2020-01-01 --end 2026-04-09

# 只扫描指定日期
python strategy/turnover_surge.py --day0 2025-12-31
```

### 6. 生成分析报告

```bash
# 4D 分组回测（输出到 output/analysis_v5_1.md 和 .xlsx）
python analyze/analyze_v5_1.py

# 极端情绪日分析
python analyze/analyze_v5_2.py

# 峰/谷时间分析
python analyze/analyze_v5_3.py
```

---

## 模块说明

### fetchers — 数据抓取

所有脚本支持**断点续传**和**增量更新**，可重复运行。

| 脚本 | 写入表 | 说明 |
|------|--------|------|
| `fetch_stocks.py` | `stocks` | 全量股票基础信息（代码、名称、交易所、上市状态） |
| `fetch_industries.py` | `stock_industries` | 申万 2021 三级行业归属 |
| `fetch_fundamentals.py` | `fundamentals` | 每日市值、PE、PB 等基本面数据 |
| `fetch_fs.py` | `financial_statements` | 季报/年报财务数据 |
| `fetch_klines.py` | `daily_kline` | 日 K 线（开高低收 + 换手率 + 涨跌幅），前复权 |

> **速率控制**：默认 `API_INTERVAL=0.1s`（约 900 次/分钟），触发 429 限流时自动重试 3 次。

---

### tools — 工具脚本

| 脚本 | 功能 |
|------|------|
| `compute_ma.py` | 从 `daily_kline` 计算 MA5/20/60/200，批量写入 `moving_averages` 表，支持断点续传（每批 300 只股票） |
| `build_calendar.py` | 以贵州茅台（600519）为基准提取交易日，写入 `trade_calendar` 表并导出 `trade_calendar.csv` |
| `check_daily_sentiment.py` | 统计每日产生换手率信号的股票数量，计算 15%/85% 情绪分位阈值，供分析脚本参考 |

---

### strategy — 信号扫描

#### `turnover_surge.py`

核心策略引擎。以某交易日为 **Day0（启动日）**，在每个三级行业 Top-3 市值股票中判断：

```
触发条件：
  ① 近 5 日均换手率 / 基准 30 日均换手率 ∈ [1.5x, 4.0x]
  ② Day0 涨幅 ≥ 4%

跟踪窗口：Day1 ～ Day9（9个交易日）
输出字段：基准期均线位置、Day0 快照、逐日收盘/换手/涨幅/均线
```

**用法：**

```bash
# 扫描单日
python strategy/turnover_surge.py --day0 2025-06-01

# 扫描区间
python strategy/turnover_surge.py --start 2023-01-01 --end 2025-12-31

# 不传参数则只处理最新一个交易日
python strategy/turnover_surge.py
```

结果写入 `turnover_surge.db` 的 `turnover_surge` 表，主键 `(day0, stock_code, day_offset)`，支持幂等写入。

---

### analyze — 回测分析

所有分析脚本的输出统一写入 `output/` 目录。

#### `analyze_v5_1.py` — 4D 分组回测

按以下 4 个维度的笛卡尔积（最多 8×27×5×4 = **4,320 组**）统计跟踪期表现：

| 维度 | 分组 | 逻辑 |
|------|------|------|
| **Dim A** — Day0 均线位置 | 8 组 | 收盘价与 MA20/60/200 各自上/下方，2³=8 |
| **Dim B** — 基准期均线强弱 | 27 组 | 基准期 30 天低于 MA20/60/200 的比例分三级（强/震/弱），3³=27 |
| **Dim C** — 换手率触发倍数 | 5 组 | [1.5,1.8) / [1.8,2.1) / [2.1,2.4) / [2.4,3.0) / ≥3.0 |
| **Dim D** — Day0 收盘涨幅 | 4 组 | 4-6% / 6-8% / 8%-涨停 / 涨停 |

统计指标：`max_gain`（最大涨幅）、`max_drawdown`（最大回撤）、`non_decline_rate`（非下跌日占比）及各指标的均值、极值、变异系数（CV）。

> **情绪过滤**：排除每日信号数量处于最低 15% 和最高 15% 的极端交易日，聚焦正常市场环境。

#### `analyze_v5_2.py` — 极端情绪专项

专门分析被 v5.1 **排除**的极冷/极热交易日，对比相同分组下表现差异。

#### `analyze_v5_3.py` — 峰/谷时间分析

在 A0（全均线下方）+ B(L3弱/L3弱/L3弱) + D2+D3（涨幅>6%）筛选条件下，统计：
- **peak_day**：跟踪期内达到最高收盘价的平均天数
- **trough_day**：跟踪期内达到最低收盘价的平均天数

为止盈止损策略提供时间维度参考。

---

## Docker 部署

项目已配置 Docker，适合通过 **Portainer** 管理长期运行的定时采集任务。

### 构建镜像

```bash
docker build -t stock-fetch-daily:latest .
```

### 使用 docker compose 启动

```bash
# 填写 Token 后启动
LIXINGER_TOKEN=你的Token docker compose up -d
```

或在 `docker-compose.yml` 中直接填写 Token：

```yaml
environment:
  - LIXINGER_TOKEN=你的Token
```

### 调度逻辑

容器启动后：
1. 立即执行一次全量/增量数据采集（`fetch_all.py`）
2. 之后每天 **23:00（UTC）/ 次日 07:00（CST）** 自动触发

> 如需调整时间，修改 `scheduler.py` 中的 `schedule.every().day.at("23:00")` 即可（时间为 UTC）。

### 目录挂载

容器内工作目录为 `/app`，数据库通过 volume 映射到宿主机：

```yaml
volumes:
  - /data/stock:/data   # 数据库读写路径
```

---

## 数据库结构

### `stock_data.db` — 主数据库

| 表名 | 主键 | 说明 |
|------|------|------|
| `stocks` | `stock_code` | 股票基础信息 |
| `stock_industries` | `(stock_code, source)` | 行业归属（支持多套体系） |
| `fundamentals` | `(stock_code, date)` | 每日基本面（市值等） |
| `financial_statements` | `(stock_code, date, type)` | 季报/年报财务数据 |
| `daily_kline` | `(stock_code, date)` | 日 K 线（前复权） |
| `moving_averages` | `(stock_code, date)` | MA5/20/60/200 |
| `trade_calendar` | `date` | 交易日历 |

### `turnover_surge.db` — 信号数据库

| 表名 | 主键 | 说明 |
|------|------|------|
| `turnover_surge` | `(day0, stock_code, day_offset)` | 信号事件及逐日跟踪数据 |

---

## 分析维度说明

### Dim A — Day0 均线位置（8 组）

| 组 | MA20 | MA60 | MA200 | 典型含义 |
|----|:----:|:----:|:-----:|----------|
| A0 | ↓ | ↓ | ↓ | 深底部，价格在所有均线下方 |
| A1 | ↓ | ↓ | ↑ | 位于 MA200 上方，MA60/20 下方 |
| A2 | ↓ | ↑ | ↓ | 位于 MA60 上方，MA200 下方（少见） |
| A3 | ↓ | ↑ | ↑ | 穿越 MA200/60，仍在 MA20 下 |
| A4 | ↑ | ↓ | ↓ | 位于 MA20 上方，MA60/200 下（少见） |
| A5 | ↑ | ↓ | ↑ | 位于 MA20/200 上方，MA60 下 |
| A6 | ↑ | ↑ | ↓ | 位于 MA20/60 上方，MA200 下 |
| A7 | ↑ | ↑ | ↑ | 强势，价格在所有均线上方 |

### Dim B — 基准期均线强弱（27 组）

格式：`MA20级别 / MA60级别 / MA200级别`

| 级别 | 定义 |
|------|------|
| **L1 强** | 基准期 30 天内，低于该均线的天数占比 < 1/3 |
| **L2 震** | 低于均线天数占比在 1/3 ～ 2/3 之间 |
| **L3 弱** | 低于均线天数占比 > 2/3（持续弱势） |

---

*数据来源：[理杏仁 Open API](https://open.lixinger.com/)*  
*行业分类：申万 2021 三级行业*
