# A股策略回测系统 (A-Share Backtesting System)

这是一个基于Python的A股股票策略回测系统。系统利用理杏仁API获取A股主板市场的历史日K线数据、股票市值信息，并执行特定策略（特别是基于“换手率倍数”与“年线位置”的策略）的回测与分析。

## 项目目标与核心策略
本系统的核心目的是通过量化手段验证特定的交易策略有效性。

**核心回测策略逻辑 (滑动窗口机制)：**
1. **基准期 (P1, 30天)**：计算该区间内个股的平均日换手率。
2. **比较期/观察期 (P2, 5天)**：计算此区间的平均换手率，并与基准期对比得出“换手比”(Ratio)。如果在P2末端，股价相对20日均线的偏离度(Bias)以及整体趋势（在250日年线之上/下）符合特定条件：
3. **结果期 (P3, 5-10天)**：进入模拟持仓期，观察该区间内的最大涨幅 (`max_gain%`) 和最大跌幅 (`max_loss%`)，评估保本率与平均最高收益。

## 项目结构与主要文件

### 1. 数据准备模块
- **`stocks_filter.py`**: 利用理杏仁API获取所有正常上市的沪深主板股票，并支持根据用户输入的最小市值要求（如50亿）进行过滤。输出文件为 `china_main_board_market_cap_filtered.csv`。
- **`tradecalender.py`**: 获取A股的实际交易日历序列（自动跳过周末与节假日），以贵州茅台(600519)的全量K线为基准提取交易日期。输出文件为 `trade_calendar.csv` (或带年份后缀)。

### 2. 回测核心模块
- **`backtest.py`**: 原始的在线回测版本。直接调用API，按照滑动窗口(P1->P2->P3)逐个日期、逐只股票获取行情进行因子计算。API调用频繁，耗时较长。
- **`backtest_offline.py`**: **推荐使用的离线多进程优化版**。需要在本地配置好SQLite数据库 (`stock_market_data.db`) 并预先下载好K线数据。利用 Python `multiprocessing` 加速回测，将回测性能提升数百倍以上。
- **`backtest2.py`**: 专项针对换手率效应的分析脚本。筛选出比较期比基准期换手率放大（ratio > 1）的股票，逐日跟踪其在结果期前5天的每日涨跌表现和胜率。

### 3. 数据与产出
- **`/output/`**: 回测结果输出目录。包括每日的报告(`*_report.csv`)和符合条件股票的详细指标数据(`*_details.csv`)。
- **`USAGE.md`**: 早期优化的笔记文档，记录了如何减少API请求提升效率的改造思路。

## 快速上手与使用

### 环境配置
项目依赖以下关键库（建议通过 `pip install -r requirements.txt` 安装，或手动安装）：
```bash
pip install pandas requests
```

必须配置理杏仁 API Token 到系统环境变量 `LIXINGER_TOKEN` 中，此 Token 用于所有的数据请求接口。

```bash
# Linux/macOS
export LIXINGER_TOKEN="你的理杏仁Token"

# Windows (CMD)
set LIXINGER_TOKEN=你的理杏仁Token
```

### 运行流程

#### 步骤 1: 生成股票池和交易日历
```bash
# 生成交易日历
python tradecalender.py

# 筛选市值符合要求的股票池（需根据提示输入最小市值，如 50）
python stocks_filter.py
```

#### 步骤 2: 运行回测测试
如果本地未建立全量 SQLite 数据库，可以使用在线回测（较慢）：
```bash
python backtest.py
```

如果已经有本地全量数据库 (`stock_market_data.db` 或 `./tools/stock_market_data.db`)，推荐执行离线策略或效应分析脚本：
```bash
# 执行多进程离线回测
python backtest_offline.py

# 执行换手率效应专项分析
python backtest2.py
```

报告将自动生成到 `output` 目录下。

## 策略分析产出维度
回测报告(`*_report.csv`) 中主要按照以下维度聚合统计数据的表现：
- **`trend`**: 年线上 / 年线下（基于MA250判断趋势）
- **`cap_group`**: 市值分位数分组 (Q1极小 - Q5极大)
- **`ratio_group`**: 换手倍数大小分组
- **`bias_group`**: 价格偏离20日均线程度（贴线、远离、下方） 
- **评估指标**: 样本数、保本率（最大跌幅未触及止损）、平均最高涨幅。
