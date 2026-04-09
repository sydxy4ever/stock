# 📈 量化分析调度与策略可视化 (Quantitative Analysis & Strategy Dashboard)

本项目是一个基于 Lixinger (理杏仁) API 数据的 A 股量化交易分析流水线。它围绕“换手率异动启动 (Turnover Surge)” 核心策略，构建了从数据自动抓取（Docker 容器化）、预处理、离线策略回测分析，到基于 Streamlit 的实时可视化看板的一整套投研工具。

## 🌟 核心特性 (Features)

- **全自动化数据管道**: 使用 Python 开发的数据抓取脚本，配合 `scheduler.py` 和 Docker 常驻容器，每天定时自动抓取股票池、行业、基本面、财报及日K线数据。
- **换手率涨停/异动归因策略 (v5)**: 寻找具有显著庄家吸筹或资金介入特征的“两阶换手倍增”策略。对比基准期（前30天）的平均换手，追踪一阶（前10-6天）和二阶（前5-1天）的换手率异常放大，并在启动日（涨幅 >= 4%）捕捉强力信号。
- **均线维度的回测分析体系**: 包含了多维度的策略表现分析代码 (`analyze_v5_1~3`)，探讨 MA20/MA60/MA200 各种底部、弱势和破位结构的非连跌率与收益回撤风险(CV系数)，结合极端市场情绪因子过滤噪音标的。
- **本地数据库持久化**: 采用轻量级 SQLite (`stock_data.db` 及 `turnover_surge.db`)，易于迁移与备份，通过 upsert 逻辑保证数据一致性与长期追踪的幂等性。
- **交互式 Streamlit 看板**: 提供图形化操作界面，不仅支持日志实时流式渲染的分步抓取调度，还能实现“选择日期，一键扫描异动股”并在表格中追踪随后的 D1~D9 的收益表现与均线站位情况。

## 📁 目录结构 (Structure)

```text
/data/stock/
├── dashboard.py               # Streamlit 可视化交互主页页面
├── fetch_all.py               # 一键全量抓取执行脚本，支持异常熔断与重试分析
├── scheduler.py               # Docker 内置的定时任务调度器 (01:00 AM)
├── docker-compose.yml         # 快速部署服务的 Docker Compose 配置
├── Dockerfile                 # 容器构建配置
├── requirements.txt           # 项目 Python 依赖
├── trade_calendar.csv         # 静态交易日历映射表
│
├── fetchers/                  # 数据采集层：通过 API 获取不同粒度的原始数据
│   ├── fetch_stocks.py          - 基础股票池录入 (规避退市股等)
│   ├── fetch_industries.py      - 申万行业数据关联 (SW 2021)
│   ├── fetch_fundamentals.py    - 市值等核心基本面数据录入
│   ├── fetch_fs.py              - 定期财务报表切片
│   └── fetch_klines.py          - 更新并追加日结量价数据
│
├── tools/                     # 工具流与衍生指标计算层
│   ├── compute_ma.py            - 基于日K计算平滑均线 (MA5/MA20/MA60/MA200)
│   ├── build_calendar.py        - 构建/校准本地化交易日序列
│   └── check_daily_sentiment.py - 大盘横向情绪统计脚本
│
├── strategy/                  # 策略投研层
│   └── turnover_surge.py        - 核心：换手率异动“两段式吸筹”打分与标的落库
│
└── analyze/                   # 离线回测评估层 (v5.x 体系)
    ├── analyze_v5_1.py          - 基础 MA 形态下的 CV（离散系数）与非跌率综合分析
    ├── analyze_v5_2.py          - 结合极度市场情绪下（Top 15% 与 Bottom 15% 即狂热与冰点）的特征表现归因
    └── analyze_v5_3.py          - 到达极值（最高点、最低回撤点）的平均时间窗口与期望盈亏比分析
```

## 🚀 部署与运行 (Deployment)

本项目完全基于 Docker 化部署方案设计，推荐使用 `docker-compose` 和 Portainer 进行持续管理。

### 1. 配置环境变量
确保你在操作系统层面或 Portainer 中配置了理杏仁的认证 Token：
```bash
export LIXINGER_TOKEN=你的理杏仁实盘API凭证
```

### 2. 启动服务 
```bash
cd /data/stock
docker-compose up -d
```
编排中主要起动两个常驻容器：
- `stock-fetch-daily`: 运行 `scheduler.py` 驻留后台，自动进行日常轮询，并将每日增量写入挂载在 `/data/stock` 的 `.db` 文件。
- `stock-dashboard`: 绑定在宿主机的 `8501` 端口，提供用于调试投研、触发扫描补库的可视化界面。

### 3. 可视化看板访问与交互
通过浏览器打开 `http://<部署服务器IP>:8501/`，即可使用控制台。
* **数据抓取调度模块 (Tab1)**: 适用于因网络限流（如 HTTP 429 Too Many Requests）导致自动化任务中断后的手工补抓，或独立运行单步抓取。页面具有基于 Subprocess 的日志实时回显能力。
* **异动策略扫描 (Tab2)**: 指定一个交易日 Day0，对当日全市场三级行业龙头池进行策略运算。输出表格包含了核心的换手率异常系数 (`trigger_ratio_1`, `trigger_ratio_2`)、价格突破以及往后数天的动态绩效切片，且支持数值重排序。

## 💡 策略设计浅析 (Strategy Brief)
本项目的“异动归因”底层模型围绕寻找具有深度资金介入、清洗浮筹后处于突破阶段的行业龙头构建，具体参数和算法规则（详参 `turnover_surge.py`）：
1. **基准期认定 (Baseline):** Day -35 到 Day -6，这 30 个交易日用来圈定该股的常态地量换手表现。
2. **两阶段观察蓄力:**
    - 一阶起势期 (Day -10 到 -6): 换手倍数需在 `1.2x ~ 1.5x` 的微热空间，暗示先头资金初步温和介入。
    - 二阶确认期 (Day -5 到 -1): 换手倍数放大并要求超越 `>1.5x` 基准上限，呈现放量共识特征。
3. **启动日准入 (Day 0):** 当天必须带有实体且涨幅不低于 4% 的 K 线触发（确认信号），方可登记。入池将快照记录当日收盘价及其同重要均线 (MA20/MA60/MA200) 的相对乖离与站稳状态。
4. **追踪记录期 (Forward):** 落库引擎自动提取并补全 Day 1 到 Day 9 这九个交易日的日内波动，为量化投研阶段（V5 衍生脚本）中基于不同 MA 维度的变异系数估值提供数据源。

## ⚠️ 数据依赖声明与建议
全流水线高度依赖 **理杏仁数据平台(Lixinger)** 的 REST API 接口通道。若初始阶段使用全量批量下载功能，极易高频触碰速率控制。日常定时抓取脚本中已加入了规避策略和限流统计，如果仍然频现 `HTTP 429` 报错，请调整休眠节拍间隔以配合平台的吞吐限制。
