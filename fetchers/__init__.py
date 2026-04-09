# fetchers — 数据抓取模块
# 负责从理杏仁 API 拉取以下数据并存入 stock_data.db：
#   fetch_stocks.py       → stocks 表（股票列表）
#   fetch_industries.py   → stock_industries 表（行业归属）
#   fetch_fundamentals.py → fundamentals 表（基本面/市值）
#   fetch_fs.py           → financial_statements 表（财务报表）
#   fetch_klines.py       → daily_kline 表（K线数据）
