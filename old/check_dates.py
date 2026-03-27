import pandas as pd
import sqlite3

# 检查日期范围
CALENDAR_FILE = "./tools/trade_calendar.csv"
DB_PATH = "./tools/stock_market_data.db"

# 加载交易日历
df_cal = pd.read_csv(CALENDAR_FILE)
trade_dates = pd.to_datetime(df_cal['date']).dt.date.tolist()

# 查找2022-01-01到2026-01-01之间的交易日
start_date = pd.to_datetime('2022-01-01').date()
end_date = pd.to_datetime('2026-01-01').date()

filtered_dates = [d for d in trade_dates if start_date <= d <= end_date]
print(f"2022-01-01 到 2026-01-01 之间的交易日数: {len(filtered_dates)}")
print(f"第一个交易日: {filtered_dates[0]}")
print(f"最后一个交易日: {filtered_dates[-1]}")

# 计算有效起始日期数（需要后续有40个交易日）
valid_starts = []
for i in range(len(filtered_dates) - 39):
    valid_starts.append(filtered_dates[i])

print(f"\n有效起始日期数（后续有40个交易日）: {len(valid_starts)}")
print(f"第一个有效起始日: {valid_starts[0]}")
print(f"最后一个有效起始日: {valid_starts[-1]}")

# 检查股票数据日期范围
conn = sqlite3.connect(DB_PATH)
query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM daily_kline"
date_range = pd.read_sql_query(query, conn)
conn.close()
print(f"\n股票数据日期范围:")
print(f"  最早: {date_range['min_date'].iloc[0]}")
print(f"  最晚: {date_range['max_date'].iloc[0]}")