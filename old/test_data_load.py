import sqlite3
import pandas as pd

# 测试数据加载
DB_PATH = "./tools/stock_market_data.db"
CALENDAR_FILE = "./tools/trade_calendar.csv"

# 1. 测试交易日历加载
print("测试交易日历加载...")
df_cal = pd.read_csv(CALENDAR_FILE)
print(f"交易日历行数: {len(df_cal)}")
print(f"前5个交易日: {df_cal['date'].head(5).tolist()}")
print(f"最后5个交易日: {df_cal['date'].tail(5).tolist()}")

# 转换为日期
trade_dates = pd.to_datetime(df_cal['date']).dt.date.tolist()
print(f"日期转换成功，示例: {trade_dates[:3]}")

# 2. 测试股票数据加载
print("\n测试股票数据加载...")
conn = sqlite3.connect(DB_PATH)
query = "SELECT code, date, close, to_r, to_avg_30, to_avg_5 FROM daily_kline LIMIT 10"
df_stock = pd.read_sql_query(query, conn)
conn.close()
print(f"股票数据示例:")
print(df_stock.head())

# 测试日期转换
df_stock['date'] = pd.to_datetime(df_stock['date']).dt.date
print(f"日期转换后: {df_stock['date'].head(3).tolist()}")

# 3. 测试完整数据加载
print("\n测试完整数据加载...")
query_full = "SELECT code, date, close, to_r, to_avg_30, to_avg_5 FROM daily_kline ORDER BY code, date"
conn = sqlite3.connect(DB_PATH)
df_full = pd.read_sql_query(query_full, conn)
conn.close()
df_full['date'] = pd.to_datetime(df_full['date']).dt.date
df_full = df_full.sort_values(['code', 'date'])
df_full['pct_change'] = df_full.groupby('code')['close'].pct_change()

print(f"完整数据形状: {df_full.shape}")
print(f"股票数量: {df_full['code'].nunique()}")
print(f"日期范围: {df_full['date'].min()} 到 {df_full['date'].max()}")

# 4. 测试一只股票的数据
sample_code = df_full['code'].iloc[0]
sample_stock = df_full[df_full['code'] == sample_code]
print(f"\n示例股票 {sample_code} 的数据:")
print(f"数据行数: {len(sample_stock)}")
print(f"日期范围: {sample_stock['date'].min()} 到 {sample_stock['date'].max()}")
print(f"前5天数据:")
print(sample_stock.head())