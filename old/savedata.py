import sqlite3
import pandas as pd
import requests
import os
import time

# --- 配置 ---
TOKEN = os.getenv("LIXINGER_TOKEN")
DB_PATH = "stock_market_data.db"
STOCK_LIST_FILE = "china_main_board_market_cap_filtered.csv"
CANDLE_URL = "https://open.lixinger.com/api/cn/company/candlestick"
INDUSTRY_URL = "https://open.lixinger.com/api/cn/company/industries"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # 1. 个股行情表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_kline (
            code TEXT,
            date TEXT,
            close REAL,
            high REAL,
            low REAL,
            to_r REAL,
            amount REAL,
            PRIMARY KEY (code, date)
        )
    ''')
    # 2. 个股信息表（包含行业名和行业代码）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_info (
            code TEXT PRIMARY KEY,
            name TEXT,
            market_cap REAL,
            industries_mixed TEXT  -- 格式：行业名(代码)|行业名(代码)...
        )
    ''')
    conn.commit()
    return conn

def get_industry_string(code):
    """仅抓取行业名和行业代码并拼接"""
    payload = {"token": TOKEN, "stockCode": code}
    try:
        resp = requests.post(INDUSTRY_URL, json=payload).json()
        if resp.get("code") == 1 and resp.get("data"):
            # 只取前20个行业位
            items = resp['data'][:20]
            # 拼接格式：电力设备(630000)|电池(630700)
            return "|".join([f"{item['name']}({item['stockCode']})" for item in items])
    except:
        return ""
    return ""

def download_data():
    stocks = pd.read_csv(STOCK_LIST_FILE)
    stocks['code_str'] = stocks['stockCode'].apply(lambda x: str(x).zfill(6))
    
    conn = init_db()
    print("🚀 开始同步本地数据库 (K线行情 + 行业代码)...")
    
    for i, row in stocks.iterrows():
        code = row['code_str']
        
        # 1. 同步行业信息
        check_info = pd.read_sql(f"SELECT code FROM stock_info WHERE code='{code}'", conn)
        if check_info.empty:
            print(f"[{i+1}/{len(stocks)}] 抓取行业: {code}")
            ind_str = get_industry_string(code)
            conn.execute(
                "INSERT INTO stock_info (code, name, market_cap, industries_mixed) VALUES (?, ?, ?, ?)",
                (code, row['name'], row['marketCap'], ind_str)
            )
            conn.commit()

        # 2. 同步 K 线行情 (https://open.lixinger.com/api/cn/company/candlestick)
        check_k = pd.read_sql(f"SELECT code FROM daily_kline WHERE code='{code}' LIMIT 1", conn)
        if check_k.empty:
            print(f"[{i+1}/{len(stocks)}] 下载 K 线: {code}")
            try:
                time.sleep(0.1) # 频率保护
                payload = {
                    "token": TOKEN,
                    "stockCode": code,
                    "type": "fc_rights",
                    "startDate": "2020-01-01",
                    "endDate": "2026-03-23"
                }
                resp = requests.post(CANDLE_URL, json=payload).json()
                if resp.get("code") == 1 and resp.get("data"):
                    df = pd.DataFrame(resp['data'])
                    df['code'] = code
                    # 只入库你需要的字段
                    keep_cols = ['code', 'date', 'close', 'high', 'low', 'to_r', 'amount']
                    df[keep_cols].to_sql('daily_kline', conn, if_exists='append', index=False)
                else:
                    print(f"⚠️ {code} 接口返回错误: {resp.get('message')}")
            except Exception as e:
                print(f"❌ {code} 通讯异常: {e}")
            
    # 建立索引确保回测速度
    conn.execute("CREATE INDEX IF NOT EXISTS idx_code_date ON daily_kline (code, date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_just_date ON daily_kline (date)")
    conn.close()
    print("✅ 数据同步完成。")

if __name__ == "__main__":
    download_data()