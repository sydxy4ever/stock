import sqlite3
import pandas as pd
import numpy as np

DB_PATH = "./tools/stock_market_data.db"
def upgrade_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cols = ["to_avg_30 REAL", "to_avg_5 REAL", "size_tag INTEGER"]
    for col in cols:
        try:
            cursor.execute(f"ALTER TABLE daily_kline ADD COLUMN {col}")
        except:
            pass
    conn.commit()
    conn.close()
    print("✅ 字段添加完毕")

def precompute():
    conn = sqlite3.connect(DB_PATH)
    
    # --- 0. 结构检查与字段添加 ---
    cursor = conn.cursor()
    cols = ["to_avg_30 REAL", "to_avg_5 REAL", "size_tag INTEGER", "market_cap REAL"]
    for col in cols:
        try:
            cursor.execute(f"ALTER TABLE daily_kline ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass # 字段已存在
    conn.commit()
    print("✅ 数据库字段检查完毕")

    # --- 1. 同步市值数据 (从 stock_info 同步到 daily_kline) ---
    # 我们假设 stock_info 里的市值是最新快照。同步后可以大大加快后续查询速度。
    print("⏳ 正在同步市值数据...")
    conn.execute("""
        UPDATE daily_kline 
        SET market_cap = (
            SELECT market_cap FROM stock_info 
            WHERE stock_info.code = daily_kline.code
        )
        WHERE market_cap IS NULL
    """)
    conn.commit()
    print("✅ 市值同步完成")

    # --- 2. 计算换手率滑动平均 (按股票) ---
    codes = pd.read_sql("SELECT DISTINCT code FROM daily_kline", conn)['code'].tolist()
    print(f"🚀 开始计算 {len(codes)} 只股票的滑动换手率 (30日 & 5日)...")
    
    for idx, code in enumerate(codes):
        # 按照 rowid 更新，这是最快的方式
        df = pd.read_sql(f"SELECT rowid, to_r FROM daily_kline WHERE code = '{code}' ORDER BY date", conn)
        if len(df) < 5: continue
        
        df['to30'] = df['to_r'].rolling(window=30).mean()
        df['to5'] = df['to_r'].rolling(window=5).mean()
        
        updates = [(row.to30, row.to5, row.rowid) for row in df.itertuples() if pd.notna(row.to5)]
        conn.executemany("UPDATE daily_kline SET to_avg_30 = ?, to_avg_5 = ? WHERE rowid = ?", updates)
        
        if (idx+1) % 200 == 0:
            conn.commit()
            print(f"   进度: {idx+1}/{len(codes)}")
    conn.commit()

    # --- 3. 计算市值分组 (按日期) ---
    dates = pd.read_sql("SELECT DISTINCT date FROM daily_kline", conn)['date'].tolist()
    print(f"🚀 开始按天计算市值分组 ({len(dates)} 天)...")
    
    for idx, date in enumerate(dates):
        # 注意：现在 market_cap 已经在 daily_kline 表里了
        df_day = pd.read_sql(f"SELECT rowid, market_cap FROM daily_kline WHERE date = '{date}' AND market_cap > 0", conn)
        if df_day.empty: continue
        
        try:
            # 三分位：1=低市值, 2=中市值, 3=高市值
            df_day['tag'] = pd.qcut(df_day['market_cap'], 3, labels=[1, 2, 3])
            updates = [(int(row.tag), row.rowid) for row in df_day.itertuples()]
            conn.executemany("UPDATE daily_kline SET size_tag = ? WHERE rowid = ?", updates)
        except (ValueError, ZeroDivisionError):
            continue 
            
        if (idx+1) % 100 == 0:
            conn.commit()
            print(f"   进度: {idx+1}/{len(dates)}")

    conn.commit()
    conn.close()
    print("🏁 全部数据预计算并固化完成！")
if __name__ == "__main__":

    upgrade_db()
    precompute()