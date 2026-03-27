import sqlite3
import pandas as pd
import numpy as np

DB_PATH = "./tools/stock_market_data.db"

def add_columns():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE daily_kline ADD COLUMN ma20 REAL")
        cursor.execute("ALTER TABLE daily_kline ADD COLUMN ma120 REAL")
        print("✅ 成功添加 ma20 和 ma120 字段")
    except sqlite3.OperationalError:
        print("ℹ️ 字段已存在，跳过添加")
    conn.commit()
    conn.close()


def precompute_ma():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. 获取所有股票代码
    print("正在获取股票列表...")
    codes_df = pd.read_sql("SELECT DISTINCT code FROM daily_kline", conn)
    codes = codes_df['code'].tolist()
    total = len(codes)
    
    print(f"开始计算 {total} 只股票的均线指标...")
    
    for idx, code in enumerate(codes):
        # 2. 使用 rowid 代替 id。rowid 是 SQLite 默认自带的唯一标识符。
        # 我们同时拉取 date, close 和 rowid
        df = pd.read_sql(
            f"SELECT rowid, date, close FROM daily_kline WHERE code = '{code}' ORDER BY date", 
            conn
        )
        
        if len(df) < 20:
            continue
            
        # 3. 使用 Pandas 计算滑动均线
        df['ma20_val'] = df['close'].rolling(window=20).mean()
        df['ma120_val'] = df['close'].rolling(window=120).mean()
        
        # 4. 过滤掉均线为空的行（前19天必定为空）
        update_data = df.dropna(subset=['ma20_val']).copy()
        
        # 5. 准备批量更新数据 [(ma20, ma120, rowid), ...]
        records = []
        for row in update_data.itertuples():
            # 将 numpy 类型转换为原生 python 类型，防止 sqlite 不兼容
            m20 = float(row.ma20_val)
            m120 = float(row.ma120_val) if pd.notna(row.ma120_val) else None
            records.append((m20, m120, row.rowid))
        
        # 6. 使用 rowid 执行批量更新
        conn.executemany(
            "UPDATE daily_kline SET ma20 = ?, ma120 = ? WHERE rowid = ?", 
            records
        )
        
        # 每处理 100 只股票提交一次事务，平衡速度和内存
        if (idx + 1) % 100 == 0:
            conn.commit()
            print(f"⏳ 已处理: {idx+1}/{total} (当前: {code})")

    conn.commit()
    conn.close()
    print("🏁 所有均线数据预处理完成！")

if __name__ == "__main__":
    
    add_columns()
    precompute_ma()