import sqlite3
import pandas as pd
import os
from multiprocessing import Pool, cpu_count

# --- 配置 ---
DB_PATH = "stock_market_data.db"
CALENDAR_FILE = "trade_calendar.csv"
SW_FILE = "sw2021.csv"
DETAILS_DIR = "daily_details"

if not os.path.exists(DETAILS_DIR):
    os.makedirs(DETAILS_DIR)

def run_daily_detail(args):
    start_idx, trade_days, stock_info_df = args
    p1_start = trade_days[start_idx]
    p2_end   = trade_days[start_idx + 34]
    p3_end   = trade_days[start_idx + 44]
    date_str = p1_start.strftime('%Y%m%d')
    
    save_path = os.path.join(DETAILS_DIR, f"{date_str}_detail.csv")
    if os.path.exists(save_path): return

    conn = sqlite3.connect(DB_PATH)
    # 读取窗口数据
    query = f"SELECT code, date, close, low, to_r FROM daily_kline WHERE date BETWEEN '{trade_days[max(0, start_idx-1)].date()}' AND '{p3_end.date()}'"
    df_win = pd.read_sql(query, conn)
    conn.close()
    
    if df_win.empty: return
    df_win['date'] = pd.to_datetime(df_win['date']).dt.tz_localize(None)
    
    day_results = []
    for code, df in df_win.groupby('code'):
        df = df.sort_values('date')
        d1 = df[(df['date'] >= p1_start) & (df['date'] <= trade_days[start_idx+29])]
        d2 = df[(df['date'] >= trade_days[start_idx+30]) & (df['date'] <= p2_end)]
        d3 = df[(df['date'] >= trade_days[start_idx+35]) & (df['date'] <= p3_end)]
        
        if len(d1) < 25 or len(d2) < 5 or len(d3) < 8: continue
        
        avg_to_p1 = d1['to_r'].mean()
        if avg_to_p1 <= 0: continue
        
        ratio = d2['to_r'].mean() / avg_to_p1
        entry_p = d3.iloc[0]['close']
        is_safe = 1 if (d3['low'].min() / entry_p) >= 0.95 else 0
        
        day_results.append({'code': code, 'ratio': ratio, 'is_safe': is_safe})
    
    if day_results:
        res_df = pd.DataFrame(day_results)
        # 合并行业信息
        res_df = res_df.merge(stock_info_df, on='code')
        res_df.to_csv(save_path, index=False)
    print(f"✅ {date_str} 明细已存")

def main():
    # 准备行业字典
    sw_df = pd.read_csv(SW_FILE)
    sw_map = dict(zip(sw_df['三级代码'].astype(str), sw_df['名称.2']))
    
    conn = sqlite3.connect(DB_PATH)
    stock_info = pd.read_sql("SELECT code, industries_mixed FROM stock_info", conn)
    conn.close()
    
    def find_sw3(text):
        for code in sw_map.keys():
            if f"({code})" in text: return sw_map[code]
        return None
    stock_info['sw3_name'] = stock_info['industries_mixed'].apply(find_sw3)
    stock_info = stock_info.dropna(subset=['sw3_name'])[['code', 'sw3_name']]

    cal_df = pd.read_csv(CALENDAR_FILE)
    trade_days = sorted(pd.to_datetime(cal_df['date']).dt.tz_localize(None).tolist())
    
    # 任务范围：2022年至今
    start_dt = pd.Timestamp("2022-01-01")
    tasks = [(i, trade_days, stock_info) for i, d in enumerate(trade_days) 
             if d >= start_dt and (i + 44) < len(trade_days)]

    with Pool(cpu_count()) as pool:
        pool.map(run_daily_detail, tasks)

if __name__ == "__main__":
    main()