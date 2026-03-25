import sqlite3
import pandas as pd
import numpy as np
import os
from multiprocessing import Pool, cpu_count

# --- 配置 ---
DB_PATH = "./tools/stock_market_data.db"
CALENDAR_FILE = "./tools/trade_calendar.csv"
SW_FILE = "./tools/sw2021.csv"
RESULT_DIR = "size_analysis_results"

if not os.path.exists(RESULT_DIR):
    os.makedirs(RESULT_DIR)

# 你锁定的几个核心行业代码 (示例：普钢, 航海装备, 炼化, 银行等)
TARGET_CODES = ['410108', '480201', '340401', '460802', '480301', '230403', '480501', '220308', '420903', '110102', '480401', '350102', '421002', '360202', '730102', '430102', '340601', '220505'] 

def run_size_backtest(args):
    start_idx, trade_days, stock_info_df = args
    p1_start, p2_end, p3_end = trade_days[start_idx], trade_days[start_idx+34], trade_days[start_idx+44]
    date_str = p1_start.strftime('%Y%m%d')
    
    conn = sqlite3.connect(DB_PATH)
    # 获取 P1-P3 窗口数据，同时获取市值
    query = f"SELECT code, date, close, high, low, to_r FROM daily_kline WHERE date BETWEEN '{trade_days[max(0, start_idx-1)].date()}' AND '{p3_end.date()}'"
    df_win = pd.read_sql(query, conn)
    conn.close()
    
    if df_win.empty: return
    df_win['date'] = pd.to_datetime(df_win['date']).dt.tz_localize(None)
    
    # 动态计算当天的市值分类 (低/中/高)
    # 逻辑：以 P1 起始点的市值作为分类基准
    mkt_labels = ['低市值', '中市值', '高市值']
    stock_info_df['size_tag'] = pd.qcut(stock_info_df['market_cap'], 3, labels=mkt_labels)

    day_samples = []
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
        
        # 判定保本 (95% 阈值)
        is_safe = 1 if (d3['low'].min() / entry_p) >= 0.95 else 0
        max_gain = (d3['high'].max() / entry_p - 1) * 100
        
        day_samples.append({
            'code': code, 
            'ratio': ratio, 
            'is_safe': is_safe, 
            'max_gain': max_gain
        })
    
    if day_samples:
        res_df = pd.DataFrame(day_samples)
        # 合并行业和市值分类
        res_df = res_df.merge(stock_info_df, on='code')
        
        # 换手分组
        res_df['ratio_grp'] = pd.cut(res_df['ratio'], bins=[1.0, 1.5, 2.0, 999], labels=['1.0-1.5', '1.5-2.0', '>2.0'])
        res_df = res_df.dropna(subset=['ratio_grp'])
        
        # 仅保留你指定的几个核心行业进行详细统计
        final_stats = res_df.groupby(['sw3_name', 'ratio_grp', 'size_tag'], observed=True).agg(
            样本数=('is_safe', 'count'),
            保本率=('is_safe', 'mean'),
            平均最高涨幅=('max_gain', 'mean')
        ).reset_index()
        
        final_stats['date'] = date_str
        final_stats.to_csv(os.path.join(RESULT_DIR, f"{date_str}_size_res.csv"), index=False, encoding='utf-8-sig')

def main():
    # 读取行业和市值基础信息
    sw_df = pd.read_csv(SW_FILE)
    sw_map = dict(zip(sw_df['三级代码'].astype(str), sw_df['名称.2']))
    
    conn = sqlite3.connect(DB_PATH)
    # 注意：这里需要 market_cap 字段
    stock_info = pd.read_sql("SELECT code, market_cap, industries_mixed FROM stock_info", conn)
    conn.close()

    def find_sw3(text):
        for code, name in sw_map.items():
            if f"({code})" in text: return name
        return None
    stock_info['sw3_name'] = stock_info['industries_mixed'].apply(find_sw3)
    stock_info = stock_info.dropna(subset=['sw3_name'])
    
    cal_df = pd.read_csv(CALENDAR_FILE)
    trade_days = sorted(pd.to_datetime(cal_df['date']).dt.tz_localize(None).tolist())
    start_dt = pd.Timestamp("2022-01-01")
    tasks = [(i, trade_days, stock_info) for i, d in enumerate(trade_days) if d >= start_dt and (i+44) < len(trade_days)]

    with Pool(cpu_count()) as pool:
        pool.map(run_size_backtest, tasks)

if __name__ == "__main__":
    main()