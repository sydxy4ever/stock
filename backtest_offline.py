import sqlite3
import pandas as pd
import os
import warnings
from bisect import bisect_left
from multiprocessing import Pool, cpu_count

# --- 基础配置 ---
DB_PATH = "stock_market_data.db"
CALENDAR_FILE = "trade_calendar.csv"
OUTPUT_DIR = "output"
warnings.filterwarnings('ignore', category=FutureWarning)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def run_single_day(args):
    """
    单个交易日的回测逻辑（被多进程调用）
    """
    start_idx, trade_days, stock_info_df = args
    
    # 核心窗口定义
    p1_start = trade_days[start_idx]
    p2_end   = trade_days[start_idx + 34]
    p3_end   = trade_days[start_idx + 44]
    
    # 为了算 MA250，查询起点往前多推 1 天（SQL 范围）
    # 实际上由于你存了全量数据，只要 code 匹配，df 里的历史数据就是全的
    date_str = p1_start.strftime('%Y%m%d')
    report_path = os.path.join(OUTPUT_DIR, f"{date_str}_report.csv")
    if os.path.exists(report_path): return

    # 重新在子进程建立数据库连接（SQLite 不支持跨进程共享连接）
    conn = sqlite3.connect(DB_PATH)
    
    # 策略：读取该股票在当前窗口及之前的所有历史，用于算均线
    # 优化：为了提速，我们只读该窗口 [p1_start - 350d, p3_end] 的行情
    buffer_idx = max(0, start_idx - 350)
    query_start = trade_days[buffer_idx]
    
    query = f"SELECT * FROM daily_kline WHERE date BETWEEN '{query_start.date()}' AND '{p3_end.date()}'"
    all_kline = pd.read_sql(query, conn)
    conn.close()

    if all_kline.empty: return
    all_kline['date'] = pd.to_datetime(all_kline['date']).dt.tz_localize(None)
    
    results = []
    # 分组处理
    for code, df in all_kline.groupby('code'):
        df = df.sort_values('date')
        
        # 切片
        d1 = df[(df['date'] >= p1_start) & (df['date'] <= trade_days[start_idx+29])]
        d2 = df[(df['date'] >= trade_days[start_idx+30]) & (df['date'] <= p2_end)]
        d3 = df[(df['date'] >= trade_days[start_idx+35]) & (df['date'] <= p3_end)]
        
        if len(d1) < 25 or len(d2) < 5 or len(d3) < 8: continue

        # 1. 换手比 Ratio
        avg_to_p1 = d1['to_r'].mean()
        if avg_to_p1 <= 0: continue
        ratio = d2['to_r'].mean() / avg_to_p1
        
        # 2. 价格与均线
        entry_price = d3.iloc[0]['close']
        hist_df = df[df['date'] <= p2_end]
        
        ma20 = hist_df.tail(20)['close'].mean()
        ma250 = hist_df.tail(250)['close'].mean() if len(hist_df) >= 250 else None
        
        # 3. 维度归类
        trend = '年线上' if (ma250 and entry_price > ma250) else '年线下'
        bias = ((entry_price - ma20) / ma20 * 100) if ma20 else 0
        
        s_info = stock_info_df[stock_info_df['code'] == code]
        if s_info.empty: continue
        s_row = s_info.iloc[0]

        results.append({
            "code": code,
            "trend": trend,
            "bias": bias,
            "ratio": ratio,
            "mktCap": s_row['market_cap'],
            "max_gain%": (d3['high'].max() / entry_price) * 100,
            "max_loss%": (d3['low'].min() / entry_price) * 100
        })

    if not results: return

    # 4. 统计分组
    res_df = pd.DataFrame(results)
    res_df['cap_group'] = pd.qcut(res_df['mktCap'], 5, labels=['Q1', 'Q2', 'Q3', 'Q4', 'Q5'], duplicates='drop')
    res_df['ratio_group'] = pd.cut(res_df['ratio'], bins=[0, 1.6, 2.2, 999], labels=['低比', '中比', '高比'])
    res_df['bias_group'] = pd.cut(res_df['bias'], bins=[-999, 0, 10, 999], labels=['下方', '贴线', '远离'])

    report = res_df.groupby(['trend', 'bias_group', 'cap_group', 'ratio_group'], observed=True).apply(
        lambda g: pd.Series({
            '样本数': len(g),
            '保本率': f"{((g['max_loss%'] > 95).sum() / len(g) * 100):.1f}%",
            '平均最高涨幅': f"{(g['max_gain%'].mean() - 100):.2f}%"
        }),
        include_groups=False # type: ignore
    )
    
    report.to_csv(report_path, encoding="utf-8-sig")
    print(f"✅ {date_str} 处理完毕")

def main():
    # 准备基础数据
    calendar_df = pd.read_csv(CALENDAR_FILE)
    trade_days = sorted(pd.to_datetime(calendar_df['date']).dt.tz_localize(None).tolist())
    
    conn = sqlite3.connect(DB_PATH)
    stock_info = pd.read_sql("SELECT code, name, market_cap FROM stock_info", conn)
    conn.close()

    # 确定回测任务列表
    start_dt = pd.Timestamp("2022-01-01") # 建议从21年开始，因为20年没年线
    end_dt   = pd.Timestamp("2026-01-29") 
    
    task_indices = [i for i, d in enumerate(trade_days) if start_dt <= d <= end_dt]
    
    # 构建任务参数包
    tasks = [(idx, trade_days, stock_info) for idx in task_indices]

    print(f"🔥 启动多进程回测，并行数: {cpu_count()}...")
    
    # 使用进程池
    with Pool(processes=cpu_count()) as pool:
        pool.map(run_single_day, tasks)

    print("\n🏁 所有回测任务已完成！结果见 /output 目录。")

if __name__ == "__main__":
    main()