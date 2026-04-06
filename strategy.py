import os
import argparse
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# --- Configuration ---
SOURCE_DB = "stock_data.db"
RESULT_DB = "strategy_results.db"

# Strategy Parameters
TRIGGER_MIN = 1.8
TRIGGER_MAX = 2.6
MIN_D1_CHANGE = 0.06  # 6%
TOP_N = 3              # Top 3 by Market Cap per Industry
BASELINE_DAYS = 30     # T-35 to T-6
RECENT_DAYS = 5        # T-5 to T-1

def init_result_db(conn):
    """Initialize the results database schema."""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS strategy_signals (
        date TEXT,
        stock_code TEXT,
        stock_name TEXT,
        industry_name TEXT,
        mc_rank INTEGER,
        baseline_to_r REAL,
        recent_to_r REAL,
        trigger_ratio REAL,
        change_pct REAL,
        close REAL,
        ma20 REAL,
        ma60 REAL,
        ma200 REAL,
        PRIMARY KEY (date, stock_code)
    )
    """)
    conn.commit()

def get_trade_calendar(conn):
    """Retrieve the sorted list of trading dates."""
    df = pd.read_sql("SELECT DISTINCT date FROM daily_kline WHERE stock_code='600519' ORDER BY date", conn)
    return df['date'].tolist()

def get_top_stocks(conn, date_str):
    """Fetch Top-N stocks per industry based on the latest market cap before or on date_str."""
    sql = """
    WITH latest_mc AS (
        SELECT f.stock_code, f.mc,
               ROW_NUMBER() OVER (PARTITION BY f.stock_code ORDER BY f.date DESC) as rn
        FROM fundamentals f
        WHERE f.date <= ? AND f.mc > 0
    ),
    industry_info AS (
        SELECT si.stock_code, si.industry_code, si.name as industry_name
        FROM stock_industries si
        WHERE si.source = 'sw_2021'
    ),
    ranked AS (
        SELECT 
            i.industry_code,
            i.industry_name,
            s.stock_code,
            s.name as stock_name,
            lm.mc,
            ROW_NUMBER() OVER (PARTITION BY i.industry_code ORDER BY lm.mc DESC) as mc_rank
        FROM latest_mc lm
        JOIN stocks s ON s.stock_code = lm.stock_code
        JOIN industry_info i ON i.stock_code = lm.stock_code
        WHERE lm.rn = 1 AND s.listing_status = 'normally_listed'
    )
    SELECT stock_code, stock_name, industry_name, mc_rank
    FROM ranked
    WHERE mc_rank <= ? AND industry_code LIKE '______' AND SUBSTR(industry_code, -2) != '00'
    """
    return pd.read_sql(sql, conn, params=(date_str, TOP_N))

def run_scanner(date_str):
    """Scan for signals on a specific date."""
    if not os.path.exists(SOURCE_DB):
        print(f"Error: {SOURCE_DB} not found.")
        return []

    conn = sqlite3.connect(SOURCE_DB)
    calendar = get_trade_calendar(conn)
    
    if date_str not in calendar:
        print(f"Error: {date_str} is not a valid trading date in {SOURCE_DB}.")
        conn.close()
        return []

    idx = calendar.index(date_str)
    if idx < 35:
        print(f"Error: Not enough historical data for {date_str}.")
        conn.close()
        return []

    # Windows
    baseline_window = calendar[idx-35 : idx-5]
    recent_window = calendar[idx-5 : idx]
    
    # Get Top Stocks
    stocks_df = get_top_stocks(conn, date_str)
    codes = stocks_df['stock_code'].tolist()
    if not codes:
        conn.close()
        return []

    # Fetch Data in Bulk
    # 1. Prices and Turnover for Baseline and Recent
    all_dates = baseline_window + recent_window + [date_str]
    placeholders = ",".join(["?"] * len(codes))
    date_placeholders = ",".join(["?"] * len(all_dates))
    
    kline_sql = f"""
    SELECT stock_code, date, close, to_r, change_pct 
    FROM daily_kline 
    WHERE stock_code IN ({placeholders}) AND date IN ({date_placeholders})
    """
    kline_df = pd.read_sql(kline_sql, conn, params=codes + all_dates)

    # 2. Moving Averages for Target Date
    ma_sql = f"SELECT stock_code, ma20, ma60, ma200 FROM moving_averages WHERE date = ? AND stock_code IN ({placeholders})"
    ma_df = pd.read_sql(ma_sql, conn, params=[date_str] + codes)
    
    conn.close()

    signals = []
    
    for _, s_row in stocks_df.iterrows():
        code = s_row['stock_code']
        s_kline = kline_df[kline_df['stock_code'] == code]
        
        # Calculate Baseline
        base_data = s_kline[s_kline['date'].isin(baseline_window)]
        if len(base_data) < 15: continue
        baseline_to_r = base_data['to_r'].mean()
        
        # Calculate Recent
        recent_data = s_kline[s_kline['date'].isin(recent_window)]
        if recent_data.empty: continue
        recent_to_r = recent_data['to_r'].mean()
        
        # Trigger Ratio
        trigger_ratio = recent_to_r / baseline_to_r if baseline_to_r > 0 else 0
        
        # Today's Snapshot
        today_row = s_kline[s_kline['date'] == date_str]
        if today_row.empty: continue
        today_row = today_row.iloc[0]
        
        # MAs
        s_ma = ma_df[ma_df['stock_code'] == code]
        if s_ma.empty: continue
        s_ma = s_ma.iloc[0]
        
        # Criteria Check
        # 1. Turnover Surge [1.8, 2.6]
        if not (TRIGGER_MIN <= trigger_ratio <= TRIGGER_MAX):
            continue
            
        # 2. Breakout Change >= 6%
        if today_row['change_pct'] < MIN_D1_CHANGE:
            continue
            
        # 3. Position: Below MA20, MA60, MA200
        close = today_row['close']
        if not (close < s_ma['ma20'] and close < s_ma['ma60'] and close < s_ma['ma200']):
            continue
            
        signals.append({
            'date': date_str,
            'stock_code': code,
            'stock_name': s_row['stock_name'],
            'industry_name': s_row['industry_name'],
            'mc_rank': s_row['mc_rank'],
            'baseline_to_r': round(baseline_to_r, 4),
            'recent_to_r': round(recent_to_r, 4),
            'trigger_ratio': round(trigger_ratio, 4),
            'change_pct': round(today_row['change_pct'], 4),
            'close': today_row['close'],
            'ma20': s_ma['ma20'],
            'ma60': s_ma['ma60'],
            'ma200': s_ma['ma200']
        })
        
    return signals

def main():
    parser = argparse.ArgumentParser(description="Golden Pocket Strategy Scanner")
    parser.add_argument("--date", type=str, help="Date to scan (YYYY-MM-DD). Defaults to latest in DB.")
    parser.add_argument("--start", type=str, help="Start date for batch scan.")
    parser.add_argument("--end", type=str, help="End date for batch scan.")
    args = parser.parse_args()

    # Init Results DB
    rconn = sqlite3.connect(RESULT_DB)
    init_result_db(rconn)
    
    # Get Date List
    conn = sqlite3.connect(SOURCE_DB)
    calendar = get_trade_calendar(conn)
    conn.close()
    
    if args.date:
        dates = [args.date]
    elif args.start:
        end_date = args.end or calendar[-1]
        dates = [d for d in calendar if args.start <= d <= end_date]
    else:
        dates = [calendar[-1]]
        
    print(f"Scanning {len(dates)} dates for signals...")
    
    all_found = 0
    for d in dates:
        print(f"Processing {d}...", end="\r")
        signals = run_scanner(d)
        if signals:
            all_found += len(signals)
            # Insert to DB
            df_signals = pd.DataFrame(signals)
            df_signals.to_sql("strategy_signals", rconn, if_exists="append", index=False, method="multi")
            rconn.commit()
            print(f"Found {len(signals)} signals on {d}: {', '.join([s['stock_name'] for s in signals])}")

    print(f"\nScan complete. Total signals found: {all_found}")
    print(f"Results saved to {RESULT_DB}")
    rconn.close()

if __name__ == "__main__":
    main()
