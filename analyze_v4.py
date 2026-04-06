import sqlite3
import pandas as pd
import numpy as np
import os

def get_max_drawdown_info(prices):
    if len(prices) == 0: 
        return 0.0, 0
    rolling_max = np.maximum.accumulate(prices)
    drawdowns = (rolling_max - prices) / rolling_max
    max_dd_idx = np.argmax(drawdowns)
    return np.max(drawdowns), max_dd_idx

def run_timing_analysis(db_path, stock_db_path):
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found.")
        return None

    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM turnover_surge WHERE day_offset IS NOT NULL", conn)
    conn.close()

    if df.empty:
        return None

    for col in ['d1_close', 'd1_ma20', 'd1_ma60', 'd1_ma200', 'trigger_ratio', 'baseline_to_r']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    events = []
    unique_main_events = df.groupby(['day1', 'stock_code'])

    stock_conn = sqlite3.connect(stock_db_path)

    for (day1, stock_code), group in unique_main_events:
        row = group.iloc[0]
        d1_close = row['d1_close']
        d1_ma20 = row['d1_ma20']
        d1_ma60 = row['d1_ma60']
        d1_ma200 = row['d1_ma200']
        trigger_ratio = row['trigger_ratio']
        baseline_to_r = row['baseline_to_r']

        if pd.isnull(trigger_ratio) or not (1.8 <= trigger_ratio <= 2.6):
            continue

        ma20_pos = "B" if (pd.notnull(d1_ma20) and d1_close < d1_ma20) else "A"
        ma60_pos = "B" if (pd.notnull(d1_ma60) and d1_close < d1_ma60) else "A"
        ma200_pos = "B" if (pd.notnull(d1_ma200) and d1_close < d1_ma200) else "A"
        ma_label = f"20{ma20_pos},60{ma60_pos},200{ma200_pos}"

        if ma_label != "20B,60B,200B":
            continue
        if pd.isnull(baseline_to_r):
            continue

        closes = group.sort_values('day_offset')['close'].tolist()
        changes = group['change_pct'].tolist()

        if not closes or any(pd.isnull(c) for c in closes):
            continue

        curr = stock_conn.cursor()
        curr.execute("SELECT change_pct FROM daily_kline WHERE stock_code=? AND date=?", (stock_code, day1))
        res = curr.fetchone()
        d1_change_pct = res[0] if res else np.nan

        if pd.isnull(d1_change_pct):
            continue

        prices_for_dd = [d1_close] + closes # Index 0 is T-day, 1..10 are T+1..T+10
        max_dd, max_dd_idx = get_max_drawdown_info(np.array(prices_for_dd))
        
        max_inc_idx = np.argmax(prices_for_dd)
        
        # Timing Logic
        if max_inc_idx < max_dd_idx:
            timing = "Rise First (Max Inc -> Max DD)"
            is_rise_first = 1
        elif max_inc_idx > max_dd_idx:
            timing = "Drop First (Max DD -> Max Inc)"
            is_rise_first = 0
        else:
            timing = "Same/Flat"
            is_rise_first = None # Exclude from binary ratio or count as 0

        events.append({
            'd1_change_pct': d1_change_pct,
            'baseline_to_r': baseline_to_r,
            'is_rise_first': is_rise_first
        })

    stock_conn.close()

    if not events:
        return None

    event_df = pd.DataFrame(events)

    d1_bins = [-float('inf'), -0.02, 0, 0.02, 0.04, 0.06, 0.08, float('inf')]
    d1_labels = ['<-2%', '[-2%, 0%)', '[0%, 2%)', '[2%, 4%)', '[4%, 6%)', '[6%, 8%)', '>8%']
    event_df['d1_group'] = pd.cut(event_df['d1_change_pct'], bins=d1_bins, labels=d1_labels, right=False)

    bl_bins = [-float('inf'), 0.01, 0.02, 0.04, float('inf')]
    bl_labels = ['<1%', '[1%, 2%)', '[2%, 4%)', '>4%']
    event_df['bl_group'] = pd.cut(event_df['baseline_to_r'], bins=bl_bins, labels=bl_labels, right=False)

    results = []
    
    for d1_g in d1_labels:
        for bl_g in bl_labels:
            subset = event_df[(event_df['d1_group'] == d1_g) & (event_df['bl_group'] == bl_g)]
            if subset.empty:
                results.append({
                    'D1 Change': d1_g,
                    'Baseline Turnover': bl_g,
                    'Count': 0,
                    'Rise First %': 0.0,
                    'Drop First %': 0.0
                })
                continue
                
            # Filter out identical days
            valid_timing = subset.dropna(subset=['is_rise_first'])
            total_valid = len(valid_timing)
            
            if total_valid > 0:
                rise_first_pct = (valid_timing['is_rise_first'].sum() / total_valid) * 100
                drop_first_pct = 100 - rise_first_pct
            else:
                rise_first_pct = 0.0
                drop_first_pct = 0.0

            results.append({
                'D1 Change': d1_g,
                'Baseline Turnover': bl_g,
                'Count': len(subset),
                'Rise First % (Max Inc -> Max DD)': f"{rise_first_pct:.2f}%",
                'Drop First % (Max DD -> Max Inc)': f"{drop_first_pct:.2f}%"
            })

    return pd.DataFrame(results)

if __name__ == "__main__":
    db_file = "turnover_surge.db"
    stock_db_file = "stock_data.db"
    
    summary = run_timing_analysis(db_file, stock_db_file)
    if summary is not None:
        # Keep only groups with counts > 0 for readability
        non_zero = summary[summary['Count'] > 0]
        print(non_zero.to_markdown(index=False))
