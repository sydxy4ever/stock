import sqlite3
import pandas as pd
import numpy as np
import os

def calculate_max_drawdown(prices):
    if len(prices) == 0: return 0.0
    rolling_max = np.maximum.accumulate(prices)
    drawdowns = (rolling_max - prices) / rolling_max
    return np.max(drawdowns)

def run_cross_analysis(db_path, stock_db_path):
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found.")
        return None

    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM turnover_surge WHERE day_offset IS NOT NULL", conn)
    conn.close()

    if df.empty:
        print("No tracking data found in database.")
        return None

    print(f"Total tracking records loaded: {len(df)}")

    # Force numeric types
    for col in ['d1_close', 'd1_ma20', 'd1_ma60', 'd1_ma200', 'trigger_ratio', 'baseline_to_r']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    events = []
    unique_main_events = df.groupby(['day1', 'stock_code'])
    print(f"Unique events in DB: {len(unique_main_events)}")

    # Connect to stock_data.db to fetch missing D1 change_pct
    stock_conn = sqlite3.connect(stock_db_path)

    for (day1, stock_code), group in unique_main_events:
        row = group.iloc[0]
        d1_close = row['d1_close']
        d1_ma20 = row['d1_ma20']
        d1_ma60 = row['d1_ma60']
        d1_ma200 = row['d1_ma200']
        trigger_ratio = row['trigger_ratio']
        baseline_to_r = row['baseline_to_r']

        # Core Filters: 1.8 <= trigger_ratio <= 2.6
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

        # Fetch D1 change_pct from stock_data.db
        curr = stock_conn.cursor()
        curr.execute("SELECT change_pct FROM daily_kline WHERE stock_code=? AND date=?", (stock_code, day1))
        res = curr.fetchone()
        d1_change_pct = res[0] if res else np.nan

        if pd.isnull(d1_change_pct):
            continue

        non_drop_ratio = sum(1 for c in changes if pd.notnull(c) and c >= 0) / len(changes) if len(changes) > 0 else 0
        max_inc = max([(c - d1_close) / d1_close for c in closes]) if len(closes) > 0 else 0
        prices_for_dd = [d1_close] + closes
        max_dd = calculate_max_drawdown(np.array(prices_for_dd))

        events.append({
            'd1_change_pct': d1_change_pct,
            'baseline_to_r': baseline_to_r,
            'non_drop_ratio': non_drop_ratio,
            'max_inc': max_inc,
            'max_dd': max_dd
        })

    stock_conn.close()

    print(f"Matched valid events count: {len(events)}")

    if not events:
        print("No events found matching all criteria.")
        return None

    event_df = pd.DataFrame(events)

    # Binning Day 1 Price Change
    d1_bins = [-float('inf'), -0.02, 0, 0.02, 0.04, 0.06, 0.08, float('inf')]
    d1_labels = ['<-2%', '[-2%, 0%)', '[0%, 2%)', '[2%, 4%)', '[4%, 6%)', '[6%, 8%)', '>8%']
    event_df['d1_group'] = pd.cut(event_df['d1_change_pct'], bins=d1_bins, labels=d1_labels, right=False)

    # Binning Baseline Turnover
    bl_bins = [-float('inf'), 0.01, 0.02, 0.04, float('inf')]
    bl_labels = ['<1%', '[1%, 2%)', '[2%, 4%)', '>4%']
    event_df['bl_group'] = pd.cut(event_df['baseline_to_r'], bins=bl_bins, labels=bl_labels, right=False)

    results = []
    
    # Cross tabulation (28 groups)
    for d1_g in d1_labels:
        for bl_g in bl_labels:
            subset = event_df[(event_df['d1_group'] == d1_g) & (event_df['bl_group'] == bl_g)]
            
            if subset.empty:
                results.append({
                    'D1 Change': d1_g,
                    'Baseline Turnover': bl_g,
                    'Count': 0,
                    'Non-drop %': 0.0,
                    'Max Inc Mean': 0.0,
                    'Max Inc CV': 0.0,
                    'Max DD Mean': 0.0,
                    'Max DD CV': 0.0
                })
                continue

            mean_inc = subset['max_inc'].mean()
            std_inc = subset['max_inc'].std()
            cv_inc = std_inc / mean_inc if mean_inc != 0 else 0

            mean_dd = subset['max_dd'].mean()
            std_dd = subset['max_dd'].std()
            cv_dd = std_dd / mean_dd if mean_dd != 0 else 0

            results.append({
                'D1 Change': d1_g,
                'Baseline Turnover': bl_g,
                'Count': len(subset),
                'Non-drop %': subset['non_drop_ratio'].mean() * 100,
                'Max Inc Mean': mean_inc * 100,
                'Max Inc CV': cv_inc,
                'Max DD Mean': mean_dd * 100,
                'Max DD CV': cv_dd
            })

    return pd.DataFrame(results)

if __name__ == "__main__":
    db_file = "turnover_surge.db"
    stock_db_file = "stock_data.db"
    
    summary = run_cross_analysis(db_file, stock_db_file)
    if summary is not None:
        output_file = "analysis_cross_v5.md"
        with open(output_file, "w") as f:
            f.write("# Cross Analysis: D1 Change vs Baseline Turnover\n")
            f.write("Filters: Turnover Surge 1.8-2.6, MA Status: 20B, 60B, 200B\n\n")
            f.write(summary.to_markdown(index=False))
            f.write("\n")
        print(f"\nAnalysis completed successfully. Results saved to {output_file}")
    else:
        print("Analysis failed or no data to summarize.")
