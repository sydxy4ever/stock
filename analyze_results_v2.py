import sqlite3
import pandas as pd
import numpy as np
import os

def calculate_max_drawdown(prices):
    """Calculate max drawdown from the peak."""
    if len(prices) == 0:
        return 0.0
    rolling_max = np.maximum.accumulate(prices)
    drawdowns = (rolling_max - prices) / rolling_max
    return np.max(drawdowns)

def run_granular_analysis(db_path):
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found.")
        return None

    conn = sqlite3.connect(db_path)
    # Load all data with tracking info
    df = pd.read_sql("SELECT * FROM turnover_surge WHERE day_offset IS NOT NULL", conn)
    conn.close()

    if df.empty:
        print("No tracking data found in database.")
        return None

    # Group by event (day1, stock_code)
    events = []
    for (day1, stock_code), group in df.groupby(['day1', 'stock_code']):
        row = group.iloc[0]
        d1_close = row['d1_close']
        d1_ma20 = row['d1_ma20']
        d1_ma60 = row['d1_ma60']
        d1_ma200 = row['d1_ma200']
        trigger_ratio = row['trigger_ratio']

        # MA intersection filtering (looking for 20B, 60B, 200B)
        ma20_pos = "B" if (pd.notnull(d1_ma20) and d1_close < d1_ma20) else "A"
        ma60_pos = "B" if (pd.notnull(d1_ma60) and d1_close < d1_ma60) else "A"
        ma200_pos = "B" if (pd.notnull(d1_ma200) and d1_close < d1_ma200) else "A"
        ma_label = f"20{ma20_pos},60{ma60_pos},200{ma200_pos}"

        # Only proceed if ma_label is 20B,60B,200B
        if ma_label != "20B,60B,200B":
            continue

        # Tracking metrics
        closes = group.sort_values('day_offset')['close'].tolist()
        changes = group['change_pct'].tolist()

        # 1. Non-dropping ratio
        non_drop_ratio = sum(1 for c in changes if c >= 0) / len(changes) if len(changes) > 0 else 0

        # 2. Max Increase
        max_inc = max([(c - d1_close) / d1_close for c in closes]) if len(closes) > 0 else 0

        # 3. Max Drawdown
        prices_for_dd = [d1_close] + closes
        max_dd = calculate_max_drawdown(np.array(prices_for_dd))

        events.append({
            'trigger_ratio': trigger_ratio,
            'non_drop_ratio': non_drop_ratio,
            'max_inc': max_inc,
            'max_dd': max_dd
        })

    if not events:
        print("No events matching 20B,60B,200B criteria.")
        return None

    event_df = pd.DataFrame(events)

    # Binning Turnover Group (1.8-4.1+, 0.1 intervals)
    # Total 24 groups: 23 intervals of 0.1 starting from 1.8, plus a group for >4.1
    bins = [1.8 + i * 0.1 for i in range(24)] + [float('inf')]
    labels = [f"{bins[i]:.1f}-{bins[i+1]:.1f}" for i in range(len(bins)-2)] + [f">{bins[-2]:.1f}"]
    
    event_df['to_group'] = pd.cut(event_df['trigger_ratio'], bins=bins, labels=labels, right=False)

    results = []
    for tg in labels:
        subset = event_df[event_df['to_group'] == tg]
        if subset.empty:
            results.append({
                'Turnover Group': tg,
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
            'Turnover Group': tg,
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
    summary = run_granular_analysis(db_file)
    if summary is not None:
        markdown_table = summary.to_markdown(index=False)
        print(markdown_table)
        
        output_file = "analysis_summary_v2.md"
        with open(output_file, "w") as f:
            f.write("# Turnover Surge Analysis (20B, 60B, 200B) Granular Summary\n\n")
            f.write(markdown_table)
        print(f"\nResults saved to {output_file}")
