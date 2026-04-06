import sqlite3
import pandas as pd
import numpy as np

def calculate_max_drawdown(prices):
    """
    Calculate max drawdown from the peak.
    Prices: list/array of prices including the base price (D1).
    """
    if len(prices) == 0:
        return 0.0
    rolling_max = np.maximum.accumulate(prices)
    drawdowns = (rolling_max - prices) / rolling_max
    return np.max(drawdowns)

def run_analysis(db_path):
    conn = sqlite3.connect(db_path)
    # Load data with tracking info
    df = pd.read_sql("SELECT * FROM turnover_surge WHERE day_offset IS NOT NULL", conn)
    conn.close()

    if df.empty:
        print("No tracking data found in database.")
        return None

    # Group by event (day1, stock_code)
    events = []
    for (day1, stock_code), group in df.groupby(['day1', 'stock_code']):
        # Baseline trigger info (same for all offsets of this event)
        row = group.iloc[0]
        d1_close = row['d1_close']
        d1_ma20 = row['d1_ma20']
        d1_ma60 = row['d1_ma60']
        d1_ma200 = row['d1_ma200']
        trigger_ratio = row['trigger_ratio']

        # MA intersection (8 combinations)
        ma20_pos = "Above" if (pd.notnull(d1_ma20) and d1_close > d1_ma20) else "Below"
        ma60_pos = "Above" if (pd.notnull(d1_ma60) and d1_close > d1_ma60) else "Below"
        ma200_pos = "Above" if (pd.notnull(d1_ma200) and d1_close > d1_ma200) else "Below"
        
        ma_label = f"20{ma20_pos[0]},60{ma60_pos[0]},200{ma200_pos[0]}"

        # Turnover Group
        if trigger_ratio < 2.5:
            to_group = "1.8-2.4"
        elif trigger_ratio < 3.0:
            to_group = "2.5-3.0"
        else:
            to_group = ">3.0"

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

        event_data = {
            'day1': day1,
            'stock_code': stock_code,
            'to_group': to_group,
            'ma_label': ma_label,
            'non_drop_ratio': non_drop_ratio,
            'max_inc': max_inc,
            'max_dd': max_dd
        }
        events.append(event_data)

    event_df = pd.DataFrame(events)

    # Cross-aggregate results for 24 groups
    to_groups = ["1.8-2.4", "2.5-3.0", ">3.0"]
    ma_labels = sorted(event_df['ma_label'].unique())

    results = []
    for tg in to_groups:
        for ma in ma_labels:
            subset = event_df[(event_df['to_group'] == tg) & (event_df['ma_label'] == ma)]
            if subset.empty:
                continue
            
            mean_inc = subset['max_inc'].mean()
            std_inc = subset['max_inc'].std()
            cv_inc = std_inc / mean_inc if mean_inc != 0 else 0

            mean_dd = subset['max_dd'].mean()
            std_dd = subset['max_dd'].std()
            cv_dd = std_dd / mean_dd if mean_dd != 0 else 0

            results.append({
                'Turnover Group': tg,
                'MA Status': ma,
                'Count': len(subset),
                'Non-drop %': subset['non_drop_ratio'].mean() * 100,
                'Max Inc Mean': mean_inc * 100,
                'Max Inc CV': cv_inc,
                'Max DD Mean': mean_dd * 100,
                'Max DD CV': cv_dd
            })

    return pd.DataFrame(results)

if __name__ == "__main__":
    import os
    db_file = "turnover_surge.db"
    if not os.path.exists(db_file):
        print(f"Error: {db_file} not found.")
    else:
        summary = run_analysis(db_file)
        if summary is not None:
            # Print as Markdown Table
            markdown_table = summary.to_markdown(index=False)
            print(markdown_table)
            
            # Save to file
            output_file = "analysis_summary.md"
            with open(output_file, "w") as f:
                f.write("# Turnover Surge Analysis Summary\n\n")
                f.write(markdown_table)
            print(f"\nResults saved to {output_file}")
