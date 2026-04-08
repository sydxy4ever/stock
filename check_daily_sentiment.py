import sqlite3
import pandas as pd
import numpy as np

def analyze_daily_counts():
    conn = sqlite3.connect('/data/stock/turnover_surge.db')
    
    # Query to get unique (day0, stock_code) and their d0_change_pct
    # We only need one row per (day0, stock_code)
    query = """
    SELECT day0, stock_code, d0_change_pct
    FROM turnover_surge
    WHERE day_offset = 1
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Filter for change > 4%
    df_filtered = df[df['d0_change_pct'] > 0.04].copy()
    
    # Count stocks per day
    daily_counts = df_filtered.groupby('day0')['stock_code'].count().reset_index()
    daily_counts.columns = ['date', 'count']
    
    if daily_counts.empty:
        print("No stocks found with d0_change_pct > 4%")
        return
    
    # Sort by count to find percentiles
    daily_counts = daily_counts.sort_values('count')
    n = len(daily_counts)
    low_idx = int(n * 0.15)
    high_idx = int(n * 0.85)
    
    low_threshold = daily_counts.iloc[low_idx]['count']
    high_threshold = daily_counts.iloc[high_idx]['count']
    
    print(f"Total trading days with surge signals: {n}")
    print(f"15th percentile count: {low_threshold}")
    print(f"85th percentile count: {high_threshold}")
    
    # Filtered days
    normal_days = daily_counts[(daily_counts['count'] >= low_threshold) & (daily_counts['count'] <= high_threshold)]
    
    print(f"Remaining days after excluding top/bottom 15%: {len(normal_days)}")
    print("\nSample of daily counts (bottom 5):")
    print(daily_counts.head(5))
    print("\nSample of daily counts (top 5):")
    print(daily_counts.tail(5))
    
    return daily_counts, low_threshold, high_threshold

if __name__ == "__main__":
    analyze_daily_counts()
