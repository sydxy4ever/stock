import os
import glob
import re

for filepath in glob.glob("/data/stock/analyze/analyze_v5_*.py"):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Replace load_data
    old_load = re.search(r'def load_data\(\) -> pd\.DataFrame:.*?return df', content, flags=re.DOTALL)
    new_load = '''def load_data() -> pd.DataFrame:
    """从 turnover_surge.db 加载所有数据"""
    import sqlite3
    conn = sqlite3.connect(SURGE_DB)
    df = pd.read_sql("SELECT * FROM turnover_surge", conn)
    conn.close()
    return df'''
    
    if old_load:
        content = content[:old_load.start()] + new_load + content[old_load.end():]

    # Replace compute_events
    old_events = re.search(r'def compute_events\([^)]+\) -> pd\.DataFrame:.*?return events', content, flags=re.DOTALL)
    new_events = '''def compute_events(df: pd.DataFrame) -> pd.DataFrame:
    """计算事件级统计指标。跳过有效跟踪天数 < MIN_TRACK_DAYS 或 d0_close <= 0 的事件。"""
    meta = df.copy()

    meta["d0_change_pct"] = pd.to_numeric(meta.get("d0_change_pct"), errors="coerce")
    meta["d0_close"]      = pd.to_numeric(meta.get("d0_close"), errors="coerce")
    meta["dm1_close"]     = pd.to_numeric(meta.get("dm1_close"), errors="coerce")
    
    mask = meta["d0_change_pct"].isna() & meta["d0_close"].notna() & meta["dm1_close"].notna() & (meta["dm1_close"] > 0)
    meta.loc[mask, "d0_change_pct"] = meta.loc[mask, "d0_close"] / meta.loc[mask, "dm1_close"] - 1

    close_cols = [f"d{i}_close" for i in range(1, 10) if f"d{i}_close" in meta.columns]
    pct_cols   = [f"d{i}_change_pct" for i in range(1, 10) if f"d{i}_change_pct" in meta.columns]
    
    closes = meta[close_cols].apply(pd.to_numeric, errors='coerce')
    pcts   = meta[pct_cols].apply(pd.to_numeric, errors='coerce')

    meta["max_close"]        = closes.max(axis=1)
    meta["min_close"]        = closes.min(axis=1)
    meta["track_days"]       = closes.notna().sum(axis=1)
    meta["non_decline_days"] = (pcts >= 0).sum(axis=1)

    events = meta[meta["track_days"] >= MIN_TRACK_DAYS].copy()
    events = events[events["d0_close"] > 0].copy()

    events["max_gain"]        = events["max_close"] / events["d0_close"] - 1
    events["max_drawdown"]    = 1 - events["min_close"] / events["d0_close"]
    events["non_decline_rate"] = events["non_decline_days"] / events["track_days"]

    if "trigger_ratio_2" in events.columns:
        events["trigger_ratio"] = events["trigger_ratio_2"]
    elif "trigger_ratio_1" in events.columns:
        events["trigger_ratio"] = events["trigger_ratio_1"]
        
    return events'''
    
    if old_events:
        content = content[:old_events.start()] + new_events + content[old_events.end():]

    # Special handling for analyze_v5_3
    # v5_3 might compute "days_to_peak" & "days_to_trough" using argmax/argmin on axis=1 instead of days indexing
    # We will refine v5_3 later if needed, let's just dump
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
