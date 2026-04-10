"""
analyze_v6_1.py
---------------
432 groups analysis for turnover_surge.db signals:
- Dim Z1: Day-1 change pct (<0, 0~3, 3~涨停前, 涨停) -> 4 groups
- Dim Z2: Day0 change pct (<0, 0~3, 3~涨停前, 涨停) -> 4 groups
- Dim B: Baseline MAs (MA20/60/200 弱中强) -> 27 groups
Total: 4 * 4 * 27 = 432 cross groups.
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

_ROOT = Path(__file__).parent.parent
SURGE_DB      = str(_ROOT / "turnover_surge.db")
STOCK_DB      = str(_ROOT / "stock_data.db")
OUTPUT_MD     = str(_ROOT / "output" / "analysis_v6_1.md")
OUTPUT_XLSX   = str(_ROOT / "output" / "analysis_v6_1.xlsx")
MIN_TRACK_DAYS = 3
MIN_COUNT_SHOW = 3

def limit_up_threshold(stock_code: str) -> float:
    sc = str(stock_code).strip()
    if sc.startswith(("300", "301", "688", "689")):
        return 0.198
    return 0.098

def load_data() -> pd.DataFrame:
    # 1. 加载主数据
    conn = sqlite3.connect(SURGE_DB)
    df = pd.read_sql("SELECT * FROM turnover_surge", conn)
    conn.close()

    if df.empty: return df

    # 2. 从 stock_data.db 补齐 dm1_change_pct
    s_conn = sqlite3.connect(STOCK_DB)
    calendar = pd.read_sql("SELECT DISTINCT date FROM daily_kline ORDER BY date", s_conn)["date"].tolist()
    
    # 构建 day0 -> dm1 映射
    day0_to_dm1 = {}
    for i, d in enumerate(calendar):
        if i > 0: day0_to_dm1[d] = calendar[i-1]
    
    df["dm1_date"] = df["day0"].map(day0_to_dm1)
    
    # 提取所有需要的 dm1_date
    need_dates = df["dm1_date"].dropna().unique().tolist()
    if need_dates:
        ph = ",".join([f"'{x}'" for x in need_dates])
        dm1_kline = pd.read_sql(f"SELECT stock_code, date, change_pct as dm1_change_pct FROM daily_kline WHERE date IN ({ph})", s_conn)
        # 合并
        df = df.merge(dm1_kline, left_on=["stock_code", "dm1_date"], right_on=["stock_code", "date"], how="left")
    else:
        df["dm1_change_pct"] = np.nan
        
    s_conn.close()
    return df

def compute_events(df: pd.DataFrame) -> pd.DataFrame:
    meta = df.copy()

    meta["d0_change_pct"] = pd.to_numeric(meta.get("d0_change_pct"), errors="coerce")
    meta["d0_close"]      = pd.to_numeric(meta.get("d0_close"), errors="coerce")
    meta["dm1_change_pct"] = pd.to_numeric(meta.get("dm1_change_pct"), errors="coerce")
    
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
    
    return events

def _bl_level(above, below):
    total = above + below
    frac  = below / total.where(total > 0, other=np.nan)
    level = pd.cut(frac, bins=[-0.001, 1/3, 2/3, 1.001], labels=[0, 1, 2])
    return level.fillna(2).astype(int)

def _classify_pct(row, col):
    val = row[col]
    if pd.isna(val): return -1
    thresh = limit_up_threshold(row["stock_code"])
    if val < 0: return 0
    elif val < 0.03: return 1
    elif val < thresh: return 2
    else: return 3

def assign_groups(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    
    # Mappings
    CHG_LABELS = {0: "<0%", 1: "0~3%", 2: "3%~涨停前", 3: "涨停"}
    LV_LABELS = {0: "L1强", 1: "L2中", 2: "L3弱"}
    
    # Dim Z1 (dm1_change_pct)
    df["_Z1"] = df.apply(lambda r: _classify_pct(r, "dm1_change_pct"), axis=1)
    df["dim_Z1"] = df["_Z1"].map(CHG_LABELS).fillna("未知")
    
    # Dim Z2 (d0_change_pct)
    df["_Z2"] = df.apply(lambda r: _classify_pct(r, "d0_change_pct"), axis=1)
    df["dim_Z2"] = df["_Z2"].map(CHG_LABELS).fillna("未知")
    
    # Dim B (Baseline MA20/60/200)
    for ma in [20, 60, 200]:
        df[f"_bl_lv{ma}"] = _bl_level(pd.to_numeric(df[f"bl_above_ma{ma}"], errors="coerce").fillna(0), 
                                      pd.to_numeric(df[f"bl_below_ma{ma}"], errors="coerce").fillna(0))
    df["dim_B"] = df["_bl_lv20"].map(LV_LABELS) + "/" + df["_bl_lv60"].map(LV_LABELS) + "/" + df["_bl_lv200"].map(LV_LABELS)
    
    return df

def _cv(x):
    m = x.mean()
    return x.std(ddof=1) / abs(m) if not pd.isna(m) and m != 0 else np.nan

def compute_group_stats(events: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    stats = events.groupby(group_cols, observed=True).agg(
        count=("max_gain", "count"),
        non_decline_avg=("non_decline_rate", "mean"),
        max_gain_avg=("max_gain", "mean"),
        max_dd_avg=("max_drawdown", "mean"),
        max_gain_min=("max_gain", "min"),
        max_dd_max=("max_drawdown", "max"),
        max_gain_cv=("max_gain", _cv),
        max_dd_cv=("max_drawdown", _cv),
    ).reset_index()
    
    for col in ["non_decline_avg", "max_gain_avg", "max_dd_avg", "max_gain_min", "max_dd_max"]:
        stats[col] = (stats[col] * 100).round(2)
        
    for col in ["max_gain_cv", "max_dd_cv"]:
        stats[col] = stats[col].round(3)
        
    stats["cv_sum"] = (stats["max_gain_cv"] + stats["max_dd_cv"]).round(3)
    
    return stats.sort_values("max_gain_avg", ascending=False).reset_index(drop=True)

def write_markdown(stats, total_events):
    lines = [
        "# 换手率异动信号交叉分组回测 (v6.1)",
        "",
        f"> 总信号数: **{total_events}**",
        f"> 最大理论交叉组数: 4(Day-1) * 4(Day0) * 27(均线) = 432",
        f"> 实际产生数据的组合数: **{len(stats)}**",
        "",
        "## 统计指标说明",
        "- **非下跌日%**: 每组Day1~9非下跌日数比例的均值",
        "- **跌幅max%**: 本组中最极端的那个个股的最大跌幅极值(亏得最惨的那只)",
        "- **涨幅min%**: 本组中最弱的那个个股的最大涨幅极值(涨得最少的那只)",
        "",
        "| Dim Z1(Day-1) | Dim Z2(Day0) | Dim B(均线状态) | 样本数 | 非下跌日% | 涨幅均% | 跌幅均% | 涨幅min% | 跌幅max% | 涨幅CV | 跌幅CV | CV和 |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    
    for _, r in stats.iterrows():
        lines.append(f"| {r['dim_Z1']} | {r['dim_Z2']} | {r['dim_B']} | {int(r['count'])} | {r['non_decline_avg']:.1f}% | {r['max_gain_avg']:.2f}% | {r['max_dd_avg']:.2f}% | {r['max_gain_min']:.2f}% | {r['max_dd_max']:.2f}% | {r['max_gain_cv']:.3f} | {r['max_dd_cv']:.3f} | {r['cv_sum']:.3f} |")
        
    Path(OUTPUT_MD).write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Markdown output: {Path(OUTPUT_MD).resolve()}")

def write_excel(events, stats):
    with pd.ExcelWriter(OUTPUT_XLSX) as writer:
        stats.to_excel(writer, sheet_name="432交叉_Summary", index=False)
        
        # Dim Z1, Dim Z2, Dim B individually
        compute_group_stats(events, ["dim_Z1"]).to_excel(writer, sheet_name="DimZ1_Day-1涨幅", index=False)
        compute_group_stats(events, ["dim_Z2"]).to_excel(writer, sheet_name="DimZ2_Day0涨幅", index=False)
        compute_group_stats(events, ["dim_B"]).to_excel(writer, sheet_name="DimB_基准阶段均线", index=False)
        
        raw_export = events[["day0", "stock_code", "name", "dim_Z1", "dim_Z2", "dim_B", "dm1_change_pct", "d0_change_pct", "non_decline_rate", "max_gain", "max_drawdown"]].copy()
        for col in ["dm1_change_pct", "d0_change_pct", "non_decline_rate", "max_gain", "max_drawdown"]:
            raw_export[col] = (raw_export[col] * 100).round(2)
            
        raw_export.to_excel(writer, sheet_name="所有信号分布", index=False)
    print(f"✅ Excel output: {Path(OUTPUT_XLSX).resolve()}")

def main():
    print("=" * 60)
    print("📈 换手率异动信号交叉分组回测分析 v6.1")
    print("=" * 60)

    print("📥 Loading data...")
    df = load_data()
    if df.empty:
        print("❌ Data empty.")
        return
        
    print("📈 Computing events metrics...")
    events = compute_events(df)
    
    print("🏷️ Assigning 432 groups (Z1 x Z2 x B)...")
    events = assign_groups(events)
    
    print("📊 Calculating statistics and extremes...")
    stats = compute_group_stats(events, ["dim_Z1", "dim_Z2", "dim_B"])
    
    write_markdown(stats, len(events))
    write_excel(events, stats)
    print("🎉 Analyzation completed.")

if __name__ == "__main__":
    main()
