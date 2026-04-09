"""
analyze_v5_2.py
---------------
针对 turnover_surge.db 中极端情绪交易日（前 15% 和 后 15%）进行专项分析。
旨在观察在市场“极冷”或“极热”情况下，原有的 4 维度策略表现是否有所改善。

分组维度：
  Dim S: 情绪类型 (Extreme Low / Extreme High)
  Dim A: Day0 均线位置
  Dim B: 基准期均线位置
  Dim C: 换手率触发倍数
  Dim D: Day0 收盘涨幅
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

# ─── 路径配置（相对于项目根目录，与脚本位置无关）────────────────────────────
_ROOT = Path(__file__).parent.parent
SURGE_DB      = str(_ROOT / "turnover_surge.db")
OUTPUT_MD     = str(_ROOT / "output" / "analysis_v5_2.md")
OUTPUT_XLSX   = str(_ROOT / "output" / "analysis_v5_2.xlsx")
MIN_TRACK_DAYS = 3
MIN_COUNT_SHOW = 3     # 极端情况样本量较少，适当降低展示阈值

# Dim A 标签
A_LABELS = {
    0: "A0(↓20↓60↓200)", 1: "A1(↓20↓60↑200)", 2: "A2(↓20↑60↓200)", 3: "A3(↓20↑60↑200)",
    4: "A4(↑20↓60↓200)", 5: "A5(↑20↓60↑200)", 6: "A6(↑20↑60↓200)", 7: "A7(↑20↑60↑200)",
}

# Dim C 标签
C_LABELS = {
    0: "C1(1.5-1.8x)", 1: "C2(1.8-2.1x)", 2: "C3(2.1-2.4x)", 3: "C4(2.4-3.0x)", 4: "C5(>3.0x)",
}

# Dim D 标签
D_LABELS = {
    0: "D1(4%-6%)", 1: "D2(6%-8%)", 2: "D3(8%-涨停)", 3: "D4(涨停)",
}

# 情绪阈值
SENTIMENT_THRESHOLD_LOW  = 0.15 
SENTIMENT_THRESHOLD_HIGH = 0.15

# ─── 核心逻辑复用 ─────────────────────────────────────────────────────────────

def limit_up_threshold(stock_code: str) -> float:
    sc = str(stock_code).strip()
    if sc.startswith(("300", "301", "688", "689")): return 0.199
    return 0.099

def load_data() -> pd.DataFrame:
    """从 turnover_surge.db 加载所有数据"""
    import sqlite3
    conn = sqlite3.connect(SURGE_DB)
    df = pd.read_sql("SELECT * FROM turnover_surge", conn)
    conn.close()
    return df

def compute_events(df: pd.DataFrame) -> pd.DataFrame:
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
        
    return events

def _bl_level(above, below):
    total = above + below
    frac  = below / total.where(total > 0, other=np.nan)
    level = pd.cut(frac, bins=[-0.001, 1/3, 2/3, 1.001], labels=[0, 1, 2])
    return level.fillna(2).astype(int)

def assign_groups(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    # Dim A
    d0_close, d0_ma20, d0_ma60, d0_ma200 = [pd.to_numeric(df[c], errors='coerce') for c in ["d0_close", "d0_ma20", "d0_ma60", "d0_ma200"]]
    a20, a60, a200 = (d0_close > d0_ma20).fillna(False).astype(int), (d0_close > d0_ma60).fillna(False).astype(int), (d0_close > d0_ma200).fillna(False).astype(int)
    df["dim_A"] = (a20 * 4 + a60 * 2 + a200).map(A_LABELS)
    # Dim B
    for ma in [20, 60, 200]:
        df[f"_bl_lv{ma}"] = _bl_level(df[f"bl_above_ma{ma}"].fillna(0), df[f"bl_below_ma{ma}"].fillna(0))
    lv_names = {0: "L1强", 1: "L2震", 2: "L3弱"}
    df["dim_B"] = df["_bl_lv20"].map(lv_names) + "/" + df["_bl_lv60"].map(lv_names) + "/" + df["_bl_lv200"].map(lv_names)
    # Dim C
    df["dim_C"] = pd.cut(pd.to_numeric(df["trigger_ratio"], errors="coerce"), bins=[1.5, 1.8, 2.1, 2.4, 3.0, float("inf")], labels=[0, 1, 2, 3, 4], right=False).map(C_LABELS)
    # Dim D
    chg, thresh = pd.to_numeric(df["d0_change_pct"], errors="coerce"), df["stock_code"].apply(limit_up_threshold)
    conds = [(chg >= 0.04) & (chg < 0.06), (chg >= 0.06) & (chg < 0.08), (chg >= 0.08) & (chg < thresh), chg >= thresh]
    df["dim_D"] = pd.Series(np.select(conds, [0, 1, 2, 3], default=-1)).map(D_LABELS).values
    return df

# ─── 极端情绪过滤 ─────────────────────────────────────────────────────────────
def filter_extreme_only(events: pd.DataFrame):
    """提取排名前 15% 和 后 15% 的极端交易日"""
    daily_counts = events[events["d0_change_pct"] > 0.04].groupby("day0")["stock_code"].count().reset_index(name="count")
    low_val  = daily_counts["count"].quantile(SENTIMENT_THRESHOLD_LOW)
    high_val = daily_counts["count"].quantile(1 - SENTIMENT_THRESHOLD_HIGH)
    
    daily_counts["sentiment_type"] = "Normal"
    daily_counts.loc[daily_counts["count"] <= low_val,  "sentiment_type"] = "Extreme Cold (Low)"
    daily_counts.loc[daily_counts["count"] >= high_val, "sentiment_type"] = "Extreme Hot (High)"
    
    extreme_info = daily_counts[daily_counts["sentiment_type"] != "Normal"][["day0", "sentiment_type", "count"]]
    df_extreme = events.merge(extreme_info, on="day0", how="inner")
    
    stats = {
        "low_val": low_val, "high_val": high_val,
        "cold_days": len(daily_counts[daily_counts["sentiment_type"] == "Extreme Cold (Low)"]),
        "hot_days": len(daily_counts[daily_counts["sentiment_type"] == "Extreme Hot (High)"]),
        "cold_events": len(df_extreme[df_extreme["sentiment_type"] == "Extreme Cold (Low)"]),
        "hot_events": len(df_extreme[df_extreme["sentiment_type"] == "Extreme Hot (High)"])
    }
    return df_extreme, stats

def _cv(x):
    m = x.mean()
    return x.std(ddof=1) / abs(m) if not pd.isna(m) and m != 0 else np.nan

def compute_group_stats(events, group_cols):
    stats = events.groupby(group_cols, observed=True).agg(
        count=("max_gain", "count"), non_decline_avg=("non_decline_rate", "mean"),
        max_gain_avg=("max_gain", "mean"), max_gain_min=("max_gain", "min"), max_gain_cv=("max_gain", _cv),
        max_dd_avg=("max_drawdown", "mean"), max_dd_max=("max_drawdown", "max"), max_dd_cv=("max_drawdown", _cv),
    ).reset_index()
    for col in ["non_decline_avg", "max_gain_avg", "max_gain_min", "max_dd_avg", "max_dd_max"]:
        stats[col] = (stats[col] * 100).round(2)
    stats["max_gain_cv"], stats["max_dd_cv"] = stats["max_gain_cv"].round(3), stats["max_dd_cv"].round(3)
    stats["cv_sum"] = (stats["max_gain_cv"] + stats["max_dd_cv"]).round(3)
    return stats.sort_values("max_gain_avg", ascending=False).reset_index(drop=True)

def write_markdown(events, extreme_stats):
    lines = [
        "# 换手率异动信号 — 极端情绪环境分析 v5.2",
        "",
        "> 本报告专门分析被 v5.1 排除掉的 **前 15% (极热)** 和 **后 15% (极冷)** 交易日。",
        "",
        "## 1. 极端环境统计",
        f"- **低位阈值 (Cold)**: <= {extreme_stats['low_val']:.0f} 只股票/日，共 {extreme_stats['cold_days']} 天，产生 {extreme_stats['cold_events']} 个信号",
        f"- **高位阈值 (Hot)**: >= {extreme_stats['high_val']:.0f} 只股票/日，共 {extreme_stats['hot_days']} 天，产生 {extreme_stats['hot_events']} 个信号",
        "",
        "---",
    ]
    
    for s_type in ["Extreme Cold (Low)", "Extreme Hot (High)"]:
        sub_events = events[events["sentiment_type"] == s_type]
        stats = compute_group_stats(sub_events, ["dim_A", "dim_B", "dim_C", "dim_D"])
        filtered = stats[stats["count"] >= MIN_COUNT_SHOW]
        
        lines += [
            f"## 2. {s_type} 环境分析",
            f"在该环境下共有 {len(sub_events)} 个有效信号。",
            "",
            "| Dim A | Dim B | Dim C | Dim D | N | 非下跌% | 涨幅均% | 涨幅CV | 跌幅均% | CV之和 |",
            "|-------|-------|-------|-------|--:|--------:|--------:|-------:|--------:|-------:|",
        ]
        for _, r in filtered.head(20).iterrows():
            lines.append(f"| {r['dim_A']} | {r['dim_B']} | {r['dim_C']} | {r['dim_D']} | {int(r['count'])} | {r['non_decline_avg']:.1f}% | {r['max_gain_avg']:.2f}% | {r['max_gain_cv']:.3f} | {r['max_dd_avg']:.2f}% | {r['cv_sum']:.3f} |")
        lines += ["", "---", ""]

    Path(OUTPUT_MD).write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Markdown 已写入 {OUTPUT_MD}")

def main():
    print("📥 加载数据...")
    df = load_data()
    events = compute_events(df)
    print(f"📉 提取极端交易日 (Top/Bottom 15%)...")
    df_extreme, extreme_stats = filter_extreme_only(events)
    df_extreme = assign_groups(df_extreme)
    
    print("📈 计算统计量...")
    write_markdown(df_extreme, extreme_stats)
    
    with pd.ExcelWriter(OUTPUT_XLSX) as writer:
        for s_type in ["Extreme Cold (Low)", "Extreme Hot (High)"]:
            sub = df_extreme[df_extreme["sentiment_type"] == s_type]
            compute_group_stats(sub, ["dim_A", "dim_B", "dim_C", "dim_D"]).to_excel(writer, sheet_name=s_type[:25], index=False)
            # Add single dimension for each
            for dim in ["dim_A", "dim_B", "dim_C", "dim_D"]:
                compute_group_stats(sub, [dim]).to_excel(writer, sheet_name=f"{s_type[:10]}_{dim[-1]}", index=False)

    print(f"✅ Excel 已写入 {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
