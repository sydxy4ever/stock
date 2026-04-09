"""
analyze_v5_3.py
---------------
针对 turnover_surge.db 中极端情绪交易日进行专项分析。
重点增加：股价达峰（Peak Day）和 触底（Trough Day）的日数统计。
重点关注维度：Dim A (A0), Dim B (L3/L3/L3), Dim D (D2/D3)。
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

# ─── 路径配置（相对于项目根目录，与脚本位置无关）────────────────────────────
_ROOT = Path(__file__).parent.parent
SURGE_DB      = str(_ROOT / "turnover_surge.db")
OUTPUT_MD     = str(_ROOT / "output" / "analysis_v5_3.md")
OUTPUT_XLSX   = str(_ROOT / "output" / "analysis_v5_3.xlsx")
MIN_TRACK_DAYS = 3
MIN_COUNT_SHOW = 3

# Dim A 标签
A_LABELS = {
    0: "A0(↓20↓60↓200)", 1: "A1(↓20↓60↑200)", 2: "A2(↓20↑60↓200)", 3: "A3(↓20↑60↑200)",
    4: "A4(↑20↓60↓200)", 5: "A5(↑20↓60↑200)", 6: "A6(↑20↑60↓200)", 7: "A7(↑20↑60↑200)",
}

# Dim C 标签 (动态生成在 assign_groups 中)
# Dim D 标签
D_LABELS = {
    0: "D1(4%-6%)", 1: "D2+D3(>6%)", 2: "D4(涨停)",
}

# 情绪阈值
SENTIMENT_THRESHOLD_LOW  = 0.15 
SENTIMENT_THRESHOLD_HIGH = 0.15

# ─── 核心逻辑 ─────────────────────────────────────────────────────────────────

def limit_up_threshold(stock_code: str) -> float:
    sc = str(stock_code).strip()
    if sc.startswith(("300", "301", "688", "689")): return 0.199
    return 0.099

def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(SURGE_DB)
    df = pd.read_sql("SELECT * FROM turnover_surge WHERE day_offset IS NOT NULL AND close IS NOT NULL", conn)
    conn.close()
    return df

def compute_events(df: pd.DataFrame) -> pd.DataFrame:
    meta_cols = [
        "day0", "stock_code", "name", "trigger_ratio",
        "d0_change_pct", "d0_close", "d0_ma20", "d0_ma60", "d0_ma200", "dm1_close",
        "bl_above_ma20", "bl_below_ma20", "bl_above_ma60", "bl_below_ma60", "bl_above_ma200", "bl_below_ma200",
        "baseline_to_r",
    ]
    meta = df.groupby(["day0", "stock_code"])[meta_cols[2:]].first().reset_index()
    meta["d0_change_pct"] = pd.to_numeric(meta["d0_change_pct"], errors="coerce")
    meta["d0_close"]      = pd.to_numeric(meta["d0_close"], errors="coerce")
    meta["dm1_close"]     = pd.to_numeric(meta["dm1_close"], errors="coerce")
    mask = meta["d0_change_pct"].isna() & meta["d0_close"].notna() & meta["dm1_close"].notna() & (meta["dm1_close"] > 0)
    meta.loc[mask, "d0_change_pct"] = meta.loc[mask, "d0_close"] / meta.loc[mask, "dm1_close"] - 1

    track = df.copy()
    track["close"] = pd.to_numeric(track["close"], errors="coerce")
    track["change_pct"] = pd.to_numeric(track["change_pct"], errors="coerce")
    track["day_offset"] = pd.to_numeric(track["day_offset"], errors="coerce")
    track = track.dropna(subset=["close"])
    
    # 找到最大/最小收盘价对应的索引
    max_idx = track.groupby(["day0", "stock_code"])["close"].idxmax()
    min_idx = track.groupby(["day0", "stock_code"])["close"].idxmin()
    
    # 基础聚合
    track_agg = track.groupby(["day0", "stock_code"]).agg(
        track_days = ("close", "count"),
        non_decline_days = ("change_pct", lambda x: (pd.to_numeric(x, errors="coerce") >= 0).sum()),
    ).reset_index()
    
    # 合并最大/最小价格及日期
    max_data = track.loc[max_idx, ["day0", "stock_code", "close", "day_offset"]].rename(
        columns={"close": "max_close", "day_offset": "max_gain_day"}
    )
    min_data = track.loc[min_idx, ["day0", "stock_code", "close", "day_offset"]].rename(
        columns={"close": "min_close", "day_offset": "max_dd_day"}
    )
    
    track_agg = track_agg.merge(max_data, on=["day0", "stock_code"]).merge(min_data, on=["day0", "stock_code"])

    events = meta.merge(track_agg, on=["day0", "stock_code"], how="inner")
    events = events[(events["track_days"] >= MIN_TRACK_DAYS) & (events["d0_close"] > 0)].copy()
    
    events["max_gain"] = events["max_close"] / events["d0_close"] - 1
    events["max_drawdown"] = 1 - events["min_close"] / events["d0_close"]
    events["non_decline_rate"] = events["non_decline_days"] / events["track_days"]
    
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
    # Dim C: 1.5开始，每隔0.1分一个组
    c_bins = np.arange(1.5, 3.1, 0.1).tolist() + [float('inf')]
    c_labels = [f"C({c:.1f}-{c+0.1:.1f}x)" for c in np.arange(1.5, 3.0, 0.1)] + ["C(>3.0x)"]
    df["dim_C"] = pd.cut(pd.to_numeric(df["trigger_ratio"], errors="coerce"), 
                         bins=c_bins, labels=c_labels, right=False)
    
    # Dim D: 合并 D2 和 D3
    chg, thresh = pd.to_numeric(df["d0_change_pct"], errors="coerce"), df["stock_code"].apply(limit_up_threshold)
    conds = [(chg >= 0.04) & (chg < 0.06), (chg >= 0.06) & (chg < thresh), chg >= thresh]
    df["dim_D"] = pd.Series(np.vectorize(lambda x: D_LABELS.get(x, "Other"))(np.select(conds, [0, 1, 2], default=-1))).values
    return df

def filter_extreme_only(events: pd.DataFrame):
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
    }
    return df_extreme, stats

def _cv(x):
    m = x.mean()
    return x.std(ddof=1) / abs(m) if not pd.isna(m) and m != 0 else np.nan

def compute_group_stats(events, group_cols):
    stats = events.groupby(group_cols, observed=True).agg(
        count=("max_gain", "count"),
        non_decline_avg=("non_decline_rate", "mean"),
        max_gain_avg=("max_gain", "mean"),
        max_gain_cv=("max_gain", _cv),
        peak_day_avg=("max_gain_day", "mean"),
        peak_day_cv=("max_gain_day", _cv),
        max_dd_avg=("max_drawdown", "mean"),
        max_dd_cv=("max_drawdown", _cv),
        trough_day_avg=("max_dd_day", "mean"),
        trough_day_cv=("max_dd_day", _cv),
    ).reset_index()
    
    # 转换为百分位
    for col in ["non_decline_avg", "max_gain_avg", "max_dd_avg"]:
        stats[col] = (stats[col] * 100).round(2)
    
    # 保留 CV 精度
    for col in ["max_gain_cv", "max_dd_cv", "peak_day_cv", "trough_day_cv"]:
        stats[col] = stats[col].round(3)
        
    # 日期取 1 位小数
    stats["peak_day_avg"] = stats["peak_day_avg"].round(1)
    stats["trough_day_avg"] = stats["trough_day_avg"].round(1)
    
    stats["cv_sum"] = (stats["max_gain_cv"] + stats["max_dd_cv"]).round(3)
    return stats.sort_values("max_gain_avg", ascending=False).reset_index(drop=True)

def write_markdown(events, extreme_stats):
    lines = [
        "# 换手率异动信号 — 达峰与触底时机分析 v5.3",
        "",
        "> 本报告重点关注 **Peak Day (达峰日数)** 和 **Trough Day (触底日数)**，以支持止盈止损策略。",
        "",
        "## 1. 极端环境概览",
        f"- **Cold 阈值**: <= {extreme_stats['low_val']:.0f} 只，共 {extreme_stats['cold_days']} 天",
        f"- **Hot 阈值**: >= {extreme_stats['high_val']:.0f} 只，共 {extreme_stats['hot_days']} 天",
        "",
        "---",
    ]
    
    for s_type in ["Extreme Cold (Low)", "Extreme Hot (High)"]:
        sub_events = events[events["sentiment_type"] == s_type]
        if sub_events.empty:
            continue
            
        # 既然已经全局过滤，这里直接按 Dim C 分组
        stats = compute_group_stats(sub_events, ["dim_C"])
        
        lines += [
            f"## 2. {s_type} 环境分析",
            f"有效信号总数: {len(sub_events)}",
            f"筛选条件: Dim A=A0, Dim B=L3/L3/L3, Dim D=D2+D3(>6%)",
            "",
            "| Dim C | N | 涨幅均% | 涨幅CV | 达峰日 | 跌幅均% | 跌幅CV | 触底日 | CV之和 |",
            "|-------|--:|--------:|-------:|-------:|--------:|-------:|-------:|-------:|",
        ]
        for _, r in stats.iterrows():
            lines.append(f"| {r['dim_C']} | {int(r['count'])} | {r['max_gain_avg']:.2f}% | {r['max_gain_cv']:.3f} | {r['peak_day_avg']} | {r['max_dd_avg']:.2f}% | {r['max_dd_cv']:.3f} | {r['trough_day_avg']} | {r['cv_sum']:.3f} |")
        
        lines += ["", "---", ""]

    Path(OUTPUT_MD).write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Markdown 已写入 {OUTPUT_MD}")

def main():
    print("📥 加载数据...")
    df = load_data()
    events = compute_events(df)
    print(f"📉 提取极端交易日...")
    df_extreme, extreme_stats = filter_extreme_only(events)
    df_extreme = assign_groups(df_extreme)
    
    # ─── 根据用户需求进行过滤 ───
    # A0(↓20↓60↓200), L3弱/L3弱/L3弱, D2+D3(>6%)
    df_extreme = df_extreme[
        (df_extreme["dim_A"].str.startswith("A0")) & 
        (df_extreme["dim_B"] == "L3弱/L3弱/L3弱") & 
        (df_extreme["dim_D"] == "D2+D3(>6%)")
    ].copy()
    
    print(f"📊 过滤后剩余信号: {len(df_extreme)}")
    
    print("📈 计算统计量并输出...")
    write_markdown(df_extreme, extreme_stats)
    
    # ─── 提取 1-9 天详情数据 (D1-D9 涨跌与换手) ───
    if not df_extreme.empty:
        # 获取相关信号的基础信息
        relevant_signals = df_extreme[["day0", "stock_code", "name", "dim_A", "dim_B", "dim_C", "dim_D", "sentiment_type", "baseline_to_r"]]
        
        # 从原始 df 中提取 Day 1 - 9 的数据
        # 首先合并信号信息，确保只取筛选后的股票日期对
        details = df.merge(relevant_signals[["day0", "stock_code"]], on=["day0", "stock_code"])
        details = details[(details["day_offset"] >= 1) & (details["day_offset"] <= 9)].copy()
        
        # 转换并透视
        details["change_pct"] = pd.to_numeric(details["change_pct"], errors="coerce") * 100
        details["to_r"] = pd.to_numeric(details["to_r"], errors="coerce")
        
        # 透视涨幅和换手率
        p_chg = details.pivot(index=["day0", "stock_code"], columns="day_offset", values="change_pct")
        p_tor = details.pivot(index=["day0", "stock_code"], columns="day_offset", values="to_r")
        
        # 列名重命名
        p_chg.columns = [f"D{int(c)}_chg%" for c in p_chg.columns]
        p_tor.columns = [f"D{int(c)}_to" for c in p_tor.columns]
        
        # 合并回基础信息表
        df_details = relevant_signals.merge(p_chg, on=["day0", "stock_code"], how="left").merge(p_tor, on=["day0", "stock_code"], how="left")
    else:
        df_details = pd.DataFrame()

    with pd.ExcelWriter(OUTPUT_XLSX) as writer:
        for s_type in ["Extreme Cold (Low)", "Extreme Hot (High)"]:
            sub = df_extreme[df_extreme["sentiment_type"] == s_type]
            if sub.empty: continue
            full_stats = compute_group_stats(sub, ["dim_C"])
            full_stats.to_excel(writer, sheet_name=s_type[:25], index=False)
        
        if not df_details.empty:
            df_details.to_excel(writer, sheet_name="Detailed_Signals", index=False)

    print(f"✅ Excel 已写入 {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
