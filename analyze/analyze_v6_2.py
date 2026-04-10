"""
analyze_v6_2.py
---------------
以「上穿均线」为核心维度，分析换手率异动信号的后续表现。
二维分组：
  - Dim P1: Day-1 对 MA20/60/200 上穿状态，2³=8 组
    上穿定义：Day-2 在线下（dm2_above_maX==0）且 Day-1 在线上（dm1_above_maX==1）
  - Dim P0: Day0 对 MA20/60/200 上穿状态，2³=8 组
    上穿定义：Day-1 在线下（dm1_above_maX==0）且 Day0 在线上（d0_above_maX==1）

最大理论组数: 8 × 8 = 64 组

Day-2 数据从 stock_data.db（daily_kline + moving_averages）动态补取。
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

_ROOT = Path(__file__).parent.parent
SURGE_DB     = str(_ROOT / "turnover_surge.db")
STOCK_DB     = str(_ROOT / "stock_data.db")
OUTPUT_MD    = str(_ROOT / "output" / "analysis_v6_2.md")
OUTPUT_XLSX  = str(_ROOT / "output" / "analysis_v6_2.xlsx")
MIN_TRACK_DAYS = 3
MIN_COUNT_SHOW = 5

PIERCE_LABELS = {
    "000": "无穿线",
    "100": "仅穿MA20",
    "010": "仅穿MA60",
    "001": "仅穿MA200",
    "110": "穿MA20+60",
    "101": "穿MA20+200",
    "011": "穿MA60+200",
    "111": "穿MA20+60+200",
}


# ─── 1. 数据加载 ─────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    """加载主库并从 stock_data.db 补取 Day-2 的均线位置"""
    conn = sqlite3.connect(SURGE_DB)
    df = pd.read_sql("SELECT * FROM turnover_surge", conn)
    conn.close()

    if df.empty:
        return df

    # 交易日历 -> 推算 dm2_date
    s_conn = sqlite3.connect(STOCK_DB)
    calendar = pd.read_sql(
        "SELECT DISTINCT date FROM daily_kline ORDER BY date", s_conn
    )["date"].tolist()

    day0_to_dm2 = {d: calendar[i - 2] for i, d in enumerate(calendar) if i >= 2}
    df["dm2_date"] = df["day0"].map(day0_to_dm2)

    # 按唯一 dm2_date（交易日，数量有限）分批查询，然后在 Python 侧 merge
    unique_dm2_dates = df["dm2_date"].dropna().unique().tolist()
    print(f"   需补取 Day-2 数据的唯一日期数：{len(unique_dm2_dates)}")

    dm2_parts = []
    chunk_size = 50   # 每批 50 个日期
    for i in range(0, len(unique_dm2_dates), chunk_size):
        chunk = unique_dm2_dates[i : i + chunk_size]
        ph = ",".join([f"'{x}'" for x in chunk])
        part = pd.read_sql(
            f"""SELECT k.stock_code, k.date AS dm2_date, k.close AS dm2_close,
                       m.ma20, m.ma60, m.ma200
                FROM daily_kline k
                LEFT JOIN moving_averages m
                       ON k.stock_code = m.stock_code AND k.date = m.date
                WHERE k.date IN ({ph})""",
            s_conn,
        )
        dm2_parts.append(part)
        if (i // chunk_size) % 10 == 0:
            print(f"   Day-2 查询进度：{min(i+chunk_size, len(unique_dm2_dates))}/{len(unique_dm2_dates)} 日期...")

    s_conn.close()

    dm2 = pd.concat(dm2_parts, ignore_index=True)

    # 仅保留信号中出现的 (stock_code, dm2_date) 对，避免膨胀
    need = df[["stock_code", "dm2_date"]].dropna(subset=["dm2_date"]).drop_duplicates()
    dm2 = dm2.merge(need, on=["stock_code", "dm2_date"], how="inner")

    df = df.merge(dm2, on=["stock_code", "dm2_date"], how="left")

    dm2_close = pd.to_numeric(df["dm2_close"], errors="coerce")
    for ma in [20, 60, 200]:
        ma_val = pd.to_numeric(df[f"ma{ma}"], errors="coerce")
        df[f"dm2_above_ma{ma}"] = ((dm2_close > ma_val) & ma_val.notna()).astype("Int8")

    df = df.drop(columns=["ma20", "ma60", "ma200", "dm2_close"], errors="ignore")
    return df


# ─── 2. 事件级指标 ───────────────────────────────────────────────────────────

def compute_events(df: pd.DataFrame) -> pd.DataFrame:
    meta = df.copy()
    meta["d0_close"] = pd.to_numeric(meta["d0_close"], errors="coerce")

    close_cols = [f"d{i}_close" for i in range(1, 10) if f"d{i}_close" in meta.columns]
    pct_cols   = [f"d{i}_change_pct" for i in range(1, 10) if f"d{i}_change_pct" in meta.columns]
    closes = meta[close_cols].apply(pd.to_numeric, errors="coerce")
    pcts   = meta[pct_cols].apply(pd.to_numeric, errors="coerce")

    meta["max_close"]        = closes.max(axis=1)
    meta["min_close"]        = closes.min(axis=1)
    meta["track_days"]       = closes.notna().sum(axis=1)
    meta["non_decline_days"] = (pcts >= 0).sum(axis=1)

    events = meta[meta["track_days"] >= MIN_TRACK_DAYS].copy()
    events = events[events["d0_close"] > 0].copy()

    events["max_gain"]         = events["max_close"] / events["d0_close"] - 1
    events["max_drawdown"]     = 1 - events["min_close"] / events["d0_close"]
    events["non_decline_rate"] = events["non_decline_days"] / events["track_days"]
    return events


# ─── 3. 分组标注 ─────────────────────────────────────────────────────────────

def _pierce_dim(df: pd.DataFrame, from_prefix: str, to_prefix: str, dim_name: str) -> pd.DataFrame:
    """
    计算某个方向的上穿标签，写入 df[dim_name]。
    from_prefix: "dm2" | "dm1"
    to_prefix:   "dm1" | "d0"
    """
    d0_close = pd.to_numeric(df["d0_close"], errors="coerce")
    bits = []
    for ma in [20, 60, 200]:
        from_below = pd.to_numeric(df.get(f"{from_prefix}_above_ma{ma}"), errors="coerce").fillna(0) == 0

        to_col = f"{to_prefix}_above_ma{ma}"
        if to_col in df.columns:
            to_above = pd.to_numeric(df[to_col], errors="coerce").fillna(0) == 1
        else:
            # 动态计算（d0_above_ma200 有时缺失）
            ma_col = f"{to_prefix}_ma{ma}"
            if ma_col in df.columns:
                ma_val = pd.to_numeric(df[ma_col], errors="coerce")
                to_above = (d0_close > ma_val) & ma_val.notna()
            else:
                to_above = pd.Series(False, index=df.index)

        bits.append((from_below & to_above).astype(int))

    key = bits[0].astype(str) + bits[1].astype(str) + bits[2].astype(str)
    df[dim_name] = key.map(PIERCE_LABELS).fillna("其他")
    return df


def assign_groups(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    df = _pierce_dim(df, from_prefix="dm2", to_prefix="dm1", dim_name="dim_P1")
    df = _pierce_dim(df, from_prefix="dm1", to_prefix="d0",  dim_name="dim_P0")
    return df


# ─── 4. 组级统计 ─────────────────────────────────────────────────────────────

def _cv(x):
    m = x.mean()
    return x.std(ddof=1) / abs(m) if not pd.isna(m) and m != 0 else np.nan


def compute_group_stats(events: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    stats = (
        events.groupby(group_cols, observed=True)
        .agg(
            count=("max_gain", "count"),
            non_decline_avg=("non_decline_rate", "mean"),
            max_gain_avg=("max_gain", "mean"),
            max_gain_min=("max_gain", "min"),
            max_gain_cv=("max_gain", _cv),
            max_dd_avg=("max_drawdown", "mean"),
            max_dd_max=("max_drawdown", "max"),
            max_dd_cv=("max_drawdown", _cv),
        )
        .reset_index()
    )
    for col in ["non_decline_avg", "max_gain_avg", "max_gain_min", "max_dd_avg", "max_dd_max"]:
        stats[col] = (stats[col] * 100).round(2)
    for col in ["max_gain_cv", "max_dd_cv"]:
        stats[col] = stats[col].round(3)
    stats["cv_sum"] = (stats["max_gain_cv"] + stats["max_dd_cv"]).round(3)
    return stats.sort_values("max_gain_avg", ascending=False).reset_index(drop=True)


# ─── 5. 输出 Markdown ────────────────────────────────────────────────────────

def write_markdown(stats_2d: pd.DataFrame, events: pd.DataFrame):
    total = len(events)
    n_show = len(stats_2d[stats_2d["count"] >= MIN_COUNT_SHOW])
    lines = [
        "# 换手率异动信号 — 均线上穿二维分组分析 v6.2",
        "",
        f"> 数据：`turnover_surge.db` + `stock_data.db`（Day-2补取）  ",
        f"> 总信号：**{total:,}**（跟踪期 ≥ {MIN_TRACK_DAYS} 天）  ",
        f"> 理论组数：8（Day-1穿线） × 8（Day0穿线）= 64  ",
        f"> 展示组数（N ≥ {MIN_COUNT_SHOW}）：**{n_show}**",
        "",
        "## 穿线标签说明",
        "",
        "| 标签 | 含义 |",
        "|------|------|",
    ]
    for key, label in PIERCE_LABELS.items():
        lines.append(f"| {label} | MA20({key[0]}) MA60({key[1]}) MA200({key[2]}) |")

    lines += [
        "",
        "---",
        "",
        f"## 二维主表（N ≥ {MIN_COUNT_SHOW}，按涨幅均值降序）",
        "",
        "| Day-1穿线 | Day0穿线 | N | 非下跌日% | 涨幅均% | 涨幅min% | 涨幅CV | 跌幅均% | 跌幅max% | 跌幅CV | CV和 |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in stats_2d[stats_2d["count"] >= MIN_COUNT_SHOW].iterrows():
        lines.append(
            f"| {r['dim_P1']} | {r['dim_P0']} "
            f"| {int(r['count'])} "
            f"| {r['non_decline_avg']:.1f}% "
            f"| {r['max_gain_avg']:.2f}% "
            f"| {r['max_gain_min']:.2f}% "
            f"| {r['max_gain_cv']:.3f} "
            f"| {r['max_dd_avg']:.2f}% "
            f"| {r['max_dd_max']:.2f}% "
            f"| {r['max_dd_cv']:.3f} "
            f"| {r['cv_sum']:.3f} |"
        )

    def dim_table(ev, col, title, hdr):
        g = compute_group_stats(ev, [col])
        rows = ["", "---", "", f"## {title}（单维度）", "",
                f"| {hdr} | N | 非下跌日% | 涨幅均% | 涨幅min% | 涨幅CV | 跌幅均% | 跌幅max% | 跌幅CV | CV和 |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for _, r in g.iterrows():
            rows.append(
                f"| {r[col]} | {int(r['count'])} "
                f"| {r['non_decline_avg']:.1f}% | {r['max_gain_avg']:.2f}% "
                f"| {r['max_gain_min']:.2f}% | {r['max_gain_cv']:.3f} "
                f"| {r['max_dd_avg']:.2f}% | {r['max_dd_max']:.2f}% "
                f"| {r['max_dd_cv']:.3f} | {r['cv_sum']:.3f} |"
            )
        return rows

    lines += dim_table(events, "dim_P1", "Dim P1 — Day-1 穿线", "Day-1穿线")
    lines += dim_table(events, "dim_P0", "Dim P0 — Day0 穿线",  "Day0穿线")
    lines += ["", "---", "", "*生成脚本：`analyze_v6_2.py`*"]

    Path(OUTPUT_MD).write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Markdown 已写入：{Path(OUTPUT_MD).resolve()}")


# ─── 6. 输出 Excel ───────────────────────────────────────────────────────────

def write_excel(events: pd.DataFrame, stats_2d: pd.DataFrame):
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        stats_2d.to_excel(writer, sheet_name="2D_P1xP0", index=False)
        compute_group_stats(events, ["dim_P1"]).to_excel(writer, sheet_name="1D_P1_Day-1穿线", index=False)
        compute_group_stats(events, ["dim_P0"]).to_excel(writer, sheet_name="1D_P0_Day0穿线", index=False)

        raw_cols = ["day0", "stock_code", "name", "dim_P1", "dim_P0",
                    "d0_change_pct", "non_decline_rate", "max_gain", "max_drawdown"]
        raw = events[[c for c in raw_cols if c in events.columns]].copy()
        for col in ["d0_change_pct", "non_decline_rate", "max_gain", "max_drawdown"]:
            if col in raw.columns:
                raw[col] = (raw[col] * 100).round(2)
        raw.to_excel(writer, sheet_name="原始信号事件", index=False)
    print(f"✅ Excel 已写入：{Path(OUTPUT_XLSX).resolve()}")


# ─── 主流程 ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("📈 换手率异动信号 — 均线上穿分析 v6.2")
    print("=" * 65)

    print("\n📥 加载数据（含 Day-2 补取）...")
    df = load_data()
    print(f"   原始行数：{len(df):,}")
    if df.empty:
        print("❌ 数据为空。")
        return

    print("\n📊 计算事件级指标...")
    events = compute_events(df)
    print(f"   有效事件：{len(events):,}")

    print("\n🏷️  分组标注...")
    events = assign_groups(events)

    for dim, label in [("dim_P1", "Day-1"), ("dim_P0", "Day0")]:
        print(f"\n   {label} 穿线分布：")
        for k, v in events[dim].value_counts().items():
            print(f"     {k:<20} {v:>5} 个")

    print("\n📈 计算二维分组统计（最多64组）...")
    stats_2d = compute_group_stats(events, ["dim_P1", "dim_P0"])
    print(f"   非空组数：{len(stats_2d)}")

    print("\n📝 写入 Markdown...")
    write_markdown(stats_2d, events)

    print("\n📊 写入 Excel...")
    write_excel(events, stats_2d)

    print(f"\n{'='*65}")
    print("✅ 完成！")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
