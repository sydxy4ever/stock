"""
analyze_v5_1.py
---------------
对 turnover_surge.db 中所有信号事件，按 4 个维度交叉分组（共 8×27×5×4 = 4,320 组），
计算各组内的跟踪期统计指标，输出 analysis_v5_1.md。

分组维度：
  Dim A: Day0 均线位置  (MA20/MA60/MA200 各独立上下，2³=8 组)
  Dim B: 基准期均线位置 (frac_below_ma20/60/200 各三分位，3³=27 组)
  Dim C: 换手率触发倍数 ([1.5,1.8)/[1.8,2.1)/[2.1,2.4)/[2.4,3.0)/3.0+，5 组)
  Dim D: Day0 收盘涨幅  (4-6%/6-8%/8%-涨停/涨停，4 组，主创涨停合并)

事件级指标（以 Day0 收盘价为基准）：
  max_gain        = max(track_close) / d0_close - 1
  max_drawdown    = 1 - min(track_close) / d0_close
  non_decline_rate = 跟踪期内 change_pct>=0 的天数 / 有效天数

组级统计：
  count, non_decline_rate_avg,
  max_gain_avg, max_gain_min, max_gain_cv,
  max_dd_avg, max_dd_max, max_dd_cv
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

# ─── 路径配置（相对于项目根目录，与脚本位置无关）────────────────────────────
_ROOT = Path(__file__).parent.parent
SURGE_DB      = str(_ROOT / "turnover_surge.db")
OUTPUT_MD     = str(_ROOT / "output" / "analysis_v5_1.md")
OUTPUT_XLSX   = str(_ROOT / "output" / "analysis_v5_1.xlsx")
MIN_TRACK_DAYS = 3     # 跟踪期有效天数下限（不足则跳过该事件）
MIN_COUNT_SHOW = 5     # 主表仅展示样本量 >= 此值的组别

# Dim A 标签
A_LABELS = {
    0: "A0(↓20↓60↓200)",
    1: "A1(↓20↓60↑200)",
    2: "A2(↓20↑60↓200)",
    3: "A3(↓20↑60↑200)",
    4: "A4(↑20↓60↓200)",
    5: "A5(↑20↓60↑200)",
    6: "A6(↑20↑60↓200)",
    7: "A7(↑20↑60↑200)",
}

# Dim C 标签
C_LABELS = {
    0: "C1(1.5-1.8x)",
    1: "C2(1.8-2.1x)",
    2: "C3(2.1-2.4x)",
    3: "C4(2.4-3.0x)",
    4: "C5(>3.0x)",
}

# Dim D 标签
D_LABELS = {
    0: "D1(4%-6%)",
    1: "D2(6%-8%)",
    2: "D3(8%-涨停)",
    3: "D4(涨停)",
}

# ─── 情绪过滤配置 ────────────────────────────────────────────────────────────
SENTIMENT_THRESHOLD_LOW  = 0.15  # 排除后 15% 的交易日（市场极冷）
SENTIMENT_THRESHOLD_HIGH = 0.15  # 排除前 15% 的交易日（市场极热）



# ─── 科创/创业板判断 ─────────────────────────────────────────────────────────
def limit_up_threshold(stock_code: str) -> float:
    """返回该股票的涨停阈值（分数形式）"""
    sc = str(stock_code).strip()
    if sc.startswith(("300", "301", "688", "689")):
        return 0.199   # 创业板/科创板 ~20%
    return 0.099       # 主板 ~10%


# ─── 1. 数据加载 ─────────────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    """从 turnover_surge.db 加载所有有效跟踪期行"""
    conn = sqlite3.connect(SURGE_DB)
    df = pd.read_sql(
        """
        SELECT
            day0, stock_code, name, trigger_ratio,
            d0_change_pct, d0_close, d0_ma20, d0_ma60, d0_ma200, dm1_close,
            bl_above_ma20, bl_below_ma20,
            bl_above_ma60, bl_below_ma60,
            bl_above_ma200, bl_below_ma200,
            day_offset, close, change_pct
        FROM turnover_surge
        WHERE day_offset IS NOT NULL
          AND close IS NOT NULL
        """,
        conn,
    )
    conn.close()
    return df


# ─── 2. 事件级指标计算 ───────────────────────────────────────────────────────
def compute_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    按 (day0, stock_code) 聚合跟踪期数据，计算事件级统计指标。
    跳过有效跟踪天数 < MIN_TRACK_DAYS 或 d0_close <= 0 的事件。
    """
    # 元数据（信号属性，每个分组内所有行相同，取第一条即可）
    meta_cols = [
        "day0", "stock_code", "name", "trigger_ratio",
        "d0_change_pct", "d0_close", "d0_ma20", "d0_ma60", "d0_ma200", "dm1_close",
        "bl_above_ma20", "bl_below_ma20",
        "bl_above_ma60", "bl_below_ma60",
        "bl_above_ma200", "bl_below_ma200",
    ]
    meta = (
        df.groupby(["day0", "stock_code"])[meta_cols[2:]]
        .first()
        .reset_index()
    )

    # 填充可能为空的 d0_change_pct
    meta["d0_change_pct"] = pd.to_numeric(meta["d0_change_pct"], errors="coerce")
    meta["d0_close"]      = pd.to_numeric(meta["d0_close"], errors="coerce")
    meta["dm1_close"]     = pd.to_numeric(meta["dm1_close"], errors="coerce")
    
    mask = meta["d0_change_pct"].isna() & meta["d0_close"].notna() & meta["dm1_close"].notna() & (meta["dm1_close"] > 0)
    meta.loc[mask, "d0_change_pct"] = meta.loc[mask, "d0_close"] / meta.loc[mask, "dm1_close"] - 1

    # 跟踪期指标（向量化，高效）
    track = df.copy()
    track["close"]      = pd.to_numeric(track["close"],      errors="coerce")
    track["change_pct"] = pd.to_numeric(track["change_pct"], errors="coerce")
    track = track.dropna(subset=["close"])

    track_agg = track.groupby(["day0", "stock_code"]).agg(
        max_close        = ("close",      "max"),
        min_close        = ("close",      "min"),
        track_days       = ("close",      "count"),
        non_decline_days = ("change_pct", lambda x: (pd.to_numeric(x, errors="coerce") >= 0).sum()),
    ).reset_index()

    # 合并并过滤
    events = meta.merge(track_agg, on=["day0", "stock_code"], how="inner")
    events = events[events["track_days"] >= MIN_TRACK_DAYS].copy()

    events["d0_close"] = pd.to_numeric(events["d0_close"], errors="coerce")
    events = events[events["d0_close"] > 0].copy()

    # 核心指标
    events["max_gain"]        = events["max_close"] / events["d0_close"] - 1
    events["max_drawdown"]    = 1 - events["min_close"] / events["d0_close"]
    events["non_decline_rate"] = events["non_decline_days"] / events["track_days"]

    return events


# ─── 3. 分组标注 ─────────────────────────────────────────────────────────────
def _bl_level(above: pd.Series, below: pd.Series) -> pd.Series:
    """
    计算基准期均线位置级别：
      L1(0): frac_below in [0, 1/3)   → 大部分时间高于均线
      L2(1): frac_below in [1/3, 2/3) → 均线上下震荡
      L3(2): frac_below in [2/3, 1]   → 大部分时间低于均线
    分母为 0（数据全缺）时归入 L3
    """
    total = above + below
    frac  = below / total.where(total > 0, other=np.nan)
    level = pd.cut(
        frac,
        bins=[-0.001, 1 / 3, 2 / 3, 1.001],
        labels=[0, 1, 2],
    )
    return level.fillna(2).astype(int)


def assign_groups(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()

    # ── Dim A: Day0 均线位置（8 组）─────────────────────────────────────────
    d0_close  = pd.to_numeric(df["d0_close"],  errors="coerce")
    d0_ma20   = pd.to_numeric(df["d0_ma20"],   errors="coerce")
    d0_ma60   = pd.to_numeric(df["d0_ma60"],   errors="coerce")
    d0_ma200  = pd.to_numeric(df["d0_ma200"],  errors="coerce")

    # 动态计算均线位置，确保哪怕数据库中该布尔列为空也能正确分组
    above_ma20  = (d0_close > d0_ma20).fillna(False).astype(int)
    above_ma60  = (d0_close > d0_ma60).fillna(False).astype(int)
    above_ma200 = (d0_close > d0_ma200).fillna(False).astype(int)

    df["_A"] = above_ma20 * 4 + above_ma60 * 2 + above_ma200
    df["dim_A"] = df["_A"].map(A_LABELS)

    # ── Dim B: 基准期均线位置（27 组）───────────────────────────────────────
    for ma in [20, 60, 200]:
        a_col = f"bl_above_ma{ma}"
        b_col = f"bl_below_ma{ma}"
        df[a_col] = pd.to_numeric(df[a_col], errors="coerce").fillna(0)
        df[b_col] = pd.to_numeric(df[b_col], errors="coerce").fillna(0)
        df[f"_bl_lv{ma}"] = _bl_level(df[a_col], df[b_col])

    lv_names = {0: "L1强", 1: "L2震", 2: "L3弱"}
    df["dim_B"] = (
        df["_bl_lv20"].map(lv_names)  + "/" +
        df["_bl_lv60"].map(lv_names)  + "/" +
        df["_bl_lv200"].map(lv_names)
    )
    df["_B"] = df["_bl_lv20"] * 9 + df["_bl_lv60"] * 3 + df["_bl_lv200"]

    # ── Dim C: 换手率倍数（5 组）────────────────────────────────────────────
    tr = pd.to_numeric(df["trigger_ratio"], errors="coerce")
    df["_C"] = pd.cut(
        tr,
        bins=[1.5, 1.8, 2.1, 2.4, 3.0, float("inf")],
        labels=[0, 1, 2, 3, 4],
        right=False,
    ).astype("Int64")
    df["dim_C"] = df["_C"].map(C_LABELS)

    # ── Dim D: Day0 涨幅（4 组）─────────────────────────────────────────────
    chg    = pd.to_numeric(df["d0_change_pct"], errors="coerce")
    thresh = df["stock_code"].apply(limit_up_threshold)

    conditions = [
        (chg >= 0.04) & (chg < 0.06),
        (chg >= 0.06) & (chg < 0.08),
        (chg >= 0.08) & (chg < thresh),
        chg >= thresh,
    ]
    df["_D"] = np.select(conditions, [0, 1, 2, 3], default=-1)
    df["dim_D"] = df["_D"].map(D_LABELS)

    # 过滤未能归类的行（理论上不应出现）
    invalid = df["dim_D"].isna() | (df["_D"] == -1) | df["dim_C"].isna()
    if invalid.any():
        print(f"  ⚠ 排除无法归类的事件：{invalid.sum()} 条")
        df = df[~invalid].copy()

    return df


# ─── 3.1 情绪过滤 ─────────────────────────────────────────────────────────────
def filter_extreme_sentiment_days(events: pd.DataFrame):
    """
    根据每日符合条件的股票数（启动日涨幅 > 4%）排除前后 15% 的交易日。
    """
    if events.empty:
        return events, {}

    # 计算每日符合条件（涨幅 > 4%）的股票数
    # 注意：events 已经是经过 compute_events 聚合后的数据，每行代表一个 (day0, stock_code)
    daily_counts = (
        events[events["d0_change_pct"] > 0.04]
        .groupby("day0")["stock_code"]
        .count()
        .reset_index(name="count")
    )
    
    if daily_counts.empty:
        print("   ⚠ 未找到涨幅 > 4% 的股票，跳过情绪过滤。")
        return events, {}

    # 计算分位数阈值
    low_q  = SENTIMENT_THRESHOLD_LOW
    high_q = 1 - SENTIMENT_THRESHOLD_HIGH
    
    low_val  = daily_counts["count"].quantile(low_q)
    high_val = daily_counts["count"].quantile(high_q)
    
    # 筛选正常的交易日
    keep_days = daily_counts[
        (daily_counts["count"] >= low_val) & (daily_counts["count"] <= high_val)
    ]["day0"].unique()
    
    total_days = daily_counts["day0"].nunique()
    keep_days_count = len(keep_days)
    
    filtered_events = events[events["day0"].isin(keep_days)].copy()
    
    stats = {
        "total_days": total_days,
        "keep_days": keep_days_count,
        "low_threshold": low_val,
        "high_threshold": high_val,
        "excluded_days": total_days - keep_days_count
    }
    
    print(f"   📉 情绪过滤完成：")
    print(f"      总交易日：{total_days}，保留：{keep_days_count}，排除：{total_days - keep_days_count}")
    print(f"      排除阈值：低分位(<= {low_val:.0f}只), 高分位(>= {high_val:.0f}只)")
    
    return filtered_events, stats



# ─── 4. 组级统计 ─────────────────────────────────────────────────────────────
def _cv(x: pd.Series) -> float:
    """变异系数 = std / |mean|，mean 为 0 时返回 NaN"""
    m = x.mean()
    if pd.isna(m) or m == 0:
        return np.nan
    return x.std(ddof=1) / abs(m)


def compute_group_stats(events: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    stats = (
        events.groupby(group_cols, observed=True)
        .agg(
            count             = ("max_gain",        "count"),
            non_decline_avg   = ("non_decline_rate", "mean"),
            max_gain_avg      = ("max_gain",         "mean"),
            max_gain_min      = ("max_gain",         "min"),
            max_gain_cv       = ("max_gain",         _cv),
            max_dd_avg        = ("max_drawdown",     "mean"),
            max_dd_max        = ("max_drawdown",     "max"),
            max_dd_cv         = ("max_drawdown",     _cv),
        )
        .reset_index()
    )

    # 百分化
    pct_cols = ["non_decline_avg", "max_gain_avg", "max_gain_min",
                "max_dd_avg", "max_dd_max"]
    for col in pct_cols:
        stats[col] = (stats[col] * 100).round(2)
    stats["max_gain_cv"] = stats["max_gain_cv"].round(3)
    stats["max_dd_cv"]   = stats["max_dd_cv"].round(3)
    stats["cv_sum"]      = (stats["max_gain_cv"] + stats["max_dd_cv"]).round(3)

    return stats.sort_values("max_gain_avg", ascending=False).reset_index(drop=True)


# ─── 5. 输出 Markdown ────────────────────────────────────────────────────────
def write_markdown(stats: pd.DataFrame, events: pd.DataFrame, sentiment_stats: dict = None):
    total_events  = len(events)
    nonempty      = len(stats)
    above_min     = (stats["count"] >= MIN_COUNT_SHOW).sum()

    lines = [
        "# 换手率异动信号 — 分组回测分析 v5.1",
        "",
        "> 数据来源：`turnover_surge.db`  ",
        f"> 信号事件总数：**{total_events:,}**（跟踪期 ≥ {MIN_TRACK_DAYS} 天）  ",
        f"> 非空分组数：**{nonempty:,}** / 4,320  ",
        f"> 展示阈值：count ≥ {MIN_COUNT_SHOW}（共 {above_min:,} 组）",
        "",
        "---",
        "",
        "## 情绪过滤说明 (Market Sentiment Filter)",
        "",
        "为了排除整体行情对个股策略的影响，本报告根据每日「符合条件股票数」剔除了极端行情：",
        f"- **统计口径**：每日启动日涨幅 > 4% 的股票总数",
        f"- **排除规则**：剔除该数量处于最低 {SENTIMENT_THRESHOLD_LOW*100:.0f}% 和最高 {SENTIMENT_THRESHOLD_HIGH*100:.0f}% 的交易日",
        "",
    ]
    
    if sentiment_stats:
        lines += [
            f"| 指标 | 数值 | 备注 |",
            f"|------|------|------|",
            f"| 总信号交易日 | {sentiment_stats['total_days']} | 有信号产出的天数 |",
            f"| 保留交易日 | {sentiment_stats['keep_days']} | 处于正常情绪区间的日子 |",
            f"| 排除交易日 | {sentiment_stats['excluded_days']} | 剔除的极端冷/热日子 |",
            f"| 低位阈值 | <= {sentiment_stats['low_threshold']:.0f} 只 | 对应后 {SENTIMENT_THRESHOLD_LOW*100:.0f}% 分位 |",
            f"| 高位阈值 | >= {sentiment_stats['high_threshold']:.0f} 只 | 对应前 {SENTIMENT_THRESHOLD_HIGH*100:.0f}% 分位 |",
            "",
        ]

    lines += [
        "---",
        "",
        "## 分组维度说明",
        "",
        "| 维度 | 分组数 | 逻辑 |",
        "|------|--------|------|",
        "| **Dim A** — Day0 均线位置 | 8 | MA20/60/200 各自上方(↑)或下方(↓)，2³=8 |",
        "| **Dim B** — 基准期均线状态 | 27 | 基准期30天低于MA20/60/200的天数比例各分三级(强/震/弱)，3³=27 |",
        "| **Dim C** — 换手率触发倍数 | 5 | [1.5,1.8)/[1.8,2.1)/[2.1,2.4)/[2.4,3.0)/3.0+ |",
        "| **Dim D** — Day0 收盘涨幅 | 4 | 4-6%/6-8%/8%-涨停/涨停（主板创业板合并） |",
        "",
        "**Dim A 编码**：",
        "",
        "| 组 | MA20 | MA60 | MA200 | 典型含义 |",
        "|----|:----:|:----:|:-----:|----------|",
        "| A0 | ↓ | ↓ | ↓ | 价格在所有均线下方（深底部） |",
        "| A1 | ↓ | ↓ | ↑ | 位于MA200上方，仍在MA60/20下 |",
        "| A2 | ↓ | ↑ | ↓ | 位于MA60上方，MA200下方（少见） |",
        "| A3 | ↓ | ↑ | ↑ | 穿越MA200/60，仅在MA20下 |",
        "| A4 | ↑ | ↓ | ↓ | 位于MA20上方，MA60/200下（少见） |",
        "| A5 | ↑ | ↓ | ↑ | 位于MA20/200上方，MA60下 |",
        "| A6 | ↑ | ↑ | ↓ | 位于MA20/60上方，MA200下 |",
        "| A7 | ↑ | ↑ | ↑ | 价格在所有均线上方（强势） |",
        "",
        "**Dim B 格式**：`MA20级别/MA60级别/MA200级别`，各级别：  ",
        "- **L1强**：基准期 0~1/3 天低于均线（大部分时间高于均线）  ",
        "- **L2震**：基准期 1/3~2/3 天低于均线（震荡整理）  ",
        "- **L3弱**：基准期 2/3~1 天低于均线（持续弱势）",
        "",
        "---",
        "",
        f"## 主表（count ≥ {MIN_COUNT_SHOW}，按最大涨幅均值降序）",
        "",
        "| Dim A | Dim B | Dim C | Dim D | N | 非下跌日% | 涨幅均% | 涨幅min% | 涨幅CV | 跌幅均% | 跌幅max% | 跌幅CV | CV之和 |",
        "|-------|-------|-------|-------|--:|----------:|--------:|---------:|-------:|--------:|---------:|-------:|-------:|",
    ]

    filtered = stats[stats["count"] >= MIN_COUNT_SHOW]
    for _, r in filtered.iterrows():
        lines.append(
            f"| {r['dim_A']} | {r['dim_B']} | {r['dim_C']} | {r['dim_D']} "
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

    # ── 附：各单维度汇总 ─────────────────────────────────────────────────────
    def single_dim_table(events, dim_col, title):
        g = (
            events.groupby(dim_col, observed=True)
            .agg(
                count          = ("max_gain",         "count"),
                non_decline_avg= ("non_decline_rate",  "mean"),
                max_gain_avg   = ("max_gain",          "mean"),
                max_gain_min   = ("max_gain",          "min"),
                max_gain_cv    = ("max_gain",          _cv),
                max_dd_avg     = ("max_drawdown",      "mean"),
                max_dd_max     = ("max_drawdown",      "max"),
                max_dd_cv      = ("max_drawdown",      _cv),
            )
            .reset_index()
            .sort_values("max_gain_avg", ascending=False)
        )
        g["cv_sum"] = (g["max_gain_cv"].fillna(0) + g["max_dd_cv"].fillna(0)).round(3)
        for col in ["non_decline_avg", "max_gain_avg", "max_gain_min", "max_dd_avg", "max_dd_max"]:
            g[col] = (g[col] * 100).round(2)

        rows = [
            "",
            "---",
            "",
            f"## {title}（单维度汇总）",
            "",
            f"| {dim_col} | N | 非下跌日% | 涨幅均% | 涨幅min% | 涨幅CV | 跌幅均% | 跌幅max% | 跌幅CV | CV之和 |",
            f"|-----------|--:|----------:|--------:|---------:|-------:|--------:|---------:|-------:|-------:|",
        ]
        for _, r in g.iterrows():
            rows.append(
                f"| {r[dim_col]} "
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
        return rows

    lines += single_dim_table(events, "dim_A", "Dim A — Day0 均线位置")
    lines += single_dim_table(events, "dim_B", "Dim B — 基准期均线状态")
    lines += single_dim_table(events, "dim_C", "Dim C — 换手率触发倍数")
    lines += single_dim_table(events, "dim_D", "Dim D — Day0 收盘涨幅")

    lines += [
        "",
        "---",
        "",
        "*生成脚本：`analyze_v5_1.py`*",
    ]

    out_path = Path(OUTPUT_MD)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ 输出已写入：{out_path.resolve()}")


# ─── 6. 输出 Excel ──────────────────────────────────────────────────────────
def write_excel(stats: pd.DataFrame, events: pd.DataFrame, output_path: str):
    """
    将分析结果导出为 Excel 文件，包含主统计表、各维度汇总及带标签的原始事件。
    """
    print(f"📊 正在生成 Excel: {output_path} ...")
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # 1. 主统计表 (4D)
        stats_4d = compute_group_stats(events, ["dim_A", "dim_B", "dim_C", "dim_D"])
        stats_4d.to_excel(writer, sheet_name="4D_全维度", index=False)

        # 2. 单维度汇总 (1D)
        for dim in ["dim_A", "dim_B", "dim_C", "dim_D"]:
            compute_group_stats(events, [dim]).to_excel(writer, sheet_name=f"1D_{dim[-1]}", index=False)

        # 3. 两维度交叉汇总 (2D)
        # Combinations: AB, AC, AD, BC, BD, CD
        comb_2d = [
            ("dim_A", "dim_B"), ("dim_A", "dim_C"), ("dim_A", "dim_D"),
            ("dim_B", "dim_C"), ("dim_B", "dim_D"), ("dim_C", "dim_D")
        ]
        for c in comb_2d:
            sheet_name = f"2D_{c[0][-1]}{c[1][-1]}"
            compute_group_stats(events, list(c)).to_excel(writer, sheet_name=sheet_name, index=False)

        # 4. 三维度交叉汇总 (3D)
        # Combinations: ABC, ABD, ACD, BCD
        comb_3d = [
            ("dim_A", "dim_B", "dim_C"), ("dim_A", "dim_B", "dim_D"),
            ("dim_A", "dim_C", "dim_D"), ("dim_B", "dim_C", "dim_D")
        ]
        for c in comb_3d:
            sheet_name = f"3D_{c[0][-1]}{c[1][-1]}{c[2][-1]}"
            compute_group_stats(events, list(c)).to_excel(writer, sheet_name=sheet_name, index=False)

        # 5. 原始事件数据 (带标签)
        export_cols = [
            "day0", "stock_code", "name", "trigger_ratio", "d0_change_pct",
            "max_gain", "max_drawdown", "non_decline_rate",
            "dim_A", "dim_B", "dim_C", "dim_D"
        ]
        raw_export = events[export_cols].copy()
        for col in ["max_gain", "max_drawdown", "non_decline_rate", "d0_change_pct"]:
            raw_export[col] = (raw_export[col] * 100).round(2)
        
        raw_export.to_excel(writer, sheet_name="原始信号事件", index=False)

    print(f"✅ Excel 已写入：{Path(output_path).resolve()}")


# ─── 主流程 ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("📊 换手率异动信号分组回测分析 v5.1")
    print("=" * 55)

    print("\n📥 加载数据...")
    df = load_data()
    print(f"   有效跟踪期行数：{len(df):,}")
    if df.empty:
        print("❌ 数据为空，请先运行 turnover_surge.py 生成数据。")
        return

    print("\n📊 计算事件级指标...")
    events = compute_events(df)
    print(f"   有效信号事件：{len(events):,} 个")
    if events.empty:
        print("❌ 无有效信号事件。")
        return

    print("\n📉 情绪过滤（排除 15% 极端交易日）...")
    events, sentiment_stats = filter_extreme_sentiment_days(events)

    print("\n🏷️  分组标注...")
    events = assign_groups(events)
    print(f"   归类后事件数：{len(events):,}")

    print("\n📈 计算分组统计（4D）...")
    stats = compute_group_stats(events, ["dim_A", "dim_B", "dim_C", "dim_D"])
    nonempty = len(stats)
    above_min = (stats["count"] >= MIN_COUNT_SHOW).sum()
    print(f"   非空分组：{nonempty:,} / 4,320")
    print(f"   count ≥ {MIN_COUNT_SHOW} 的组：{above_min:,}")

    print("\n📝 写入 Markdown...")
    write_markdown(stats, events, sentiment_stats)

    print("\n📊 写入 Excel...")
    write_excel(stats, events, OUTPUT_XLSX)


    print(f"\n{'='*55}")
    print("✅ 完成！")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
