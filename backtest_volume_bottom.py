"""
backtest_volume_bottom.py
--------------------------
30+5+5 底部放量启动策略回测引擎

策略逻辑：
  P1 (30日): 背景期 → 计算基准平均换手率
  P2 (5日):  放量期 → 触发条件：
              1. P2末日收盘 < MA200（年线以下）
              2. mean(to_r_P2) / mean(to_r_P1) > 1.5（放量1.5倍）
              3. std(close_P2)/mean(close_P2) < 0.03（企稳，CV<3%）
  P3 (5日):  观察期 → 记录每日：涨跌幅、换手率、是否上穿MA5/MA20

输出到 output/ 目录：
  signals_raw.csv        — 每条信号明细（含P3全部指标）
  report_by_cap.csv      — 按市值五分位统计
  report_by_industry.csv — 按申万2021行业统计
  report_cross.csv       — 行业×市值交叉表

使用方式:
  python backtest_volume_bottom.py

依赖: daily_kline, moving_averages, fundamentals, stock_industries 表均已填充
"""

import os
import sqlite3
import numpy as np
import pandas as pd

DB_PATH    = "stock_data.db"
OUTPUT_DIR = "output"

# ─── 策略参数（可调整）────────────────────────────────────────────────────────────
P1_DAYS   = 30    # 背景期
P2_DAYS   = 5     # 放量期
P3_DAYS   = 5     # 观察期
RATIO_MIN = 1.5   # P2/P1换手率倍数阈值
CV_MAX    = 0.03  # P2收盘价变异系数阈值（企稳，CV < 3%）

# ─── 数据加载 ────────────────────────────────────────────────────────────────────

def load_data():
    print("📂 加载数据...")
    conn = sqlite3.connect(DB_PATH)

    # 1. K线（收盘价 + 换手率）
    kline = pd.read_sql_query("""
        SELECT stock_code, date, close, to_r
        FROM daily_kline
        WHERE close IS NOT NULL AND to_r IS NOT NULL
        ORDER BY stock_code, date
    """, conn)
    kline["date"] = pd.to_datetime(kline["date"])

    # 2. 均线
    ma = pd.read_sql_query("""
        SELECT stock_code, date, ma5, ma20, ma200
        FROM moving_averages
        ORDER BY stock_code, date
    """, conn)
    ma["date"] = pd.to_datetime(ma["date"])

    # 3. 市值（取最近可用，forward fill）
    mc_df = pd.read_sql_query("""
        SELECT stock_code, date, mc
        FROM fundamentals
        WHERE mc IS NOT NULL
        ORDER BY stock_code, date
    """, conn)
    mc_df["date"] = pd.to_datetime(mc_df["date"])

    # 4. 行业（申万2021，取细粒度：stockCode长度最长的那一级）
    ind_df = pd.read_sql_query("""
        SELECT si.stock_code, si.industry_code, si.name AS industry_name
        FROM stock_industries si
        INNER JOIN (
            SELECT stock_code, MAX(LENGTH(industry_code)) AS max_len
            FROM stock_industries
            WHERE source = 'sw_2021'
            GROUP BY stock_code
        ) best ON si.stock_code = best.stock_code
                AND LENGTH(si.industry_code) = best.max_len
                AND si.source = 'sw_2021'
    """, conn)

    conn.close()
    print(f"   K线: {len(kline):,} 条  股票: {kline['stock_code'].nunique()} 只")
    print(f"   均线: {len(ma):,} 条")
    print(f"   市值: {len(mc_df):,} 条  行业: {len(ind_df)} 只")
    return kline, ma, mc_df, ind_df


def merge_all(kline, ma, mc_df):
    """合并K线+均线，市值用最近可用值 forward fill"""
    df = pd.merge(kline, ma, on=["stock_code", "date"], how="inner")
    df = df.sort_values(["stock_code", "date"]).reset_index(drop=True)

    # 市值：merge_asof（每股按日期最近匹配，forward fill）
    mc_df = mc_df.sort_values(["stock_code", "date"])
    df = df.sort_values(["stock_code", "date"])
    df = pd.merge_asof(
        df, mc_df,
        on="date", by="stock_code",
        direction="backward"   # 取最近的过去日期
    )
    return df


# ─── 信号扫描（纯向量化）────────────────────────────────────────────────────────

def compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    对每只股票计算滑动窗口指标，返回所有P2末日的信号行。
    使用 pandas groupby + rolling，避免 Python 逐行循环。
    """
    print("\n🔍 计算信号...")

    records = []

    for code, grp in df.groupby("stock_code", sort=False):
        grp = grp.sort_values("date").reset_index(drop=True)
        n = len(grp)

        if n < P1_DAYS + P2_DAYS + P3_DAYS:
            continue

        closes   = grp["close"].values
        to_rs    = grp["to_r"].values
        ma5s     = grp["ma5"].values
        ma20s    = grp["ma20"].values
        ma200s   = grp["ma200"].values
        mcs      = grp["mc"].values
        dates    = grp["date"].values

        total = P1_DAYS + P2_DAYS + P3_DAYS

        for i in range(P1_DAYS + P2_DAYS, n - P3_DAYS):
            # 窗口索引
            p1_start = i - P1_DAYS - P2_DAYS
            p1_end   = i - P2_DAYS          # P1: [p1_start, p1_end)
            p2_start = i - P2_DAYS
            p2_end   = i                    # P2: [p2_start, p2_end)  末日 = i-1
            p3_start = i
            p3_end   = i + P3_DAYS          # P3: [p3_start, p3_end)

            p1_to_r = to_rs[p1_start:p1_end]
            p2_to_r = to_rs[p2_start:p2_end]
            p2_close = closes[p2_start:p2_end]

            # 条件1：P2末日在年线以下
            entry_price = closes[i - 1]    # P2末日收盘 = P3入场价
            ma200_at_entry = ma200s[i - 1]
            if np.isnan(ma200_at_entry) or entry_price >= ma200_at_entry:
                continue

            # 条件2：P2均换手 / P1均换手 > 1.5
            p1_mean = p1_to_r.mean()
            if p1_mean == 0:
                continue
            ratio = p2_to_r.mean() / p1_mean
            if ratio < RATIO_MIN:
                continue

            # 条件3：企稳 — P2收盘价变异系数 CV < 3%
            p2_mean_close = p2_close.mean()
            if p2_mean_close == 0:
                continue
            cv = p2_close.std() / p2_mean_close
            if cv >= CV_MAX:
                continue

            # ── 信号触发！记录P3每日数据 ──
            p3_closes  = closes[p3_start:p3_end]
            p3_to_rs   = to_rs[p3_start:p3_end]
            p3_ma5s    = ma5s[p3_start:p3_end]
            p3_ma20s   = ma20s[p3_start:p3_end]

            row = {
                "signal_date":   pd.Timestamp(dates[i - 1]).strftime("%Y-%m-%d"),
                "stock_code":    code,
                "entry_price":   round(float(entry_price), 4),
                "ma200":         round(float(ma200_at_entry), 4),
                "p1_avg_to_r":   round(float(p1_mean), 6),
                "p2_avg_to_r":   round(float(p2_to_r.mean()), 6),
                "ratio":         round(float(ratio), 4),
                "p2_cv":         round(float(cv), 6),
                "mc":            float(mcs[i - 1]) if not np.isnan(mcs[i - 1]) else None,
            }

            # P3 每日指标（5天 × 4 指标）
            for d in range(P3_DAYS):
                n_idx = d + 1
                pct = (p3_closes[d] / entry_price - 1) if entry_price > 0 else None
                row[f"p3_d{n_idx}_pct"]       = round(float(pct), 6) if pct is not None else None
                row[f"p3_d{n_idx}_to_r"]      = round(float(p3_to_rs[d]), 6)
                row[f"p3_d{n_idx}_above_ma5"]  = int(p3_closes[d] > p3_ma5s[d]) if not np.isnan(p3_ma5s[d]) else None
                row[f"p3_d{n_idx}_above_ma20"] = int(p3_closes[d] > p3_ma20s[d]) if not np.isnan(p3_ma20s[d]) else None

            # 汇总指标
            pcts = [row.get(f"p3_d{d+1}_pct") for d in range(P3_DAYS) if row.get(f"p3_d{d+1}_pct") is not None]
            if pcts:
                row["max_gain"]     = round(max(pcts), 6)
                row["max_drawdown"] = round(min(pcts), 6)
                row["breakeven"]    = int(all(p >= 0 for p in pcts))
            else:
                row["max_gain"] = row["max_drawdown"] = row["breakeven"] = None

            records.append(row)

    signals = pd.DataFrame(records)
    print(f"   找到信号: {len(signals):,} 条")
    return signals


# ─── 分组统计 ────────────────────────────────────────────────────────────────────

def add_mc_group(signals: pd.DataFrame) -> pd.DataFrame:
    """按市值五分位分组"""
    valid = signals["mc"].notna()
    mc_vals = signals.loc[valid, "mc"]
    try:
        signals.loc[valid, "mc_group"] = pd.qcut(
            mc_vals, 5,
            labels=["Q1_极小", "Q2_偏小", "Q3_中等", "Q4_偏大", "Q5_极大"]
        )
    except Exception:
        signals["mc_group"] = "未知"
    return signals


def add_industry(signals: pd.DataFrame, ind_df: pd.DataFrame) -> pd.DataFrame:
    """关联申万2021行业"""
    return pd.merge(signals, ind_df, on="stock_code", how="left")


def build_reports(signals: pd.DataFrame):
    """生成3份统计报告"""
    agg_cols = {
        "样本数":       ("max_gain", "count"),
        "平均最大涨幅": ("max_gain", "mean"),
        "平均最大回撤": ("max_drawdown", "mean"),
        "保本率":       ("breakeven", "mean"),
    }

    # 1. 按市值分组
    by_cap = signals.groupby("mc_group", observed=True).agg(**agg_cols).round(4)
    by_cap = by_cap.sort_values("平均最大涨幅", ascending=False)

    # 2. 按行业
    by_ind = signals.groupby("industry_name", observed=True).agg(**agg_cols).round(4)
    by_ind = by_ind.sort_values("平均最大涨幅", ascending=False)

    # 3. 行业 × 市值 交叉
    cross = signals.groupby(["industry_name", "mc_group"], observed=True).agg(**agg_cols).round(4)

    return by_cap, by_ind, cross


# ─── 主流程 ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("📈 30+5+5 底部放量启动策略回测")
    print(f"   P1={P1_DAYS}日背景期  P2={P2_DAYS}日放量期  P3={P3_DAYS}日观察期")
    print(f"   放量阈值: {RATIO_MIN}x  企稳CV阈值: {CV_MAX*100:.0f}%")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 加载 & 合并
    kline, ma, mc_df, ind_df = load_data()
    df = merge_all(kline, ma, mc_df)
    print(f"   合并后: {len(df):,} 条记录")

    # 扫描信号
    signals = compute_signals(df)

    if signals.empty:
        print("⚠ 未找到任何符合条件的信号，请检查数据是否完整。")
        return

    # 丰富标签
    signals = add_mc_group(signals)
    signals = add_industry(signals, ind_df)

    # 报告
    print("\n📊 生成报告...")
    by_cap, by_ind, cross = build_reports(signals)

    # 输出文件
    raw_path = f"{OUTPUT_DIR}/signals_raw.csv"
    cap_path = f"{OUTPUT_DIR}/report_by_cap.csv"
    ind_path = f"{OUTPUT_DIR}/report_by_industry.csv"
    crs_path = f"{OUTPUT_DIR}/report_cross.csv"

    signals.to_csv(raw_path, index=False, encoding="utf-8-sig")
    by_cap.to_csv(cap_path, encoding="utf-8-sig")
    by_ind.to_csv(ind_path, encoding="utf-8-sig")
    cross.to_csv(crs_path, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    print("✅ 完成！输出文件：")
    print(f"   {raw_path:40s}  ({len(signals):,} 行信号明细)")
    print(f"   {cap_path:40s}  (按市值分组)")
    print(f"   {ind_path:40s}  (按行业排序)")
    print(f"   {crs_path:40s}  (行业×市值交叉)")

    print("\n── 市值分组Top预览 ──")
    print(by_cap.to_string())
    print("\n── 行业Top10 ──")
    print(by_ind.head(10).to_string())
    print("=" * 60)


if __name__ == "__main__":
    main()
