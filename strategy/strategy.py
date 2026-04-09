"""
turnover_surge.py
--------------------------
以某交易日 **Day0（启动日）** 为基准，在 sw_2021 **三级行业**龙头股（市值 Top-N）中，
筛选 Day(-5)~Day(-1) 平均换手率超过 Day(-35)~Day(-6) 基准换手率 1.5~4 倍的股票，
并在 Day0 涨幅 ≥ 4% 的前提下记录 Day(1)~Day(9) 每天的换手率比例和涨幅。

日期窗口（均为交易日）：
    Day(-35) ~ Day(-6)  →  基准期（30个交易日）
    Day(-5)  ~ Day(-1)  →  观察期（5个交易日）
    Day0                →  启动日（触发条件：换手率倍数 1.5x~4x & 涨幅≥4%）
    Day(1)   ~ Day(9)   →  跟踪期（9个交易日）

新增：基准期均线位置分析
    bl_above_ma20/60/200  基准期收盘价高于均线的天数
    bl_below_ma20/60/200  基准期收盘价低于均线的天数
    （可用于区分持续下跌、横盘整理等趋势模式）

输出 CSV / DB 字段：
    day0, industry_code, industry_name, stock_code, name, mc_rank,
    baseline_to_r, recent_to_r, trigger_ratio,
    bl_above_ma20, bl_below_ma20, bl_above_ma60, bl_below_ma60,
    bl_above_ma200, bl_below_ma200,
    # Day0 快照（触发时刻均线位置）
    d0_close, d0_change_pct,
    d0_ma5, d0_ma20, d0_ma60, d0_ma200,
    d0_vs_ma5, d0_vs_ma20, d0_vs_ma60,
    d0_above_ma20, d0_above_ma60,
    # 跟踪期每日数据
    day_offset, date, close, to_r, to_r_ratio, change_pct,
    ma5, ma20, ma60, ma200,
    vs_ma5, vs_ma20, vs_ma60
"""

import os
import argparse
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

# ─── 配置 ───────────────────────────────────────────────────────────────────────
_ROOT            = Path(__file__).parent.parent
DB_PATH          = os.getenv("DB_PATH", str(_ROOT / "stock_data.db"))
RESULT_DB_PATH   = os.getenv("STRATEGY_DB_PATH", str(_ROOT / "strategy.db"))   # 分析结果数据库
OUTPUT_DIR       = "output"
INDUSTRY_SOURCE  = "sw_2021"    # 行业体系
TOP_N            = 3            # 每个三级行业龙头股数量
BASELINE_DAYS    = 30           # Day(-35)~Day(-6)，共30个交易日
RECENT_DAYS      = 5            # Day(-5)~Day(-1)，共5个交易日
FORWARD_DAYS     = 9            # Day(1)~Day(9)，跟踪期9个交易日
TRIGGER_RATIO_MIN = 1.5         # 换手率触发倍数下限
TRIGGER_RATIO_MAX = 4.0         # 换手率触发倍数上限
MIN_D0_CHANGE    = 0.04         # Day0（启动日）涨幅门槛（4%）
MIN_BASELINE_OBS = 15           # 基准期有效观测天数下限（不足则跳过）


# ─── 申万2021 三级行业判断 ────────────────────────────────────────────────────────
def is_level3(industry_code: str) -> bool:
    code = str(industry_code).strip()
    if len(code) < 4: return False
    return code[-2:] != "00"


# ─── 交易日历 ────────────────────────────────────────────────────────────────────
def load_trade_calendar(conn: sqlite3.Connection) -> pd.Series:
    try:
        cal = pd.read_sql("SELECT date FROM trade_calendar ORDER BY date", conn)["date"]
    except:
        cal = pd.Series()
    if cal.empty:
        try:
            cal = pd.read_sql("SELECT DISTINCT date FROM daily_kline WHERE stock_code='600519' ORDER BY date", conn)["date"]
        except:
            cal = pd.read_sql("SELECT DISTINCT date FROM daily_kline ORDER BY date", conn)["date"]
    return cal.reset_index(drop=True)


def get_day_index(calendar: pd.Series, day0_str: str) -> int | None:
    mask = calendar <= day0_str
    if not mask.any(): return None
    return int(mask[::-1].idxmax())


def get_date_windows(calendar: pd.Series, idx: int) -> dict[str, list[str]]:
    baseline_start = idx - 40
    baseline_end   = idx - 10
    recent1_start  = idx - 10
    recent1_end    = idx - 5
    recent2_start  = idx - 5
    recent2_end    = idx
    forward_start  = idx + 1
    forward_end    = idx + FORWARD_DAYS + 1

    if baseline_start < 0: return {}
    n_cal = len(calendar)
    return {
        "baseline": calendar.iloc[baseline_start:baseline_end].tolist(),
        "recent1":  calendar.iloc[recent1_start:recent1_end].tolist(),
        "recent2":  calendar.iloc[recent2_start:recent2_end].tolist(),
        "forward":  calendar.iloc[forward_start:min(forward_end, n_cal)].tolist(),
    }


# ─── 核心逻辑 ──────────────────────────────────────────────────────────────────
def get_top_stocks(conn, day0_str, top_n):
    sql = """
        WITH recent_mc AS (
            SELECT f.stock_code, f.mc, ROW_NUMBER() OVER (PARTITION BY f.stock_code ORDER BY f.date DESC) AS rn
            FROM fundamentals f WHERE f.mc > 0
        ),
        ind AS (SELECT si.stock_code, si.industry_code, si.name AS industry_name FROM stock_industries si WHERE si.source = :source),
        combined AS (
            SELECT s.stock_code, s.name, rm.mc, i.industry_code, i.industry_name,
                   ROW_NUMBER() OVER (PARTITION BY i.industry_code ORDER BY rm.mc DESC) AS mc_rank
            FROM recent_mc rm
            JOIN stocks s ON s.stock_code = rm.stock_code AND s.listing_status = 'normally_listed'
            JOIN ind i ON i.stock_code = rm.stock_code
            WHERE rm.rn = 1
        )
        SELECT stock_code, name, mc, industry_code, industry_name, mc_rank FROM combined WHERE mc_rank <= :top_n
    """
    df = pd.read_sql(sql, conn, params={"day0": day0_str, "source": INDUSTRY_SOURCE, "top_n": top_n})
    return df[df["industry_code"].apply(is_level3)].copy()


def get_kline_slice(conn, codes, dates):
    if not codes or not dates: return pd.DataFrame()
    ph_c, ph_d = ",".join([f"'{c}'" for c in codes]), ",".join([f"'{d}'" for d in dates])
    return pd.read_sql(f"SELECT stock_code, date, close, to_r, change_pct FROM daily_kline WHERE stock_code IN ({ph_c}) AND date IN ({ph_d})", conn)


def get_ma_slice(conn, codes, dates):
    if not codes or not dates: return pd.DataFrame()
    ph_c, ph_d = ",".join([f"'{c}'" for c in codes]), ",".join([f"'{d}'" for d in dates])
    return pd.read_sql(f"SELECT stock_code, date, ma5, ma20, ma60, ma200 FROM moving_averages WHERE stock_code IN ({ph_c}) AND date IN ({ph_d})", conn)


def compute_baseline_ma_position(kline_baseline, ma_baseline):
    if kline_baseline.empty or ma_baseline.empty:
        codes = kline_baseline["stock_code"].unique() if not kline_baseline.empty else []
        return pd.DataFrame([{"stock_code":c, "bl_above_ma20":0, "bl_below_ma20":0, "bl_above_ma60":0, "bl_below_ma60":0, "bl_above_ma200":0, "bl_below_ma200":0} for c in codes])
    merged = kline_baseline[["stock_code", "date", "close"]].merge(ma_baseline[["stock_code", "date", "ma20", "ma60", "ma200"]], on=["stock_code", "date"], how="left")
    def _count(grp):
        c, m20, m60, m200 = pd.to_numeric(grp["close"]), pd.to_numeric(grp["ma20"]), pd.to_numeric(grp["ma60"]), pd.to_numeric(grp["ma200"])
        return pd.Series({
            "bl_above_ma20": int((c > m20).sum()), "bl_below_ma20": int((c < m20).sum()),
            "bl_above_ma60": int((c > m60).sum()), "bl_below_ma60": int((c < m60).sum()),
            "bl_above_ma200": int((c > m200).sum()), "bl_below_ma200": int((c < m200).sum()),
        })
    return merged.groupby("stock_code", sort=False).apply(_count, include_groups=False).reset_index()


def analyze_day(conn, day0_str, calendar):
    idx = get_day_index(calendar, day0_str)
    if idx is None: return pd.DataFrame(), {"date": day0_str, "total_stocks": 0, "surge_count": 0, "signal_count": 0}
    win = get_date_windows(calendar, idx)
    if not win: return pd.DataFrame(), {"date": day0_str, "total_stocks": 0, "surge_count": 0, "signal_count": 0}
    
    top_stocks = get_top_stocks(conn, day0_str, TOP_N)
    if top_stocks.empty: return pd.DataFrame(), {"date": day0_str, "total_stocks": 0, "surge_count": 0, "signal_count": 0}
    
    codes = top_stocks["stock_code"].tolist()
    hist_dates = win["baseline"] + win["recent1"] + win["recent2"]
    kline_hist = get_kline_slice(conn, codes, hist_dates)
    if kline_hist.empty: return pd.DataFrame(), {"date": day0_str, "total_stocks": len(codes), "surge_count": 0, "signal_count": 0}
    
    bl_df = kline_hist[kline_hist["date"].isin(win["baseline"])].groupby("stock_code")["to_r"].agg(baseline_to_r="mean", baseline_obs="count").reset_index()
    rc1_df = kline_hist[kline_hist["date"].isin(win["recent1"])].groupby("stock_code")["to_r"].mean().rename("recent1_to_r").reset_index()
    rc2_df = kline_hist[kline_hist["date"].isin(win["recent2"])].groupby("stock_code")["to_r"].mean().rename("recent2_to_r").reset_index()
    bl_ma_data = get_ma_slice(conn, codes, win["baseline"])
    bl_ma_pos = compute_baseline_ma_position(kline_hist[kline_hist["date"].isin(win["baseline"])], bl_ma_data)
    
    sig = top_stocks.merge(bl_df, on="stock_code").merge(rc1_df, on="stock_code").merge(rc2_df, on="stock_code").merge(bl_ma_pos, on="stock_code")
    sig["trigger_ratio_1"] = sig["recent1_to_r"] / sig["baseline_to_r"]
    sig["trigger_ratio_2"] = sig["recent2_to_r"] / sig["baseline_to_r"]
    
    triggered = sig[(sig["trigger_ratio_1"] >= 1.2) & (sig["trigger_ratio_1"] <= 1.5) & (sig["trigger_ratio_2"] > 1.5) & (sig["baseline_obs"] >= MIN_BASELINE_OBS)].copy()
    surge_count = len(triggered)
    
    t_codes = triggered["stock_code"].tolist()
    kline_d0 = get_kline_slice(conn, t_codes, [day0_str])
    if kline_d0.empty: return pd.DataFrame(), {"date": day0_str, "total_stocks": len(codes), "surge_count": surge_count, "signal_count": 0}
    kline_d0 = kline_d0[kline_d0["change_pct"] >= MIN_D0_CHANGE].copy()
    if kline_d0.empty: return pd.DataFrame(), {"date": day0_str, "total_stocks": len(codes), "surge_count": surge_count, "signal_count": 0}
    
    triggered = triggered.merge(kline_d0[["stock_code", "close", "change_pct", "to_r"]].rename(columns={"close":"d0_close", "change_pct":"d0_change_pct", "to_r":"d0_to_r"}), on="stock_code")
    ma_d0 = get_ma_slice(conn, triggered["stock_code"].tolist(), [day0_str])
    if not ma_d0.empty:
        triggered = triggered.merge(ma_d0[["stock_code", "ma5", "ma20", "ma60", "ma200"]].rename(columns={"ma5":"d0_ma5", "ma20":"d0_ma20", "ma60":"d0_ma60", "ma200":"d0_ma200"}), on="stock_code", how="left")
    else:
        for c in ["d0_ma5", "d0_ma20", "d0_ma60", "d0_ma200"]: triggered[c] = np.nan
        
    d0_c = pd.to_numeric(triggered["d0_close"])
    m5, m20, m60 = pd.to_numeric(triggered["d0_ma5"]), pd.to_numeric(triggered["d0_ma20"]), pd.to_numeric(triggered["d0_ma60"])
    triggered["d0_vs_ma5"], triggered["d0_vs_ma20"], triggered["d0_vs_ma60"] = (d0_c/m5).round(4), (d0_c/m20).round(4), (d0_c/m60).round(4)
    triggered["d0_above_ma20"] = ((d0_c > m20) & m20.notna()).astype("Int8")
    triggered["d0_above_ma60"] = ((d0_c > m60) & m60.notna()).astype("Int8")
    
    # Day(-1) 快照
    dm1 = win["recent2"][-1]
    kline_dm1 = kline_hist[kline_hist["date"] == dm1][["stock_code", "close", "change_pct"]].rename(columns={"close": "dm1_close", "change_pct": "dm1_change_pct"})
    ma_dm1 = get_ma_slice(conn, t_codes, [dm1])
    if not ma_dm1.empty and not kline_dm1.empty:
        dm1_snap = kline_dm1.merge(ma_dm1, on="stock_code", how="left")
        dm1_c, dm1_pct, dm1_m20, dm1_m60, dm1_m200 = pd.to_numeric(dm1_snap["dm1_close"]), pd.to_numeric(dm1_snap["dm1_change_pct"]), pd.to_numeric(dm1_snap["ma20"]), pd.to_numeric(dm1_snap["ma60"]), pd.to_numeric(dm1_snap["ma200"])
        
        # 判断 Day(-1) 是否涨停: 创业/科创板 19.8%, 主板 9.8%
        is_kc_cy = dm1_snap["stock_code"].str.startswith(("300", "688"))
        dm1_snap["dm1_is_limit_up"] = ((is_kc_cy & (dm1_pct >= 0.198)) | (~is_kc_cy & (dm1_pct >= 0.098))).astype("Int8")
        
        dm1_snap["dm1_above_ma20"] = ((dm1_c > dm1_m20) & dm1_m20.notna()).astype("Int8")
        dm1_snap["dm1_above_ma60"] = ((dm1_c > dm1_m60) & dm1_m60.notna()).astype("Int8")
        dm1_snap["dm1_above_ma200"] = ((dm1_c > dm1_m200) & dm1_m200.notna()).astype("Int8")
        
        # 新策略条件：1. Day-1 涨停；2. Day-1 收盘价 < MA20 且 < MA60
        valid_dm1 = dm1_snap[
            (dm1_snap["dm1_is_limit_up"] == 1) &
            (dm1_c < dm1_m20) &
            (dm1_c < dm1_m60)
        ]
        
        triggered = triggered.merge(valid_dm1[["stock_code", "dm1_close", "dm1_is_limit_up", "dm1_above_ma20", "dm1_above_ma60", "dm1_above_ma200"]], on="stock_code", how="inner")
    else:
        triggered = triggered.iloc[0:0] # 无法验证新策略，直接清空
    
    # 跟踪期
    fwd_dates = win["forward"]
    kline_fwd = get_kline_slice(conn, t_codes, fwd_dates)
    ma_fwd = get_ma_slice(conn, t_codes, fwd_dates)
    if kline_fwd.empty: return _format_result(triggered, day0_str, []), {"date": day0_str, "total_stocks": len(codes), "surge_count": surge_count, "signal_count": len(triggered)}
    
    kline_fwd = kline_fwd.merge(ma_fwd, on=["stock_code", "date"], how="left")
    kline_fwd["day_offset"] = kline_fwd["date"].map({d: i+1 for i, d in enumerate(fwd_dates)})
    kline_fwd = kline_fwd.merge(triggered, on="stock_code", suffixes=("", "_meta"))
    kline_fwd["to_r_ratio"] = (kline_fwd["to_r"] / kline_fwd["baseline_to_r"]).round(4)
    kline_fwd["vs_ma5"] = (kline_fwd["close"] / kline_fwd["ma5"]).round(4)
    kline_fwd["vs_ma20"] = (kline_fwd["close"] / kline_fwd["ma20"]).round(4)
    kline_fwd["vs_ma60"] = (kline_fwd["close"] / kline_fwd["ma60"]).round(4)
    
    kline_fwd["total_change_pct"] = ((kline_fwd["close"] / kline_fwd["d0_close"]) - 1).round(4)
    kline_fwd = kline_fwd.sort_values(["stock_code", "date"])
    kline_fwd["prev_to_r"] = kline_fwd.groupby("stock_code")["to_r"].shift(1)
    kline_fwd["prev_to_r"] = kline_fwd["prev_to_r"].fillna(kline_fwd["d0_to_r"])
    kline_fwd["to_r_change_rate"] = ((kline_fwd["to_r"] / kline_fwd["prev_to_r"]) - 1).round(4)
    kline_fwd = kline_fwd.drop(columns=["prev_to_r"])
    
    return _format_result(kline_fwd, day0_str, fwd_dates), {"date": day0_str, "total_stocks": len(codes), "surge_count": surge_count, "signal_count": len(triggered)}

def _format_result(df, day0_str, fwd_dates):
    df["day0"] = day0_str
    cols = ["day0", "industry_code", "industry_name", "stock_code", "name", "mc_rank", "baseline_to_r", "recent1_to_r", "trigger_ratio_1", "recent2_to_r", "trigger_ratio_2", "bl_above_ma20", "bl_below_ma20", "bl_above_ma60", "bl_below_ma60", "bl_above_ma200", "bl_below_ma200", "dm1_close", "dm1_is_limit_up", "dm1_above_ma20", "dm1_above_ma60", "dm1_above_ma200", "d0_change_pct", "d0_close", "d0_ma5", "d0_ma20", "d0_ma60", "d0_ma200", "d0_vs_ma5", "d0_vs_ma20", "d0_vs_ma60", "d0_above_ma20", "d0_above_ma60", "day_offset", "date", "close", "to_r", "to_r_ratio", "change_pct", "total_change_pct", "to_r_change_rate", "ma5", "ma20", "ma60", "ma200", "vs_ma5", "vs_ma20", "vs_ma60"]
    for c in cols:
        if c not in df.columns: df[c] = pd.NA
    return df[cols].copy()

def init_result_db(rconn):
    try:
        rconn.execute("ALTER TABLE strategy ADD COLUMN dm1_is_limit_up INTEGER")
    except sqlite3.OperationalError:
        pass
    try:
        rconn.execute("ALTER TABLE strategy ADD COLUMN total_change_pct REAL")
        rconn.execute("ALTER TABLE strategy ADD COLUMN to_r_change_rate REAL")
    except sqlite3.OperationalError:
        pass
    
    rconn.execute("""
    CREATE TABLE IF NOT EXISTS strategy (
        day0 TEXT, industry_code TEXT, industry_name TEXT, stock_code TEXT, name TEXT, mc_rank INTEGER,
        baseline_to_r REAL, recent1_to_r REAL, trigger_ratio_1 REAL, recent2_to_r REAL, trigger_ratio_2 REAL,
        bl_above_ma20 INTEGER, bl_below_ma20 INTEGER, bl_above_ma60 INTEGER, bl_below_ma60 INTEGER, bl_above_ma200 INTEGER, bl_below_ma200 INTEGER,
        dm1_close REAL, dm1_is_limit_up INTEGER, dm1_above_ma20 INTEGER, dm1_above_ma60 INTEGER, dm1_above_ma200 INTEGER,
        d0_change_pct REAL, d0_close REAL, d0_ma5 REAL, d0_ma20 REAL, d0_ma60 REAL, d0_ma200 REAL,
        d0_vs_ma5 REAL, d0_vs_ma20 REAL, d0_vs_ma60 REAL, d0_above_ma20 INTEGER, d0_above_ma60 INTEGER,
        day_offset INTEGER, date TEXT, close REAL, to_r REAL, to_r_ratio REAL, change_pct REAL, total_change_pct REAL, to_r_change_rate REAL,
        ma5 REAL, ma20 REAL, ma60 REAL, ma200 REAL, vs_ma5 REAL, vs_ma20 REAL, vs_ma60 REAL,
        PRIMARY KEY (day0, stock_code, day_offset)
    )""")
    rconn.execute("CREATE INDEX IF NOT EXISTS idx_st_day0 ON strategy(day0)")
    rconn.commit()

def save_to_result_db(rconn, df):
    cols = ["day0", "industry_code", "industry_name", "stock_code", "name", "mc_rank", "baseline_to_r", "recent1_to_r", "trigger_ratio_1", "recent2_to_r", "trigger_ratio_2", "bl_above_ma20", "bl_below_ma20", "bl_above_ma60", "bl_below_ma60", "bl_above_ma200", "bl_below_ma200", "dm1_close", "dm1_is_limit_up", "dm1_above_ma20", "dm1_above_ma60", "dm1_above_ma200", "d0_change_pct", "d0_close", "d0_ma5", "d0_ma20", "d0_ma60", "d0_ma200", "d0_vs_ma5", "d0_vs_ma20", "d0_vs_ma60", "d0_above_ma20", "d0_above_ma60", "day_offset", "date", "close", "to_r", "to_r_ratio", "change_pct", "total_change_pct", "to_r_change_rate", "ma5", "ma20", "ma60", "ma200", "vs_ma5", "vs_ma20", "vs_ma60"]
    update_cols = [c for c in cols if c not in ["day0", "stock_code", "day_offset"]]
    update_set = ", ".join([f"{c}=excluded.{c}" for c in update_cols])
    
    def _cast(v):
        if pd.isna(v): return None
        if hasattr(v, 'item'): return v.item()
        return v
        
    rows = [tuple(_cast(v) for v in row) for row in df[cols].itertuples(index=False)]
    rconn.executemany(f"INSERT INTO strategy ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))}) ON CONFLICT(day0, stock_code, day_offset) DO UPDATE SET {update_set}", rows)
    rconn.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day0", type=str)
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()
    conn = sqlite3.connect(DB_PATH)
    rconn = sqlite3.connect(RESULT_DB_PATH)
    init_result_db(rconn)
    calendar = load_trade_calendar(conn)
    dates = [args.day0] if args.day0 else calendar[(calendar >= (args.start or calendar.iloc[-1])) & (calendar <= (args.end or calendar.iloc[-1]))].tolist()
    for day in dates:
        print(f"Day0={day}", end="...")
        res, meta = analyze_day(conn, day, calendar)
        if not res.empty: save_to_result_db(rconn, res); print(f"✅ {len(res)}只")
        else: print(" 无信号")
    conn.close(); rconn.close()

if __name__ == "__main__": main()
