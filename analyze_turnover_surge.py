"""
analyze_turnover_surge.py
--------------------------
以某交易日 Day1 为基准，在 sw_2021 **三级行业**龙头股（市值 Top-N）中，
筛选 Day(-5)~Day(-1) 平均换手率超过 Day(-35)~Day(-6) 基准换手率 1.5 倍的股票，
并记录 Day(1)~Day(10) 每天的换手率比例和涨幅。

日期窗口（均为交易日）：
    Day(-35) ~ Day(-6)  →  基准期（30个交易日）
    Day(-5)  ~ Day(-1)  →  观察期（5个交易日）
    Day(1)   ~ Day(10)  →  跟踪期（10个交易日）

输出 CSV 字段：
    day1, industry_code, industry_name, stock_code, name, mc_rank,
    baseline_to_r, recent_to_r, trigger_ratio,
    # Day1 快照（触发时刻均线位置）
    d1_close, d1_ma5, d1_ma20, d1_ma60, d1_ma200,
    d1_vs_ma5, d1_vs_ma20, d1_vs_ma60,   # close / maXX
    d1_above_ma20, d1_above_ma60,         # bool
    # 跟踪期每日数据
    day_offset, date, close, to_r, to_r_ratio, change_pct,
    ma5, ma20, ma60, ma200,
    vs_ma5, vs_ma20, vs_ma60              # close / maXX

用法：
    # 分析单日
    python analyze_turnover_surge.py --day1 2026-03-28

    # 批量分析一段时间（追加到同一个 CSV）
    python analyze_turnover_surge.py --start 2025-01-01 --end 2026-03-28

    # 列出 sw_2021 行业代码格式（用于调试）
    python analyze_turnover_surge.py --show-industries
"""

import os
import argparse
import sqlite3
import pandas as pd
from datetime import date as date_type

# ─── 配置 ───────────────────────────────────────────────────────────────────────
DB_PATH          = "stock_data.db"
RESULT_DB_PATH   = "turnover_surge.db"   # 分析结果数据库
OUTPUT_DIR       = "output"
INDUSTRY_SOURCE  = "sw_2021"    # 行业体系
TOP_N            = 3            # 每个三级行业龙头股数量
BASELINE_DAYS    = 30           # Day(-35)~Day(-6)，共30个交易日
RECENT_DAYS      = 5            # Day(-5)~Day(-1)，共5个交易日
FORWARD_DAYS     = 10           # Day(1)~Day(10)
TRIGGER_RATIO    = 1.8          # 触发倍数阈值
MIN_D1_CHANGE    = 0.03         # Day1 涨幅门槛（如 0.03 表示 3%）
MIN_BASELINE_OBS = 15           # 基准期有效观测天数下限（不足则跳过）


# ─── 申万2021 三级行业判断 ────────────────────────────────────────────────────────
def is_level3(industry_code: str) -> bool:
    """
    申万行业代码规律（6位数字）：
      一级：末4位全为0，如 110000
      二级：末2位全为0，如 110100
      三级：末2位非全0，如 110101
    """
    code = str(industry_code).strip()
    if len(code) < 4:
        return False
    return code[-2:] != "00"


# ─── 交易日历 ────────────────────────────────────────────────────────────────────
def load_trade_calendar(conn: sqlite3.Connection) -> pd.Series:
    """
    加载交易日历，返回有序 Series（index=0,1,2...，value=日期字符串）。
    若 trade_calendar 表为空，自动从 daily_kline 提取贵州茅台(600519)的交易日作为备选。
    """
    cal = pd.read_sql("SELECT date FROM trade_calendar ORDER BY date", conn)["date"]
    if cal.empty:
        print("  ⚠ trade_calendar 表为空，从 daily_kline(600519) 自动提取交易日历...")
        cal = pd.read_sql(
            "SELECT DISTINCT date FROM daily_kline "
            "WHERE stock_code='600519' ORDER BY date",
            conn
        )["date"]
    if cal.empty:
        # 600519 也没有，退而求其次用全市场 distinct date（慢但兜底）
        print("  ⚠ 未找到 600519 数据，改用全市场 distinct date（较慢）...")
        cal = pd.read_sql(
            "SELECT DISTINCT date FROM daily_kline ORDER BY date",
            conn
        )["date"]
    return cal.reset_index(drop=True)


def get_day_index(calendar: pd.Series, day1_str: str) -> int | None:
    """返回 day1 在 trade_calendar 中的位置（取 <= day1 的最后一个）"""
    mask = calendar <= day1_str
    if not mask.any():
        return None
    return int(mask[::-1].idxmax())


def get_date_windows(calendar: pd.Series, idx: int) -> dict[str, list[str]]:
    """
    根据 Day1 在日历中的下标，返回各时间窗口的日期列表。
    返回 None 表示历史数据不足。
    """
    # Day(-35), ..., Day(-6)  → indices [idx-35, idx-6]，共30天
    baseline_start = idx - 35
    baseline_end   = idx - 5   # slice 不含右端，所以 idx-5 对应 Day(-6)+1=Day(-5)前
    # Day(-5), ..., Day(-1)   → indices [idx-5, idx-1]，共5天
    recent_start = idx - 5
    recent_end   = idx         # slice 不含 idx，即 Day(-1)+1=Day0前
    # Day(1),  ..., Day(10)   → indices [idx+1, idx+10]
    forward_start = idx + 1
    forward_end   = idx + 11

    if baseline_start < 0:
        return {}

    n_cal = len(calendar)
    return {
        "baseline": calendar.iloc[baseline_start:baseline_end].tolist(),
        "recent":   calendar.iloc[recent_start:recent_end].tolist(),
        "forward":  calendar.iloc[forward_start:min(forward_end, n_cal)].tolist(),
    }


# ─── 龙头股获取 ──────────────────────────────────────────────────────────────────
def get_top_stocks(conn: sqlite3.Connection, day1_str: str, top_n: int = TOP_N) -> pd.DataFrame:
    """
    获取每个 sw_2021 三级行业市值前 top_n 的正常上市股票。
    市值取 day1 之前最新的 fundamentals 记录。

    返回列：stock_code, name, industry_code, industry_name, mc, mc_rank
    """
    sql = """
        WITH recent_mc AS (
            -- 每只股票取 day1 前最新市值（只取一条）
            SELECT f.stock_code, f.mc,
                   ROW_NUMBER() OVER (
                       PARTITION BY f.stock_code ORDER BY f.date DESC
                   ) AS rn
            FROM fundamentals f
            WHERE f.date <= :day1
              AND f.mc IS NOT NULL
              AND f.mc > 0
        ),
        ind AS (
            SELECT si.stock_code, si.industry_code, si.name AS industry_name
            FROM stock_industries si
            WHERE si.source = :source
        ),
        combined AS (
            SELECT
                s.stock_code,
                s.name,
                rm.mc,
                i.industry_code,
                i.industry_name,
                ROW_NUMBER() OVER (
                    PARTITION BY i.industry_code ORDER BY rm.mc DESC
                ) AS mc_rank
            FROM recent_mc rm
            JOIN stocks s
                ON s.stock_code = rm.stock_code
               AND s.listing_status = 'normally_listed'
            JOIN ind i ON i.stock_code = rm.stock_code
            WHERE rm.rn = 1
        )
        SELECT stock_code, name, mc, industry_code, industry_name, mc_rank
        FROM combined
        WHERE mc_rank <= :top_n
        ORDER BY industry_code, mc_rank
    """
    df = pd.read_sql(sql, conn, params={
        "day1":   day1_str,
        "source": INDUSTRY_SOURCE,
        "top_n":  top_n,
    })

    # 过滤三级行业
    df = df[df["industry_code"].apply(is_level3)].copy()
    return df


# ─── K线 / 均线数据获取 ──────────────────────────────────────────────────────────
def get_kline_slice(conn: sqlite3.Connection, codes: list[str], dates: list[str]) -> pd.DataFrame:
    """批量获取指定股票在指定交易日列表中的 close / to_r / change_pct"""
    if not codes or not dates:
        return pd.DataFrame()
    ph_c = ",".join(f"'{c}'" for c in codes)
    ph_d = ",".join(f"'{d}'" for d in dates)
    sql = f"""
        SELECT stock_code, date, close, to_r, change_pct
        FROM daily_kline
        WHERE stock_code IN ({ph_c})
          AND date IN ({ph_d})
    """
    return pd.read_sql(sql, conn)


def get_ma_slice(conn: sqlite3.Connection, codes: list[str], dates: list[str]) -> pd.DataFrame:
    """批量获取指定股票在指定交易日列表中的 MA5/MA20/MA60/MA200"""
    if not codes or not dates:
        return pd.DataFrame()
    ph_c = ",".join(f"'{c}'" for c in codes)
    ph_d = ",".join(f"'{d}'" for d in dates)
    sql = f"""
        SELECT stock_code, date, ma5, ma20, ma60, ma200
        FROM moving_averages
        WHERE stock_code IN ({ph_c})
          AND date IN ({ph_d})
    """
    return pd.read_sql(sql, conn)


# ─── 核心分析函数 ────────────────────────────────────────────────────────────────
def analyze_day(
    conn: sqlite3.Connection,
    day1_str: str,
    calendar: pd.Series,
) -> pd.DataFrame:
    """
    以 day1_str 为 Day1，对三级行业龙头股执行换手率异动分析。

    Returns:
        包含触发信号和跟踪期数据的 DataFrame；无信号时返回空 DataFrame。
    """
    # 1. 确认 Day1 在交易日历中的位置
    idx = get_day_index(calendar, day1_str)
    if idx is None:
        return pd.DataFrame()

    windows = get_date_windows(calendar, idx)
    if not windows:
        return pd.DataFrame()

    baseline_dates = windows["baseline"]
    recent_dates   = windows["recent"]
    forward_dates  = windows["forward"]

    if len(baseline_dates) < MIN_BASELINE_OBS or len(recent_dates) == 0:
        return pd.DataFrame()

    # 2. 获取三级行业龙头股
    top_stocks = get_top_stocks(conn, day1_str, TOP_N)
    if top_stocks.empty:
        return pd.DataFrame()

    codes = top_stocks["stock_code"].tolist()

    # 3. 获取历史换手率（基准期 + 观察期）
    lookback_dates = baseline_dates + recent_dates
    kline_hist = get_kline_slice(conn, codes, lookback_dates)
    if kline_hist.empty:
        return pd.DataFrame()

    # 计算基准换手率（Day-35 ~ Day-6 均值）
    baseline_df = (
        kline_hist[kline_hist["date"].isin(baseline_dates)]
        .groupby("stock_code")["to_r"]
        .agg(baseline_to_r="mean", baseline_obs="count")
        .reset_index()
    )

    # 计算近期换手率（Day-5 ~ Day-1 均值）
    recent_df = (
        kline_hist[kline_hist["date"].isin(recent_dates)]
        .groupby("stock_code")["to_r"]
        .mean()
        .rename("recent_to_r")
        .reset_index()
    )

    # 4. 合并，计算触发比例，筛选触发股票
    sig = top_stocks.merge(baseline_df, on="stock_code", how="left")
    sig = sig.merge(recent_df, on="stock_code", how="left")
    sig["trigger_ratio"] = sig["recent_to_r"] / sig["baseline_to_r"]

    triggered = sig[
        (sig["trigger_ratio"] >= TRIGGER_RATIO)
        & (sig["baseline_obs"] >= MIN_BASELINE_OBS)
        & sig["baseline_to_r"].notna()
        & sig["recent_to_r"].notna()
    ].copy()

    if triggered.empty:
        return pd.DataFrame()

    # 5. 获取 Day1 的收盘价和均线（触发快照）
    t_codes = triggered["stock_code"].tolist()
    day1_dates = [day1_str]

    kline_d1 = get_kline_slice(conn, t_codes, day1_dates)
    ma_d1    = get_ma_slice(conn, t_codes, day1_dates)

    # 合并 Day1 快照到 triggered
    if not kline_d1.empty:
        # 增加精度并筛选 Day1 涨幅
        kline_d1 = kline_d1[kline_d1["change_pct"] >= MIN_D1_CHANGE].copy()
        if kline_d1.empty:
            return pd.DataFrame()
            
        d1_data = kline_d1[["stock_code", "close", "change_pct"]].rename(columns={
            "close": "d1_close",
            "change_pct": "d1_change_pct"
        })
        triggered = triggered.merge(d1_data, on="stock_code", how="inner")
    else:
        return pd.DataFrame()

    if not ma_d1.empty:
        d1_ma = ma_d1[["stock_code", "ma5", "ma20", "ma60", "ma200"]].rename(columns={
            "ma5": "d1_ma5", "ma20": "d1_ma20", "ma60": "d1_ma60", "ma200": "d1_ma200",
        })
        triggered = triggered.merge(d1_ma, on="stock_code", how="left")
    else:
        for c in ["d1_ma5", "d1_ma20", "d1_ma60", "d1_ma200"]:
            triggered[c] = pd.NA

    # 计算 Day1 价格对均线比例
    triggered["d1_vs_ma5"]  = (triggered["d1_close"] / triggered["d1_ma5"]).round(4)
    triggered["d1_vs_ma20"] = (triggered["d1_close"] / triggered["d1_ma20"]).round(4)
    triggered["d1_vs_ma60"] = (triggered["d1_close"] / triggered["d1_ma60"]).round(4)
    triggered["d1_above_ma20"] = (triggered["d1_close"] > triggered["d1_ma20"]).astype("Int8")
    triggered["d1_above_ma60"] = (triggered["d1_close"] > triggered["d1_ma60"]).astype("Int8")

    # 6. 获取 Day(1)~Day(10) 的换手率、涨幅、均线
    kline_fwd = get_kline_slice(conn, t_codes, forward_dates)
    ma_fwd    = get_ma_slice(conn, t_codes, forward_dates)

    if kline_fwd.empty:
        # 没有未来数据（Day1 接近最新交易日），返回仅含触发信号的记录
        triggered["day1"]       = day1_str
        triggered["day_offset"] = pd.NA
        triggered["date"]       = pd.NA
        triggered["close"]      = pd.NA
        triggered["to_r"]       = pd.NA
        triggered["to_r_ratio"] = pd.NA
        triggered["change_pct"] = pd.NA
        triggered["ma5"]        = pd.NA
        triggered["ma20"]       = pd.NA
        triggered["ma60"]       = pd.NA
        triggered["ma200"]      = pd.NA
        triggered["vs_ma5"]     = pd.NA
        triggered["vs_ma20"]    = pd.NA
        triggered["vs_ma60"]    = pd.NA
        return _format_result(triggered, has_forward=False)

    # 计算 day_offset
    date_to_offset = {d: i + 1 for i, d in enumerate(forward_dates)}
    kline_fwd = kline_fwd.copy()
    kline_fwd["day_offset"] = kline_fwd["date"].map(date_to_offset)

    # 合并均线到跟踪期
    if not ma_fwd.empty:
        kline_fwd = kline_fwd.merge(ma_fwd, on=["stock_code", "date"], how="left")
    else:
        for c in ["ma5", "ma20", "ma60", "ma200"]:
            kline_fwd[c] = pd.NA

    # 合并触发信息（含 Day1 均线快照）
    trigger_cols = [
        "stock_code", "name", "mc", "mc_rank",
        "industry_code", "industry_name",
        "baseline_to_r", "recent_to_r", "trigger_ratio",
        "d1_close", "d1_ma5", "d1_ma20", "d1_ma60", "d1_ma200",
        "d1_vs_ma5", "d1_vs_ma20", "d1_vs_ma60",
        "d1_above_ma20", "d1_above_ma60",
    ]
    kline_fwd = kline_fwd.merge(triggered[trigger_cols], on="stock_code", how="inner")

    # 计算跟踪期每天的价格对均线比例
    kline_fwd["to_r_ratio"] = (kline_fwd["to_r"] / kline_fwd["baseline_to_r"]).round(4)
    kline_fwd["vs_ma5"]  = (kline_fwd["close"] / kline_fwd["ma5"]).round(4)
    kline_fwd["vs_ma20"] = (kline_fwd["close"] / kline_fwd["ma20"]).round(4)
    kline_fwd["vs_ma60"] = (kline_fwd["close"] / kline_fwd["ma60"]).round(4)
    kline_fwd["day1"] = day1_str

    return _format_result(kline_fwd, has_forward=True)


def _format_result(df: pd.DataFrame, has_forward: bool) -> pd.DataFrame:
    """统一输出列顺序和类型"""
    # 基础列（触发信号 + Day1 均线快照）
    base_cols = [
        "day1",
        "industry_code", "industry_name",
        "stock_code", "name", "mc_rank",
        "baseline_to_r", "recent_to_r", "trigger_ratio",
        # Day1 均线快照
        "d1_change_pct", "d1_close", 
        "d1_ma5", "d1_ma20", "d1_ma60", "d1_ma200",
        "d1_vs_ma5", "d1_vs_ma20", "d1_vs_ma60",
        "d1_above_ma20", "d1_above_ma60",
    ]
    # 跟踪期列
    if has_forward:
        fwd_cols = [
            "day_offset", "date",
            "close", "to_r", "to_r_ratio", "change_pct",
            "ma5", "ma20", "ma60", "ma200",
            "vs_ma5", "vs_ma20", "vs_ma60",
        ]
    else:
        fwd_cols = [
            "day_offset", "date",
            "close", "to_r", "to_r_ratio", "change_pct",
            "ma5", "ma20", "ma60", "ma200",
            "vs_ma5", "vs_ma20", "vs_ma60",
        ]

    cols = base_cols + fwd_cols

    # 对齐列（可能缺列时补 NA）
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[cols].copy()

    # 数值精度
    round4 = [
        "baseline_to_r", "recent_to_r", "trigger_ratio",
        "to_r_ratio", "change_pct", "d1_change_pct",
        "d1_close", "d1_vs_ma5", "d1_vs_ma20", "d1_vs_ma60",
        "close", "vs_ma5", "vs_ma20", "vs_ma60",
    ]
    for col in round4:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    return df.sort_values(["industry_code", "stock_code", "day_offset"]).reset_index(drop=True)


# ─── 辅助：展示行业格式（用于调试） ────────────────────────────────────────────────
def show_industries(conn: sqlite3.Connection):
    sql = """
        SELECT industry_code, name, source, COUNT(*) as stock_cnt
        FROM stock_industries
        WHERE source = ?
        GROUP BY industry_code, name
        ORDER BY industry_code
        LIMIT 60
    """
    df = pd.read_sql(sql, conn, params=[INDUSTRY_SOURCE])
    if df.empty:
        print(f"⚠ 未找到 source='{INDUSTRY_SOURCE}' 的行业数据")
        return
    df["is_level3"] = df["industry_code"].apply(is_level3)
    df["code_len"]  = df["industry_code"].str.len()
    print(df.to_string(index=False))
    print(f"\n三级行业数量（is_level3=True）: {df[df['is_level3']]['industry_code'].nunique()}")


# ─── 结果数据库 ─────────────────────────────────────────────────────────────────
_RESULT_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS turnover_surge (
    day1            TEXT    NOT NULL,
    industry_code   TEXT,
    industry_name   TEXT,
    stock_code      TEXT    NOT NULL,
    name            TEXT,
    mc_rank         INTEGER,
    baseline_to_r   REAL,
    recent_to_r     REAL,
    trigger_ratio   REAL,
    d1_change_pct   REAL,
    d1_close        REAL,
    d1_ma5          REAL,
    d1_ma20         REAL,
    d1_ma60         REAL,
    d1_ma200        REAL,
    d1_vs_ma5       REAL,
    d1_vs_ma20      REAL,
    d1_vs_ma60      REAL,
    d1_above_ma20   INTEGER,
    d1_above_ma60   INTEGER,
    day_offset      INTEGER,
    date            TEXT,
    close           REAL,
    to_r            REAL,
    to_r_ratio      REAL,
    change_pct      REAL,
    ma5             REAL,
    ma20            REAL,
    ma60            REAL,
    ma200           REAL,
    vs_ma5          REAL,
    vs_ma20         REAL,
    vs_ma60         REAL,
    PRIMARY KEY (day1, stock_code, day_offset)
);
"""
_RESULT_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_ts_day1       ON turnover_surge(day1);",
    "CREATE INDEX IF NOT EXISTS idx_ts_stock      ON turnover_surge(stock_code);",
    "CREATE INDEX IF NOT EXISTS idx_ts_industry   ON turnover_surge(industry_code);",
]


def init_result_db(rconn: sqlite3.Connection):
    """初始化结果数据库的表结构与索引"""
    rconn.execute(_RESULT_CREATE_SQL)
    for idx_sql in _RESULT_INDEXES:
        rconn.execute(idx_sql)
    rconn.commit()


def save_to_result_db(rconn: sqlite3.Connection, df: pd.DataFrame):
    """将单日分析结果写入结果数据库（已存在的主键自动忽略）"""
    cols = [
        "day1","industry_code","industry_name","stock_code","name","mc_rank",
        "baseline_to_r","recent_to_r","trigger_ratio",
        "d1_change_pct","d1_close","d1_ma5","d1_ma20","d1_ma60","d1_ma200",
        "d1_vs_ma5","d1_vs_ma20","d1_vs_ma60","d1_above_ma20","d1_above_ma60",
        "day_offset","date","close","to_r","to_r_ratio","change_pct",
        "ma5","ma20","ma60","ma200","vs_ma5","vs_ma20","vs_ma60",
    ]
    placeholders = ",".join(["?"] * len(cols))
    col_names    = ",".join(cols)
    for col in cols:
        if col not in df.columns:
            df[col] = None
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df[cols].itertuples(index=False)
    ]
    rconn.executemany(
        f"INSERT OR IGNORE INTO turnover_surge ({col_names}) VALUES ({placeholders})",
        rows
    )
    rconn.commit()


# ─── 主入口 ─────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    parser = argparse.ArgumentParser(
        description="换手率异动选股分析工具（sw_2021 三级行业龙头股）"
    )
    parser.add_argument("--day1",  type=str, help="单日分析，格式 YYYY-MM-DD")
    parser.add_argument("--start", type=str, help="批量分析起始日（交易日）")
    parser.add_argument("--end",   type=str, help="批量分析结束日（默认最新交易日）")
    parser.add_argument("--output", type=str, default=None, help="指定输出目录")
    parser.add_argument("--result-db", type=str, default=RESULT_DB_PATH,
                        help=f"结果 SQLite 数据库路径（默认：{RESULT_DB_PATH}）")
    parser.add_argument("--no-db", action="store_true", help="不写入结果数据库")
    parser.add_argument("--no-csv", action="store_true", help="不保存 CSV 文件")
    parser.add_argument("--show-industries", action="store_true",
                        help="列出 sw_2021 行业代码样本（调试用）")
    args = parser.parse_args()

    conn  = sqlite3.connect(DB_PATH, timeout=60)

    # 初始化结果数据库
    rconn = None
    if not args.no_db:
        rconn = sqlite3.connect(args.result_db, timeout=60)
        init_result_db(rconn)
        print(f"💾 结果数据库：{args.result_db}")

    # 调试模式
    if args.show_industries:
        show_industries(conn)
        conn.close()
        return

    # 加载交易日历
    calendar = load_trade_calendar(conn)
    if calendar.empty:
        print("❌ 无法获取交易日历，请确保 daily_kline 表已有数据。")
        conn.close()
        return
    print(f"📅 交易日历：{calendar.iloc[0]} → {calendar.iloc[-1]}（{len(calendar)} 个交易日）")
    print(f"   行业来源：{INDUSTRY_SOURCE}  |  每业龙头：Top-{TOP_N}  |  触发倍数：{TRIGGER_RATIO}x\n")

    # 确定 Day1 列表
    if args.day1:
        day1_list = [args.day1]
    elif args.start:
        end_d = args.end or calendar.iloc[-1]
        day1_list = calendar[(calendar >= args.start) & (calendar <= end_d)].tolist()
    else:
        # 默认：最新交易日
        day1_list = [calendar.iloc[-1]]

    print(f"待分析 Day1 数量：{len(day1_list)} 个\n")

    triggered_stocks = 0
    triggered_days   = 0
    saved_files      = []
    skipped_days     = 0

    # 确定输出目录（--output 现在表示输出目录，单日分析时也可当文件路径）
    out_dir = args.output if args.output else OUTPUT_DIR

    for i, day1 in enumerate(day1_list, 1):
        # 每天独立的 CSV 路径
        day_tag  = day1.replace("-", "")
        day_path = os.path.join(out_dir, f"turnover_surge_{day_tag}.csv")

        # 断点续跑：优先以 DB 中是否已有该日记录为准，其次检查 CSV
        if rconn is not None:
            already = rconn.execute(
                "SELECT 1 FROM turnover_surge WHERE day1=? LIMIT 1", (day1,)
            ).fetchone()
            if already:
                print(f"  [{i:5d}/{len(day1_list)}] Day1={day1}  ⏩ DB 已有，跳过")
                skipped_days += 1
                continue
        elif not args.no_csv and os.path.exists(day_path):
            print(f"  [{i:5d}/{len(day1_list)}] Day1={day1}  ⏩ CSV 已存在，跳过")
            skipped_days += 1
            continue

        print(f"  [{i:5d}/{len(day1_list)}] Day1={day1}", end="  ...")
        result = analyze_day(conn, day1, calendar)
        if result.empty:
            print("  无触发")
        else:
            n_stocks = result["stock_code"].nunique()
            triggered_stocks += n_stocks
            triggered_days   += 1
            parts = []
            # 写入结果数据库
            if rconn is not None:
                save_to_result_db(rconn, result.copy())
                parts.append(f"DB({args.result_db})")
            # 保存 CSV
            if not args.no_csv:
                os.makedirs(out_dir, exist_ok=True)
                result.to_csv(day_path, index=False, encoding="utf-8-sig")
                saved_files.append(day_path)
                parts.append(f"CSV({day_path})")
            dest = " & ".join(parts)
            print(f"  ✓ {n_stocks} 只触发，{len(result)} 条记录 → {dest}")

    conn.close()
    if rconn is not None:
        rconn.close()

    print(f"\n{'='*58}")
    print("✅ 分析完成！")
    print(f"   分析交易日：{len(day1_list)} 个")
    if skipped_days:
        print(f"   跳过（已存在）：{skipped_days} 个")
    print(f"   触发交易日：{triggered_days} 个")
    print(f"   触发股票数：{triggered_stocks}")
    print(f"   保存文件数：{len(saved_files)}")
    if saved_files:
        print(f"   输出目录：  {out_dir}")
    print(f"{'='*58}")


if __name__ == "__main__":
    main()
