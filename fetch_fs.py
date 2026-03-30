"""
fetch_fs.py
-----------
从理杏仁 API 批量获取 A 股季度财务报表数据（营收、净利润、ROE等）。

5个接口对应 stocks.fs_table_type 字段：
  non_financial   -> /cn/company/fs/non_financial
  bank            -> /cn/company/fs/bank
  security        -> /cn/company/fs/security
  insurance       -> /cn/company/fs/insurance
  other_financial -> /cn/company/fs/other_financial

与基本面数据的区别：
  - 数据为季度频率（财报日期）
  - 指标格式为 [granularity].[table].[field].[calcType]
  - API返回嵌套JSON，需要展平
  - 历史模式下每次只能查1只股票（API限制）

使用方式:
    export LIXINGER_TOKEN="你的Token"

    # 获取最新一期财报（默认，快速）
    python fetch_fs.py

    # 获取历史财报数据（从2020年起，逐股请求）
    python fetch_fs.py --history
"""

import os
import sys
import time
import sqlite3
import requests
from datetime import date

# ─── 配置 ───────────────────────────────────────────────────────────────────────
TOKEN        = os.getenv("LIXINGER_TOKEN")
DB_PATH      = "stock_data.db"
BATCH_SIZE   = 100    # 快照模式每批股票数（历史模式固定为1）
API_INTERVAL = 0.25   # 秒

HISTORY_START = "2020-01-01"

# 5个财报接口 URL（key = fs_table_type）
ENDPOINTS = {
    "non_financial":   "https://open.lixinger.com/api/cn/company/fs/non_financial",
    "bank":            "https://open.lixinger.com/api/cn/company/fs/bank",
    "security":        "https://open.lixinger.com/api/cn/company/fs/security",
    "insurance":       "https://open.lixinger.com/api/cn/company/fs/insurance",
    "other_financial": "https://open.lixinger.com/api/cn/company/fs/other_financial",
}

# 要抓取的季度财报指标（格式: granularity.table.field.calcType）
# 所有公司类型均使用 q.ps.* 利润表前缀；
# 金融类公司（银行/证券/保险等）不支持 gp_m/ebit/ebitda，
# 其余通用指标两类公司均适用。

# 金融类公司指标（bank / security / insurance / other_financial）
_FINANCIAL_METRICS = [
    "q.ps.toi.t",           # 营业总收入
    "q.ps.oi.t",            # 营业收入
    "q.ps.op.t",            # 营业利润
    "q.ps.np.t",            # 净利润
    "q.ps.npatoshopc.t",    # 归母净利润
    "q.ps.np_s_r.t",        # 净利润率
    "q.ps.wroe.t",          # 加权ROE
    "q.ps.beps.t",          # 基本每股收益
]

METRICS_BY_TYPE: dict[str, list[str]] = {
    "non_financial": [
        "q.ps.toi.t",           # 营业总收入
        "q.ps.oi.t",            # 营业收入
        "q.ps.op.t",            # 营业利润
        "q.ps.np.t",            # 净利润
        "q.ps.npatoshopc.t",    # 归母净利润
        "q.ps.gp_m.t",          # 毛利率（金融类不适用）
        "q.ps.np_s_r.t",        # 净利润率
        "q.ps.wroe.t",          # 加权ROE
        "q.ps.beps.t",          # 基本每股收益
        "q.ps.ebit.t",          # EBIT（金融类不适用）
        "q.ps.ebitda.t",        # EBITDA（金融类不适用）
    ],
    "bank":            _FINANCIAL_METRICS,
    "security":        _FINANCIAL_METRICS,
    "insurance":       _FINANCIAL_METRICS,
    "other_financial": _FINANCIAL_METRICS,
}

# 所有指标的并集（用于建表），去重保序
_seen: set[str] = set()
ALL_METRICS: list[str] = []
for _ml in METRICS_BY_TYPE.values():
    for _m in _ml:
        if _m not in _seen:
            _seen.add(_m)
            ALL_METRICS.append(_m)

METRICS_LIST = ALL_METRICS

# ─── 数据库 ──────────────────────────────────────────────────────────────────────

# 将指标名称中的"."替换为"_"，作为数据库列名
def metric_to_col(metric: str) -> str:
    return metric.replace(".", "_")

METRIC_COLS = [metric_to_col(m) for m in METRICS_LIST]


def init_table(conn: sqlite3.Connection):
    """建立 financial_statements 表，并自动补全缺少的列（向前兼容）"""
    col_defs = "\n".join(f"            {col}  REAL," for col in METRIC_COLS)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS financial_statements (
            stock_code      TEXT    NOT NULL,
            date            TEXT    NOT NULL,   -- 财报截止日
            report_date     TEXT,               -- 公告日期
            standard_date   TEXT,               -- 标准财年日期
            report_type     TEXT,               -- 财报类型
            currency        TEXT,               -- 货币类型
            {col_defs}
            updated_at      TEXT DEFAULT (datetime('now', 'localtime')),
            PRIMARY KEY (stock_code, date, report_type)
        )
    """)
    # 兼容旧表：若缺少列则自动 ALTER TABLE ADD COLUMN
    existing = {row[1] for row in conn.execute("PRAGMA table_info(financial_statements)")}
    for col in METRIC_COLS:
        if col not in existing:
            conn.execute(f"ALTER TABLE financial_statements ADD COLUMN {col} REAL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fs_date ON financial_statements (date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fs_code ON financial_statements (stock_code)")
    conn.commit()


def get_stocks_by_type(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """按 fs_table_type 分组读取所有正常上市股票代码"""
    rows = conn.execute("""
        SELECT stock_code, fs_table_type FROM stocks
        WHERE listing_status = 'normally_listed'
        ORDER BY stock_code
    """).fetchall()
    groups: dict[str, list[str]] = {}
    for code, fs_type in rows:
        fs_type = fs_type if fs_type in ENDPOINTS else "non_financial"
        groups.setdefault(fs_type, []).append(code)
    return groups


def get_last_date(conn: sqlite3.Connection, stock_code: str) -> str | None:
    """获取该股票最新的财报日期"""
    row = conn.execute(
        "SELECT MAX(date) FROM financial_statements WHERE stock_code = ?",
        (stock_code,)
    ).fetchone()
    return row[0] if row and row[0] else None


# ─── 嵌套JSON展平 ────────────────────────────────────────────────────────────────

def extract_metric(raw: dict, metric: str):
    """
    从嵌套响应中提取指标值。
    例如：metric="q.ps.toi.t" 对应 raw["q"]["ps"]["toi"]["t"]
    """
    parts = metric.split(".")
    val = raw
    try:
        for p in parts:
            val = val[p]
        return val
    except (KeyError, TypeError):
        return None


# ─── API 调用 ───────────────────────────────────────────────────────────────────

def fetch_batch(
    url: str,
    codes: list[str],
    metrics: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    single_date: str | None = None,
) -> list[dict]:
    """调用财报接口，获取一批股票的数据"""
    payload: dict = {
        "token":       TOKEN,
        "stockCodes":  codes,
        "metricsList": metrics,
    }
    if single_date:
        payload["date"] = single_date
    elif start_date:
        payload["startDate"] = start_date
        if end_date:
            payload["endDate"] = end_date

    try:
        resp = requests.post(url, json=payload, timeout=20)
        if resp.status_code != 200:
            print(f"\n      ⚠ HTTP {resp.status_code}: {resp.text[:200]}")
            return []
        data_json = resp.json()
        if data_json.get("code") == 1:
            return data_json.get("data") or []
        else:
            msg = data_json.get("message", "")
            if msg:
                print(f"\n      ⚠ API错误: {msg}")
            return []
    except Exception as e:
        print(f"\n      ⚠ 请求异常: {e}")
        return []


# ─── 数据解析与入库 ─────────────────────────────────────────────────────────────

def parse_record(raw: dict) -> dict | None:
    """解析单条财报记录（展平嵌套JSON）"""
    raw_date = raw.get("date", "")
    if not raw_date:
        return None
    if "T" in raw_date:
        raw_date = raw_date[:10]

    code = raw.get("stockCode", "")
    if not code:
        return None

    def clean_date(d):
        if d and "T" in d:
            return d[:10]
        return d

    record = {
        "stock_code":    code,
        "date":          raw_date,
        "report_date":   clean_date(raw.get("reportDate", "")),
        "standard_date": clean_date(raw.get("standardDate", "")),
        "report_type":   raw.get("reportType", ""),
        "currency":      raw.get("currency", ""),
    }

    # 展平各指标
    for metric, col in zip(METRICS_LIST, METRIC_COLS):
        record[col] = extract_metric(raw, metric)

    return record


def upsert_records(conn: sqlite3.Connection, records: list[dict]):
    """批量写入财报数据，冲突时更新"""
    col_set = ", ".join(f"{c} = excluded.{c}" for c in METRIC_COLS)
    sql = f"""
        INSERT INTO financial_statements (
            stock_code, date, report_date, standard_date, report_type, currency,
            {", ".join(METRIC_COLS)}
        ) VALUES (
            :stock_code, :date, :report_date, :standard_date, :report_type, :currency,
            {", ".join(":" + c for c in METRIC_COLS)}
        )
        ON CONFLICT(stock_code, date, report_type) DO UPDATE SET
            report_date   = excluded.report_date,
            standard_date = excluded.standard_date,
            currency      = excluded.currency,
            {col_set},
            updated_at    = datetime('now', 'localtime')
    """
    conn.executemany(sql, records)
    conn.commit()


# ─── 主流程 ─────────────────────────────────────────────────────────────────────

def run_snapshot_mode(conn: sqlite3.Connection, groups: dict[str, list[str]]) -> int:
    """快照模式：获取最新一期财报（latest 关键字）"""
    print("\n  模式: 最新财报快照 (latest)\n")
    total_written = 0

    for fs_type, codes in groups.items():
        url = ENDPOINTS[fs_type]
        n_batches = (len(codes) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  [{fs_type}] {len(codes)} 只，{n_batches} 批")

        for bi in range(n_batches):
            batch = codes[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
            print(f"    批次 {bi+1}/{n_batches} ({batch[0]}~{batch[-1]})", end=" ... ")
            time.sleep(API_INTERVAL)

            raw_data = fetch_batch(url, batch, METRICS_BY_TYPE[fs_type], single_date="latest")
            if not raw_data:
                print("无数据")
                continue

            records = [r for raw in raw_data if (r := parse_record(raw))]
            if records:
                upsert_records(conn, records)
                total_written += len(records)
                print(f"✓ {len(records)} 条")
            else:
                print("解析为空")

    return total_written


def run_history_mode(conn: sqlite3.Connection, groups: dict[str, list[str]]) -> int:
    """历史模式：逐股获取 2020-01-01 至今的财报数据"""
    today = date.today().strftime("%Y-%m-%d")
    print(f"\n  模式: 历史财报 ({HISTORY_START} ~ {today})\n")
    print("  ⚠ 历史模式每只股票单独请求（API限制），请耐心等待。\n")

    all_codes = [
        (code, fs_type)
        for fs_type, codes in groups.items()
        for code in codes
    ]
    total = len(all_codes)
    total_written = 0
    start_time = time.time()

    for i, (code, fs_type) in enumerate(all_codes, 1):
        last_date = get_last_date(conn, code)
        fetch_start = last_date if last_date and last_date > HISTORY_START else HISTORY_START

        print(f"  [{i:4d}/{total}] {code}  {fetch_start}→{today}", end=" ... ")
        url = ENDPOINTS.get(fs_type, ENDPOINTS["non_financial"])

        time.sleep(API_INTERVAL)
        metrics = METRICS_BY_TYPE.get(fs_type, METRICS_BY_TYPE["non_financial"])
        raw_data = fetch_batch(url, [code], metrics, start_date=fetch_start, end_date=today)

        if not raw_data:
            print("无数据")
            continue

        records = [r for raw in raw_data if (r := parse_record(raw))]
        if records:
            upsert_records(conn, records)
            total_written += len(records)
            print(f"✓ {len(records)} 条")
        else:
            print("解析为空")

        if i % 200 == 0:
            elapsed = time.time() - start_time
            eta = elapsed / i * (total - i)
            print(f"\n  ── {i}/{total} ({i/total*100:.1f}%) | 写入:{total_written:,} | 剩余:{eta/60:.1f}分钟 ──\n")

    return total_written


def main():
    if not TOKEN:
        print("❌ 未找到环境变量 LIXINGER_TOKEN")
        return

    history_mode = "--history" in sys.argv

    print("=" * 60)
    print("📋 财务报表数据抓取工具 (理杏仁 API)")
    print(f"   指标数: {len(METRICS_LIST)} 个")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    init_table(conn)

    groups = get_stocks_by_type(conn)
    if not groups:
        print("❌ stocks 表为空，请先运行 fetch_stocks.py")
        conn.close()
        return

    total_stocks = sum(len(v) for v in groups.items() if isinstance(v, list))
    total_stocks = sum(len(v) for v in groups.values())
    print(f"\n  股票总数: {total_stocks}")
    for fs_type, codes in groups.items():
        print(f"    {fs_type:20s}: {len(codes)} 只  →  {ENDPOINTS[fs_type]}")

    start_time = time.time()
    written = run_history_mode(conn, groups) if history_mode else run_snapshot_mode(conn, groups)
    conn.close()

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("✅ 完成！")
    print(f"   写入记录: {written:,}")
    print(f"   总耗时:   {elapsed:.1f} 秒")
    print(f"   数据库:   {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
