"""
fetch_fundamentals.py
---------------------
从理杏仁 API 批量获取 A 股基本面数据（PE、PB、市值等）。

5个接口对应 stocks.fs_table_type 字段：
  non_financial   -> /cn/company/fundamental/non_financial
  bank            -> /cn/company/fundamental/bank
  security        -> /cn/company/fundamental/security
  insurance       -> /cn/company/fundamental/insurance
  other_financial -> /cn/company/fundamental/other_financial

使用方式:
    export LIXINGER_TOKEN="你的Token"

    # 获取当日最新快照（默认模式，每天运行一次积累历史）
    python fetch_fundamentals.py

    # 获取历史数据（从2020年起，数据量大，耗时较长）
    python fetch_fundamentals.py --history

输出: stock_data.db 中的 fundamentals 表
"""

import os
import sys
import time
import sqlite3
import requests
from datetime import date

# ─── 配置 ───────────────────────────────────────────────────────────────────────
TOKEN        = os.getenv("LIXINGER_TOKEN")
DB_PATH      = os.getenv("DB_PATH", "stock_data.db")
BATCH_SIZE   = 100    # API 每次最多 100 只
API_INTERVAL = 0.1   # 秒，每分钟约 900 次 (保持在 1000 次安全线下)

# 历史模式起始日期
HISTORY_START = "2020-01-01"

# 5个接口 URL（key = fs_table_type）
ENDPOINTS = {
    "non_financial":   "https://open.lixinger.com/api/cn/company/fundamental/non_financial",
    "bank":            "https://open.lixinger.com/api/cn/company/fundamental/bank",
    "security":        "https://open.lixinger.com/api/cn/company/fundamental/security",
    "insurance":       "https://open.lixinger.com/api/cn/company/fundamental/insurance",
    "other_financial": "https://open.lixinger.com/api/cn/company/fundamental/other_financial",
}

# 要抓取的指标（通用于5个接口）
# 注：部分指标在金融类公司中可能无数据，会自动为 NULL
METRICS_LIST = [
    "mc",       # 总市值
    "cmc",      # 流通市值
    "pe_ttm",   # PE-TTM
    "pb",       # PB 市净率
    "ps_ttm",   # PS-TTM
    "dyr",      # 股息率
    "spc",      # 涨跌幅
    "to_r",     # 换手率
    "ta",       # 成交金额
]

# ─── 数据库 ──────────────────────────────────────────────────────────────────────

def init_table(conn: sqlite3.Connection):
    """建立 fundamentals 表及索引"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals (
            stock_code  TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            mc          REAL,
            cmc         REAL,
            pe_ttm      REAL,
            pb          REAL,
            ps_ttm      REAL,
            dyr         REAL,
            spc         REAL,
            to_r        REAL,
            ta          REAL,
            updated_at  TEXT DEFAULT (datetime('now', 'localtime')),
            PRIMARY KEY (stock_code, date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fund_date ON fundamentals (date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fund_code ON fundamentals (stock_code)")
    conn.commit()


def get_stocks_by_type(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """按 fs_table_type 分组读取所有正常上市股票代码"""
    rows = conn.execute("""
        SELECT stock_code, fs_table_type
        FROM stocks
        WHERE listing_status = 'normally_listed'
        ORDER BY stock_code
    """).fetchall()

    groups: dict[str, list[str]] = {}
    for code, fs_type in rows:
        # 未知类型归为 non_financial
        fs_type = fs_type if fs_type in ENDPOINTS else "non_financial"
        groups.setdefault(fs_type, []).append(code)
    return groups


def get_last_date(conn: sqlite3.Connection, stock_code: str) -> str | None:
    """获取该股票在 fundamentals 表中最新的日期（用于增量更新）"""
    row = conn.execute(
        "SELECT MAX(date) FROM fundamentals WHERE stock_code = ?",
        (stock_code,)
    ).fetchone()
    return row[0] if row and row[0] else None


# ─── API 调用 ───────────────────────────────────────────────────────────────────

def fetch_batch(
    url: str,
    codes: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    single_date: str | None = None,
) -> list[dict]:
    """
    获取一批股票的基本面数据。
    - single_date: 指定单日（如 "2026-03-27"）
    - start_date + end_date: 历史区间
    """
    payload: dict = {
        "token":       TOKEN,
        "stockCodes":  codes,
        "metricsList": METRICS_LIST,
    }
    if single_date:
        payload["date"] = single_date
    elif start_date:
        payload["startDate"] = start_date
        if end_date:
            payload["endDate"] = end_date

    max_retries = 3
    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(url, json=payload, timeout=20)
            if resp.status_code == 429:
                if attempt < max_retries:
                    print(f"\n      ⚠ HTTP 429: 请求过快。等待 2s 后进行第 {attempt+1} 次重试...")
                    time.sleep(2)
                    continue
                else:
                    print("\n      ⚠ HTTP 429: 已达到最大重试次数，放弃本次请求。")
                    return []
            
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
            if attempt < max_retries:
                time.sleep(1)
                continue
            print(f"\n      ⚠ 请求异常: {e}")
            return []


# ─── 数据解析与入库 ─────────────────────────────────────────────────────────────

def parse_record(raw: dict) -> dict | None:
    """解析单条基本面记录"""
    raw_date = raw.get("date", "")
    if not raw_date:
        return None
    if "T" in raw_date:
        raw_date = raw_date[:10]
    code = raw.get("stockCode", "")
    if not code:
        return None
    return {
        "stock_code": code,
        "date":       raw_date,
        "mc":         raw.get("mc"),
        "cmc":        raw.get("cmc"),
        "pe_ttm":     raw.get("pe_ttm"),
        "pb":         raw.get("pb"),
        "ps_ttm":     raw.get("ps_ttm"),
        "dyr":        raw.get("dyr"),
        "spc":        raw.get("spc"),
        "to_r":       raw.get("to_r"),
        "ta":         raw.get("ta"),
    }


def upsert_records(conn: sqlite3.Connection, records: list[dict]):
    """批量写入基本面数据，冲突时更新"""
    sql = """
        INSERT INTO fundamentals
            (stock_code, date, mc, cmc, pe_ttm, pb, ps_ttm, dyr, spc, to_r, ta)
        VALUES
            (:stock_code, :date, :mc, :cmc, :pe_ttm, :pb, :ps_ttm, :dyr, :spc, :to_r, :ta)
        ON CONFLICT(stock_code, date) DO UPDATE SET
            mc       = excluded.mc,
            cmc      = excluded.cmc,
            pe_ttm   = excluded.pe_ttm,
            pb       = excluded.pb,
            ps_ttm   = excluded.ps_ttm,
            dyr      = excluded.dyr,
            spc      = excluded.spc,
            to_r     = excluded.to_r,
            ta       = excluded.ta,
            updated_at = datetime('now', 'localtime')
    """
    conn.executemany(sql, records)
    conn.commit()


# ─── 主流程 ─────────────────────────────────────────────────────────────────────

def run_snapshot_mode(conn: sqlite3.Connection, groups: dict[str, list[str]]):
    """快照模式：只获取今日最新数据"""
    today = date.today().strftime("%Y-%m-%d")
    print(f"\n  模式: 当日快照 ({today})\n")

    total_written = 0
    for fs_type, codes in groups.items():
        url = ENDPOINTS[fs_type]
        n_batches = (len(codes) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  [{fs_type}] {len(codes)} 只股票，{n_batches} 批")

        for bi in range(n_batches):
            batch = codes[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
            print(f"    批次 {bi+1}/{n_batches} ({batch[0]}~{batch[-1]})", end=" ... ")
            time.sleep(API_INTERVAL)

            raw_data = fetch_batch(url, batch, single_date=today)
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


def run_history_mode(conn: sqlite3.Connection, groups: dict[str, list[str]]):
    """
    历史模式：按股票逐只获取 2020-01-01 至今的全量数据。
    当 stockCodes 长度>1 时，API 不支持 startDate，
    因此历史模式下每次只传 1 只股票。
    """
    today = date.today().strftime("%Y-%m-%d")
    print(f"\n  模式: 历史全量 ({HISTORY_START} ~ {today})\n")
    print("  ⚠ 历史模式每只股票单独请求，速度较慢，请耐心等待。\n")

    all_codes_with_type = []
    for fs_type, codes in groups.items():
        for code in codes:
            all_codes_with_type.append((code, fs_type))

    total = len(all_codes_with_type)
    total_written = 0
    start_time = time.time()

    for i, (code, fs_type) in enumerate(all_codes_with_type, 1):
        # 增量：找最新日期，只拉新数据
        last_date = get_last_date(conn, code)
        fetch_start = HISTORY_START
        if last_date and last_date >= today:
            if i % 200 == 0:
                _print_progress(i, total, total_written, start_time)
            continue
        if last_date and last_date > HISTORY_START:
            fetch_start = last_date  # API 会包含该日期，重复时 upsert 覆盖

        print(f"  [{i:4d}/{total}] {code}  {fetch_start}→{today}", end=" ... ")
        url = ENDPOINTS.get(fs_type, ENDPOINTS["non_financial"])

        time.sleep(API_INTERVAL)
        raw_data = fetch_batch(url, [code], start_date=fetch_start, end_date=today)

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
            _print_progress(i, total, total_written, start_time)

    return total_written


def _print_progress(i, total, written, start_time):
    elapsed = time.time() - start_time
    speed = i / elapsed if elapsed > 0 else 1
    eta = (total - i) / speed
    print(f"\n  ── {i}/{total} ({i/total*100:.1f}%) | 写入:{written:,} | 预计剩余:{eta/60:.1f}分钟 ──\n")


def main():
    if not TOKEN:
        print("❌ 未找到环境变量 LIXINGER_TOKEN")
        return

    # 判断运行模式
    history_mode = "--history" in sys.argv

    print("=" * 60)
    print("📊 基本面数据抓取工具 (理杏仁 API)")
    print(f"   指标: {', '.join(METRICS_LIST)}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    init_table(conn)

    groups = get_stocks_by_type(conn)
    if not groups:
        print("❌ stocks 表为空，请先运行 fetch_stocks.py")
        conn.close()
        return

    total_stocks = sum(len(v) for v in groups.values())
    print(f"\n  股票总数: {total_stocks}")
    for fs_type, codes in groups.items():
        print(f"    {fs_type:20s}: {len(codes)} 只")

    start_time = time.time()

    if history_mode:
        written = run_history_mode(conn, groups)
    else:
        written = run_snapshot_mode(conn, groups)

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
