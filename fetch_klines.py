"""
fetch_klines.py
---------------
从理杏仁 API 批量下载 A 股 K 线数据（2020-01-01 至今），存入 SQLite 数据库。

依赖:
    fetch_stocks.py 已运行，stock_data.db 中存有 stocks 表。

使用方式:
    export LIXINGER_TOKEN="你的Token"
    python fetch_klines.py

功能:
    - 自动读取 stock_data.db 中的股票列表
    - 断点续传：已下载的股票跳过，只下载新增或需要更新的
    - 增量更新：已有数据的股票只下载最后日期之后的新数据
    - 速率控制：每次 API 调用之间至少间隔 0.25 秒（≤4次/秒）
    - 按股票代码建立索引，保证查询速度
"""

import os
import time
import sqlite3
import requests
from datetime import date, datetime, timedelta

# ─── 配置 ───────────────────────────────────────────────────────────────────────
TOKEN        = os.getenv("LIXINGER_TOKEN")
DB_PATH      = os.getenv("DB_PATH", "stock_data.db")
API_URL      = "https://open.lixinger.com/api/cn/company/candlestick"
START_DATE   = "2020-01-01"
KLINE_TYPE   = "lxr_fc_rights"  # 理杏仁前复权（免费Token推荐）
API_INTERVAL = 0.25          # 秒，保证每秒 ≤ 4 次

# ─── 数据库 ──────────────────────────────────────────────────────────────────────

def init_kline_table(conn: sqlite3.Connection):
    """建立 daily_kline 表及索引（如果不存在）"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_kline (
            stock_code  TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            open        REAL,
            close       REAL,
            high        REAL,
            low         REAL,
            volume      REAL,
            amount      REAL,
            change_pct  REAL,
            to_r        REAL,
            PRIMARY KEY (stock_code, date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_kline_date
        ON daily_kline (date)
    """)
    conn.commit()


def get_stock_list(conn: sqlite3.Connection) -> list[dict]:
    """
    读取 stocks 表中正常上市的股票列表。
    返回列表，每项包含 stock_code 和 name。
    """
    rows = conn.execute("""
        SELECT stock_code, name, exchange, fs_table_type
        FROM stocks
        WHERE listing_status = 'normally_listed'
        ORDER BY stock_code
    """).fetchall()
    return [
        {"stock_code": r[0], "name": r[1], "exchange": r[2], "fs_table_type": r[3]}
        for r in rows
    ]


def get_last_date(conn: sqlite3.Connection, stock_code: str) -> str | None:
    """获取该股票在数据库中最晚的日期，用于增量更新"""
    row = conn.execute(
        "SELECT MAX(date) FROM daily_kline WHERE stock_code = ?",
        (stock_code,)
    ).fetchone()
    return row[0] if row and row[0] else None


# ─── API 调用 ───────────────────────────────────────────────────────────────────

def fetch_kline(stock_code: str, start_date: str, end_date: str) -> list[dict]:
    """
    调用理杏仁 K 线接口，返回原始 data 列表。
    失败时返回空列表。
    """
    payload = {
        "token":     TOKEN,
        "stockCode": stock_code,
        "type":      KLINE_TYPE,
        "startDate": start_date,
        "endDate":   end_date,
    }
    try:
        resp = requests.post(API_URL, json=payload, timeout=20)
        # 不调用 raise_for_status()，直接解析响应体
        if resp.status_code != 200:
            # 打印实际的错误响应体，方便调试
            print(f"      ⚠ HTTP {resp.status_code} [{stock_code}]: {resp.text[:200]}")
            return []
        data_json = resp.json()
        if data_json.get("code") == 1:
            return data_json.get("data") or []
        else:
            msg = data_json.get('message', '')
            if msg:  # 只有有错误信息才打印
                print(f"      ⚠ API错误 [{stock_code}]: {msg}")
            return []
    except Exception as e:
        print(f"      ⚠ 异常 [{stock_code}]: {e}")
        return []


# ─── 数据解析与入库 ─────────────────────────────────────────────────────────────

def parse_record(raw: dict, stock_code: str) -> dict:
    """将 API 返回的单条 K 线记录解析为数据库行"""
    raw_date = raw.get("date", "")
    # 将 "2026-03-26T00:00:00+08:00" 截取为 "2026-03-26"
    if "T" in raw_date:
        raw_date = raw_date[:10]
    return {
        "stock_code": stock_code,
        "date":       raw_date,
        "open":       raw.get("open"),
        "close":      raw.get("close"),
        "high":       raw.get("high"),
        "low":        raw.get("low"),
        "volume":     raw.get("volume"),
        "amount":     raw.get("amount"),
        "change_pct": raw.get("change"),
        "to_r":       raw.get("to_r"),
    }


def upsert_klines(conn: sqlite3.Connection, records: list[dict]):
    """批量写入 K 线，主键冲突时更新"""
    sql = """
        INSERT INTO daily_kline
            (stock_code, date, open, close, high, low, volume, amount, change_pct, to_r)
        VALUES
            (:stock_code, :date, :open, :close, :high, :low, :volume, :amount, :change_pct, :to_r)
        ON CONFLICT(stock_code, date) DO UPDATE SET
            open       = excluded.open,
            close      = excluded.close,
            high       = excluded.high,
            low        = excluded.low,
            volume     = excluded.volume,
            amount     = excluded.amount,
            change_pct = excluded.change_pct,
            to_r       = excluded.to_r
    """
    conn.executemany(sql, records)
    conn.commit()


# ─── 主流程 ─────────────────────────────────────────────────────────────────────

def main():
    if not TOKEN:
        print("❌ 错误：未找到环境变量 LIXINGER_TOKEN，请先执行：")
        print("   export LIXINGER_TOKEN='你的Token'")
        return

    print("=" * 60)
    print("📈 A股K线数据批量下载工具 (理杏仁 API)")
    print("=" * 60)

    # 连接数据库
    conn = sqlite3.connect(DB_PATH)
    init_kline_table(conn)

    # 读取股票列表
    stocks = get_stock_list(conn)
    if not stocks:
        print("❌ stocks 表为空，请先运行 fetch_stocks.py")
        conn.close()
        return

    total = len(stocks)
    today_str = date.today().strftime("%Y-%m-%d")
    print(f"\n  股票总数（正常上市）: {total}")
    print(f"  下载区间: {START_DATE} ~ {today_str}")
    print(f"  复权类型: {KLINE_TYPE} (前复权)\n")

    # 统计
    skipped      = 0   # 已是最新，无需下载
    downloaded   = 0   # 本次实际下载的股票数
    total_rows   = 0   # 本次写入总行数
    errors       = 0

    start_time = time.time()

    for i, stock in enumerate(stocks, 1):
        code = stock["stock_code"]
        name = stock["name"]

        # 判断增量：找到数据库中该股票的最新日期
        last_date = get_last_date(conn, code)

        if last_date is not None:
            # 从最新日期的下一天开始拉取
            next_day = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            if next_day > today_str:
                # 已是最新
                skipped += 1
                if i % 100 == 0:
                    _print_progress(i, total, skipped, downloaded, total_rows, start_time)
                continue
            fetch_start = next_day
            mode_label = f"增量({last_date}→)"
        else:
            fetch_start = START_DATE
            mode_label = "全量"

        print(f"  [{i:4d}/{total}] {code} {name:12s}  {mode_label}", end=" ... ")

        time.sleep(API_INTERVAL)
        raw_data = fetch_kline(code, fetch_start, today_str)

        if not raw_data:
            print("无数据")
            errors += 1
            continue

        records = [parse_record(r, code) for r in raw_data]
        upsert_klines(conn, records)

        downloaded += 1
        total_rows += len(records)
        print(f"✓ {len(records)} 条")

        # 每100只股票打印一次进度
        if i % 100 == 0:
            _print_progress(i, total, skipped, downloaded, total_rows, start_time)

    # 完成
    conn.close()
    elapsed = time.time() - start_time

    print("\n" + "=" * 60)
    print("✅ 下载完成！")
    print(f"   处理股票: {total} 只")
    print(f"   跳过(已最新): {skipped} 只")
    print(f"   本次下载: {downloaded} 只")
    print(f"   写入行数: {total_rows:,}")
    print(f"   异常/无数据: {errors} 只")
    print(f"   总耗时: {elapsed/60:.1f} 分钟")
    print(f"   数据库: {DB_PATH}")
    print("=" * 60)


def _print_progress(i, total, skipped, downloaded, total_rows, start_time):
    elapsed = time.time() - start_time
    speed = i / elapsed if elapsed > 0 else 0
    remaining = (total - i) / speed if speed > 0 else 0
    print(f"\n  ── 进度 {i}/{total} ({i/total*100:.1f}%) | "
          f"已下载:{downloaded} 跳过:{skipped} "
          f"行数:{total_rows:,} | "
          f"预计剩余:{remaining/60:.1f}分钟 ──\n")


if __name__ == "__main__":
    main()
