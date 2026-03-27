"""
compute_ma.py
-------------
从本地 daily_kline 表的收盘价，计算每只股票每日的均线：
    ma5, ma20, ma60, ma200

结果写入 moving_averages 表，主键 (stock_code, date)，支持增量更新。

使用方式:
    python compute_ma.py
"""

import sqlite3
import pandas as pd
import time

DB_PATH    = "stock_data.db"
BATCH_SIZE = 300   # 每批处理的股票数，控制内存

WINDOWS = {"ma5": 5, "ma20": 20, "ma60": 60, "ma200": 200}

# ─── 数据库 ──────────────────────────────────────────────────────────────────────

def init_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS moving_averages (
            stock_code  TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            ma5         REAL,
            ma20        REAL,
            ma60        REAL,
            ma200       REAL,
            PRIMARY KEY (stock_code, date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ma_date ON moving_averages (date)")
    conn.commit()


def get_all_codes(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT stock_code FROM daily_kline ORDER BY stock_code"
    ).fetchall()
    return [r[0] for r in rows]


def get_last_date(conn: sqlite3.Connection, code: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(date) FROM moving_averages WHERE stock_code = ?", (code,)
    ).fetchone()
    return row[0] if row and row[0] else None


def load_close_prices(conn: sqlite3.Connection, codes: list[str]) -> pd.DataFrame:
    placeholders = ",".join("?" * len(codes))
    df = pd.read_sql_query(
        f"""
        SELECT stock_code, date, close
        FROM daily_kline
        WHERE stock_code IN ({placeholders})
          AND close IS NOT NULL
        ORDER BY stock_code, date
        """,
        conn,
        params=codes,
    )
    df["date"] = pd.to_datetime(df["date"])
    return df


def compute_ma(df: pd.DataFrame) -> pd.DataFrame:
    """按股票分组，计算滚动均线（min_periods=1，开始阶段用有效数据均值）"""
    results = []
    for code, grp in df.groupby("stock_code", sort=False):
        grp = grp.sort_values("date").reset_index(drop=True)
        out = pd.DataFrame({
            "stock_code": grp["stock_code"],
            "date":       grp["date"].dt.strftime("%Y-%m-%d"),
        })
        for col, w in WINDOWS.items():
            out[col] = grp["close"].rolling(w, min_periods=w).mean().round(4)
        results.append(out)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def upsert_df(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    sql = """
        INSERT INTO moving_averages (stock_code, date, ma5, ma20, ma60, ma200)
        VALUES (:stock_code, :date, :ma5, :ma20, :ma60, :ma200)
        ON CONFLICT(stock_code, date) DO UPDATE SET
            ma5   = excluded.ma5,
            ma20  = excluded.ma20,
            ma60  = excluded.ma60,
            ma200 = excluded.ma200
    """
    records = df.where(df.notna(), None).to_dict(orient="records")
    conn.executemany(sql, records)
    conn.commit()
    return len(records)


# ─── 主流程 ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("📈 均线计算工具 (MA5 / MA20 / MA60 / MA200)")
    print("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    init_table(conn)

    codes = get_all_codes(conn)
    if not codes:
        print("❌ daily_kline 表为空，请先运行 fetch_klines.py")
        conn.close()
        return

    # 断点续传：跳过已全量计算的股票（moving_averages 中有记录的）
    done = {
        r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM moving_averages"
        ).fetchall()
    }
    remaining = [c for c in codes if c not in done]
    skipped = len(codes) - len(remaining)

    print(f"\n  K线中共 {len(codes)} 只股票")
    if skipped:
        print(f"  断点续传：已完成 {skipped} 只，剩余 {len(remaining)} 只")
    else:
        print(f"  全部重新计算")

    total_written = 0
    start_time = time.time()
    total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE

    for bi in range(total_batches):
        batch = remaining[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]

        df_raw    = load_close_prices(conn, batch)
        df_result = compute_ma(df_raw)
        written   = upsert_df(conn, df_result)
        total_written += written

        elapsed = time.time() - start_time
        done_batches = bi + 1
        eta = elapsed / done_batches * (total_batches - done_batches) if done_batches < total_batches else 0
        print(f"  批次 [{bi+1:3d}/{total_batches}]  {len(batch)} 只  写入 {written:,} 条  "
              f"| 累计 {total_written:,} | 剩余 {eta:.0f}s")

    conn.close()
    elapsed = time.time() - start_time

    print("\n" + "=" * 55)
    print("✅ 完成！")
    print(f"   写入记录: {total_written:,}")
    print(f"   总耗时:   {elapsed:.1f} 秒")
    print(f"   数据库:   {DB_PATH}")
    print("=" * 55)


if __name__ == "__main__":
    main()
