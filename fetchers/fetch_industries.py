"""
fetch_industries.py
-------------------
从理杏仁 API 获取所有股票的行业分类信息（申万/申万2021/国证），
存入 stock_data.db 的 stock_industries 表。

注意：行业接口每次只能查一只股票，约5000只股票耗时20分钟左右。
断点续传：已有数据的股票直接跳过。

使用方式:
    export LIXINGER_TOKEN="你的Token"
    python fetch_industries.py
"""

import os
import time
import sqlite3
import requests

TOKEN        = os.getenv("LIXINGER_TOKEN")
DB_PATH      = os.getenv("DB_PATH", "stock_data.db")
API_URL      = "https://open.lixinger.com/api/cn/company/industries"
API_INTERVAL = 0.1 # 每分钟约 900 次 (略低于限额，更稳定)

# ─── 数据库 ──────────────────────────────────────────────────────────────────────

def init_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_industries (
            stock_code      TEXT    NOT NULL,
            industry_code   TEXT    NOT NULL,
            name            TEXT,
            source          TEXT,
            area_code       TEXT,
            updated_at      TEXT DEFAULT (datetime('now', 'localtime')),
            PRIMARY KEY (stock_code, industry_code, source)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ind_source ON stock_industries (source)")
    conn.commit()


def get_all_codes(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("""
        SELECT stock_code FROM stocks
        WHERE listing_status = 'normally_listed'
        ORDER BY stock_code
    """).fetchall()
    return [r[0] for r in rows]


def get_done_codes(conn: sqlite3.Connection) -> set[str]:
    """已有行业数据的股票（跳过）"""
    rows = conn.execute(
        "SELECT DISTINCT stock_code FROM stock_industries"
    ).fetchall()
    return {r[0] for r in rows}


# ─── API 调用 ───────────────────────────────────────────────────────────────────

def fetch_industries(stock_code: str) -> list[dict]:
    payload = {"token": TOKEN, "stockCode": stock_code}
    max_retries = 3
    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(API_URL, json=payload, timeout=15)
            if resp.status_code == 429:
                if attempt < max_retries:
                    print(f"      ⚠ HTTP 429: 请求过快。等待 2s 后进行第 {attempt+1} 次重试...")
                    time.sleep(2)
                    continue
                else:
                    print("      ⚠ HTTP 429: 已达到最大重试次数，放弃本次请求。")
                    return []
            
            if resp.status_code != 200:
                print(f"      ⚠ HTTP {resp.status_code}: {resp.text[:100]}")
                return []
                
            data_json = resp.json()
            return data_json.get("data") or [] if data_json.get("code") == 1 else []
        except Exception as e:
            if attempt < max_retries:
                time.sleep(1)
                continue
            return []


# ─── 数据入库 ───────────────────────────────────────────────────────────────────

def upsert_industries(conn: sqlite3.Connection, stock_code: str, data: list[dict]):
    sql = """
        INSERT INTO stock_industries (stock_code, industry_code, name, source, area_code)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, industry_code, source) DO UPDATE SET
            name       = excluded.name,
            area_code  = excluded.area_code,
            updated_at = datetime('now', 'localtime')
    """
    records = [
        (stock_code,
         row.get("stockCode", ""),
         row.get("name", ""),
         row.get("source", ""),
         row.get("areaCode", ""))
        for row in data
        if row.get("stockCode")
    ]
    conn.executemany(sql, records)
    conn.commit()


# ─── 主流程 ─────────────────────────────────────────────────────────────────────

def main():
    if not TOKEN:
        print("❌ 未找到环境变量 LIXINGER_TOKEN")
        return

    print("=" * 55)
    print("🏭 股票行业分类抓取工具 (理杏仁 API)")
    print("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    init_table(conn)

    all_codes = get_all_codes(conn)
    done_codes = get_done_codes(conn)
    remaining  = [c for c in all_codes if c not in done_codes]
    skipped    = len(all_codes) - len(remaining)

    print(f"\n  股票总数: {len(all_codes)}")
    if skipped:
        print(f"  断点续传：已完成 {skipped} 只，剩余 {len(remaining)} 只")
    total = len(remaining)
    print(f"  预计耗时: {total * API_INTERVAL / 60:.1f} 分钟\n")

    total_written = 0
    errors        = 0
    start_time    = time.time()

    for i, code in enumerate(remaining, 1):
        time.sleep(API_INTERVAL)
        data = fetch_industries(code)

        if data:
            upsert_industries(conn, code, data)
            total_written += len(data)
            # 每20只打印一次进度
            if i % 20 == 0:
                elapsed = time.time() - start_time
                eta = elapsed / i * (total - i)
                print(f"  [{i:4d}/{total}] 已写入 {total_written} 条 | 剩余 {eta/60:.1f} 分钟")
        else:
            errors += 1
            if i <= 5 or i % 500 == 0:
                print(f"  [{i:4d}/{total}] {code} 无数据")

    conn.close()
    elapsed = time.time() - start_time

    print("\n" + "=" * 55)
    print("✅ 完成！")
    print(f"   处理股票: {total}")
    print(f"   写入记录: {total_written}")
    print(f"   无数据:   {errors}")
    print(f"   总耗时:   {elapsed/60:.1f} 分钟")
    print(f"   数据库:   {DB_PATH}")
    print("=" * 55)


if __name__ == "__main__":
    main()
