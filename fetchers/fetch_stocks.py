"""
fetch_stocks.py
---------------
从理杏仁 API 获取所有 A 股股票的基础信息，并存入 SQLite 数据库。

使用方式:
    export LIXINGER_TOKEN="你的Token"
    python fetch_stocks.py

输出:
    stock_data.db  (SQLite数据库，表名: stocks)
"""

import os
import time
import sqlite3
import requests

# ─── 配置 ──────────────────────────────────────────────────────────────────────
TOKEN = os.getenv("LIXINGER_TOKEN")
API_URL = "https://open.lixinger.com/api/cn/company"
DB_PATH = os.getenv("DB_PATH", "stock_data.db")

# 每页返回约500条，理杏仁目前约有5600+只股票，12页足够
# 但我们做自动终止：当某页 data 为空时停止
MAX_PAGES = 20
API_INTERVAL = 0.1  # 秒，每秒约14.2次，每分钟约850次（微调避开1000次限额）

# ─── 数据库初始化 ───────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    """建表（如果不存在）"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            stock_code            TEXT PRIMARY KEY,
            name                  TEXT,
            exchange              TEXT,
            market                TEXT,
            area_code             TEXT,
            fs_table_type         TEXT,
            listing_status        TEXT,
            ipo_date              TEXT,
            delisted_date         TEXT,
            mutual_markets        TEXT,   -- JSON数组转为逗号分隔字符串
            mutual_market_flag    INTEGER,
            margin_flag           INTEGER,
            updated_at            TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_stocks_exchange
        ON stocks (exchange)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_stocks_listing_status
        ON stocks (listing_status)
    """)
    conn.commit()
    print("[DB] 数据库表初始化完成。")


# ─── API 调用 ───────────────────────────────────────────────────────────────────

def fetch_page(page_index: int) -> dict:
    """获取单页股票数据，返回原始响应 JSON"""
    payload = {
        "token": TOKEN,
        "pageIndex": page_index,
    }
    
    max_retries = 3
    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(API_URL, json=payload, timeout=20)
            if resp.status_code == 429:
                if attempt < max_retries:
                    print(f"\n      ⚠ HTTP 429: 请求过快。等待 2s 后进行第 {attempt+1} 次重试...")
                    time.sleep(2)
                    continue
                else:
                    print("\n      ⚠ HTTP 429: 已达到最大重试次数，放弃本次请求。")
            
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                # 针对非 429 的连接错误也可以尝试重试一次
                time.sleep(1)
                continue
            raise e


def fetch_all_stocks() -> list[dict]:
    """自动分页，获取所有股票数据"""
    all_stocks = []

    for page in range(MAX_PAGES):
        print(f"  正在获取第 {page} 页...", end=" ", flush=True)
        try:
            data_json = fetch_page(page)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"\n      ⚠ HTTP 429: {e.response.text[:100]}")
            else:
                print(f"请求失败: {e}")
            break
        except Exception as e:
            print(f"请求发生异常: {e}")
            break

        if data_json.get("code") != 1:
            print(f"API错误: {data_json.get('message')}")
            break

        page_data = data_json.get("data", [])
        if not page_data:
            print("已到达最后一页，停止。")
            break

        all_stocks.extend(page_data)
        total = data_json.get("total", "?")
        print(f"获取 {len(page_data)} 条，累计 {len(all_stocks)}/{total}")

        # 如果已经取完了所有数据就提前退出
        if isinstance(total, int) and len(all_stocks) >= total:
            print("  已获取全部数据，提前结束分页。")
            break

        time.sleep(API_INTERVAL)

    return all_stocks


# ─── 数据处理与入库 ─────────────────────────────────────────────────────────────

def parse_stock(raw: dict) -> dict:
    """将API原始字段映射到数据库字段"""
    mutual = raw.get("mutualMarkets") or []
    mutual_str = ",".join(mutual) if isinstance(mutual, list) else ""

    return {
        "stock_code":         raw.get("stockCode", ""),
        "name":               raw.get("name", ""),
        "exchange":           raw.get("exchange", ""),
        "market":             raw.get("market", ""),
        "area_code":          raw.get("areaCode", ""),
        "fs_table_type":      raw.get("fsTableType", ""),
        "listing_status":     raw.get("listingStatus", ""),
        "ipo_date":           raw.get("ipoDate", ""),
        "delisted_date":      raw.get("delistedDate", ""),
        "mutual_markets":     mutual_str,
        "mutual_market_flag": int(raw.get("mutualMarketFlag", False)),
        "margin_flag":        int(raw.get("marginTradingAndSecuritiesLendingFlag", False)),
    }


def upsert_stocks(conn: sqlite3.Connection, stocks: list[dict]):
    """批量插入或更新股票信息"""
    sql = """
        INSERT INTO stocks (
            stock_code, name, exchange, market, area_code,
            fs_table_type, listing_status, ipo_date, delisted_date,
            mutual_markets, mutual_market_flag, margin_flag
        ) VALUES (
            :stock_code, :name, :exchange, :market, :area_code,
            :fs_table_type, :listing_status, :ipo_date, :delisted_date,
            :mutual_markets, :mutual_market_flag, :margin_flag
        )
        ON CONFLICT(stock_code) DO UPDATE SET
            name               = excluded.name,
            exchange           = excluded.exchange,
            market             = excluded.market,
            area_code          = excluded.area_code,
            fs_table_type      = excluded.fs_table_type,
            listing_status     = excluded.listing_status,
            ipo_date           = excluded.ipo_date,
            delisted_date      = excluded.delisted_date,
            mutual_markets     = excluded.mutual_markets,
            mutual_market_flag = excluded.mutual_market_flag,
            margin_flag        = excluded.margin_flag,
            updated_at         = datetime('now', 'localtime')
    """
    parsed = [parse_stock(s) for s in stocks]
    # 从根本上过滤北交所股票（82、83、87、92 开头）
    parsed = [p for p in parsed if not p["stock_code"].startswith(("82", "83", "87", "92"))]
    
    conn.executemany(sql, parsed)
    conn.commit()
    print(f"[DB] 已写入/更新 {len(parsed)} 条股票记录。")


# ─── 统计信息输出 ───────────────────────────────────────────────────────────────

def print_summary(conn: sqlite3.Connection):
    """打印数据库中的统计摘要"""
    print("\n" + "=" * 50)
    print("📊 数据库统计摘要")
    print("=" * 50)

    total = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    print(f"  总股票数:        {total}")

    # 按上市状态分布
    print("\n  📌 上市状态分布:")
    for row in conn.execute(
        "SELECT listing_status, COUNT(*) AS cnt FROM stocks GROUP BY listing_status ORDER BY cnt DESC"
    ):
        print(f"    {row[0]:40s}  {row[1]}")

    # 按交易所分布
    print("\n  🏦 交易所分布:")
    for row in conn.execute(
        "SELECT exchange, COUNT(*) AS cnt FROM stocks GROUP BY exchange ORDER BY cnt DESC"
    ):
        print(f"    {row[0]:10s}  {row[1]}")

    # 陆股通
    lgt = conn.execute(
        "SELECT COUNT(*) FROM stocks WHERE mutual_market_flag = 1"
    ).fetchone()[0]
    print(f"\n  🔗 陆股通标的:    {lgt}")

    # 融资融券
    margin = conn.execute(
        "SELECT COUNT(*) FROM stocks WHERE margin_flag = 1"
    ).fetchone()[0]
    print(f"  💹 融资融券标的:  {margin}")
    print("=" * 50)


# ─── 主入口 ─────────────────────────────────────────────────────────────────────

def main():
    if not TOKEN:
        print("❌ 错误：未找到环境变量 LIXINGER_TOKEN，请先执行：")
        print("   export LIXINGER_TOKEN='你的Token'")
        return

    print("=" * 50)
    print("🚀 股票基础信息同步工具 (理杏仁 API)")
    print("=" * 50)

    # 1. 初始化数据库
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # 2. 分页获取所有股票
    print("\n[API] 开始获取股票数据...")
    start = time.time()
    raw_stocks = fetch_all_stocks()
    elapsed = time.time() - start
    print(f"\n[API] 获取完成，共 {len(raw_stocks)} 条，耗时 {elapsed:.1f} 秒")

    if not raw_stocks:
        print("❌ 未获取到任何数据，请检查 Token 和网络。")
        conn.close()
        return

    # 3. 入库
    print("\n[DB] 写入数据库...")
    upsert_stocks(conn, raw_stocks)

    # 4. 统计摘要
    print_summary(conn)
    conn.close()

    print(f"\n✅ 完成！数据已保存到: {DB_PATH}")


if __name__ == "__main__":
    main()
