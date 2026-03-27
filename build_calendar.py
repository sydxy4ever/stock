"""
build_calendar.py
-----------------
从本地 daily_kline 表中提取所有交易日（以600519茅台为基准），
在 stock_data.db 中建立 trade_calendar 表，并导出 trade_calendar.csv。

如果茅台数据尚未入库（fetch_klines.py 未运行），则调用 API 获取。

使用方式:
    python build_calendar.py
"""

import os
import sqlite3
import requests
import pandas as pd
from datetime import date

DB_PATH       = "stock_data.db"
CALENDAR_CSV  = "trade_calendar.csv"
REFERENCE_CODE = "600519"     # 贵州茅台，从不停牌，是最稳定的日历基准
TOKEN          = os.getenv("LIXINGER_TOKEN")
START_DATE     = "2020-01-01"

# ─── 数据库 ──────────────────────────────────────────────────────────────────────

def init_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_calendar (
            date TEXT PRIMARY KEY
        )
    """)
    conn.commit()


def get_dates_from_db(conn: sqlite3.Connection) -> list[str]:
    """从 daily_kline 读取茅台的所有日期"""
    rows = conn.execute("""
        SELECT DISTINCT date FROM daily_kline
        WHERE stock_code = ?
          AND date >= ?
        ORDER BY date
    """, (REFERENCE_CODE, START_DATE)).fetchall()
    return [r[0] for r in rows]


def get_dates_from_api() -> list[str]:
    """调用K线接口获取茅台历史日期作为备选"""
    if not TOKEN:
        print("❌ 未设置 LIXINGER_TOKEN，无法调用API")
        return []
    print(f"  daily_kline 中未找到 {REFERENCE_CODE} 数据，调用API获取...")
    today = date.today().strftime("%Y-%m-%d")
    payload = {
        "token":     TOKEN,
        "stockCode": REFERENCE_CODE,
        "type":      "lxr_fc_rights",
        "startDate": START_DATE,
        "endDate":   today,
    }
    try:
        resp = requests.post(
            "https://open.lixinger.com/api/cn/company/candlestick",
            json=payload, timeout=20
        )
        data_json = resp.json()
        if data_json.get("code") == 1:
            dates = sorted({
                r["date"][:10] for r in data_json["data"] if "date" in r
            })
            return dates
        else:
            print(f"  API错误: {data_json.get('message')}")
            return []
    except Exception as e:
        print(f"  API异常: {e}")
        return []


# ─── 主流程 ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("📅 交易日历生成工具")
    print("=" * 50)

    conn = sqlite3.connect(DB_PATH)
    init_table(conn)

    # 1. 优先从本地数据库取
    print(f"\n  从 daily_kline 读取 {REFERENCE_CODE} 的日期...")
    dates = get_dates_from_db(conn)

    # 2. 本地没有则调 API
    if not dates:
        dates = get_dates_from_api()

    if not dates:
        print("❌ 无法获取交易日历")
        conn.close()
        return

    # 3. 写入数据库
    conn.executemany(
        "INSERT OR IGNORE INTO trade_calendar (date) VALUES (?)",
        [(d,) for d in dates]
    )
    conn.commit()

    # 4. 导出 CSV
    pd.DataFrame({"date": dates}).to_csv(CALENDAR_CSV, index=False)

    conn.close()

    print(f"\n✅ 完成！")
    print(f"   交易日总数: {len(dates)}")
    print(f"   范围: {dates[0]} → {dates[-1]}")
    print(f"   数据库表: trade_calendar")
    print(f"   CSV文件:  {CALENDAR_CSV}")

if __name__ == "__main__":
    main()
