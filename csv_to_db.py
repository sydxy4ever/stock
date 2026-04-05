"""
csv_to_db.py
------------
将 output/ 目录下所有 turnover_surge_*.csv 批量导入 SQLite 数据库。
已存在的记录（按 day1 + stock_code + day_offset 去重）会被跳过。

用法：
    python csv_to_db.py                         # 默认读 output/，写 turnover_surge.db
    python csv_to_db.py --csv-dir output_v2 --db my.db
"""

import os
import glob
import argparse
import sqlite3
import pandas as pd

OUTPUT_DIR = "output"
DB_PATH    = "turnover_surge.db"

CREATE_TABLE_SQL = """
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

CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_day1        ON turnover_surge(day1);",
    "CREATE INDEX IF NOT EXISTS idx_stock_code  ON turnover_surge(stock_code);",
    "CREATE INDEX IF NOT EXISTS idx_industry    ON turnover_surge(industry_code);",
]


def init_db(conn: sqlite3.Connection):
    conn.execute(CREATE_TABLE_SQL)
    for idx_sql in CREATE_INDEX_SQL:
        conn.execute(idx_sql)
    conn.commit()


def import_csv(conn: sqlite3.Connection, csv_path: str) -> tuple[int, int]:
    """
    将单个 CSV 导入数据库。返回 (inserted, skipped)。
    """
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()

    # 确保必要列存在
    if "day1" not in df.columns or "stock_code" not in df.columns:
        print(f"  ⚠ 跳过（列名不符）: {csv_path}")
        return 0, 0

    # 补充缺失列为 NULL
    expected_cols = [
        "day1","industry_code","industry_name","stock_code","name","mc_rank",
        "baseline_to_r","recent_to_r","trigger_ratio",
        "d1_change_pct","d1_close","d1_ma5","d1_ma20","d1_ma60","d1_ma200",
        "d1_vs_ma5","d1_vs_ma20","d1_vs_ma60","d1_above_ma20","d1_above_ma60",
        "day_offset","date","close","to_r","to_r_ratio","change_pct",
        "ma5","ma20","ma60","ma200","vs_ma5","vs_ma20","vs_ma60",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df = df[expected_cols]

    inserted = 0
    skipped  = 0
    for _, row in df.iterrows():
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO turnover_surge
                (day1,industry_code,industry_name,stock_code,name,mc_rank,
                 baseline_to_r,recent_to_r,trigger_ratio,
                 d1_change_pct,d1_close,d1_ma5,d1_ma20,d1_ma60,d1_ma200,
                 d1_vs_ma5,d1_vs_ma20,d1_vs_ma60,d1_above_ma20,d1_above_ma60,
                 day_offset,date,close,to_r,to_r_ratio,change_pct,
                 ma5,ma20,ma60,ma200,vs_ma5,vs_ma20,vs_ma60)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                tuple(None if pd.isna(v) else v for v in row)
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  ⚠ 行写入失败: {e}")
            skipped += 1

    conn.commit()
    return inserted, skipped


def main():
    parser = argparse.ArgumentParser(description="将 CSV 批量导入 turnover_surge SQLite 数据库")
    parser.add_argument("--csv-dir", default=OUTPUT_DIR, help="CSV 目录")
    parser.add_argument("--db",      default=DB_PATH,    help="SQLite 数据库路径")
    args = parser.parse_args()

    pattern = os.path.join(args.csv_dir, "turnover_surge_*.csv")
    csv_files = sorted(glob.glob(pattern))

    if not csv_files:
        print(f"❌ 未找到任何 CSV 文件：{pattern}")
        return

    print(f"📂 找到 {len(csv_files)} 个 CSV 文件，目标数据库：{args.db}\n")

    conn = sqlite3.connect(args.db)
    init_db(conn)

    total_inserted = 0
    total_skipped  = 0

    for i, csv_path in enumerate(csv_files, 1):
        fname = os.path.basename(csv_path)
        inserted, skipped = import_csv(conn, csv_path)
        total_inserted += inserted
        total_skipped  += skipped
        print(f"  [{i:4d}/{len(csv_files)}] {fname}  →  插入 {inserted} 条，跳过 {skipped} 条")

    conn.close()

    print(f"\n{'='*55}")
    print(f"✅ 导入完成！")
    print(f"   总插入：{total_inserted} 条")
    print(f"   总跳过：{total_skipped} 条（已存在）")
    print(f"   数据库：{args.db}")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
