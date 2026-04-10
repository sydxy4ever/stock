import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

_ROOT = Path(__file__).parent.resolve()
SURGE_DB = str(_ROOT / "turnover_surge.db")
STOCK_DB = str(_ROOT / "stock_data.db")

def main():
    print("🚀 开始进行 D-5 到 D0 数据打补丁...")
    
    # 1. 连接数据库
    surge_conn = sqlite3.connect(SURGE_DB)
    stock_conn = sqlite3.connect(STOCK_DB)

    # 2. 获取现有的 turnover_surge 表
    df = pd.read_sql("SELECT * FROM turnover_surge", surge_conn)
    if df.empty:
        print("表是空的，无需打补丁。")
        return

    # 3. 获取日历
    calendar_df = pd.read_sql("SELECT DISTINCT date FROM daily_kline ORDER BY date", stock_conn)
    calendar = calendar_df["date"].tolist()
    
    date_to_idx = {d: i for i, d in enumerate(calendar)}
    
    # 构建所有需要的查询参数 (stock_code, date, offset)
    print(f"📊 正在为 {len(df)} 条数据推算所需的历史日期 (D-6 到 D0)...")
    
    # 找出每个 day0 对应的 D-6 到 D0
    day0_dates = df["day0"].unique()
    
    day_map = {} # day0_date -> {offset: actual_date}
    for d0 in day0_dates:
        if d0 in date_to_idx:
            idx = date_to_idx[d0]
            day_map[d0] = {offset: calendar[idx + offset] if idx + offset >= 0 else None for offset in range(-6, 1)}
    
    # 准备要拉取的所有 (stock_code, date) 组合以防内存爆炸，但因为我们只要6天，数据量大概 50000 * 7 = 35万行，直接用 SQL 批量拉取会很快。
    print("📥 拉取 D-6 到 D0 的K线与均线...")
    stocks = df["stock_code"].unique().tolist()
    
    # 直接拉取这些股票的所有 K线 和 MA （只拉取日期在相关范围内的）
    all_needed_dates = set()
    for d0, mapping in day_map.items():
        for off, d in mapping.items():
            if d: all_needed_dates.add(d)
            
    needed_dates_list = list(all_needed_dates)
    
    # 分批拉取 daily_kline, moving_averages
    kline_parts = []
    chunk = 200
    for i in range(0, len(needed_dates_list), chunk):
        d_chunk = needed_dates_list[i:i+chunk]
        ph = ",".join([f"'{x}'" for x in d_chunk])
        part = pd.read_sql(f"""
            SELECT stock_code, date, close, change_pct 
            FROM daily_kline 
            WHERE date IN ({ph})
        """, stock_conn)
        kline_parts.append(part)
    kline_df = pd.concat(kline_parts, ignore_index=True)
    
    ma_parts = []
    for i in range(0, len(needed_dates_list), chunk):
        d_chunk = needed_dates_list[i:i+chunk]
        ph = ",".join([f"'{x}'" for x in d_chunk])
        part = pd.read_sql(f"""
            SELECT stock_code, date, ma20, ma60, ma200 
            FROM moving_averages 
            WHERE date IN ({ph})
        """, stock_conn)
        ma_parts.append(part)
    ma_df = pd.concat(ma_parts, ignore_index=True)
    
    # 合并 K 线和均线
    kma_df = kline_df.merge(ma_df, on=["stock_code", "date"], how="left")
    
    # 建个快速查询字典（或使用 merge）
    print("⚙️ 开始计算穿线与涨停指标...")
    
    # 计算所有行的 is_limit_up
    is_kc_cy = kma_df["stock_code"].astype(str).str.startswith(("688", "300"))
    pct = pd.to_numeric(kma_df["change_pct"])
    kma_df["is_limit_up"] = ((~is_kc_cy & (pct >= 0.098)) | (is_kc_cy & (pct >= 0.198))).astype(int)
    
    # 计算在均线上的 boolean
    c = pd.to_numeric(kma_df["close"])
    m20 = pd.to_numeric(kma_df["ma20"])
    m60 = pd.to_numeric(kma_df["ma60"])
    m200 = pd.to_numeric(kma_df["ma200"])
    kma_df["above_20"] = ((c > m20) & m20.notna()).astype(int)
    kma_df["above_60"] = ((c > m60) & m60.notna()).astype(int)
    kma_df["above_200"] = ((c > m200) & m200.notna()).astype(int)
    
    kma_grouped = kma_df.set_index(["stock_code", "date"])
    
    # 逐个生成新列
    new_cols_data = {
        "stock_code": df["stock_code"],
        "day0": df["day0"],
    }
    
    for offset in range(-5, 1):
        prefix = f"dm{abs(offset)}" if offset < 0 else "d0"
        
        # 准备这 5 万条数据对应的 当前日 及 前一日 的 key
        cur_dates = df["day0"].map(lambda x: day_map.get(x, {}).get(offset))
        prev_dates = df["day0"].map(lambda x: day_map.get(x, {}).get(offset - 1))
        
        cur_keys = pd.MultiIndex.from_arrays([df["stock_code"], cur_dates])
        prev_keys = pd.MultiIndex.from_arrays([df["stock_code"], prev_dates])
        
        # 找数据
        cur_data = kma_grouped.reindex(cur_keys)
        prev_data = kma_grouped.reindex(prev_keys)
        
        # 赋值
        new_cols_data[f"{prefix}_change_pct"] = cur_data["change_pct"].values
        new_cols_data[f"{prefix}_is_limit_up"] = cur_data["is_limit_up"].values
        
        new_cols_data[f"{prefix}_above_ma20"] = cur_data["above_20"].values
        new_cols_data[f"{prefix}_above_ma60"] = cur_data["above_60"].values
        new_cols_data[f"{prefix}_above_ma200"] = cur_data["above_200"].values
        
        # 计算上穿
        new_cols_data[f"{prefix}_pierce_ma20"] = ((cur_data["above_20"].values == 1) & (prev_data["above_20"].values == 0)).astype(int)
        new_cols_data[f"{prefix}_pierce_ma60"] = ((cur_data["above_60"].values == 1) & (prev_data["above_60"].values == 0)).astype(int)
        new_cols_data[f"{prefix}_pierce_ma200"] = ((cur_data["above_200"].values == 1) & (prev_data["above_200"].values == 0)).astype(int)
        
    patch_df = pd.DataFrame(new_cols_data)
    
    # 移除原表中与 prefix 冲突的旧列
    drop_prefixes = ["dm1_", "dm2_", "dm3_", "dm4_", "dm5_", "d0_change_pct", "d0_is_limit_up", "d0_above_ma", "d0_pierce_ma"]
    cols_to_drop = [c for c in df.columns if any(c.startswith(p) for p in drop_prefixes)]
    # 特别保留 d0_close 等
    preserve = ["d0_close", "d0_vs_ma5", "d0_vs_ma20", "d0_vs_ma60", "d0_ma5", "d0_ma20", "d0_ma60", "d0_ma200"]
    cols_to_drop = [c for c in cols_to_drop if c not in preserve]
    
    df = df.drop(columns=cols_to_drop, errors="ignore")
    
    # 合并 patch_df
    patched = df.merge(patch_df, on=["stock_code", "day0"], how="left")
    
    print(f"📦 正在回写 {len(patched)} 条记录，这是全量新字段列表：")
    for offset in range(-5, 1):
        prefix = f"dm{abs(offset)}" if offset < 0 else "d0"
        print(f"   [{prefix}] change_pct, is_limit_up, above_20/60/200, pierce_20/60/200")
        
    patched.to_sql("turnover_surge_new", surge_conn, if_exists="replace", index=False)
    
    # 删旧表，重命名新表
    surge_conn.execute("DROP TABLE turnover_surge")
    surge_conn.execute("ALTER TABLE turnover_surge_new RENAME TO turnover_surge")
    surge_conn.execute("CREATE INDEX idx_ts_day0 ON turnover_surge(day0)")
    surge_conn.commit()
    
    surge_conn.close()
    stock_conn.close()
    
    print("\n✅ 数据补丁已打完，turnover_surge.db 现在拥有 D-5 乃至 D0 的完整多维指标。")

if __name__ == "__main__":
    main()
