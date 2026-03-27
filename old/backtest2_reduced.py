import sqlite3
import pandas as pd
import os
import time

# --- 配置 ---
DB_PATH = "./tools/stock_market_data.db"
CALENDAR_FILE = "./tools/trade_calendar.csv"
OUTPUT_FILE = "./output/backtest_reduced_results.csv"

def load_trade_calendar():
    """加载交易日历，返回日期列表"""
    df = pd.read_csv(CALENDAR_FILE)
    trade_dates = pd.to_datetime(df['date']).dt.date.tolist()
    return trade_dates

def load_stock_data(start_date='2022-01-01', end_date='2026-01-01'):
    """加载指定日期范围内的股票数据"""
    conn = sqlite3.connect(DB_PATH)
    query = f"""
    SELECT code, date, close, to_r, to_avg_30, to_avg_5
    FROM daily_kline
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    ORDER BY code, date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df.sort_values(['code', 'date'])
    df['pct_change'] = df.groupby('code')['close'].pct_change()
    return df

def get_valid_start_dates(trade_dates, start='2022-01-01', end='2026-01-01', max_dates=50):
    """获取有效起始日期，最多返回max_dates个"""
    start_date = pd.to_datetime(start).date()
    end_date = pd.to_datetime(end).date()
    filtered = [d for d in trade_dates if start_date <= d <= end_date]

    # 需要至少40个交易日
    valid_starts = []
    for i in range(len(filtered) - 39):
        valid_starts.append(filtered[i])
        if len(valid_starts) >= max_dates:
            break

    return valid_starts

def analyze_turnover_effect(start_date, trade_dates, stock_df):
    """分析从start_date开始的换手率效应"""
    try:
        idx = trade_dates.index(start_date)
    except ValueError:
        return pd.DataFrame()

    if idx + 39 >= len(trade_dates):
        return pd.DataFrame()

    # 获取三个窗口的日期
    baseline_dates = trade_dates[idx:idx+30]
    compare_dates = trade_dates[idx+30:idx+35]
    observe_dates = trade_dates[idx+35:idx+40]

    # 计算基准期平均换手率
    baseline_data = stock_df[stock_df['date'].isin(baseline_dates)]
    baseline_avg = baseline_data.groupby('code')['to_r'].mean().reset_index()
    baseline_avg.columns = ['code', 'baseline_to_avg']

    # 计算比较期平均换手率
    compare_data = stock_df[stock_df['date'].isin(compare_dates)]
    compare_avg = compare_data.groupby('code')['to_r'].mean().reset_index()
    compare_avg.columns = ['code', 'compare_to_avg']

    # 合并并计算比率
    merged = pd.merge(baseline_avg, compare_avg, on='code', how='inner')
    merged['ratio'] = merged['compare_to_avg'] / merged['baseline_to_avg']
    selected_codes = merged[merged['ratio'] > 1]['code'].tolist()

    if not selected_codes:
        return pd.DataFrame()

    # 获取观察期数据
    observe_data = stock_df[(stock_df['code'].isin(selected_codes)) &
                            (stock_df['date'].isin(observe_dates))]
    observe_data = observe_data.sort_values(['code', 'date'])

    # 添加元数据
    observe_data['start_date'] = start_date
    observe_data['window_type'] = 'observe'
    date_to_day = {date: i+1 for i, date in enumerate(observe_dates)}
    observe_data['day_in_window'] = observe_data['date'].map(date_to_day)

    # 添加换手率信息
    code_info = merged[merged['code'].isin(selected_codes)].set_index('code')
    observe_data['baseline_to_avg'] = observe_data['code'].map(code_info['baseline_to_avg'])
    observe_data['compare_to_avg'] = observe_data['code'].map(code_info['compare_to_avg'])
    observe_data['ratio'] = observe_data['code'].map(code_info['ratio'])

    return observe_data

def main():
    print("换手率效应分析 (缩减版本 - 前50个起始日)")
    print("=" * 50)

    start_time = time.time()

    print("1. 加载交易日历...")
    trade_dates = load_trade_calendar()
    print(f"   共 {len(trade_dates)} 个交易日")

    print("\n2. 加载股票数据(2022-2026)...")
    stock_df = load_stock_data()
    print(f"   共 {stock_df['code'].nunique()} 只股票，{len(stock_df)} 条日线数据")

    print("\n3. 筛选起始日期(前50个)...")
    start_dates = get_valid_start_dates(trade_dates, max_dates=50)
    print(f"   共 {len(start_dates)} 个起始日期")
    print(f"   日期范围: {start_dates[0]} 到 {start_dates[-1]}")

    print("\n4. 开始分析...")
    all_results = []
    for i, start_date in enumerate(start_dates):
        result = analyze_turnover_effect(start_date, trade_dates, stock_df)
        if not result.empty:
            all_results.append(result)

        if (i + 1) % 10 == 0:
            elapsed = time.time() - start_time
            print(f"   已完成 {i+1}/{len(start_dates)} 个起始日，耗时 {elapsed:.1f} 秒")

    print("\n5. 生成结果...")
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

        total_time = time.time() - start_time

        print(f"\n分析完成! 总耗时: {total_time:.1f} 秒")
        print(f"结果文件: {OUTPUT_FILE}")
        print(f"\n统计信息:")
        print(f"  - 符合条件的股票总数: {final_df['code'].nunique()}")
        print(f"  - 总观察记录数: {len(final_df)}")
        print(f"  - 平均每个起始日符合条件的股票数: {final_df['code'].nunique() / len(start_dates):.1f}")

        print(f"\n换手率统计:")
        print(f"  - 平均基准换手率: {final_df['baseline_to_avg'].mean():.4f}")
        print(f"  - 平均比较换手率: {final_df['compare_to_avg'].mean():.4f}")
        print(f"  - 平均比率: {final_df['ratio'].mean():.4f}")

        print(f"\n股价表现:")
        print(f"  - 观察期平均日涨跌幅: {final_df['pct_change'].mean():.4%}")
        print(f"  - 上涨记录占比: {(final_df['pct_change'] > 0).mean():.2%}")

        # 每日表现
        print(f"\n观察期每日表现:")
        for day in range(1, 6):
            day_data = final_df[final_df['day_in_window'] == day]
            avg_pct = day_data['pct_change'].mean()
            count = len(day_data)
            print(f"  第{day}天: {count}条记录, 平均 {avg_pct:.4%}")

        # 性能估算
        avg_time_per_start = total_time / len(start_dates)
        total_starts = 930  # 完整数据中的起始日数
        estimated_total_time = avg_time_per_start * total_starts / 60  # 分钟
        print(f"\n性能估算:")
        print(f"  - 每个起始日平均耗时: {avg_time_per_start:.2f} 秒")
        print(f"  - 完整分析预估耗时: {estimated_total_time:.1f} 分钟")
    else:
        print("没有符合条件的记录")

    print("\n" + "=" * 50)

if __name__ == "__main__":
    main()