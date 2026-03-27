import sqlite3
import pandas as pd
import os
from datetime import datetime

# --- 配置 ---
DB_PATH = "./tools/stock_market_data.db"
CALENDAR_FILE = "./tools/trade_calendar.csv"
OUTPUT_FILE = "./output/backtest_test_results.csv"

def load_trade_calendar():
    """加载交易日历，返回日期列表"""
    df = pd.read_csv(CALENDAR_FILE)
    trade_dates = pd.to_datetime(df['date']).dt.date.tolist()
    return trade_dates

def load_stock_data():
    """加载股票日线数据，返回DataFrame"""
    conn = sqlite3.connect(DB_PATH)
    # 只加载2022年的数据以减少内存使用
    query = """
    SELECT code, date, close, to_r, to_avg_30, to_avg_5
    FROM daily_kline
    WHERE date >= '2022-01-01' AND date <= '2022-12-31'
    ORDER BY code, date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df.sort_values(['code', 'date'])
    # 计算每日涨跌幅 (使用前一日收盘价)
    df['pct_change'] = df.groupby('code')['close'].pct_change()
    return df

def get_valid_start_dates(trade_dates, start='2022-01-01', end='2022-12-31'):
    """获取在指定范围内的起始日期，并确保每个起始日期后有至少30+5+5个交易日"""
    start_date = pd.to_datetime(start).date()
    end_date = pd.to_datetime(end).date()

    # 找到起始日期的索引
    try:
        start_idx = trade_dates.index(start_date)
    except ValueError:
        # 如果起始日期不是交易日，找到第一个大于等于起始日期的交易日
        start_idx = next((i for i, d in enumerate(trade_dates) if d >= start_date), 0)

    # 找到结束日期的索引（最后一个小于等于结束日期的交易日）
    end_idx = len(trade_dates) - 1
    for i, d in enumerate(trade_dates):
        if d > end_date:
            end_idx = i - 1
            break

    if start_idx >= end_idx - 39:
        return []

    # 生成有效起始日期列表（只取前10个用于测试）
    valid_starts = []
    max_starts = min(10, end_idx - 39 - start_idx + 1)
    for i in range(start_idx, start_idx + max_starts):
        valid_starts.append(trade_dates[i])

    return valid_starts

def analyze_turnover_effect(start_date, trade_dates, stock_df):
    """
    分析从start_date开始的换手率效应
    返回一个DataFrame，包含符合条件的股票在观察期的每日数据
    """
    # 找到起始日期在交易日历中的索引
    try:
        idx = trade_dates.index(start_date)
    except ValueError:
        return pd.DataFrame()  # 起始日非交易日

    # 检查是否有足够的后续交易日
    if idx + 39 >= len(trade_dates):
        return pd.DataFrame()

    # 获取三个窗口的日期
    baseline_dates = trade_dates[idx:idx+30]          # 第0-29天 (30天基准期)
    compare_dates = trade_dates[idx+30:idx+35]       # 第30-34天 (5天比较期)
    observe_dates = trade_dates[idx+35:idx+40]       # 第35-39天 (5天观察期)

    # 筛选股票数据，只保留在基准期有数据的股票
    baseline_data = stock_df[stock_df['date'].isin(baseline_dates)]
    # 计算每只股票在基准期的平均换手率 (使用to_r)
    baseline_avg = baseline_data.groupby('code')['to_r'].mean().reset_index()
    baseline_avg.columns = ['code', 'baseline_to_avg']

    # 获取比较期数据
    compare_data = stock_df[stock_df['date'].isin(compare_dates)]
    compare_avg = compare_data.groupby('code')['to_r'].mean().reset_index()
    compare_avg.columns = ['code', 'compare_to_avg']

    # 合并基准和比较数据
    merged = pd.merge(baseline_avg, compare_avg, on='code', how='inner')
    # 计算比率
    merged['ratio'] = merged['compare_to_avg'] / merged['baseline_to_avg']
    # 筛选比率 > 1 的股票
    selected_codes = merged[merged['ratio'] > 1]['code'].tolist()

    if not selected_codes:
        return pd.DataFrame()

    # 获取观察期数据
    observe_data = stock_df[(stock_df['code'].isin(selected_codes)) &
                            (stock_df['date'].isin(observe_dates))]
    # 按股票和日期排序
    observe_data = observe_data.sort_values(['code', 'date'])

    # 添加起始日期和窗口信息
    observe_data['start_date'] = start_date
    observe_data['window_type'] = 'observe'
    # 添加观察期内的相对天数 (1-5)
    date_to_day = {date: i+1 for i, date in enumerate(observe_dates)}
    observe_data['day_in_window'] = observe_data['date'].map(date_to_day)

    # 添加基准和比较期的平均换手率信息
    code_info = merged[merged['code'].isin(selected_codes)].set_index('code')
    observe_data['baseline_to_avg'] = observe_data['code'].map(code_info['baseline_to_avg'])
    observe_data['compare_to_avg'] = observe_data['code'].map(code_info['compare_to_avg'])
    observe_data['ratio'] = observe_data['code'].map(code_info['ratio'])

    return observe_data

def main():
    print("加载交易日历...")
    trade_dates = load_trade_calendar()
    print(f"共 {len(trade_dates)} 个交易日")

    print("加载股票数据(2022年)...")
    stock_df = load_stock_data()
    print(f"共 {stock_df['code'].nunique()} 只股票，{len(stock_df)} 条日线数据")

    print("筛选起始日期...")
    start_dates = get_valid_start_dates(trade_dates)
    print(f"共有 {len(start_dates)} 个有效起始日期（测试用前10个）")
    print(f"起始日期: {start_dates}")

    all_results = []
    for i, start_date in enumerate(start_dates):
        print(f"处理进度: {i+1}/{len(start_dates)}，当前起始日: {start_date}")

        result = analyze_turnover_effect(start_date, trade_dates, stock_df)
        if not result.empty:
            all_results.append(result)
            print(f"  找到 {result['code'].nunique()} 只符合条件的股票")

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        # 确保输出目录存在
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\n结果已保存至 {OUTPUT_FILE}")
        print(f"共 {final_df['code'].nunique()} 只股票符合条件")
        print(f"共 {len(final_df)} 条观察记录")

        # 输出汇总统计
        print("\n汇总统计:")
        print(f"平均基准换手率: {final_df['baseline_to_avg'].mean():.4f}")
        print(f"平均比较换手率: {final_df['compare_to_avg'].mean():.4f}")
        print(f"平均比率: {final_df['ratio'].mean():.4f}")
        print(f"观察期平均日涨跌幅: {final_df['pct_change'].mean():.4%}")

        # 按观察期天数统计平均涨跌幅
        for day in range(1, 6):
            day_data = final_df[final_df['day_in_window'] == day]
            avg_pct = day_data['pct_change'].mean()
            count = len(day_data)
            print(f"观察期第{day}天: {count}条记录, 平均涨跌幅: {avg_pct:.4%}")
    else:
        print("没有符合条件的记录")

if __name__ == "__main__":
    main()