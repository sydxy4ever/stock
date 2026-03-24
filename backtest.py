import pandas as pd
import requests
import time
import os
from bisect import bisect_left

# --- 基础配置 ---
TOKEN = os.getenv("LIXINGER_TOKEN")
INPUT_FILE = "china_main_board_market_cap_filtered.csv"
CALENDAR_FILE = "trade_calendar.csv"
CANDLE_API_URL = "https://open.lixinger.com/api/cn/company/candlestick"
OUTPUT_DIR = "output"

# 确保输出目录存在
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def load_calendar():
    """加载已抓取的 2020 至今交易日历"""
    if not os.path.exists(CALENDAR_FILE):
        raise FileNotFoundError(f"请先确保 {CALENDAR_FILE} 存在")
    df = pd.read_csv(CALENDAR_FILE)
    return sorted(pd.to_datetime(df['date']).tolist())

def run_backtest_by_index(start_idx, trade_days, stock_list):
    """
    核心回测逻辑：
    P1(30天基准) -> P2(5天观察) -> P3(10天结果)
    """
    # 确定三段绝对日期
    p1_start = trade_days[start_idx]
    p1_end   = trade_days[start_idx + 29]
    p2_start = trade_days[start_idx + 30]
    p2_end   = trade_days[start_idx + 34]
    p3_start = trade_days[start_idx + 35]
    p3_end   = trade_days[start_idx + 44]
    
    date_str = p1_start.strftime('%Y%m%d')
    report_path = os.path.join(OUTPUT_DIR, f"{date_str}_report.csv")
    detail_path = os.path.join(OUTPUT_DIR, f"{date_str}_details.csv")

    # 自动续传检查
    if os.path.exists(report_path):
        return

    print(f"\n" + "="*70)
    print(f"🔎 正在回测基准起点: {p1_start.date()} (索引: {start_idx})")
    print(f"📅 窗口: P1[{p1_start.date()}~{p1_end.date()}] P2[{p2_start.date()}~{p2_end.date()}] P3[{p3_start.date()}~{p3_end.date()}]")

    results = []
    
    for i, (_, row) in enumerate(stock_list.iterrows()):
        code = row['stockCodeStr']
        print(f"   进度: [{i+1}/{len(stock_list)}] 处理中: {code}", end="\r")
        
        try:
            # 频率控制（根据 Token 级别调整，0.1s 是安全值）
            time.sleep(0.1)
            
            payload = {
                "token": TOKEN, 
                "stockCode": code, 
                "startDate": p1_start.strftime('%Y-%m-%d'), 
                "endDate": p3_end.strftime('%Y-%m-%d'), 
                "type": "fc_rights"
            }
            resp = requests.post(CANDLE_API_URL, json=payload, timeout=10).json()
            if resp.get("code") != 1 or not resp.get("data"):
                continue
            
            df = pd.DataFrame(resp["data"])
            df['date'] = pd.to_datetime(df['date']).map(lambda x: x.replace(tzinfo=None))
            df = df.sort_values('date').reset_index(drop=True)

            # 切片
            d1 = df[(df['date'] >= p1_start) & (df['date'] <= p1_end)]
            d2 = df[(df['date'] >= p2_start) & (df['date'] <= p2_end)]
            d3 = df[(df['date'] >= p3_start) & (df['date'] <= p3_end)]
            
            # 过滤停牌过多的样本
            if len(d1) < 24 or len(d2) < 4 or len(d3) < 8:
                continue

            # --- 计算因子 ---
            # 1. 换手比
            avg_to_p1 = d1['to_r'].mean()
            ratio = d2['to_r'].mean() / avg_to_p1 if avg_to_p1 > 0 else 0
            
            # 2. 观察期成交额
            avg_ta_p2 = d2['amount'].mean()
            
            # 3. 进场点(P3第一天)状态
            entry_row = d3.iloc[0]
            price_entry = entry_row['close']
            
            # 4. 技术指标 (以P2结束点为锚点)
            hist_df = df[df['date'] <= p2_end]
            ma20 = hist_df.tail(20)['close'].mean()
            ma250 = hist_df.tail(250)['close'].mean() if len(hist_df) >= 250 else None
            
            bias = ((price_entry - ma20) / ma20 * 100) if ma20 else 0
            trend = '年线上' if (ma250 and price_entry > ma250) else '年线下'

            results.append({
                "code": code,
                "name": row['name'],
                "mktCap": row['marketCap'],
                "avg_ta": avg_ta_p2,
                "trend": trend,
                "ratio": round(ratio, 4),
                "bias": round(bias, 2),
                "max_gain%": round((d3['high'].max() / price_entry) * 100, 2),
                "max_loss%": round((d3['low'].min() / price_entry) * 100, 2)
            })
        except Exception:
            continue

    if not results:
        return

    # --- 900组细分统计 ---
    res_df = pd.DataFrame(results)
    
    # 动态分位分组
    res_df['cap_group'] = pd.qcut(res_df['mktCap'], 5, labels=['MC_Q1极小', 'MC_Q2偏小', 'MC_Q3中等', 'MC_Q4偏大', 'MC_Q5极大'], duplicates='drop')
    res_df['amount_group'] = pd.qcut(res_df['avg_ta'], 5, labels=['TA_Q1极低', 'TA_Q2偏低', 'TA_Q3中等', 'TA_Q4活跃', 'TA_Q5极高'], duplicates='drop')
    res_df['ratio_group'] = pd.cut(res_df['ratio'], bins=[0, 1.3, 1.6, 1.9, 2.2, 2.5, 999], labels=['1.0-1.3', '1.3-1.6', '1.6-1.9', '1.9-2.2', '2.2-2.5', '>2.5'])
    res_df['bias_group'] = pd.cut(res_df['bias'], bins=[-999, 0, 10, 999], labels=['下方', '贴线', '远离'])

    # 生成日报
    report = res_df.groupby(['trend', 'cap_group', 'amount_group', 'ratio_group', 'bias_group'], observed=True).apply(
        lambda g: pd.Series({
            '样本数': len(g),
            '保本率': (g['max_loss%'] > 97).sum() / len(g),
            '平均最大涨幅': g['max_gain%'].mean() - 100
        })
    )

    # 保存文件
    report.to_csv(report_path, encoding="utf-8-sig")
    res_df.to_csv(detail_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ 已保存报告至: {report_path}")

# --- 执行区 ---
if __name__ == "__main__":
    calendar = load_calendar()
    stocks = pd.read_csv(INPUT_FILE)
    stocks['stockCodeStr'] = stocks['stockCode'].apply(lambda x: str(x).zfill(6))

    # 定义全量回测的时间范围
    # 注意：确保 calendar 中有足够的前置和后置日期
    global_start_date = pd.Timestamp("2023-01-01")
    global_end_date = pd.Timestamp("2025-01-01")

    # 找到起点和终点的索引
    start_idx_limit = bisect_left(calendar, global_start_date)
    end_idx_limit = bisect_left(calendar, global_end_date)

    print(f"🚀 启动全量回测流...")
    print(f"📅 总计天数: {end_idx_limit - start_idx_limit} 交易日")

    # 逐日滑动窗口循环
    for current_idx in range(start_idx_limit, end_idx_limit):
        run_backtest_by_index(current_idx, calendar, stocks)

    print("\n" + "★"*40)
    print("所有日期窗口已处理完毕，请检查 /output 目录。")
    print("★"*40)