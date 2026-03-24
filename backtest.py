import pandas as pd
import requests
import time
import os

# --- 核心配置 ---
TOKEN = os.getenv("LIXINGER_TOKEN") 
TR_API_URL = "https://open.lixinger.com/api/cn/company/hot/tr"
CANDLE_API_URL = "https://open.lixinger.com/api/cn/company/candlestick"
INPUT_FILE = "china_main_board_market_cap_filtered.csv"
OUTPUT_FILE = "900_groups_full_samples.csv"

def rate_limit(interval=0.2):
    time.sleep(interval)

def fetch_market_liquidity(stock_codes):
    amounts = {}
    print(f"正在获取 {len(stock_codes)} 只股票的实时成交额数据...")
    for i in range(0, len(stock_codes), 100):
        batch = stock_codes[i:i+100]
        try:
            rate_limit(0.3)
            resp = requests.post(TR_API_URL, json={"token": TOKEN, "stockCodes": batch}).json()
            if resp.get("code") == 1:
                for item in resp['data']:
                    amounts[item['stockCode']] = item.get('ta', 0)
        except Exception: pass
    return amounts

def run_900_groups_backtest():
    if not os.path.exists(INPUT_FILE): return
    
    raw_df = pd.read_csv(INPUT_FILE)
    raw_df['stockCodeStr'] = raw_df['stockCode'].apply(lambda x: str(x).zfill(6))
    all_codes = raw_df['stockCodeStr'].tolist()
    
    # 1. 预处理：成交额五分位
    amount_dict = fetch_market_liquidity(all_codes)
    raw_df['current_ta'] = raw_df['stockCodeStr'].map(amount_dict)
    valid_ta = raw_df[raw_df['current_ta'] > 0].copy()
    raw_df.loc[valid_ta.index, 'amount_group'] = pd.qcut(valid_ta['current_ta'], 5, 
                                                       labels=['TA_Q1极低', 'TA_Q2偏低', 'TA_Q3中等', 'TA_Q4活跃', 'TA_Q5极高'])

    # 2. 预处理：市值五分位 (新增修改点)
    raw_df['cap_group'] = pd.qcut(raw_df['marketCap'], 5, 
                                  labels=['MC_Q1极小', 'MC_Q2偏小', 'MC_Q3中等', 'MC_Q4偏大', 'MC_Q5极大'])

    fetch_start = "2024-01-01" 
    p1_s, p1_e = pd.Timestamp("2025-09-01"), pd.Timestamp("2025-10-10")
    p2_s, p2_e = pd.Timestamp("2025-10-13"), pd.Timestamp("2025-10-17")
    p3_s, p3_e = pd.Timestamp("2025-10-20"), pd.Timestamp("2025-10-31")

    results = []
    print(f"\n🚀 开始 900 组超细分回测 (市值5组 + 成交额5组)...")

    for i, (_, row) in enumerate(raw_df.iterrows()):
        code = row['stockCodeStr']
        if pd.isna(row['amount_group']): continue
        
        print(f"进度: [{i+1}/{len(raw_df)}] 分析中: {code}", end="\r")
        
        try:
            rate_limit(0.15)
            payload = {"token": TOKEN, "stockCode": code, "startDate": fetch_start, "endDate": "2025-10-31", "type": "fc_rights"}
            resp = requests.post(CANDLE_API_URL, json=payload, timeout=15).json()
            if resp.get("code") != 1 or not resp.get("data"): continue
            
            df = pd.DataFrame(resp["data"])
            df['date'] = pd.to_datetime(df['date']).map(lambda x: x.replace(tzinfo=None))
            df = df.sort_values('date').reset_index(drop=True)

            df['ma20'] = df['close'].rolling(window=20).mean()
            df['ma250'] = df['close'].rolling(window=250).mean()
            
            d1, d2, d3 = df[(df['date']>=p1_s)&(df['date']<=p1_e)], df[(df['date']>=p2_s)&(df['date']<=p2_e)], df[(df['date']>=p3_s)&(df['date']<=p3_e)]
            if d1.empty or d2.empty or d3.empty: continue

            ratio = d2['to_r'].mean() / d1['to_r'].mean() if d1['to_r'].mean() > 0 else 0
            start_info = d3.iloc[0]
            price_base = start_info['close']
            
            bias_val = ((price_base - start_info['ma20']) / start_info['ma20'] * 100) if start_info['ma20'] else 0
            trend_tag = '年线上' if (start_info['ma250'] and price_base > start_info['ma250']) else '年线下'

            results.append({
                "code": code, "name": row['name'], "mktCap": row['marketCap'],
                "amount_group": row['amount_group'], "cap_group": row['cap_group'], "trend": trend_tag,
                "ratio": round(ratio, 4), "bias": round(bias_val, 2),
                "max_gain%": round((d3['high'].max() / price_base) * 100, 2),
                "max_loss%": round((d3['low'].min() / price_base) * 100, 2)
            })
        except: continue

    res_df = pd.DataFrame(results)
    if res_df.empty: return

    # 3. 细化 Ratio 和 Bias 分组
    res_df['ratio_group'] = pd.cut(res_df['ratio'], 
                                   bins=[0, 1.3, 1.6, 1.9, 2.2, 2.5, 999], 
                                   labels=['1.0-1.3', '1.3-1.6', '1.6-1.9', '1.9-2.2', '2.2-2.5', '>2.5'])
    res_df['bias_group'] = pd.cut(res_df['bias'], bins=[-999, 0, 10, 999], labels=['下方', '贴线', '远离'])

    # 4. 统计聚合
    def agg_report(g):
        total = len(g)
        safe_rate = (g['max_loss%'] > 97).sum() / total * 100
        worst = g.loc[g['max_loss%'].idxmin()]
        return pd.Series({
            '样本数': total,
            '保本率': f"{safe_rate:.2f}%",
            '平均回撤': f"{(100 - g['max_loss%'].mean()):.2f}%",
            '最惨个股': f"{worst['name']}({100 - worst['max_loss%']:.1f}%)",
            '平均涨幅': f"{(g['max_gain%'].mean() - 100):.2f}%"
        })

    group_keys = ['trend', 'cap_group', 'amount_group', 'ratio_group', 'bias_group']
    report = res_df.groupby(group_keys, observed=True).apply(agg_report)

    report.to_csv("900_groups_final_report.csv", encoding="utf-8-sig")
    res_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n✨ 900组细分回测完成！")

if __name__ == "__main__":
    run_900_groups_backtest()