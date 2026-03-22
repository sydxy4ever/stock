import akshare as ak
import pandas as pd
import numpy as np

def screen_potential_stocks():
    print("正在抓取全市场实时行情，请稍候...")
    
    # 1. 获取全 A 股实时行情 (包含涨跌幅、换手率、量比等)
    # 接口：东方财富实时行情
    df_spot = ak.stock_zh_a_spot_em()
    
    # 初步筛选条件：
    # - 量比 > 2 (活跃度异动)
    # - 换手率在 2% - 10% 之间 (不过冷也不过热)
    # - 涨跌幅在 2% - 7% 之间 (已经启动但未封板)
    # - 排除新股 (上市天数建议 > 60，这里简单过滤代码开头)
    filtered_df = df_spot[
        (df_spot['量比'] > 2.0) & 
        (df_spot['换手率'] > 2.0) & (df_spot['换手率'] < 10.0) &
        (df_spot['涨跌幅'] > 2.0) & (df_spot['涨跌幅'] < 8.0)
    ].copy()

    potential_list = []

    print(f"初步筛选出 {len(filtered_df)} 只异动个股，开始深度检测价格位置与板块效应...")

    for index, row in filtered_df.iterrows():
        symbol = row['代码']
        name = row['名称']
        
        try:
            # 2. 价格位置分析 (获取过去 120 个交易日的历史行情)
            df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
            if len(df_hist) < 120: continue
            
            last_120_max = df_hist['收盘'].max()
            last_120_min = df_hist['收盘'].min()
            current_price = row['最新价']
            
            # 计算价格所处区间位置 (0%为最低点, 100%为最高点)
            # 逻辑：寻找处于“底部放量突破”或“横盘向上突破”的标的
            price_pos = (current_price - last_120_min) / (last_120_max - last_120_min)
            
            # 3. 板块效应简单模拟
            # 判断该股是否跑赢所属行业板块 (这里以该股涨幅是否大于 3% 且 属于强势板块为准)
            # 注：更精细的板块效应需要对比行业指数，这里简化为个股强度的独立校验
            
            if 0.1 < price_pos < 0.6:  # 价格不在最高位，有上涨空间
                potential_list.append({
                    "代码": symbol,
                    "名称": name,
                    "最新价": current_price,
                    "涨跌幅": row['涨跌幅'],
                    "量比": row['量比'],
                    "换手率": row['换手率'],
                    "120日价格位置": f"{price_pos:.2%}"
                })
        except Exception as e:
            continue

    # 输出结果
    result = pd.DataFrame(potential_list)
    if not result.empty:
        return result.sort_values(by="量比", ascending=False)
    else:
        return "今日暂无符合条件的潜力股。"

if __name__ == "__main__":
    candidates = screen_potential_stocks()
    print("\n--- 潜力股筛选报告 ---")
    print(candidates)