import akshare as ak
import pandas as pd
import time
import random

def screen_potential_stocks():
    print("正在抓取全市场实时行情，请稍候...")
    
    try:
        # 1. 获取全 A 股实时行情
        # 增加重试逻辑
        df_spot = None
        for _ in range(3):
            try:
                df_spot = ak.stock_zh_a_spot_em()
            except Exception as e:
                print(f"抓取失败，具体原因：{e}")
        
        if df_spot is None:
            print("无法获取实时行情，请检查网络。")
            return

        # 初步过滤
        filtered_df = df_spot[
            (df_spot['量比'] > 2.0) & 
            (df_spot['换手率'] > 2.0) & (df_spot['换手率'] < 10.0) &
            (df_spot['涨跌幅'] > 2.0) & (df_spot['涨跌幅'] < 8.0)
        ].copy()

        potential_list = []
        total = len(filtered_df)
        print(f"初步筛选出 {total} 只异动个股，开始深度检测...")

        for i, (index, row) in enumerate(filtered_df.iterrows()):
            symbol = row['代码']
            
            # 关键：添加随机延迟，模拟真人操作，防止被封 IP
            time.sleep(random.uniform(0.5, 1.5)) 
            
            try:
                # 获取历史数据
                df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
                if len(df_hist) < 120: continue
                
                last_120_max = df_hist['收盘'].max()
                last_120_min = df_hist['收盘'].min()
                current_price = row['最新价']
                
                price_pos = (current_price - last_120_min) / (last_120_max - last_120_min)
                
                if 0.1 < price_pos < 0.6:
                    potential_list.append({
                        "代码": symbol,
                        "名称": row['名称'],
                        "最新价": current_price,
                        "量比": row['量比'],
                        "120日位置": f"{price_pos:.2%}"
                    })
                
                if (i+1) % 10 == 0:
                    print(f"进度: {i+1}/{total}...")

            except Exception as e:
                # 单只股票报错不中断程序
                continue

        result = pd.DataFrame(potential_list)
        return result.sort_values(by="量比", ascending=False) if not result.empty else "暂无符合条件标的"

    except Exception as e:
        print(f"发生致命错误: {e}")
        return None

if __name__ == "__main__":
    res = screen_potential_stocks()
    print("\n--- 最终潜力股报告 ---")
    print(res)