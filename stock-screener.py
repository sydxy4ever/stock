import akshare as ak
import pandas as pd
import time
import random
import os

def screen_all_market():
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 正在启动全市场扫描 (约 5300+ 只股票)...")
    
    try:
        # 策略 1: 一次性获取全 A 股行情快照 (减少请求频率)
        # 这个接口包含：代码, 名称, 最新价, 涨跌幅, 换手率, 量比, 市盈率等
        df_all = ak.stock_zh_a_spot_em()
        
        if df_all is None or df_all.empty:
            print("错误：未能获取实时行情。请检查网络或是否为非交易日维护期。")
            return

        # 策略 2: 严苛的初步筛选 (大幅缩小需要二次请求历史数据的范围)
        # 条件：量比 > 2.2, 2% < 涨幅 < 8%, 换手率 > 2%
        mask = (
            (df_all['量比'] > 2.2) & 
            (df_all['涨跌幅'] > 2.0) & (df_all['涨跌幅'] < 8.0) &
            (df_all['换手率'] > 2.0) & (df_all['换手率'] < 15.0)
        )
        candidates = df_all[mask].copy()
        
        total_found = len(candidates)
        print(f"初步发现 {total_found} 只异动个股。开始深度计算价格位置...")

        final_list = []
        
        # 策略 3: 只对异动个股进行历史回溯 (降低封号风险)
        for i, (idx, row) in enumerate(candidates.iterrows()):
            symbol = row['代码']
            name = row['名称']
            
            # 必须加延迟！否则 Docker 极易断连
            time.sleep(random.uniform(1.0, 2.0))
            
            try:
                # 获取 120 日历史行情用于计算位置
                df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
                
                if df_hist is not None and len(df_hist) >= 120:
                    recent = df_hist.tail(120)
                    h_max = recent['收盘'].max()
                    l_min = recent['收盘'].min()
                    curr = row['最新价']
                    
                    # 价格位置计算：(当前价-最低)/(最高-最低)
                    pos = (curr - l_min) / (h_max - l_min) if (h_max - l_min) != 0 else 0
                    
                    # 只选在相对低位启动的（避开已经翻倍的股票）
                    if 0.1 <= pos <= 0.6:
                        final_list.append({
                            "代码": symbol,
                            "名称": name,
                            "最新价": curr,
                            "涨幅%": row['涨跌幅'],
                            "量比": row['量比'],
                            "换手率%": row['换手率'],
                            "位置%": round(pos * 100, 2)
                        })
                
                if (i + 1) % 10 == 0:
                    print(f"已完成: {i + 1} / {total_found}")

            except Exception:
                continue

        # 策略 4: 结果排序与输出
        if final_list:
            result = pd.DataFrame(final_list)
            result = result.sort_values(by="量比", ascending=False)
            
            print("\n" + "="*60)
            print(f"全市场筛选完成！共锁定 {len(result)} 只潜力标的")
            print("="*60)
            print(result.to_string(index=False))
            
            # 持久化存储 (Docker 映射目录)
            result.to_csv("all_market_potential.csv", index=False, encoding='utf_8_sig')
        else:
            print("全市场扫描完毕，未发现符合低位放量特征的个股。")

    except Exception as e:
        print(f"发生意外错误: {e}")

if __name__ == "__main__":
    screen_all_market()