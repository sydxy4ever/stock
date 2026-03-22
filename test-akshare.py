import os
import time
import random
import pandas as pd

# 1. 彻底禁用环境代理 (Windows 环境下 Conda 经常自动读取系统代理)
os.environ['no_proxy'] = '*'
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''

import akshare as ak

def is_main_board(code):
    """主板代码过滤逻辑"""
    # 处理新浪/东财可能出现的 sh/sz 前缀
    c = str(code).replace('sh', '').replace('sz', '')
    # 沪市主板: 600, 601, 603, 605 | 深市主板: 000, 001, 002
    return c.startswith(('600', '601', '603', '605', '000', '001', '002'))

def run_windows_screener():
    print(f"[{time.strftime('%H:%M:%S')}] 正在获取主板实时快照 (新浪源)...")
    
    try:
        # 获取行情
        df_all = ak.stock_zh_a_spot() 
        if df_all is None or df_all.empty:
            print("未能获取数据，请确认网络直连是否正常。")
            return

        # 过滤主板
        df_all['is_main'] = df_all['symbol'].apply(is_main_board)
        df_main = df_all[df_all['is_main'] == True].copy()

        # 数值转换
        df_main['trade'] = pd.to_numeric(df_main['trade'], errors='coerce')
        df_main['changeratio'] = pd.to_numeric(df_main['changeratio'], errors='coerce')
        df_main['amount'] = pd.to_numeric(df_main['amount'], errors='coerce')

        # 策略过滤：涨幅 2%~8%
        candidates = df_main[
            (df_main['changeratio'] >= 2.0) & 
            (df_main['changeratio'] <= 8.0)
        ].copy()

        print(f"筛选到 {len(candidates)} 只主板异动股，正在同步行业板块与历史位置...")

        # 获取行业分类（为了 Windows 下的稳定性，使用东财实时行情接口作为行业库）
        try:
            df_industry = ak.stock_zh_a_spot_em()
            ind_map = dict(zip(df_industry['代码'], df_industry['板块名称']))
        except:
            ind_map = {}

        final_picks = []
        # 扫描前 40 只（防止 Windows 下高频请求被封）
        scan_list = candidates.sort_values(by='changeratio', ascending=False).head(40)

        for i, (idx, row) in enumerate(scan_list.iterrows()):
            code = row['symbol'].replace('sh', '').replace('sz', '')
            time.sleep(random.uniform(0.4, 0.8)) # 稍微慢一点更稳
            
            try:
                df_hist = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
                if df_hist is not None and len(df_hist) >= 120:
                    recent = df_hist.tail(120)
                    h_max = recent['收盘'].max()
                    l_min = recent['收盘'].min()
                    curr = row['trade']
                    pos = (curr - l_min) / (h_max - l_min) if (h_max - l_min) != 0 else 0
                    
                    if pos <= 0.55: # 低位
                        final_picks.append({
                            "代码": code,
                            "名称": row['name'],
                            "所属行业": ind_map.get(code, "主板其他"),
                            "最新价": curr,
                            "涨幅%": row['changeratio'],
                            "半年位置%": round(pos * 100, 1),
                            "成交额(万)": round(row['amount'] / 10000, 0)
                        })
            except:
                continue

        if final_picks:
            res_df = pd.DataFrame(final_picks).sort_values(by=["所属行业", "涨幅%"], ascending=[True, False])
            print("\n" + "="*70)
            print(f"Windows 主板潜力股报告 | {time.strftime('%Y-%m-%d %H:%M')}")
            print("="*70)
            print(res_df.to_string(index=False))
            
            # Windows 下必须使用 utf_8_sig 否则 Excel 打开 CSV 会乱码
            res_df.to_csv("main_board_picks.csv", index=False, encoding="utf_8_sig")
            print(f"\n结果已保存至: {os.getcwd()}\\main_board_picks.csv")
        else:
            print("未匹配到符合条件的主板个股。")

    except Exception as e:
        print(f"运行出错: {e}")

if __name__ == "__main__":
    run_windows_screener()