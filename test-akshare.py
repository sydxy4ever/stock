import os
import akshare as ak
import pandas as pd
import time

# 禁用代理
os.environ['no_proxy'] = '*'

def download_snapshot():
    print(f"[{time.strftime('%H:%M:%S')}] 正在从东财下载全市场实时快照...")
    try:
        # 这个接口包含：代码, 名称, 涨跌幅, 最新价, 成交额, 换手率, 量比, 市盈率, 板块等
        df = ak.stock_zh_a_spot_em()
        if df is not None and not df.empty:
            filename = f"market_snapshot_{time.strftime('%Y%m%d')}.csv"
            df.to_csv(filename, index=False, encoding="utf_8_sig")
            print(f"下载成功！文件名: {filename}")
            return filename
    except Exception as e:
        print(f"下载失败: {e}")
    return None

if __name__ == "__main__":
    download_snapshot()