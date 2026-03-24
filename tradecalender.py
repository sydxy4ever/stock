import requests
import pandas as pd
import os

# 确保已设置环境变量
TOKEN = os.getenv("LIXINGER_TOKEN")
CALENDAR_FILE = "trade_calendar_2025.csv"

def generate_trade_calendar():
    """使用个股数据作为基准，获取 2025 年完整交易日列表"""
    if not TOKEN:
        print("错误: 请先设置环境变量 LIXINGER_TOKEN")
        return

    print("📅 正在同步交易日历 (基准: 600519)...")
    
    # 改用个股 K 线接口，稳定性最高
    url = "https://open.lixinger.com/api/cn/company/candlestick"
    payload = {
        "token": TOKEN,
        "stockCode": "600519",  # 贵州茅台
        "startDate": "2020-01-01", 
        "endDate": "2026-03-23",
        "type": "fc_rights"     # 前复权，确保数据连续
    }

    try:
        response = requests.post(url, json=payload)
        resp = response.json()
        
        if resp.get("code") == 1:
            data = resp.get('data')
            if not data:
                print("失败: 接口返回 data 为空。请检查 Token 有效性。")
                return

            df = pd.DataFrame(data)
            
            # 调试点：打印列名，防止 'date' 键名不一致
            if 'date' not in df.columns:
                print(f"错误: 返回数据中不包含 'date' 键。当前的列名有: {df.columns.tolist()}")
                # 有些接口可能返回 'd' 或 'dateTime'，如果存在则重命名
                if 'd' in df.columns: df.rename(columns={'d': 'date'}, inplace=True)
                elif 'dateTime' in df.columns: df.rename(columns={'dateTime': 'date'}, inplace=True)
                else: return

            # 转换日期格式
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df.sort_values('date')
            
            # 只保留日期列并去重（防止复权数据重复）
            calendar = df[['date']].drop_duplicates()
            calendar.to_csv(CALENDAR_FILE, index=False)
            
            print(f"✅ 日历已生成！文件: {CALENDAR_FILE}")
            print(f"总计交易日: {len(calendar)} 天")
            print(f"范围: {calendar['date'].iloc[0]} 至 {calendar['date'].iloc[-1]}")
            return calendar['date'].tolist()
        else:
            print(f"接口返回错误: {resp.get('message')}")
    except Exception as e:
        print(f"程序运行异常: {e}")

if __name__ == "__main__":
    generate_trade_calendar()