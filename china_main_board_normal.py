import requests
import csv
import os
import time

def sync_main_board_stocks():
    # 1. 获取环境变量中的 Token
    token = os.getenv("LIXINGER_TOKEN")
    if not token:
        print("错误：未找到系统变量 LIXINGER_TOKEN，请检查配置。")
        return

    # 确定保存路径为当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "china_main_board_normal.csv")
    
    url = "https://open.lixinger.com/api/cn/company"
    results = []

    print(f"🚀 开始获取第 0-11 页的『正常上市』主板股票...")

    for i in range(12):
        payload = {
            "token": token,
            "pageIndex": i
        }
        try:
            response = requests.post(url, json=payload, timeout=15)
            data_json = response.json()
            
            if data_json.get("code") == 1:
                stocks = data_json.get("data", [])
                for s in stocks:
                    code = s.get("stockCode", "")
                    ex = s.get("exchange", "")
                    status = s.get("listingStatus", "")
                    
                    # 组合过滤条件：
                    # 1. 状态必须是正常上市 (normally_listed)
                    # 2. 必须是主板：沪市(60) 或 深市(000, 001)
                    is_normally_listed = (status == "normally_listed")
                    is_main_board = (ex == "sh" and code.startswith("60")) or \
                                    (ex == "sz" and code.startswith(("000", "001")))
                    
                    if is_normally_listed and is_main_board:
                        results.append({
                            "stockCode": code,
                            "name": s.get("name"),
                            "exchange": ex,
                            "listingStatus": status
                        })
                print(f"已处理第 {i} 页，累计匹配数: {len(results)}")
            else:
                print(f"第 {i} 页请求异常: {data_json.get('message')}")
            
            # 频率控制，理杏仁 API 建议不要并发太高
            time.sleep(0.2)
            
        except Exception as e:
            print(f"第 {i} 页执行出错: {e}")

    # 2. 写入 CSV 文件
    if results:
        # 按照股票代码去重（以防分页数据重叠）
        unique_results = {s['stockCode']: s for s in results}.values()
        
        with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=["stockCode", "name", "exchange", "listingStatus"])
            writer.writeheader()
            writer.writerows(unique_results)
        
        print("\n" + "="*30)
        print(f"✅ 处理完成！")
        print(f"总计找到正常上市主板股票: {len(unique_results)} 只")
        print(f"文件位置: {file_path}")
        print("="*30)
    else:
        print("\n❌ 未抓取到任何匹配数据。")

if __name__ == "__main__":
    sync_main_board_stocks()