import requests
import csv
import os
import time
import datetime
from typing import List, Dict, Any, Optional

# API 速率限制：每秒最多5次调用
_LAST_API_CALL_TIME = 0
_MIN_API_INTERVAL = 0.2  # 秒，1000ms/5 = 200ms = 0.2s

def rate_limit():
    """确保API调用频率不超过每秒5次"""
    global _LAST_API_CALL_TIME
    current_time = time.time()
    time_since_last_call = current_time - _LAST_API_CALL_TIME

    if time_since_last_call < _MIN_API_INTERVAL:
        sleep_time = _MIN_API_INTERVAL - time_since_last_call
        time.sleep(sleep_time)

    _LAST_API_CALL_TIME = time.time()

def get_previous_trading_day(base_date: Optional[datetime.date] = None) -> datetime.date:
    """获取上一个交易日（简单版本，不考虑节假日）

    参数:
        base_date: 基准日期，默认今天

    返回:
        上一个交易日的日期
    """
    if base_date is None:
        base_date = datetime.date.today()

    # A股交易日为周一至周五
    # 计算上一个交易日
    if base_date.weekday() == 0:  # 周一
        days_back = 3  # 回到上周五
    elif base_date.weekday() == 6:  # 周日
        days_back = 2  # 回到上周五
    elif base_date.weekday() == 5:  # 周六
        days_back = 1  # 回到上周五
    else:
        days_back = 1  # 周二到周五，回到前一天

    previous_day = base_date - datetime.timedelta(days=days_back)

    print(f"📅 当前日期: {base_date}, 上一个交易日: {previous_day}")
    return previous_day

def get_main_board_stocks() -> List[Dict[str, Any]]:
    """获取所有正常上市的主板股票"""
    token = os.getenv("LIXINGER_TOKEN")
    if not token:
        print("错误：未找到系统变量 LIXINGER_TOKEN，请检查配置。")
        return []

    url = "https://open.lixinger.com/api/cn/company"
    results = []

    print("🚀 开始获取第 0-11 页的『正常上市』主板股票...")

    for i in range(12):
        payload = {
            "token": token,
            "pageIndex": i
        }
        try:
            rate_limit()  # API 速率限制
            response = requests.post(url, json=payload, timeout=15)
            data_json = response.json()

            if data_json.get("code") == 1:
                stocks = data_json.get("data", [])
                for s in stocks:
                    code = s.get("stockCode", "")
                    ex = s.get("exchange", "")
                    status = s.get("listingStatus", "")
                    fs_table_type = s.get("fsTableType", "non_financial")  # 默认为非金融

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
                            "listingStatus": status,
                            "fsTableType": fs_table_type
                        })
                print(f"已处理第 {i} 页，累计匹配数: {len(results)}")
            else:
                print(f"第 {i} 页请求异常: {data_json.get('message')}")

            # 频率控制由 rate_limit() 函数统一管理

        except Exception as e:
            print(f"第 {i} 页执行出错: {e}")

    # 去重
    unique_results = list({s['stockCode']: s for s in results}.values())
    print(f"✅ 获取完成！总计找到正常上市主板股票: {len(unique_results)} 只")
    return unique_results

def get_market_cap_batch(stocks_batch: List[Dict[str, Any]], token: str) -> Dict[str, float]:
    """批量获取股票的市值数据（每批最多100只）

    参数:
        stocks_batch: 股票信息列表，包含stockCode和fsTableType
        token: API token

    返回:
        字典: {股票代码: 市值}
    """
    # 按行业类型分组
    stocks_by_type = {}
    for stock in stocks_batch:
        fs_type = stock.get("fsTableType", "non_financial")
        if fs_type not in stocks_by_type:
            stocks_by_type[fs_type] = []
        stocks_by_type[fs_type].append(stock)

    market_caps = {}

    # 对每个行业类型分别调用API
    for fs_type, type_stocks in stocks_by_type.items():
        # 构建对应行业的API URL
        if fs_type == "non_financial":
            url = "https://open.lixinger.com/api/cn/company/fundamental/non_financial"
        elif fs_type == "bank":
            url = "https://open.lixinger.com/api/cn/company/fundamental/bank"
        elif fs_type == "security":
            url = "https://open.lixinger.com/api/cn/company/fundamental/security"
        elif fs_type == "insurance":
            url = "https://open.lixinger.com/api/cn/company/fundamental/insurance"
        elif fs_type == "other_financial":
            url = "https://open.lixinger.com/api/cn/company/fundamental/other_financial"
        else:
            print(f"⚠️ 未知的行业类型: {fs_type}，使用默认的非金融接口")
            url = "https://open.lixinger.com/api/cn/company/fundamental/non_financial"

        stock_codes = [s["stockCode"] for s in type_stocks]

        # 使用上一个交易日的数据
        query_date = get_previous_trading_day()

        # 根据API文档：当传入startDate时只能传入一个股票代码
        # 因此我们使用date参数获取最新数据
        payload = {
            "token": token,
            "stockCodes": stock_codes,
            "date": query_date.strftime("%Y-%m-%d"),
            "metricsList": ["mc"]        # 只获取市值指标
        }

        # 调试信息
        print(f"  请求参数: date={query_date.strftime('%Y-%m-%d')}, 股票数={len(stock_codes)}")

        try:
            print(f"  调用{fs_type}接口，获取{len(stock_codes)}只股票的市值...")
            rate_limit()  # API 速率限制
            response = requests.post(url, json=payload, timeout=15)
            data_json = response.json()

            if data_json.get("code") == 1:
                data_list = data_json.get("data", [])
                print(f"  成功获取{len(data_list)}条数据")
                # 解析返回数据
                for item in data_list:
                    stock_code = item.get("stockCode")
                    # 根据API示例，mc是直接的数字值，单位可能是元
                    # 示例: "mc": 1717383888142.8 (单位：元)
                    mc_value = item.get("mc")

                    if mc_value is not None:
                        # 转换为亿元（1亿 = 100,000,000）
                        mc_in_billion = mc_value / 100000000
                        market_caps[stock_code] = mc_in_billion
                        print(f"    {stock_code}: {mc_in_billion:.2f} 亿元")
                    else:
                        market_caps[stock_code] = 0
                        print(f"    {stock_code}: 无市值数据")
            else:
                print(f"  获取{fs_type}类型市值数据失败: {data_json}")
                print(f"  错误信息: {data_json.get('message')}")
                print(f"  状态码: {data_json.get('code')}")

            # 频率控制由 rate_limit() 函数统一管理

        except Exception as e:
            print(f"  获取{fs_type}类型市值数据出错: {e}")

    return market_caps

def filter_stocks_by_market_cap(stocks: List[Dict[str, Any]], token: str, min_market_cap: float = 30.0) -> List[Dict[str, Any]]:
    """筛选市值大于指定值的股票（单位：亿元）"""
    filtered_stocks = []

    # 分批处理，每批最多100只股票
    batch_size = 100
    total_batches = (len(stocks) + batch_size - 1) // batch_size

    print(f"📊 开始获取 {len(stocks)} 只股票的市值数据，分 {total_batches} 批处理...")

    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i+batch_size]
        stock_codes = [s["stockCode"] for s in batch]

        print(f"正在处理第 {i//batch_size + 1}/{total_batches} 批，{len(stock_codes)} 只股票...")

        market_caps = get_market_cap_batch(batch, token)

        # 将市值数据添加到股票信息中
        for stock in batch:
            stock_code = stock["stockCode"]
            market_cap = market_caps.get(stock_code, 0)

            if market_cap >= min_market_cap:
                filtered_stocks.append({
                    "stockCode": stock_code,
                    "name": stock["name"],
                    "exchange": stock["exchange"],
                    "listingStatus": stock["listingStatus"],
                    "fsTableType": stock.get("fsTableType", "non_financial"),
                    "marketCap": round(market_cap, 2)  # 保留两位小数
                })

        # API频率控制由 rate_limit() 函数统一管理

    print(f"✅ 市值筛选完成！符合条件（市值 ≥ {min_market_cap} 亿元）的股票: {len(filtered_stocks)} 只")
    return filtered_stocks

def save_to_csv(stocks: List[Dict[str, Any]], filename: str):
    """保存股票数据到CSV文件"""
    if not stocks:
        print("❌ 没有数据可保存")
        return

    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, filename)

    fieldnames = ["stockCode", "name", "exchange", "listingStatus", "fsTableType", "marketCap"]

    with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(stocks)

    print(f"💾 数据已保存到: {file_path}")
    print(f"📁 文件包含 {len(stocks)} 行记录")

def main():
    # 检查环境变量
    token = os.getenv("LIXINGER_TOKEN")
    if not token:
        print("错误：未找到系统变量 LIXINGER_TOKEN，请检查配置。")
        print("请设置环境变量: LIXINGER_TOKEN=你的理杏仁Token")
        return

    print("=" * 50)
    print("🏢 A股主板股票市值筛选器")
    print("=" * 50)

    # 步骤1: 获取所有主板股票
    print("\n📈 步骤1: 获取所有正常上市主板股票...")
    all_stocks = get_main_board_stocks()

    if not all_stocks:
        print("❌ 未能获取到主板股票数据，程序终止")
        return

    # 步骤2: 筛选市值大于30亿的股票
    print(f"\n💰 步骤2: 筛选市值 ≥ 30 亿元的股票...")
    filtered_stocks = filter_stocks_by_market_cap(all_stocks, token, min_market_cap=30.0)

    # 步骤3: 保存结果
    print(f"\n💾 步骤3: 保存筛选结果...")
    save_to_csv(filtered_stocks, "china_main_board_market_cap_filtered.csv")

    # 统计信息
    print("\n" + "=" * 50)
    print("📊 统计信息")
    print("=" * 50)
    print(f"总主板股票数: {len(all_stocks)}")
    print(f"市值 ≥ 30 亿元的股票数: {len(filtered_stocks)}")
    print(f"占比: {len(filtered_stocks)/len(all_stocks)*100:.1f}%")

    # 按市值排序显示前10名
    if filtered_stocks:
        sorted_stocks = sorted(filtered_stocks, key=lambda x: x["marketCap"], reverse=True)
        print(f"\n🏆 市值前10名:")
        for i, stock in enumerate(sorted_stocks[:10], 1):
            print(f"{i:2d}. {stock['stockCode']} {stock['name']}: {stock['marketCap']:.2f} 亿元")

if __name__ == "__main__":
    main()