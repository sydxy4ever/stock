import schedule
import time
import logging
from fetch_all import run_all

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def job():
    logging.info("到达预定时间(01:00)。触发日常抓取任务。")
    run_all()
    logging.info("日常抓取任务全部完成。等待下一次触发...")

# 设定每天凌晨 1:00 运行
schedule.every().day.at("23:00").do(job)

if __name__ == "__main__":
    logging.info("Scheduler 已启动。配置抓取时间：每天凌晨 01:00")
    
    # 防止错过第一次启动可能需要测试的情况，这里可选： 
    run_all() # 如果想要容器启动时立即抓取一次，取消注释此行
    
    while True:
        schedule.run_pending()
        time.sleep(10) # 每10秒检查一次，减少CPU占用
