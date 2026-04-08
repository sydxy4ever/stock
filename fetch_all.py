import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCRIPTS = [
    "fetch_stocks.py",
    "fetch_industries.py",
    "fetch_fundamentals.py",
    "fetch_fs.py",
    "fetch_klines.py"
]

def run_all():
    logging.info("=========================================")
    logging.info("开始执行日常数据抓取任务...")
    logging.info("=========================================")
    
    for script in SCRIPTS:
        logging.info(f"正在启动任务: {script}")
        try:
            # 顺序阻塞运行
            result = subprocess.run(
                [sys.executable, "-u", script], 
                check=True, 
                text=True
            )
            logging.info(f"任务 {script} 成功完成。")
        except subprocess.CalledProcessError as e:
            logging.error(f"-----------------------------------------")
            logging.error(f"任务 {script} 执行失败! 退出码: {e.returncode}")
            logging.error(f"中断后续抓取任务...")
            logging.error(f"-----------------------------------------")
            break
        except Exception as e:
            logging.error(f"-----------------------------------------")
            logging.error(f"运行 {script} 时发生未知错误: {e}")
            logging.error(f"中断后续抓取任务...")
            logging.error(f"-----------------------------------------")
            break

    logging.info("=========================================")
    logging.info("全部可执行抓取阶段已结束。")
    logging.info("=========================================")

if __name__ == "__main__":
    run_all()
