import subprocess
import sys
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCRIPTS = [
    "fetchers/fetch_stocks.py",
    "fetchers/fetch_industries.py",
    "fetchers/fetch_fundamentals.py",
    "fetchers/fetch_fs.py",
    "fetchers/fetch_klines.py",
]

def run_all():
    start_time = time.time()
    
    logging.info("=========================================")
    logging.info("开始执行日常数据抓取任务...")
    logging.info("=========================================")
    
    total_429_count = 0
    
    for script in SCRIPTS:
        logging.info(f"正在启动任务: {script}")
        try:
            # 使用 Popen 启动子进程，以便实时读取和处理日志
            process = subprocess.Popen(
                [sys.executable, "-u", script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            # 实时读取 stdout 并同步输出到终端，同时累加 429 错误
            for line in process.stdout:
                line_content = line.strip()
                # 打印到控制台让 Portainer 看到实时日志
                print(line_content, flush=True)
                
                # 统计 429 报错关键字
                if "HTTP 429" in line:
                    total_429_count += 1
            
            process.wait()
            
            if process.returncode == 0:
                logging.info(f"任务 {script} 成功完成。")
            else:
                logging.error(f"任务 {script} 异常终止，退出码: {process.returncode}")
                # 即使一个失败了，我们也继续后续脚本还是中断？用户之前逻辑是中断
                logging.error("中断后续抓取任务...")
                break
                
        except Exception as e:
            logging.error(f"-----------------------------------------")
            logging.error(f"运行 {script} 时发生未知错误: {e}")
            logging.error(f"中断后续抓取任务...")
            logging.error(f"-----------------------------------------")
            break

    logging.info("=========================================")
    logging.info("全部可执行抓取阶段已结束。")
    if total_429_count > 0:
        logging.warning(f"⚠️  统计报告：本次执行全过程共触发 {total_429_count} 次 HTTP 429 限流错误。")
        logging.warning("建议：考虑进一步调大 API_INTERVAL 间隔或检查网络代理。")
    else:
        logging.info("✅ 统计报告：本次执行未触发任何 HTTP 429 限流错误，运行非常稳健。")
        
    elapsed = time.time() - start_time
    logging.info(f"⏱️  总耗时: {elapsed/60:.2f} 分钟 ({elapsed:.1f} 秒)")
    logging.info("=========================================")

if __name__ == "__main__":
    run_all()
