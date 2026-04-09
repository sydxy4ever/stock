import os
import sys
import time
import sqlite3
import subprocess
import pandas as pd
import streamlit as st
from pathlib import Path

# --- 页面基本配置 ---
st.set_page_config(
    page_title="量化分析调度控制台",
    page_icon="📈",
    layout="wide",
)

st.title("📈 量化分析调度与策略可视化")

# --- 数据库路径配置 ---
_ROOT = Path(__file__).parent
DB_PATH = os.getenv("DB_PATH", str(_ROOT / "stock_data.db"))
TURNOVER_DB_PATH = os.getenv("TURNOVER_DB_PATH", str(_ROOT / "turnover_surge.db"))


def get_latest_trade_date():
    """获取数据库里面最新一天的K线日期，如果没有则返回本地当天"""
    try:
        if os.path.exists(DB_PATH):
            with sqlite3.connect(DB_PATH) as conn:
                res = conn.execute("SELECT MAX(date) FROM daily_kline").fetchone()
                if res and res[0]:
                    return res[0]
    except Exception:
        pass
    import datetime
    return datetime.date.today().strftime("%Y-%m-%d")

# --- 脚本执行相关的流式捕获函数 ---
def run_script(script_path, title, args=None):
    st.subheader(f"执行任务：{title}")
    
    # 防止按钮互斥或误触，可以使用 st.spinner
    cmd = [sys.executable, "-u", script_path]
    if args:
        cmd.extend(args)
    
    output_container = st.empty()
    logs = []
    
    with st.spinner(f"{title} 运行中..."):
        start_time = time.time()
        try:
            # 开启子进程
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            # 流式读取 stdout
            for line in process.stdout:
                line_content = line.strip()
                logs.append(line_content)
                # 为了防止页面卡顿，这里我们最多保留末尾100行在界面一直刷新
                display_logs = logs[-50:]
                log_text = "\n".join(display_logs)
                output_container.code(log_text, language="log")

            process.wait()
            elapsed = time.time() - start_time
            
            if process.returncode == 0:
                st.success(f"✅ {title} 执行完成！耗时: {elapsed:.1f} 秒")
            else:
                st.error(f"❌ {title} 异常退出，返回码: {process.returncode}")
                
        except Exception as e:
            st.error(f"执行发生异常: {str(e)}")

# --- UI：左右分栏结构 ---
tab1, tab2 = st.tabs(["🚀 数据抓取调度", "📊 Turnover Surge 异动策略"])

with tab1:
    st.markdown("### 👉 分步 / 全量数据抓取")
    st.info("提示：这会直接调用对应的 Python 脚本。如果配置了 LIXINGER_TOKEN 就可以生效。")
    
    c1, c2, c3 = st.columns(3)
    c4, c5, c6 = st.columns(3)
    
    if c1.button("1. 获取股票池 (fetch_stocks)", use_container_width=True):
        run_script("fetchers/fetch_stocks.py", "获取股票池")
    if c2.button("2. 获取申万行业 (fetch_industries)", use_container_width=True):
        run_script("fetchers/fetch_industries.py", "获取申万行业")
    if c3.button("3. 获取基本面 (fetch_fundamentals)", use_container_width=True):
        run_script("fetchers/fetch_fundamentals.py", "获取基本面")
        
    if c4.button("4. 获取财报数据 (fetch_fs)", use_container_width=True):
        run_script("fetchers/fetch_fs.py", "获取财报数据")
    if c5.button("5. 获取日K线 (fetch_klines)", use_container_width=True):
        run_script("fetchers/fetch_klines.py", "获取日K线数据")
    if c6.button("6. 计算缺失均线 (compute_ma)", use_container_width=True):
        run_script("tools/compute_ma.py", "计算均线数据")
    
    st.markdown("---")
    if st.button("🔥 一键执行全日任务流水线 (fetch_all)", type="primary", use_container_width=True):
        run_script("fetch_all.py", "全日调度 (fetch_all.py)")


with tab2:
    st.markdown("### 🔎 扫描异动信号")
    
    # 巧妙：默认获取数据库中最后一日
    latest_date = get_latest_trade_date()
    # 仅保留日期以匹配选择器格式
    default_date_val = pd.to_datetime(latest_date).date()
    
    col_date, col_btn = st.columns([1, 1])
    with col_date:
        selected_date = st.date_input("选择扫描启动日 (Day0)", value=default_date_val)
    with col_btn:
        st.write("") # Spacer
        st.write("") # Spacer
        scan_btn = st.button("🚀 执行扫描", type="primary", use_container_width=True)
    
    str_date = selected_date.strftime("%Y-%m-%d")
    
    if scan_btn:
        run_script("strategy/turnover_surge.py", f"异动扫描 ({str_date})", args=["--day0", str_date])
        # 执行完毕后刷新缓存数据区
        
    st.markdown("---")
    st.markdown("### 📊 扫描结果预览")
    
    # 加载 SQLite 中的结果
    if not os.path.exists(TURNOVER_DB_PATH):
        st.warning(f"尚未发现策略数据库 [{TURNOVER_DB_PATH}]，请先运行策略扫描。")
    else:
        with sqlite3.connect(TURNOVER_DB_PATH) as conn:
            query = "SELECT * FROM turnover_surge WHERE day0 = ?"
            df = pd.read_sql_query(query, conn, params=(str_date,))
        
        if df.empty:
            st.info(f"{str_date} 没有符合异动条件的标的，或该日数据未被扫描记录。")
        else:
            st.success(f"📌 {str_date} 共产生 {len(df)} 条跟踪记录（包含了该日产生信号的股票以及它未来9日的走势）")
            # 因为数据是每日拆开的 (day_offset 1~9)，我们可以透视一下更适合观看：
            # 这里先直接展示原始宽表，让用户能直观排序和过滤
            st.dataframe(df, use_container_width=True, hide_index=True)
