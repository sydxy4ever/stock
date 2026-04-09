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
STRATEGY_DB_PATH = os.getenv("STRATEGY_DB_PATH", str(_ROOT / "strategy.db"))


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
tab1, tab2, tab3 = st.tabs(["🚀 数据抓取调度", "📊 Turnover Surge 异动策略", "📉 策略回测下载"])

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
        run_script("strategy/strategy.py", f"异动扫描 ({str_date})", args=["--day0", str_date])
        # 执行完毕后刷新缓存数据区
        
    st.markdown("---")
    st.markdown("### 📊 扫描结果预览")
    
    # 加载 SQLite 中的结果
    if not os.path.exists(STRATEGY_DB_PATH):
        st.warning(f"尚未发现策略数据库 [{STRATEGY_DB_PATH}]，请先运行策略扫描。")
    else:
        with sqlite3.connect(STRATEGY_DB_PATH) as conn:
            query = "SELECT * FROM strategy WHERE day0 = ?"
            df = pd.read_sql_query(query, conn, params=(str_date,))
        
        if df.empty:
            st.info(f"{str_date} 没有符合异动条件的标的，或该日数据未被扫描记录。")
        else:
            unique_stocks_count = df["stock_code"].nunique()
            st.success(f"📌 {str_date} 共产生 {unique_stocks_count} 只异动股票标的。")
            
            # --- 精简与美化数据 ---
            # 1. 甄选我们要展示的核心基础字段
            base_cols = [
                "stock_code", "name", "industry_name", 
                "baseline_to_r", "recent1_to_r", "trigger_ratio_1", "recent2_to_r", "trigger_ratio_2", 
                "d0_close", "d0_change_pct",
                "d0_above_ma20", "d0_above_ma60"
            ]
            valid_base = [c for c in base_cols if c in df.columns]
            
            # 每个股票只保留一行基础信息
            base_df = df.drop_duplicates(subset=["stock_code"])[valid_base].set_index("stock_code")
            
            # 2. 提取并透视未来 N 日的涨跌幅
            # 去除没有 day_offset 的脏数据
            fwd = df.dropna(subset=["day_offset"]).copy()
            if not fwd.empty:
                fwd["day_offset"] = pd.to_numeric(fwd["day_offset"], errors="coerce").fillna(0).astype(int)
                pivot_df = fwd.pivot(index="stock_code", columns="day_offset", values="change_pct")
                # 将列名改为 D1_涨幅, D2_涨幅...
                pivot_df.columns = [f"D{c}_涨幅%" for c in pivot_df.columns]
                
                # 拼合到基础表
                final_df = base_df.join(pivot_df).reset_index()
            else:
                final_df = base_df.reset_index()
            
            # 3. 字段格式微调方便阅读
            if "baseline_to_r" in final_df.columns:
                final_df["baseline_to_r"] = (pd.to_numeric(final_df["baseline_to_r"], errors="coerce") * 100).round(2).astype(str) + "%"
            if "recent1_to_r" in final_df.columns:
                final_df["recent1_to_r"] = (pd.to_numeric(final_df["recent1_to_r"], errors="coerce") * 100).round(2).astype(str) + "%"
            if "recent2_to_r" in final_df.columns:
                final_df["recent2_to_r"] = (pd.to_numeric(final_df["recent2_to_r"], errors="coerce") * 100).round(2).astype(str) + "%"
            if "trigger_ratio_1" in final_df.columns:
                final_df["trigger_ratio_1"] = pd.to_numeric(final_df["trigger_ratio_1"], errors="coerce").round(2).astype(str) + " 倍"
            if "trigger_ratio_2" in final_df.columns:
                final_df["trigger_ratio_2"] = pd.to_numeric(final_df["trigger_ratio_2"], errors="coerce").round(2).astype(str) + " 倍"
            
            if "d0_change_pct" in final_df.columns:
                final_df["d0_change_pct"] = (pd.to_numeric(final_df["d0_change_pct"], errors="coerce") * 100).round(2).astype(str) + "%"
            
            # 把前瞻涨幅也乘以 100 变成百分比形式
            for col in final_df.columns:
                if col.startswith("D") and "_涨幅" in col:
                    final_df[col] = (pd.to_numeric(final_df[col], errors="coerce") * 100).round(2).astype(str) + "%"
            
            # 重命名表头为中文方便阅读
            rename_dict = {
                "stock_code": "股票代码",
                "name": "股票名称",
                "industry_name": "所属行业",
                "baseline_to_r": "基准换手率",
                "recent1_to_r": "一阶换手",
                "trigger_ratio_1": "一阶倍数",
                "recent2_to_r": "二阶换手",
                "trigger_ratio_2": "二阶倍数",
                "d0_close": "收盘价",
                "d0_change_pct": "当日涨幅",
                "d0_above_ma20": "站上MA20",
                "d0_above_ma60": "站上MA60"
            }
            final_df = final_df.rename(columns=rename_dict)
            
            # 默认按二阶倍数从大到小排
            if "二阶倍数" in final_df.columns:
                # 排序前把 " 倍" 截掉变成 Float 进行排序
                final_df["_sort_val"] = final_df["二阶倍数"].str.replace(" 倍", "").astype(float)
                final_df = final_df.sort_values("_sort_val", ascending=False).drop(columns=["_sort_val"])

            st.dataframe(final_df, use_container_width=True, hide_index=True)

with tab3:
    st.markdown("### 📉 策略回测数据导出")
    st.info("从数据库中提取历史区间内的策略触发记录及后续多日跟踪行情，导出为 CSV 文件供本地回测分析。")
    
    col_bt1, col_bt2 = st.columns(2)
    with col_bt1:
        bt_start = st.date_input("起始日期", value=pd.to_datetime("2019-01-01").date())
    with col_bt2:
        bt_end = st.date_input("结束日期", value=pd.to_datetime("2026-03-01").date())
        
    str_start = bt_start.strftime("%Y-%m-%d")
    str_end = bt_end.strftime("%Y-%m-%d")
    
    if st.button("🔍 提取回测数据", type="primary", use_container_width=True):
        st.session_state["bt_start"] = str_start
        st.session_state["bt_end"] = str_end

    # 通过 st.session_state 确保即便执行到 download_button 的时候，页面重载之后依然可以提供真实的数据
    if "bt_start" in st.session_state and "bt_end" in st.session_state:
        q_start = st.session_state["bt_start"]
        q_end = st.session_state["bt_end"]
        
        if not os.path.exists(STRATEGY_DB_PATH):
            st.error("尚未生成策略数据库。请先在【异动策略】页面执行扫描。")
        else:
            with st.spinner(f"正在读取 {q_start} 到 {q_end} 的数据..."):
                with sqlite3.connect(STRATEGY_DB_PATH) as conn:
                    query = "SELECT * FROM strategy WHERE day0 >= ? AND day0 <= ? ORDER BY day0, stock_code, day_offset"
                    try:
                        df_bt = pd.read_sql_query(query, conn, params=(q_start, q_end))
                    except Exception as e:
                        df_bt = pd.DataFrame()
                        st.error(f"提取失败: {e}")
            
            if df_bt.empty:
                st.warning(f"区间 {q_start} 至 {q_end} 没有扫描记录，或数据为空。")
            else:
                st.success(f"✅ 查询成功！在 {q_start} 至 {q_end} 期间，包含 {len(df_bt)} 条记录详细行情数据。")
                
                # 防止由于中文字符串导致在 Excel 乱码，建议以 utf-8-sig 编码
                csv_bytes = df_bt.to_csv(index=False).encode("utf-8-sig")
                
                st.download_button(
                    label=f"💾 点击下载 CSV 文件 ({q_start} ~ {q_end})",
                    data=csv_bytes,
                    file_name=f"turnover_surge_backtest_{q_start}_to_{q_end}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

