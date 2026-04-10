"""
pages/1_信号分析.py
换手率异动信号 — 交互式过滤分析

Filters:
  - 基准期 MA20/60/200 强弱 (L1/L2/L3)
  - Day-1 均线位置 (在线上/下)
  - Day0 均线状态 (上穿/在线上/在线下)
  - Day-1 涨停是否
  - Day0 涨跌幅范围

Stats shown:
  - 信号数, 非下跌日%, 最大涨跌幅, CV
  - D1-D9 日均涨幅柱状图
  - D1-D9 累计涨幅折线图
  - 最大涨跌幅分布直方图
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path

# ─── 页面配置 ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="信号交互分析 | 换手率异动",
    page_icon="🔬",
    layout="wide",
)

import os

_ROOT = Path(__file__).parent.parent
SURGE_DB = os.getenv("TURNOVER_DB_PATH", str(_ROOT / "turnover_surge.db"))

# ─── 样式 ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* 侧边栏背景 */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
[data-testid="stSidebar"] [data-testid="stMarkdown"] h3 {
    color: #38bdf8 !important;
    border-bottom: 1px solid #334155;
    padding-bottom: 4px;
    margin-top: 12px;
}

/* 指标卡片 */
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #1e293b, #0f172a);
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 16px;
    box-shadow: 0 4px 16px rgba(0,0,0,0.3);
}
[data-testid="metric-container"] label {
    color: #94a3b8 !important;
    font-size: 0.78rem;
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #f1f5f9 !important;
    font-size: 1.6rem;
    font-weight: 700;
}

/* 筛选结果横幅 */
.result-banner {
    background: linear-gradient(90deg, #0ea5e9, #6366f1);
    border-radius: 10px;
    padding: 10px 20px;
    margin: 8px 0 20px 0;
    color: white;
    font-weight: 600;
    font-size: 1.05rem;
}

/* 分组标题 */
.section-title {
    color: #38bdf8;
    font-size: 1.1rem;
    font-weight: 700;
    margin: 24px 0 8px 0;
    border-left: 3px solid #38bdf8;
    padding-left: 10px;
}
</style>
""", unsafe_allow_html=True)


# ─── 数据加载与预处理 (带缓存) ───────────────────────────────────────────────

@st.cache_data(show_spinner="📊 加载信号数据库...")
def load_events() -> pd.DataFrame:
    """加载 turnover_surge.db 并预计算所有衍生指标"""
    if not Path(SURGE_DB).exists():
        return pd.DataFrame()

    conn = sqlite3.connect(SURGE_DB)
    df = pd.read_sql("SELECT * FROM turnover_surge", conn)
    conn.close()

    if df.empty:
        return df

    # ── 跟踪期指标 ─────────────────────────────────────────
    df["d0_close"] = pd.to_numeric(df["d0_close"], errors="coerce")
    close_cols = [f"d{i}_close" for i in range(1, 10) if f"d{i}_close" in df.columns]
    pct_cols   = [f"d{i}_change_pct" for i in range(1, 10) if f"d{i}_change_pct" in df.columns]
    tc_cols    = [f"d{i}_total_change_pct" for i in range(1, 10) if f"d{i}_total_change_pct" in df.columns]

    closes = df[close_cols].apply(pd.to_numeric, errors="coerce")
    pcts   = df[pct_cols].apply(pd.to_numeric, errors="coerce")

    df["track_days"]       = closes.notna().sum(axis=1)
    df["non_decline_days"] = (pcts >= 0).sum(axis=1)
    df["max_close"]        = closes.max(axis=1)
    df["min_close"]        = closes.min(axis=1)

    valid = df[(df["track_days"] >= 3) & (df["d0_close"] > 0)].copy()
    valid["max_gain"]         = valid["max_close"] / valid["d0_close"] - 1
    valid["max_drawdown"]     = 1 - valid["min_close"] / valid["d0_close"]
    valid["non_decline_rate"] = valid["non_decline_days"] / valid["track_days"]
    valid["d0_change_pct"]    = pd.to_numeric(valid["d0_change_pct"], errors="coerce")

    # ── 基准期均线强弱 ─────────────────────────────────────
    for ma in [20, 60, 200]:
        a = pd.to_numeric(valid.get(f"bl_above_ma{ma}", 0), errors="coerce").fillna(0)
        b = pd.to_numeric(valid.get(f"bl_below_ma{ma}", 0), errors="coerce").fillna(0)
        total = a + b
        frac = b / total.where(total > 0, other=np.nan)
        lv = pd.cut(frac, bins=[-0.001, 1/3, 2/3, 1.001], labels=["L1强", "L2中", "L3弱"])
        valid[f"bl_lv{ma}"] = lv.astype(str).replace("nan", "未知")

    return valid


# ─── 统计计算 ─────────────────────────────────────────────────────────────────

def _cv(s: pd.Series) -> float:
    m = s.mean()
    return s.std(ddof=1) / abs(m) if (not pd.isna(m) and m != 0) else np.nan


def compute_stats(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    g = df["max_gain"]
    d = df["max_drawdown"]
    return {
        "count":          len(df),
        "non_decline_avg": df["non_decline_rate"].mean() * 100,
        # 涨幅 (max_gain)
        "gain_p10":  g.quantile(0.10) * 100,
        "gain_q25":  g.quantile(0.25) * 100,
        "gain_med":  g.quantile(0.50) * 100,
        "gain_q75":  g.quantile(0.75) * 100,
        "gain_p90":  g.quantile(0.90) * 100,
        "gain_skew": float(g.skew()),
        # 跌幅 (max_drawdown)
        "dd_p10":    d.quantile(0.10) * 100,
        "dd_q25":    d.quantile(0.25) * 100,
        "dd_med":    d.quantile(0.50) * 100,
        "dd_q75":    d.quantile(0.75) * 100,
        "dd_p90":    d.quantile(0.90) * 100,
        "dd_skew":   float(d.skew()),
    }


# ─── 主界面 ──────────────────────────────────────────────────────────────────

def main():
    st.title("🔬 换手率异动信号 — 交互式过滤分析")

    events = load_events()

    if events.empty:
        st.error("❌ 未找到数据。请先运行 `python ./strategy/turnover_surge.py` 生成数据库。")
        return

    total = len(events)

    # ═══════════════════════════════════════════════════════
    # 侧边栏筛选
    # ═══════════════════════════════════════════════════════
    with st.sidebar:
        st.markdown("## ⚙️ 筛选条件")
        st.caption(f"全量信号：**{total:,}** 条")

        if st.button("🔄 重置所有筛选", use_container_width=True, type="secondary"):
            st.rerun()

        st.divider()

        # ── 基准期均线强弱 ─────────────────────────────────
        st.markdown("### 📊 基准期均线强弱")
        st.caption("前30个交易日收盘价高于/低于均线的天数比例")
        bl20  = st.multiselect("MA20",  ["L1强", "L2中", "L3弱"], default=[], key="bl20")
        bl60  = st.multiselect("MA60",  ["L1强", "L2中", "L3弱"], default=[], key="bl60")
        bl200 = st.multiselect("MA200", ["L1强", "L2中", "L3弱"], default=[], key="bl200")

        # ── D-5 到 Day0 多日状态过滤矩阵 ────────────────────────────
        st.markdown("### 🗓️ 多日状态过滤 (D-5 ~ D0)")
        days = ["D-5 (dm5)", "D-4 (dm4)", "D-3 (dm3)", "D-2 (dm2)", "D-1 (dm1)", "Day0 (d0)"]
        prefixes = ["dm5", "dm4", "dm3", "dm2", "dm1", "d0"]
        opt_ma = ["不限", "上穿↑", "在之上", "在之下"]
        opt_lu = ["不限", "是", "否"]
        
        default_data = {
            "天数": days,
            "前缀": prefixes,
            "MA20": ["不限"] * 6,
            "MA60": ["不限"] * 6,
            "MA200": ["不限"] * 6,
            "涨停": ["不限"] * 6,
            "最小涨幅%": [-20.0] * 6,
            "最大涨幅%": [25.0] * 6,
        }
        
        matrix_df = pd.DataFrame(default_data)
        
        edited_df = st.data_editor(
            matrix_df,
            column_config={
                "天数": st.column_config.TextColumn("天数", disabled=True),
                "前缀": None, # 隐藏
                "MA20": st.column_config.SelectboxColumn("MA20", options=opt_ma, default="不限"),
                "MA60": st.column_config.SelectboxColumn("MA60", options=opt_ma, default="不限"),
                "MA200": st.column_config.SelectboxColumn("MA200", options=opt_ma, default="不限"),
                "涨停": st.column_config.SelectboxColumn("涨停", options=opt_lu, default="不限"),
                "最小涨幅%": st.column_config.NumberColumn("最小涨幅%", step=0.5, format="%.1f"),
                "最大涨幅%": st.column_config.NumberColumn("最大涨幅%", step=0.5, format="%.1f"),
            },
            hide_index=True,
            use_container_width=True
        )

    # ═══════════════════════════════════════════════════════
    # 应用筛选
    # ═══════════════════════════════════════════════════════
    f = events.copy()

    # 基准期均线
    if bl20:  f = f[f["bl_lv20"].isin(bl20)]
    if bl60:  f = f[f["bl_lv60"].isin(bl60)]
    if bl200: f = f[f["bl_lv200"].isin(bl200)]

    # 矩阵过滤逻辑
    for _, row in edited_df.iterrows():
        prefix = row["前缀"]
        if prefix not in ["dm5", "dm4", "dm3", "dm2", "dm1", "d0"]:
            continue
            
        # 涨跌幅范围
        min_pct = row["最小涨幅%"]
        max_pct = row["最大涨幅%"]
        if f"{prefix}_change_pct" in f.columns:
            pct_col = pd.to_numeric(f[f"{prefix}_change_pct"], errors="coerce") * 100
            # 放行 NaN 或者处于区间的
            f = f[(pct_col >= min_pct) & (pct_col <= max_pct) | pct_col.isna()]

        # 涨停
        lu = row["涨停"]
        if lu == "是" and f"{prefix}_is_limit_up" in f.columns:
            f = f[pd.to_numeric(f[f"{prefix}_is_limit_up"], errors="coerce").fillna(0) == 1]
        elif lu == "否" and f"{prefix}_is_limit_up" in f.columns:
            f = f[pd.to_numeric(f[f"{prefix}_is_limit_up"], errors="coerce").fillna(0) != 1]

        # 均线位置 & 穿线
        for ma in [20, 60, 200]:
            sel = row[f"MA{ma}"]
            if sel == "上穿↑" and f"{prefix}_pierce_ma{ma}" in f.columns:
                f = f[f[f"{prefix}_pierce_ma{ma}"] == 1]
            elif sel == "在之上" and f"{prefix}_above_ma{ma}" in f.columns:
                f = f[f[f"{prefix}_above_ma{ma}"] == 1]
            elif sel == "在之下" and f"{prefix}_above_ma{ma}" in f.columns:
                f = f[f[f"{prefix}_above_ma{ma}"] == 0]

    n = len(f)

    # ═══════════════════════════════════════════════════════
    # 结果展示
    # ═══════════════════════════════════════════════════════
    pct_of_total = n / total * 100 if total else 0
    st.markdown(
        f'<div class="result-banner">✅ 当前筛选：<strong>{n:,}</strong> 条信号'
        f'（占总量 {pct_of_total:.1f}%）</div>',
        unsafe_allow_html=True,
    )

    if n == 0:
        st.warning("当前筛选条件下没有信号，请放宽条件。")
        return

    stats = compute_stats(f)

    # ── 指标卡片：非下跌日 + 信号数 ──────────────────────────
    c1, c2 = st.columns(2)
    c1.metric("信号数", f"{n:,}")
    c2.metric("非下跌日%", f"{stats['non_decline_avg']:.1f}%")

    # ── 涨幅五数概括 ────────────────────────────────────────
    st.markdown('<div class="section-title">📈 最大涨幅分布（五数概括）</div>', unsafe_allow_html=True)
    g1, g2, g3, g4, g5, g6 = st.columns(6)
    g1.metric("P10（底部10%）",   f"{stats['gain_p10']:.2f}%")
    g2.metric("Q25（下四分位）",   f"{stats['gain_q25']:.2f}%")
    g3.metric("中位数 P50",        f"{stats['gain_med']:.2f}%")
    g4.metric("Q75（上四分位）",   f"{stats['gain_q75']:.2f}%")
    g5.metric("P90（顶部10%）",   f"{stats['gain_p90']:.2f}%")
    g6.metric("偏度",              f"{stats['gain_skew']:.2f}")

    # ── 跌幅五数概括 ────────────────────────────────────────
    st.markdown('<div class="section-title">📉 最大跌幅分布（五数概括）</div>', unsafe_allow_html=True)
    d1, d2, d3, d4, d5, d6 = st.columns(6)
    d1.metric("P10（底部10%）",   f"{stats['dd_p10']:.2f}%")
    d2.metric("Q25（下四分位）",   f"{stats['dd_q25']:.2f}%")
    d3.metric("中位数 P50",        f"{stats['dd_med']:.2f}%")
    d4.metric("Q75（上四分位）",   f"{stats['dd_q75']:.2f}%")
    d5.metric("P90（顶部10%）",   f"{stats['dd_p90']:.2f}%")
    d6.metric("偏度",              f"{stats['dd_skew']:.2f}")

    st.divider()

    # ── 图表区 ─────────────────────────────────────────────
    left_col, right_col = st.columns([3, 2])

    with left_col:

        def _quartile_band_chart(cols, title, label_fn):
            """通用：为一组数值列绘制 Q25/中位数/Q75 分位色带图"""
            labels, q25s, meds, q75s = [], [], [], []
            for col in cols:
                if col not in f.columns:
                    continue
                v = pd.to_numeric(f[col], errors="coerce").dropna() * 100
                if len(v) < 3:
                    continue
                labels.append(label_fn(col))
                q25s.append(float(v.quantile(0.25)))
                meds.append(float(v.quantile(0.50)))
                q75s.append(float(v.quantile(0.75)))

            if not labels:
                return None

            fig = go.Figure()
            # Q25~Q75 色带
            fig.add_trace(go.Scatter(
                x=labels + labels[::-1],
                y=q75s + q25s[::-1],
                fill="toself",
                fillcolor="rgba(56,189,248,0.18)",
                line=dict(color="rgba(0,0,0,0)"),
                name="Q25~Q75区间",
                showlegend=True,
                hoverinfo="skip",
            ))
            # Q25 虚线
            fig.add_trace(go.Scatter(
                x=labels, y=q25s,
                mode="lines",
                line=dict(color="#64748b", width=1, dash="dot"),
                name="Q25",
            ))
            # Q75 虚线
            fig.add_trace(go.Scatter(
                x=labels, y=q75s,
                mode="lines",
                line=dict(color="#64748b", width=1, dash="dot"),
                name="Q75",
            ))
            # 中位数实线
            med_colors = ["#22c55e" if v >= 0 else "#ef4444" for v in meds]
            fig.add_trace(go.Scatter(
                x=labels, y=meds,
                mode="lines+markers+text",
                line=dict(color="#38bdf8", width=2.5),
                marker=dict(color=med_colors, size=9, line=dict(color="#38bdf8", width=1.5)),
                text=[f"{v:.2f}%" for v in meds],
                textposition="top center",
                name="中位数",
            ))
            fig.add_hline(y=0, line_dash="dot", line_color="#475569", line_width=1)
            fig.update_layout(
                title=title,
                yaxis_title="%",
                template="plotly_dark",
                height=370,
                margin=dict(t=50, b=20, l=20, r=20),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", y=1.08, x=0),
            )
            return fig

        # D1-D9 当日涨跌幅
        fig_daily = _quartile_band_chart(
            cols=[f"d{i}_change_pct" for i in range(1, 10)],
            title="📊 D1~D9 当日涨跌幅（中位数 + Q25/Q75色带）",
            label_fn=lambda c: c.replace("_change_pct", "").upper(),
        )
        if fig_daily:
            st.plotly_chart(fig_daily, use_container_width=True)

        # D1-D9 累计涨跌幅
        fig_cum = _quartile_band_chart(
            cols=[f"d{i}_total_change_pct" for i in range(1, 10)],
            title="📈 D1~D9 累计涨跌幅（相对Day0，中位数 + Q25/Q75色带）",
            label_fn=lambda c: c.split("_total")[0].upper(),
        )
        if fig_cum:
            st.plotly_chart(fig_cum, use_container_width=True)

    with right_col:
        # 最大涨幅 & 最大跌幅 箱线图
        gain_vals = (f["max_gain"] * 100).dropna().tolist()
        dd_vals   = (f["max_drawdown"] * 100).dropna().tolist()

        fig_box = go.Figure()
        fig_box.add_trace(go.Box(
            y=gain_vals,
            name="最大涨幅",
            marker_color="#34d399",
            boxmean="sd",           # 同时显示均值和标准差
            boxpoints=False,        # 不画离群点，避免五万条数据导致前端渲染崩溃而白屏
            line_color="#34d399",
            fillcolor="rgba(52,211,153,0.2)",
        ))
        fig_box.add_trace(go.Box(
            y=dd_vals,
            name="最大跌幅",
            marker_color="#f87171",
            boxmean="sd",
            boxpoints=False,
            line_color="#f87171",
            fillcolor="rgba(248,113,113,0.2)",
        ))
        fig_box.update_layout(
            title="涨跌幅分布箱线图（含均值菱形 ◆）",
            yaxis_title="%",
            template="plotly_dark",
            height=660,
            margin=dict(t=50, b=20, l=20, r=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", y=1.06),
        )
        st.plotly_chart(fig_box, use_container_width=True)

    st.divider()

    # ── 信号明细表 ─────────────────────────────────────────
    st.markdown('<div class="section-title">📋 筛选信号明细</div>', unsafe_allow_html=True)

    display_cols = {
        "day0": "日期",
        "stock_code": "股票代码",
        "name": "名称",
        "industry_name": "行业",
        "trigger_ratio_2": "二阶倍数",
        "non_decline_rate": "非下跌率",
        "max_gain": "最大涨幅",
        "max_drawdown": "最大跌幅",
        "d0_change_pct": "d0涨跌",
        "dm1_change_pct": "dm1涨跌",
        "dm2_change_pct": "dm2涨跌",
        "dm3_change_pct": "dm3涨跌",
    }

    show_df = f[[c for c in display_cols if c in f.columns]].rename(columns=display_cols).copy()

    pct_list = ["d0_change_pct", "dm1_change_pct", "dm2_change_pct", "dm3_change_pct", "non_decline_rate", "max_gain", "max_drawdown"]
    for col_orig, col_name in display_cols.items():
        if col_name in show_df.columns and col_orig in pct_list:
            show_df[col_name] = (pd.to_numeric(show_df[col_name], errors="coerce") * 100).round(2).astype(str) + "%"

    st.dataframe(show_df.sort_values("日期", ascending=False), use_container_width=True, hide_index=True)

    # 下载按钮
    csv = f.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "💾 下载筛选结果 CSV",
        data=csv,
        file_name="filtered_signals.csv",
        mime="text/csv",
        use_container_width=True,
    )


main()
