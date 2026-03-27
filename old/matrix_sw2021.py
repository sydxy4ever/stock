import pandas as pd
import glob
import os
import numpy as np

# --- 配置 ---
DETAILS_DIR = "daily_details"
SW_FILE = "./tools/sw2021.csv"
OUTPUT_FILE = "./output/matrix.csv"

def generate_matrix():
    # 1. 加载行业映射
    sw_df = pd.read_csv(SW_FILE, encoding='utf-8')
        
    name_to_code = dict(zip(sw_df['名称.2'], sw_df['三级代码'].astype(str)))
    
    all_files = glob.glob(os.path.join(DETAILS_DIR, "*.csv"))
    all_files.sort()
    
    if not all_files:
        print("❌ 未发现明细文件。")
        return

    matrix_data = []

    print(f"📂 正在聚合数据并识别空样本...")

    for f in all_files:
        date_raw = os.path.basename(f).split('_')[0]
        date_label = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        
        df = pd.read_csv(f)
        
        # 换手比区间划分
        df['ratio_grp'] = pd.cut(
            df['ratio'], 
            bins=[1.0, 1.5, 2.0, 999.0], 
            labels=['1.0-1.5', '1.5-2.0', '>2.0']
        )
        
        # 按照 行业名称 和 换手区间 分组
        # 注意：这里我们只计算均值，如果某组没有数据，mean() 会返回 NaN
        daily_stats = df.groupby(['sw3_name', 'ratio_grp'], observed=True)['is_safe'].mean()
        
        for (sw3_name, rg), safety_rate in daily_stats.items():
            sw3_code = name_to_code.get(sw3_name, "Unknown")
            
            # 逻辑：
            # 如果 safety_rate 是 NaN -> 说明没样本，保持为 NaN
            # 如果 safety_rate >= 0.8 -> 1
            # 否则 -> 0
            if pd.isna(safety_rate):
                val = np.nan
            else:
                val = 1 if safety_rate >= 0.8 else 0
            
            matrix_data.append({
                'date': date_label,
                '行业名称': sw3_name,
                '行业代码': sw3_code,
                '换手区间': rg,
                'status': val
            })
            
        print(f"📊 已处理日期: {date_label}", end='\r')

    # 3. 构建多列索引矩阵
    print("\n\n✨ 正在构建矩阵并标记 NA...")
    full_df = pd.DataFrame(matrix_data)
    
    matrix = full_df.pivot_table(
        index=['行业名称', '行业代码', '换手区间'], 
        columns='date', 
        values='status',
        aggfunc='first'
    )
    
    # 4. 关键：将 NaN 显式填充为字符串 "NA"
    # 注意：一旦填充了 "NA"，整个 DataFrame 会变成 Object 类型，
    # 这样可以保留 1, 0 和 NA 的区分，但不再能直接进行数学运算
    matrix_with_na = matrix.fillna("NA")
    
    # 5. 保存结果
    matrix_with_na.reset_index().to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    
    print(f"🏁 任务完成！矩阵已保存至: {OUTPUT_FILE}")
    print("💡 提示：在 Excel 中，1=高胜率，0=有样本但低胜率，NA=无样本。")

if __name__ == "__main__":
    generate_matrix()