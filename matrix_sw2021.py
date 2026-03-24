import pandas as pd
import glob
import os

# --- 配置 ---
DETAILS_DIR = "daily_details"
SW_FILE = "./tools/sw2021.csv"
OUTPUT_FILE = "./output/matrix.csv"

def generate_matrix():
    # 1. 加载行业字典 (为了在聚合时反向查询代码)
    sw_df = pd.read_csv(SW_FILE, encoding='utf-8')

        
    # 建立 名称 -> 代码 的映射
    name_to_code = dict(zip(sw_df['名称.2'], sw_df['三级代码'].astype(str)))
    
    # 2. 获取并排序明细文件
    all_files = glob.glob(os.path.join(DETAILS_DIR, "*.csv"))
    all_files.sort()
    
    if not all_files:
        print("❌ 未发现明细文件，请检查 daily_details 文件夹。")
        return

    matrix_data = []

    print(f"📂 正在从 {len(all_files)} 个明细文件中聚合数据...")

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
        df = df.dropna(subset=['ratio_grp'])
        
        if df.empty:
            continue

        # 按 [行业名称] 和 [换手区间] 分组计算保本率均值
        daily_stats = df.groupby(['sw3_name', 'ratio_grp'], observed=True)['is_safe'].mean()
        
        for (sw3_name, rg), safety_rate in daily_stats.items():
            # 获取对应的行业代码
            sw3_code = name_to_code.get(sw3_name, "Unknown")
            is_active = 1 if safety_rate >= 0.8 else 0
            
            matrix_data.append({
                'date': date_label,
                '行业名称': sw3_name,
                '行业代码': sw3_code,
                '换手区间': rg,
                'status': is_active
            })
            
        print(f"📊 已处理日期: {date_label}", end='\r')

    # 3. 构建长表并透视
    print("\n\n✨ 正在构建多列索引矩阵...")
    full_df = pd.DataFrame(matrix_data)
    
    # 使用 pivot_table，将 行业名称、行业代码、换手区间 作为行索引 (index)
    # 将日期作为列 (columns)
    matrix = full_df.pivot_table(
        index=['行业名称', '行业代码', '换手区间'], 
        columns='date', 
        values='status',
        aggfunc='first' # status 已经是唯一的
    )
    
    # 填充缺失值为 0
    matrix = matrix.fillna(0).astype(int)
    
    # 4. 保存结果
    # reset_index() 将多级索引重新变为普通的列，方便在 Excel 中筛选
    matrix.reset_index().to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    
    print(f"🏁 任务完成！矩阵已保存至: {OUTPUT_FILE}")
    print(f"📈 最终规格: {matrix.shape[0]} 行 x {matrix.shape[1]} 列 (日期)")

if __name__ == "__main__":
    generate_matrix()