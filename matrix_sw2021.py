import pandas as pd
import glob
import os

# --- 配置 ---
DETAILS_DIR = "daily_details"
OUTPUT_FILE = "sw3_safety_matrix.csv"

def generate_matrix():
    # 1. 获取所有明细文件并排序
    all_files = glob.glob(os.path.join(DETAILS_DIR, "*.csv"))
    all_files.sort()
    
    if not all_files:
        print("❌ 未在 daily_details 文件夹中发现明细文件，请先运行生成脚本。")
        return

    matrix_data = []

    print(f"📂 正在从 {len(all_files)} 个明细文件中聚合数据...")

    for f in all_files:
        # 从文件名提取日期 (例如 20220104_detail.csv -> 2022-01-04)
        filename = os.path.basename(f)
        date_raw = filename.split('_')[0]
        date_label = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        
        # 读取当天的个股明细
        df = pd.read_csv(f)
        
        # 2. 换手比区间划分 (按照你的要求: 1-1.5, 1.5-2, >2)
        # bins 定义为 [1.0, 1.5, 2.0, 无穷大]
        df['ratio_grp'] = pd.cut(
            df['ratio'], 
            bins=[1.0, 1.5, 2.0, 999.0], 
            labels=['1.0-1.5', '1.5-2.0', '>2.0']
        )
        
        # 剔除不在区间内的数据（例如 ratio < 1 的）
        df = df.dropna(subset=['ratio_grp'])
        
        if df.empty:
            continue

        # 3. 按 [三级行业] 和 [换手区间] 分组计算保本率 (is_safe 的均值)
        # observed=True 确保只显示存在的分类组合
        daily_stats = df.groupby(['sw3_name', 'ratio_grp'], observed=True)['is_safe'].mean()
        
        for (sw3, rg), safety_rate in daily_stats.items():
            # 4. 二值化：胜率 > 80% 标为 1
            is_active = 1 if safety_rate >= 0.8 else 0
            
            matrix_data.append({
                'date': date_label,
                'label': f"{sw3}_{rg}",
                'status': is_active
            })
            
        print(f"📊 已处理日期: {date_label}", end='\r')

    # 5. 构建透视表：纵轴为 label (行业_区间)，横轴为 date
    print("\n\n✨ 正在进行矩阵透视变换...")
    final_df = pd.DataFrame(matrix_data)
    
    # pivot 参数：index 为行，columns 为列，values 为填充内容
    matrix = final_df.pivot(index='label', columns='date', values='status')
    
    # 填充缺失值为 0 (某些行业在某些天可能没有样本)
    matrix = matrix.fillna(0).astype(int)
    
    # 6. 保存结果
    matrix.to_csv(OUTPUT_FILE, encoding="utf-8-sig")
    print(f"🏁 任务完成！胜率矩阵已保存至: {OUTPUT_FILE}")
    print(f"📈 矩阵规模: {matrix.shape[0]} 行 (行业组合) x {matrix.shape[1]} 列 (交易日)")

if __name__ == "__main__":
    generate_matrix()