import pandas as pd
import sys
import os

def csv_to_excel(csv_file_path, excel_file_path=None):
    """
    将 CSV 文件转换为 Excel 文件 (.xlsx)
    
    参数:
        csv_file_path (str): 输入的 CSV 文件路径
        excel_file_path (str): 输出的 Excel 文件路径（可选，默认与 CSV 同名但扩展名为 .xlsx）
    """
    if not os.path.isfile(csv_file_path):
        print(f"错误: 找不到文件 {csv_file_path}")
        return

    if excel_file_path is None:
        # 默认输出文件名：将 .csv 替换为 .xlsx
        base_name = os.path.splitext(csv_file_path)[0]
        excel_file_path = base_name + '.xlsx'

    try:
        # 读取 CSV 文件
        df = pd.read_csv(csv_file_path, encoding='utf-8')
    except UnicodeDecodeError:
        # 如果 UTF-8 失败，尝试用 latin1 或其他编码
        df = pd.read_csv(csv_file_path, encoding='latin1')

    # 写入 Excel 文件
    df.to_excel(excel_file_path, index=False, engine='openpyxl')

    print(f"✅ 成功将 '{csv_file_path}' 转换为 '{excel_file_path}'")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python csv_to_excel.py <input.csv> [output.xlsx]")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_excel = sys.argv[2] if len(sys.argv) > 2 else None

    csv_to_excel(input_csv, output_excel)