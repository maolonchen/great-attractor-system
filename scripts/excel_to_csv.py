import sys
import os
import pandas as pd

# 必需字段（用于自动检测表头）
REQUIRED_HEADERS = ["字段名称", "字段注释", "字段类型", "字段样本"]

def find_header_row(df_preview, required_headers):
    """
    在前 20 行中查找包含所有 REQUIRED_HEADERS 的行作为表头
    返回 (header_row_index_in_df, actual_header_list)
    """
    required_set = set(required_headers)
    for i in range(min(20, len(df_preview))):
        row = df_preview.iloc[i]
        # 尝试把这一行当作列名
        candidate_headers = [str(x).strip() for x in row if pd.notna(x)]
        if required_set.issubset(set(candidate_headers)):
            return i, list(row)
    return None, None

def convert_excel_to_csv(excel_path):
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"文件不存在: {excel_path}")

    try:
        xl = pd.ExcelFile(excel_path)
    except Exception as e:
        raise RuntimeError(f"无法打开 Excel 文件: {e}")

    output_files = []

    for sheet_name in xl.sheet_names:
        print(f"🔍 正在处理 Sheet: {sheet_name}")

        # 先读前 20 行用于检测表头
        preview_df = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=20, header=None, dtype=str)

        header_idx, headers = find_header_row(preview_df, REQUIRED_HEADERS)
        if header_idx is None:
            print(f"⚠️  Sheet '{sheet_name}' 未找到包含所有必需字段的表头，跳过")
            continue

        # 重新读取整个 sheet，指定 header 行
        df = pd.read_excel(
            excel_path,
            sheet_name=sheet_name,
            header=header_idx,
            dtype=str,
            keep_default_na=False,
            na_filter=False
        )

        # 确保必需字段存在
        missing = set(REQUIRED_HEADERS) - set(df.columns)
        if missing:
            print(f"⚠️  Sheet '{sheet_name}' 实际缺少字段: {missing}，跳过")
            continue

        # 构造输出路径
        base_name = os.path.splitext(excel_path)[0]
        if len(xl.sheet_names) == 1:
            csv_path = f"{base_name}.csv"
        else:
            safe_sheet = "".join(c if c.isalnum() or c in (' ', '_') else '_' for c in sheet_name)
            csv_path = f"{base_name}_{safe_sheet}.csv"

        # 保存为 CSV（UTF-8 with BOM）
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        output_files.append(csv_path)
        print(f"✅ 已保存: {csv_path}")

    if not output_files:
        raise RuntimeError("❌ 未找到任何有效的数据表，请检查 Excel 文件内容和字段名")

    return output_files

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("使用方法: python convert_excel_to_csv.py <input.xlsx>")
        sys.exit(1)

    excel_file = sys.argv[1]
    try:
        csv_files = convert_excel_to_csv(excel_file)
        print(f"\n🎉 转换完成！共生成 {len(csv_files)} 个 CSV 文件:")
        for f in csv_files:
            print(f"  - {f}")
    except Exception as e:
        print(f"❌ 转换失败: {e}")
        sys.exit(1)