#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel处理器模块
负责将Excel文件转换为CSV格式，以及将处理后的CSV转换回Excel格式
"""

import os
import pandas as pd

# 必需字段（用于自动检测表头）
REQUIRED_HEADERS = ["字段名称", "字段注释", "字段类型", "字段样本"]


class ExcelProcessor:
    """Excel文件处理器"""

    @staticmethod
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

    def convert_excel_to_csv(self, excel_path):
        """
        将Excel文件转换为CSV文件
        
        参数:
            excel_path (str): 输入的Excel文件路径
            
        返回:
            list: 生成的CSV文件路径列表
        """
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

            header_idx, headers = self.find_header_row(preview_df, REQUIRED_HEADERS)
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

            # 构造输出路径 - 保存到data/raw目录
            base_name = os.path.splitext(os.path.basename(excel_path))[0]
            if len(xl.sheet_names) == 1:
                csv_path = f"data/raw/{base_name}.csv"
            else:
                safe_sheet = "".join(c if c.isalnum() or c in (' ', '_') else '_' for c in sheet_name)
                csv_path = f"data/raw/{base_name}_{safe_sheet}.csv"

            # 保存为 CSV（UTF-8 with BOM）
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            output_files.append(csv_path)
            print(f"✅ 已保存: {csv_path}")

        if not output_files:
            raise RuntimeError("❌ 未找到任何有效的数据表，请检查 Excel 文件内容和字段名")

        return output_files

    @staticmethod
    def csv_to_excel(csv_file_path, excel_file_path=None):
        """
        将 CSV 文件转换为 Excel 文件 (.xlsx)
        
        参数:
            csv_file_path (str): 输入的 CSV 文件路径
            excel_file_path (str): 输出的 Excel 文件路径（可选，默认与 CSV 同名但扩展名为 .xlsx）
            
        返回:
            str: 生成的Excel文件路径
        """
        if not os.path.isfile(csv_file_path):
            raise FileNotFoundError(f"找不到文件 {csv_file_path}")

        if excel_file_path is None:
            # 默认输出文件名：将 .csv 替换为 .xlsx，并保存到data/processed目录
            # 使用更友好的命名方式，去掉UUID前缀
            base_name = os.path.splitext(os.path.basename(csv_file_path))[0]
            # 移除可能存在的UUID前缀
            if '_' in base_name:
                parts = base_name.split('_', 1)  # Split only on the first underscore
                # Check if the first part looks like a UUID
                if len(parts[0]) == 36 and '-' in parts[0]:  # UUID length and format
                    # Use the part after the UUID
                    friendly_name = parts[1]
                else:
                    # Keep the original name
                    friendly_name = base_name
            else:
                # No underscore, keep as is
                friendly_name = base_name
                
            excel_file_path = f"data/processed/processed_{friendly_name}.xlsx"

        try:
            # 读取 CSV 文件
            df = pd.read_csv(csv_file_path, encoding='utf-8')
        except UnicodeDecodeError:
            # 如果 UTF-8 失败，尝试用 latin1 或其他编码
            df = pd.read_csv(csv_file_path, encoding='latin1')

        # 确保输出目录存在
        os.makedirs(os.path.dirname(excel_file_path), exist_ok=True)

        # 写入 Excel 文件
        df.to_excel(excel_file_path, index=False, engine='openpyxl')

        print(f"✅ 成功将 '{csv_file_path}' 转换为 '{excel_file_path}'")
        return excel_file_path