#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据处理流水线脚本
整合Excel处理、标准数据处理和用户数据处理的完整流程
"""

import sys
import os
import asyncio
import argparse

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.processors.excel_processor import ExcelProcessor
from app.processors.standard_data_processor import StandardDataProcessor
from app.processors.user_data_processor import UserDataProcessor


def process_standard_data():
    """处理标准数据并构建知识库"""
    print("🔧 开始处理标准数据...")
    try:
        processor = StandardDataProcessor()
        inserted_count = asyncio.run(processor.process_standard_data())
        print(f"✅ 标准数据处理完成，共插入 {inserted_count} 条记录")
        return True
    except Exception as e:
        print(f"❌ 标准数据处理失败: {e}")
        return False


def convert_excel_to_csv(excel_file):
    """将Excel文件转换为CSV"""
    print(f"🔄 开始将Excel文件 {excel_file} 转换为CSV...")
    try:
        processor = ExcelProcessor()
        csv_files = processor.convert_excel_to_csv(excel_file)
        print(f"✅ Excel转换完成，生成的CSV文件: {csv_files}")
        return csv_files
    except Exception as e:
        print(f"❌ Excel转换失败: {e}")
        return None


def process_user_data(csv_file):
    """处理用户CSV数据"""
    print(f"🧠 开始处理用户数据文件 {csv_file}...")
    try:
        processor = UserDataProcessor()
        output_csv, output_json = asyncio.run(processor.process_csv_file(csv_file))
        print(f"✅ 用户数据处理完成")
        return output_csv, output_json
    except Exception as e:
        print(f"❌ 用户数据处理失败: {e}")
        return None, None


def convert_csv_to_excel(csv_file):
    """将处理后的CSV文件转换为Excel"""
    print(f"📊 开始将CSV文件 {csv_file} 转换为Excel...")
    try:
        processor = ExcelProcessor()
        excel_file = processor.csv_to_excel(csv_file)
        print(f"✅ CSV转换完成，生成的Excel文件: {excel_file}")
        return excel_file
    except Exception as e:
        print(f"❌ CSV转换失败: {e}")
        return None


def run_full_pipeline(excel_file):
    """运行完整的处理流程"""
    print("🚀 启动完整数据处理流程...")
    
    # 1. 处理标准数据（构建知识库）
    if not process_standard_data():
        print("❌ 流程中断：标准数据处理失败")
        return False
    
    # 2. 将Excel转换为CSV
    csv_files = convert_excel_to_csv(excel_file)
    if not csv_files:
        print("❌ 流程中断：Excel转换失败")
        return False
    
    # 3. 处理每个CSV文件
    result_files = []
    for csv_file in csv_files:
        output_csv, output_json = process_user_data(csv_file)
        if output_csv and output_json:
            result_files.append(output_csv)
        else:
            print(f"❌ 流程中断：处理CSV文件 {csv_file} 失败")
            return False
    
    # 4. 将处理后的CSV转换为Excel
    final_excel_files = []
    for result_file in result_files:
        excel_file = convert_csv_to_excel(result_file)
        if excel_file:
            final_excel_files.append(excel_file)
        else:
            print(f"❌ 流程中断：转换CSV文件 {result_file} 失败")
            return False
    
    print("🎉 完整数据处理流程执行完毕！")
    print(f"📋 最终生成的Excel文件: {final_excel_files}")
    return True


def run_user_processing_pipeline(excel_file):
    """运行用户数据处理流程（不包括标准数据处理）"""
    print("🚀 启动用户数据处理流程...")
    
    # 1. 将Excel转换为CSV
    csv_files = convert_excel_to_csv(excel_file)
    if not csv_files:
        print("❌ 流程中断：Excel转换失败")
        return False
    
    # 2. 处理每个CSV文件
    result_files = []
    for csv_file in csv_files:
        output_csv, output_json = process_user_data(csv_file)
        if output_csv and output_json:
            result_files.append(output_csv)
        else:
            print(f"❌ 流程中断：处理CSV文件 {csv_file} 失败")
            return False
    
    # 3. 将处理后的CSV转换为Excel
    final_excel_files = []
    for result_file in result_files:
        excel_file = convert_csv_to_excel(result_file)
        if excel_file:
            final_excel_files.append(excel_file)
        else:
            print(f"❌ 流程中断：转换CSV文件 {result_file} 失败")
            return False
    
    print("🎉 用户数据处理流程执行完毕！")
    print(f"📋 最终生成的Excel文件: {final_excel_files}")
    return True


def main():
    parser = argparse.ArgumentParser(description='数据处理流水线')
    parser.add_argument('--mode', choices=['full', 'user-process', 'standard', 'excel2csv', 'csv2excel', 'user'], 
                       default='full', help='运行模式')
    parser.add_argument('--file', help='输入文件路径')
    parser.add_argument('--csv-file', help='CSV文件路径（用于用户数据处理）')
    
    args = parser.parse_args()
    
    if args.mode == 'full':
        if not args.file:
            print("❌ 错误：完整流程需要指定输入的Excel文件 (--file)")
            return 1
        if not os.path.exists(args.file):
            print(f"❌ 错误：文件 {args.file} 不存在")
            return 1
        success = run_full_pipeline(args.file)
        return 0 if success else 1
    
    elif args.mode == 'user-process':
        if not args.file:
            print("❌ 错误：用户处理流程需要指定输入的Excel文件 (--file)")
            return 1
        if not os.path.exists(args.file):
            print(f"❌ 错误：文件 {args.file} 不存在")
            return 1
        success = run_user_processing_pipeline(args.file)
        return 0 if success else 1
    
    elif args.mode == 'standard':
        success = process_standard_data()
        return 0 if success else 1
    
    elif args.mode == 'excel2csv':
        if not args.file:
            print("❌ 错误：需要指定输入的Excel文件 (--file)")
            return 1
        if not os.path.exists(args.file):
            print(f"❌ 错误：文件 {args.file} 不存在")
            return 1
        csv_files = convert_excel_to_csv(args.file)
        return 0 if csv_files else 1
    
    elif args.mode == 'user':
        if not args.csv_file:
            print("❌ 错误：需要指定输入的CSV文件 (--csv-file)")
            return 1
        if not os.path.exists(args.csv_file):
            print(f"❌ 错误：文件 {args.csv_file} 不存在")
            return 1
        output_csv, output_json = process_user_data(args.csv_file)
        return 0 if (output_csv and output_json) else 1
    
    elif args.mode == 'csv2excel':
        if not args.file:
            print("❌ 错误：需要指定输入的CSV文件 (--file)")
            return 1
        if not os.path.exists(args.file):
            print(f"❌ 错误：文件 {args.file} 不存在")
            return 1
        excel_file = convert_csv_to_excel(args.file)
        return 0 if excel_file else 1


if __name__ == "__main__":
    sys.exit(main())
    


"""
# 运行完整流程
python scripts/run_processing_pipeline.py --mode full --file input.xlsx

# 仅处理标准数据
python scripts/run_processing_pipeline.py --mode standard

# 仅转换Excel为CSV
python scripts/run_processing_pipeline.py --mode excel2csv --file input.xlsx

# 仅处理用户数据
python scripts/run_processing_pipeline.py --mode user --csv-file input.csv

# 仅转换CSV为Excel
python scripts/run_processing_pipeline.py --mode csv2excel --file input.csv

# 运行用户数据处理流程（不包括标准数据处理）
python scripts/run_processing_pipeline.py --mode user-process --file input.xlsx       data/raw/5g急救-优先.xlsx     data/raw/test-2.xlsx
"""