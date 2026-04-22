import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
from app.core.vectoring import VectorClient
from pymilvus import DataType
import asyncio
import re

async def process_standard_data():
    """
    处理标准数据文件，提取所需字段并存储到Milvus数据库
    """
    # 初始化向量客户端
    vector_client = VectorClient()
    
    # 检查是否已存在名为"standard_telecom"的集合，如果存在则删除
    if vector_client.milvus_client.has_collection("standard_telecom"):
        vector_client.milvus_client.drop_collection("standard_telecom")
        print("已删除现有的 standard_telecom 集合")
    
    # 创建新的集合
    schema = vector_client.milvus_client.create_schema(
        auto_id=False,
        description="Standard data elements with embeddings"
    )
    
    # 添加字段
    schema.add_field("id", datatype=DataType.INT64, is_primary=True, auto_id=True)
    schema.add_field("vector", datatype=DataType.FLOAT_VECTOR, dim=vector_client.embedding_dim)
    schema.add_field("true_element", datatype=DataType.VARCHAR, max_length=65535)
    schema.add_field("classification", datatype=DataType.VARCHAR, max_length=65535)
    schema.add_field("grading", datatype=DataType.VARCHAR, max_length=65535)
    
    # 创建集合
    vector_client.milvus_client.create_collection(
        collection_name="standard_telecom",
        schema=schema,
        consistency_level="Strong"
    )
    
    # 创建向量索引
    index_params = vector_client.milvus_client.prepare_index_params()
    index_params.add_index(
        field_name="vector",
        index_type="FLAT",
        metric_type="IP"
    )
    vector_client.milvus_client.create_index(
        collection_name="standard_telecom",
        index_params=index_params
    )
    
    # 读取数据文件
    data_file_path = "data/standards/standard.jsonl"
    data_to_insert = []
    
    with open(data_file_path, 'r', encoding='utf-8') as file:
        for line_num, line in enumerate(file, 1):
            try:
                record = json.loads(line.strip())
                header = record.get("header", {})
                data = record.get("data", {})
                
                # 获取各字段索引位置
                level1_index = None
                level2_index = None
                level3_index = None
                feature_index = None
                grade_index = None
                real_data_index = None
                
                for idx, name in header.items():
                    if name == "一级分类":
                        level1_index = idx
                    elif name == "二级分类":
                        level2_index = idx
                    elif name == "三级分类":
                        level3_index = idx
                    elif name == "对应特征":
                        feature_index = idx
                    elif name == "等级":
                        grade_index = idx
                    elif name == "真实数据":
                        real_data_index = idx
                
                # 提取真实数据
                true_elements = data.get(real_data_index, [])
                if isinstance(true_elements, list):
                    true_elements_list = true_elements
                else:
                    # 如果不是列表，按逗号分割
                    true_elements_list = str(true_elements).split(',') if true_elements else []
                    # 清理空白字符
                    true_elements_list = [elem.strip() for elem in true_elements_list if elem.strip()]
                
                # 提取分类信息（优先级：三级分类 > 二级分类 > 一级分类）
                classification = ""
                if data.get(level3_index, "").strip():
                    classification = data.get(level3_index, "")
                elif data.get(level2_index, "").strip():
                    classification = data.get(level2_index, "")
                elif data.get(level1_index, "").strip():
                    classification = data.get(level1_index, "")
                
                # 提取等级信息并选择最高等级
                grade_raw = data.get(grade_index, "")
                grading = extract_highest_grade(grade_raw)
                
                # 提取特征文本
                feature_text = data.get(feature_index, "")
                
                # 为每个真实元素创建一条记录
                for element in true_elements_list:
                    # 构造特征+真实数据的字符串用于向量化
                    # vector_text = f"{feature_text},{element}" if feature_text else element
                    vector_text = element
                    # 准备数据插入
                    data_to_insert.append({
                        "true_element": element,
                        "classification": classification,
                        "grading": grading,
                        "text_for_vector": vector_text  # 临时字段，用于向量化
                    })
                
            except json.JSONDecodeError as e:
                print(f"第 {line_num} 行 JSON 解析错误: {e}")
            except Exception as e:
                print(f"处理第 {line_num} 行时发生错误: {e}")
    
    # 向量化文本
    texts_for_vector = [item.pop("text_for_vector") for item in data_to_insert]  # 移除临时字段
    embeddings = await vector_client.get_embeddings(texts_for_vector)
    
    # 将向量添加到数据中
    for i, embedding in enumerate(embeddings):
        data_to_insert[i]["vector"] = embedding
    
    # 插入数据到数据库
    if data_to_insert:
        result = vector_client.milvus_client.insert(
            collection_name="standard_telecom",
            data=data_to_insert
        )
        print(f"成功插入 {len(result['ids'])} 条记录到 standard_telecom 集合")
    else:
        print("没有数据需要插入")

def extract_highest_grade(grade_text):
    """
    从等级文本中提取最高等级
    如"第4级/第3级/第2级"中提取"第4级"
    """
    if not grade_text:
        return ""
    
    # 查找所有类似"第N级"的模式
    grades = re.findall(r'第(\d+)级', grade_text)
    if grades:
        # 取最大的数字作为最高等级
        highest_grade = max(int(g) for g in grades)
        return f"第{highest_grade}级"
    else:
        return grade_text

if __name__ == "__main__":
    asyncio.run(process_standard_data())