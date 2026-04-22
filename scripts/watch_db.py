#!/usr/bin/env python3
# view_vector_db.py


from pymilvus import MilvusClient
import json

DB_PATH = 'data/db/milvus_data.db'
milvus_client = MilvusClient(DB_PATH)

collection_name = 'standard_telecom'

# 测试基本连接和查询
print("测试数据库连接...")
collections = milvus_client.list_collections()
print("所有集合:", collections)

if collection_name in collections:
    print(f"\n查询集合 {collection_name}...")
    
    # 获取统计信息
    stats = milvus_client.get_collection_stats(collection_name)
    print("统计信息:", stats)
    
    # 尝试简单的ID查询
    try:
        results = milvus_client.query(
            collection_name=collection_name,
            filter="id >= 0",
            limit=10000,
            output_fields=["true_element"]
        )
        print("ID查询结果:", results)
    except Exception as e:
        print("ID查询失败:", e)
    
    # 尝试查询所有字段
    try:
        results = milvus_client.query(
            collection_name=collection_name,
            filter="true_element == '项目编号'",
            limit=2,
            output_fields=["*"]
        )
        print("\n所有字段查询结果:")
        for i, result in enumerate(results):
            print(f"记录 {i+1}: {result}")
    except Exception as e:
        print("查询所有字段失败:", e)