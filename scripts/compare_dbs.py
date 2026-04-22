#!/usr/bin/env python3

from pymilvus import MilvusClient

def compare_true_elements():
    # 连接两个数据库
    client1 = MilvusClient('data/db/milvus_data.db')
    client2 = MilvusClient('data/db/milvus_data_ori.db')
    
    collection_name = 'standard_telecom'
    
    # 检查集合是否存在
    if collection_name not in client1.list_collections():
        print("在milvus_data.db中未找到standard_telecom集合")
        return
        
    if collection_name not in client2.list_collections():
        print("在milvus_data_ori.db中未找到standard_telecom集合")
        return
    
    # 获取两个数据库中的所有记录
    try:
        # 先获取统计信息以确定记录总数
        stats1 = client1.get_collection_stats(collection_name)
        stats2 = client2.get_collection_stats(collection_name)
        
        total_count1 = stats1.get('row_count', 0)
        total_count2 = stats2.get('row_count', 0)
        
        print(f"milvus_data.db 中的记录数: {total_count1}")
        print(f"milvus_data_ori.db 中的记录数: {total_count2}")
        print(f"记录数差异: {total_count1 - total_count2}")
        
        # 分批获取所有记录
        batch_size = 1000
        elements1 = {}
        elements2 = {}
        
        # 获取milvus_data.db中的所有记录
        for offset in range(0, total_count1, batch_size):
            results = client1.query(
                collection_name=collection_name,
                filter="id >= 0",
                limit=min(batch_size, total_count1 - offset),
                offset=offset,
                output_fields=["id", "true_element"]
            )
            for r in results:
                # 根据实际结构处理数据
                if 'entity' in r:
                    elements1[r['entity']['true_element']] = r['entity']['id']
                else:
                    # 直接访问字段
                    elements1[r['true_element']] = r['id']
        
        # 获取milvus_data_ori.db中的所有记录
        for offset in range(0, total_count2, batch_size):
            results = client2.query(
                collection_name=collection_name,
                filter="id >= 0",
                limit=min(batch_size, total_count2 - offset),
                offset=offset,
                output_fields=["id", "true_element"]
            )
            for r in results:
                # 根据实际结构处理数据
                if 'entity' in r:
                    elements2[r['entity']['true_element']] = r['entity']['id']
                else:
                    # 直接访问字段
                    elements2[r['true_element']] = r['id']
        
        # 找出不同的记录
        only_in_db1 = set(elements1.keys()) - set(elements2.keys())
        only_in_db2 = set(elements2.keys()) - set(elements1.keys())
        
        print(f"\n仅在milvus_data.db中存在的记录数: {len(only_in_db1)}")
        print(f"仅在milvus_data_ori.db中存在的记录数: {len(only_in_db2)}")
        
        if only_in_db1:
            print("\n=== 仅在milvus_data.db中存在的5条记录 ===")
            count = 0
            for elem in only_in_db1:
                if count >= 5:  # 只显示5条
                    break
                print(f"  ID: {elements1[elem]}, Element: {elem}")
                count += 1
                
        if only_in_db2:
            print("\n=== 仅在milvus_data_ori.db中存在的记录 ===")
            for elem in only_in_db2:
                print(f"  ID: {elements2[elem]}, Element: {elem}")
                
        # 显示一些相同的记录
        common_elements = set(elements1.keys()) & set(elements2.keys())
        print(f"\n两个数据库中都存在的记录数: {len(common_elements)}")
        
    except Exception as e:
        print(f"查询过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    compare_true_elements()