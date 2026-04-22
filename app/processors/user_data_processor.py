import asyncio
import os
import json
import csv
from tqdm import tqdm

from app.core.vectoring import VectorClient

REQUIRED_HEADERS = ["字段名称", "字段注释"]
NEW_COLUMNS = ["数据分类(AI判定)", "数据等级(AI判定)", "AI命中元素"]
CHUNK_SIZE = 10


class UserDataProcessor:
    """用户数据处理器"""

    def __init__(self):
        self.vector_client = VectorClient()

    @staticmethod
    def count_lines(filepath):
        """快速统计 CSV 行数（不含 header）"""
        with open(filepath, 'rb') as f:
            count = sum(1 for _ in f)
        return max(0, count - 1)  # 减去表头

    async def process_chunk(self, items):
        """处理一批数据"""
        texts = [item["text"] for item in items]
        try:
            embeddings = await self.vector_client.get_embeddings(texts)
        except Exception as e:
            print(f"\n⚠️ 嵌入失败: {e}")
            return [("", "", "")] * len(items)

        results = []
        for emb in embeddings:
            try:
                res = self.vector_client.milvus_client.search(
                    collection_name="standard_telecom",
                    data=[emb],
                    limit=1,
                    output_fields=["classification", "grading", "true_element"],
                    search_params={"metric_type": "IP", "params": {"nprobe": 10}}
                )
                if res and res[0]:
                    ent = res[0][0]["entity"]
                    results.append((ent["classification"], ent["grading"], ent["true_element"]))
                else:
                    results.append(("", "", ""))
            except Exception as e:
                print(f"\n⚠️ 搜索失败: {e}")
                results.append(("", "", ""))
        return results

    async def process_csv_file(self, csv_path):
        """处理用户CSV文件"""
        if not self.vector_client.milvus_client.has_collection("standard_telecom"):
            raise RuntimeError("❌ standard_telecom 集合不存在")
        self.vector_client.milvus_client.load_collection("standard_telecom")
        print("✅ Milvus 集合已加载")

        total_rows = self.count_lines(csv_path)
        print(f"📊 文件总行数（不含表头）: {total_rows}")

        # 保存处理结果到data/processed目录，使用更友好的命名方式
        base_name = os.path.splitext(os.path.basename(csv_path))[0]
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
            
        output_csv = f"data/processed/processed_{friendly_name}_result.csv"
        output_json = "data/processed/similarity_results.json"

        json_out = []

        with open(csv_path, "r", encoding="utf-8-sig") as fin, \
                open(output_csv, "w", encoding="utf-8-sig", newline="") as fout:

            reader = csv.DictReader(fin)
            actual_headers = reader.fieldnames or []
            missing = set(REQUIRED_HEADERS) - set(actual_headers)
            if missing:
                raise ValueError(f"❌ 缺少字段: {missing}")

            writer = csv.writer(fout)
            writer.writerow(actual_headers + NEW_COLUMNS)

            buffer = []
            processed = 0

            # 初始化进度条
            pbar = tqdm(total=total_rows, desc="处理中", unit="行", mininterval=1.0)

            async def flush():
                nonlocal buffer, processed, json_out
                if not buffer:
                    return
                results = await self.process_chunk(buffer)
                for item, (cls, grd, elem) in zip(buffer, results):
                    row = [item["original"][h] for h in actual_headers]
                    writer.writerow(row + [cls, grd, elem])
                    json_out.append({
                        "row_index": item["row_index"],
                        "classification": cls,
                        "grading": grd,
                        "true_element": elem
                    })
                processed += len(buffer)
                pbar.update(len(buffer))  # 更新进度条
                buffer.clear()

            row_num = 2  # 从第2行开始（跳过表头）
            for row in reader:
                field_name = str(row.get("字段名称", "")).strip()
                field_comment = str(row.get("字段注释", "")).strip()

                # 根据新需求修改逻辑：
                # 如果"字段注释"不为空，则只使用"字段注释"
                # 如果"字段注释"为空，则使用原有的逻辑（同时使用"字段名称"和"字段注释"）
                if field_comment:
                    text = field_comment
                else:
                    text = " ".join([field_name, field_comment])

                buffer.append({
                    "row_index": row_num,
                    "text": text,
                    "original": row
                })

                if len(buffer) >= CHUNK_SIZE:
                    await flush()
                row_num += 1

            await flush()
            pbar.close()

        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(json_out, f, ensure_ascii=False, indent=2)

        print(f"\n🎉 处理完成！共 {processed} 行")
        print(f"📁 CSV 结果: {output_csv}")
        print(f"📄 JSON 结果: {output_json}")
        
        return output_csv, output_json

    async def process_csv_file_with_progress(self, task_id, csv_path, progress_tracker):
        """处理用户CSV文件并实时更新进度"""
        if not self.vector_client.milvus_client.has_collection("standard_telecom"):
            raise RuntimeError("❌ standard_telecom 集合不存在")
        self.vector_client.milvus_client.load_collection("standard_telecom")
        print("✅ Milvus 集合已加载")

        total_rows = self.count_lines(csv_path)
        print(f"📊 文件总行数（不含表头）: {total_rows}")

        # 保存处理结果到data/processed目录，使用更友好的命名方式
        base_name = os.path.splitext(os.path.basename(csv_path))[0]
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
            
        output_csv = f"data/processed/processed_{friendly_name}_result.csv"
        output_json = "data/processed/similarity_results.json"

        json_out = []

        with open(csv_path, "r", encoding="utf-8-sig") as fin, \
                open(output_csv, "w", encoding="utf-8-sig", newline="") as fout:

            reader = csv.DictReader(fin)
            actual_headers = reader.fieldnames or []
            missing = set(REQUIRED_HEADERS) - set(actual_headers)
            if missing:
                raise ValueError(f"❌ 缺少字段: {missing}")

            writer = csv.writer(fout)
            writer.writerow(actual_headers + NEW_COLUMNS)

            buffer = []
            processed = 0

            # 初始化进度条
            pbar = tqdm(total=total_rows, desc="处理中", unit="行", mininterval=0.1)

            async def flush():
                nonlocal buffer, processed, json_out
                if not buffer:
                    return
                results = await self.process_chunk(buffer)
                for item, (cls, grd, elem) in zip(buffer, results):
                    row = [item["original"][h] for h in actual_headers]
                    writer.writerow(row + [cls, grd, elem])
                    json_out.append({
                        "row_index": item["row_index"],
                        "classification": cls,
                        "grading": grd,
                        "true_element": elem
                    })
                processed += len(buffer)
                pbar.update(len(buffer))  # 更新进度条
                
                # 实时更新进度跟踪信息
                if task_id in progress_tracker:
                    progress_tracker[task_id]["processed_rows"] += len(buffer)
                    # 同时更新processing_tasks中的进度信息，以便前端能够获取到
                    from main import processing_tasks
                    if task_id in processing_tasks and total_rows > 0:
                        progress_percent = min(100, (progress_tracker[task_id]["processed_rows"] / total_rows) * 100)
                        processing_tasks[task_id]["progress"] = progress_percent
                        processing_tasks[task_id]["processed_rows"] = progress_tracker[task_id]["processed_rows"]
                
                # 强制刷新缓冲区确保实时更新
                fout.flush()
                buffer.clear()

            row_num = 2  # 从第2行开始（跳过表头）
            for row in reader:
                field_name = str(row.get("字段名称", "")).strip()
                field_comment = str(row.get("字段注释", "")).strip()

                # 根据新需求修改逻辑：
                # 如果"字段注释"不为空，则只使用"字段注释"
                # 如果"字段注释"为空，则使用原有的逻辑（同时使用"字段名称"和"字段注释"）
                if field_comment:
                    text = field_comment
                else:
                    text = " ".join([field_name, field_comment])

                buffer.append({
                    "row_index": row_num,
                    "text": text,
                    "original": row
                })

                # 减少块大小以提高实时性
                if len(buffer) >= CHUNK_SIZE:
                    await flush()
                row_num += 1

            await flush()
            pbar.close()

        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(json_out, f, ensure_ascii=False, indent=2)

        print(f"\n🎉 处理完成！共 {processed} 行")
        print(f"📁 CSV 结果: {output_csv}")
        print(f"📄 JSON 结果: {output_json}")
        
        return output_csv, output_json
