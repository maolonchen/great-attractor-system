# scripts/process_user_csv_streaming.py
import asyncio
import sys
import os
import time
import json
import csv
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.core.vectoring import VectorClient

REQUIRED_HEADERS = ["字段名称", "字段注释"]
NEW_COLUMNS = ["数据分类(AI判定)", "数据等级(AI判定)", "AI命中元素"]
CHUNK_SIZE = 500


def count_lines(filepath):
    """快速统计 CSV 行数（不含 header）"""
    with open(filepath, 'rb') as f:
        count = sum(1 for _ in f)
    return max(0, count - 1)  # 减去表头


# async def process_chunk(vector_client, items):
#     texts = [item["text"] for item in items]
#     try:
#         embeddings = await vector_client.get_embeddings(texts)
#     except Exception as e:
#         print(f"\n⚠️ 嵌入失败: {e}")
#         return [("", "", "")] * len(items)

#     results = []
#     for emb in embeddings:
#         try:
#             res = vector_client.milvus_client.search(
#                 collection_name="standard_telecom",
#                 data=[emb],
#                 limit=1,
#                 output_fields=["classification", "grading", "true_element"],
#                 search_params={"metric_type": "IP", "params": {"nprobe": 10}}
#             )
#             if res and res[0]:
#                 ent = res[0][0]["entity"]
#                 results.append((ent["classification"], ent["grading"], ent["true_element"]))
#             else:
#                 results.append(("", "", ""))
#         except Exception as e:
#             print(f"\n⚠️ 搜索失败: {e}")
#             results.append(("", "", ""))
#     return results


# async def process_csv_file(csv_path):
#     vector_client = VectorClient()
#     if not vector_client.milvus_client.has_collection("standard_telecom"):
#         raise RuntimeError("❌ standard_telecom 集合不存在")
#     vector_client.milvus_client.load_collection("standard_telecom")
#     print("✅ Milvus 集合已加载")

#     total_rows = count_lines(csv_path)
#     print(f"📊 文件总行数（不含表头）: {total_rows}")

#     output_csv = csv_path.replace(".csv", "_result.csv")
#     output_json = "similarity_results.json"

#     json_out = []

#     with open(csv_path, "r", encoding="utf-8-sig") as fin, \
#          open(output_csv, "w", encoding="utf-8-sig", newline="") as fout:

#         reader = csv.DictReader(fin)
#         actual_headers = reader.fieldnames or []
#         missing = set(REQUIRED_HEADERS) - set(actual_headers)
#         if missing:
#             raise ValueError(f"❌ 缺少字段: {missing}")

#         writer = csv.writer(fout)
#         writer.writerow(actual_headers + NEW_COLUMNS)

#         buffer = []
#         processed = 0

#         # 初始化进度条
#         pbar = tqdm(total=total_rows, desc="处理中", unit="行", mininterval=1.0)

#         async def flush():
#             nonlocal buffer, processed, json_out
#             if not buffer:
#                 return
#             results = await process_chunk(vector_client, buffer)
#             for item, (cls, grd, elem) in zip(buffer, results):
#                 row = [item["original"][h] for h in actual_headers]
#                 writer.writerow(row + [cls, grd, elem])
#                 json_out.append({
#                     "row_index": item["row_index"],
#                     "classification": cls,
#                     "grading": grd,
#                     "true_element": elem
#                 })
#             processed += len(buffer)
#             pbar.update(len(buffer))  # 更新进度条
#             buffer.clear()

#         row_num = 2  # 从第2行开始（跳过表头）
#         for row in reader:
#             vals = []
#             for h in REQUIRED_HEADERS:
#                 v = str(row.get(h, "")).strip()
#                 if h == "字段样本":
#                     v = v[:20]
#                 vals.append(v)
#             text = " ".join(vals)

#             buffer.append({
#                 "row_index": row_num,
#                 "text": text,
#                 "original": row
#             })

#             if len(buffer) >= CHUNK_SIZE:
#                 await flush()
#             row_num += 1

#         await flush()
#         pbar.close()

#     with open(output_json, "w", encoding="utf-8") as f:
#         json.dump(json_out, f, ensure_ascii=False, indent=2)

#     print(f"\n🎉 处理完成！共 {processed} 行")
#     print(f"📁 CSV 结果: {output_csv}")
#     print(f"📄 JSON 结果: {output_json}")


# if __name__ == "__main__":
#     if len(sys.argv) != 2:
#         print("用法: python process_user_csv_streaming.py <input.csv>")
#         sys.exit(1)
#     start = time.time()
#     try:
#         asyncio.run(process_csv_file(sys.argv[1]))
#     except Exception as e:
#         print(f"\n❌ 错误: {e}")
#         sys.exit(1)
#     print(f"\n⏱️ 总耗时: {time.time() - start:.2f} 秒")

# ... existing code ...
async def process_chunk(vector_client, items):
    texts = [item["text"] for item in items]
    try:
        embeddings = await vector_client.get_embeddings(texts)
    except Exception as e:
        print(f"\n⚠️ 嵌入失败: {e}")
        return [("", "", "")] * len(items)

    results = []
    for emb in embeddings:
        try:
            res = vector_client.milvus_client.search(
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


async def process_csv_file(csv_path):
    vector_client = VectorClient()
    if not vector_client.milvus_client.has_collection("standard_telecom"):
        raise RuntimeError("❌ standard_telecom 集合不存在")
    vector_client.milvus_client.load_collection("standard_telecom")
    print("✅ Milvus 集合已加载")

    total_rows = count_lines(csv_path)
    print(f"📊 文件总行数（不含表头）: {total_rows}")

    output_csv = csv_path.replace(".csv", "_result.csv")
    output_json = "similarity_results.json"

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
            results = await process_chunk(vector_client, buffer)
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


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python process_user_csv_streaming.py <input.csv>")
        sys.exit(1)
    start = time.time()
    try:
        asyncio.run(process_csv_file(sys.argv[1]))
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        sys.exit(1)
    print(f"\n⏱️ 总耗时: {time.time() - start:.2f} 秒")