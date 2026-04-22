import argparse
import uvicorn
import json
import os
import asyncio
import uuid
from datetime import datetime
from typing import List, Dict, Any
import shutil
import json
import aiohttp

from fastapi import FastAPI, Request, File, UploadFile, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

# 导入项目中的处理模块
from app.processors.standard_data_processor import StandardDataProcessor
from app.processors.user_data_processor import UserDataProcessor
from app.processors.excel_processor import ExcelProcessor
from app.core.vectoring import VectorClient
from app.core.config import EmbeddingConfig

app = FastAPI(title="Great Attractor system")

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

llm_status = {
    "status": "normal",  # normal, overload, error
    "last_check": None
}

# 挂载静态文件目录
app.mount("/static", StaticFiles(directory="static"), name="static")

# 存储任务状态
processing_tasks = {}

# 存储任务的实时处理信息
processing_progress = {}

# 确保static目录存在
os.makedirs("static", exist_ok=True)
# 修改目录结构：使用data/raw存储上传文件，data/processed存储处理后的文件
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

# 自定义JSON编码器以处理datetime对象
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

@app.get("/", response_class=HTMLResponse)
async def read_index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content)

@app.get("/api/classifications")
async def get_classifications():
    """获取所有分类数据"""
    try:
        # 从 standard.jsonl 文件中读取分类数据
        standard_file = "data/standards/standard.jsonl"
        classifications = {}
        
        if os.path.exists(standard_file):
            with open(standard_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            record = json.loads(line.strip())
                            data = record.get("data", {})
                            header = record.get("header", {})
                            
                            # 找到分类索引
                            level1_idx = level2_idx = level3_idx = level4_idx = level5_idx = None
                            level6_idx = level7_idx = level8_idx = level9_idx = None
                            real_data_idx = None
                            
                            for idx, name in header.items():
                                if name == "一级分类":
                                    level1_idx = idx
                                elif name == "二级分类":
                                    level2_idx = idx
                                elif name == "三级分类":
                                    level3_idx = idx
                                elif name == "四级分类":
                                    level4_idx = idx
                                elif name == "五级分类":
                                    level5_idx = idx
                                elif name == "六级分类":
                                    level6_idx = idx
                                elif name == "七级分类":
                                    level7_idx = idx
                                elif name == "八级分类":
                                    level8_idx = idx
                                elif name == "九级分类":
                                    level9_idx = idx
                                elif name == "真实数据":
                                    real_data_idx = idx
                            
                            # 提取分类信息（优先级：九级分类 > 八级分类 > 七级分类 > 六级分类 > 五级分类 > 四级分类 > 三级分类 > 二级分类 > 一级分类）
                            classification = ""
                            if level9_idx is not None and data.get(level9_idx, "").strip():
                                classification = data.get(level9_idx, "")
                            elif level8_idx is not None and data.get(level8_idx, "").strip():
                                classification = data.get(level8_idx, "")
                            elif level7_idx is not None and data.get(level7_idx, "").strip():
                                classification = data.get(level7_idx, "")
                            elif level6_idx is not None and data.get(level6_idx, "").strip():
                                classification = data.get(level6_idx, "")
                            elif level5_idx is not None and data.get(level5_idx, "").strip():
                                classification = data.get(level5_idx, "")
                            elif level4_idx is not None and data.get(level4_idx, "").strip():
                                classification = data.get(level4_idx, "")
                            elif level3_idx is not None and data.get(level3_idx, "").strip():
                                classification = data.get(level3_idx, "")
                            elif level2_idx is not None and data.get(level2_idx, "").strip():
                                classification = data.get(level2_idx, "")
                            elif level1_idx is not None and data.get(level1_idx, "").strip():
                                classification = data.get(level1_idx, "")
                            
                            # 提取真实数据元素
                            if real_data_idx is not None and classification:
                                elements = data.get(real_data_idx, [])
                                if isinstance(elements, list):
                                    if classification not in classifications:
                                        classifications[classification] = []
                                    classifications[classification].extend([elem.strip() for elem in elements if elem.strip()])
                        except json.JSONDecodeError:
                            continue
        
        return JSONResponse(content=classifications)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.post("/api/classifications")
async def update_classifications(classifications: Dict[str, List[str]]):
    """更新分类数据并重建向量数据库"""
    try:
        # 读取原始文件内容
        standard_file = "data/standards/standard.jsonl"
        original_records = []
        
        if os.path.exists(standard_file):
            with open(standard_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            record = json.loads(line.strip())
                            original_records.append(record)
                        except json.JSONDecodeError:
                            continue
        
        # 构建新的分类数据
        new_records = []
        classification_set = set(classifications.keys())
        processed_classifications = set()
        
        # 更新已有的记录
        for record in original_records:
            data = record.get("data", {})
            header = record.get("header", {})
            
            # 找到分类索引
            level1_idx = level2_idx = level3_idx = level4_idx = level5_idx = None
            level6_idx = level7_idx = level8_idx = level9_idx = None
            real_data_idx = None
            
            for idx, name in header.items():
                if name == "一级分类":
                    level1_idx = idx
                elif name == "二级分类":
                    level2_idx = idx
                elif name == "三级分类":
                    level3_idx = idx
                elif name == "四级分类":
                    level4_idx = idx
                elif name == "五级分类":
                    level5_idx = idx
                elif name == "六级分类":
                    level6_idx = idx
                elif name == "七级分类":
                    level7_idx = idx
                elif name == "八级分类":
                    level8_idx = idx
                elif name == "九级分类":
                    level9_idx = idx
                elif name == "真实数据":
                    real_data_idx = idx
            
            # 提取当前记录的分类
            current_classification = ""
            if level9_idx is not None and data.get(level9_idx, "").strip():
                current_classification = data.get(level9_idx, "")
            elif level8_idx is not None and data.get(level8_idx, "").strip():
                current_classification = data.get(level8_idx, "")
            elif level7_idx is not None and data.get(level7_idx, "").strip():
                current_classification = data.get(level7_idx, "")
            elif level6_idx is not None and data.get(level6_idx, "").strip():
                current_classification = data.get(level6_idx, "")
            elif level5_idx is not None and data.get(level5_idx, "").strip():
                current_classification = data.get(level5_idx, "")
            elif level4_idx is not None and data.get(level4_idx, "").strip():
                current_classification = data.get(level4_idx, "")
            elif level3_idx is not None and data.get(level3_idx, "").strip():
                current_classification = data.get(level3_idx, "")
            elif level2_idx is not None and data.get(level2_idx, "").strip():
                current_classification = data.get(level2_idx, "")
            elif level1_idx is not None and data.get(level1_idx, "").strip():
                current_classification = data.get(level1_idx, "")
            
            # 如果这个分类在更新列表中，则更新它
            if current_classification in classification_set:
                # 更新真实数据
                if real_data_idx is not None:
                    data[real_data_idx] = classifications[current_classification]
                record["data"] = data
                new_records.append(record)
                processed_classifications.add(current_classification)
            else:
                new_records.append(record)
        
        # 添加新增的分类
        for classification, elements in classifications.items():
            if classification not in processed_classifications:
                # 为新分类创建记录
                new_record = {
                    "header": {
                        "0": "一级分类",
                        "1": "二级分类",
                        "2": "三级分类",
                        "3": "四级分类",
                        "4": "对应特征",
                        "5": "等级",
                        "6": "条件",
                        "7": "真实数据"
                    },
                    "data": {
                        "0": "",
                        "1": "",
                        "2": "",
                        "3": classification,  # 使用四级分类存储
                        "4": "",
                        "5": "",
                        "6": "",
                        "7": elements
                    }
                }
                new_records.append(new_record)
        
        # 写回文件
        with open(standard_file, 'w', encoding='utf-8') as f:
            for record in new_records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        # 在后台重建向量数据库
        async def rebuild_database():
            try:
                processor = StandardDataProcessor()
                await processor.process_standard_data()
            except Exception as e:
                print(f"重建数据库时出错: {e}")
        
        # 启动后台任务
        asyncio.create_task(rebuild_database())
        
        return JSONResponse(content={"status": "success", "message": "分类数据已更新，正在重建向量数据库"})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.post("/api/process-files")
async def process_files(background_tasks: BackgroundTasks, files: List[UploadFile] = File(...)):
    """处理上传的Excel文件"""
    try:
        # 生成任务ID
        task_id = str(uuid.uuid4())
        
        # 创建任务状态
        processing_tasks[task_id] = {
            "status": "starting",
            "progress": 0,
            "total_rows": 0,
            "processed_rows": 0,
            "start_time": datetime.now().isoformat(),  # 保持为字符串
            "end_time": None,
            "elapsed_time": 0,
            "avg_time": 0,
            "files": [],
            "result_files": [],
            "excel_files": []  # 添加Excel文件列表
        }
        
        # 初始化处理进度跟踪，start_time也用字符串
        processing_progress[task_id] = {
            "processed_rows": 0,
            "start_time": datetime.now().isoformat()  # 改为字符串
        }
        
        # 保存上传的文件到data/raw目录
        saved_files = []
        for file in files:
            file_path = f"data/raw/{task_id}_{file.filename}"
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            saved_files.append(file_path)
        
        processing_tasks[task_id]["files"] = saved_files
        
        # 在后台开始处理
        background_tasks.add_task(run_processing_pipeline, task_id, saved_files)
        
        return JSONResponse(content={
            "status": "processing",
            "message": "文件处理已启动",
            "task_id": task_id
        })
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

async def run_processing_pipeline(task_id: str, file_paths: List[str]):
    """运行文件处理流水线"""
    try:
        processing_tasks[task_id]["status"] = "processing"
        
        # 初始化处理器
        excel_processor = ExcelProcessor()
        user_processor = UserDataProcessor()
        
        result_files = []
        excel_files = []
        
        # 处理每个文件
        total_rows = 0
        processed_rows = 0
        
        for i, file_path in enumerate(file_paths):
            # 更新进度
            processing_tasks[task_id]["status"] = f"处理文件 {i+1}/{len(file_paths)}"
            
            # 转换Excel到CSV
            csv_files = excel_processor.convert_excel_to_csv(file_path)
            
            # 处理每个CSV文件
            for csv_file in csv_files:
                # 统计总行数
                rows_in_file = user_processor.count_lines(csv_file)
                total_rows += rows_in_file
                processing_tasks[task_id]["total_rows"] = total_rows  # 实时更新总行数
                
                # 处理CSV文件
                output_csv, output_json = await user_processor.process_csv_file_with_progress(task_id, csv_file, processing_progress)
                
                # 更新已处理行数
                if task_id in processing_progress:
                    processed_rows = processing_progress[task_id]["processed_rows"]
                
                result_files.append(output_csv)
                
                # 将处理后的CSV转换为Excel
                excel_file = excel_processor.csv_to_excel(output_csv)
                if excel_file:
                    # 确保我们存储的是绝对路径
                    abs_excel_file = os.path.abspath(excel_file)
                    excel_files.append(abs_excel_file)
        
        # 保存结果
        processing_tasks[task_id]["total_rows"] = total_rows
        processing_tasks[task_id]["processed_rows"] = processed_rows
        processing_tasks[task_id]["result_files"] = result_files
        processing_tasks[task_id]["excel_files"] = excel_files
        processing_tasks[task_id]["status"] = "completed"
        processing_tasks[task_id]["end_time"] = datetime.now().isoformat()
        
        # 计算处理时间
        start_time = datetime.fromisoformat(processing_tasks[task_id]["start_time"])
        end_time = datetime.fromisoformat(processing_tasks[task_id]["end_time"])
        elapsed_time = (end_time - start_time).total_seconds()
        processing_tasks[task_id]["elapsed_time"] = elapsed_time
        
        # 计算平均处理速度（每条记录的平均处理时间，单位毫秒）
        if elapsed_time > 0 and processing_tasks[task_id]["total_rows"] > 0:
            processing_tasks[task_id]["avg_time"] = (elapsed_time / processing_tasks[task_id]["total_rows"]) * 1000
        
        # 清理进度跟踪
        if task_id in processing_progress:
            del processing_progress[task_id]
        
    except Exception as e:
        processing_tasks[task_id]["status"] = "error"
        processing_tasks[task_id]["error"] = str(e)
        processing_tasks[task_id]["end_time"] = datetime.now().isoformat()
        
@app.get("/api/task-status/{task_id}")
async def get_task_status(task_id: str):
    """获取任务状态"""
    if task_id not in processing_tasks:
        return JSONResponse(content={"error": "任务不存在"}, status_code=404)
    
    task_info = processing_tasks[task_id].copy()
    
    # 计算实时的耗时和处理速度信息
    if task_info["status"] == "processing" and task_info["start_time"]:
        start_time = datetime.fromisoformat(task_info["start_time"])
        elapsed = (datetime.now() - start_time).total_seconds()
        task_info["elapsed_time"] = elapsed
        
        # 如果有进度跟踪信息，更新已处理行数和实时速度
        if task_id in processing_progress:
            processed_rows = processing_progress[task_id]["processed_rows"]
            task_info["processed_rows"] = processed_rows
            
            # 实时计算总行数估计（如果尚未设置）
            if task_info["total_rows"] == 0:
                # 在处理初期，我们不知道总行数，但可以根据已处理的行数进行估计
                task_info["total_rows"] = processed_rows if processed_rows > 0 else 1
            
            # 计算实时速度（条/秒）
            if elapsed > 0 and processed_rows > 0:
                # avg_time是每条记录的平均处理时间（毫秒）
                task_info["avg_time"] = (elapsed / processed_rows * 1000)
                
                # 计算实时进度百分比
                if task_info["total_rows"] and task_info["total_rows"] > 0:
                    task_info["progress"] = min(100, (processed_rows / task_info["total_rows"]) * 100)
    
    # 使用自定义编码器处理datetime对象
    return JSONResponse(content=json.loads(json.dumps(task_info, cls=DateTimeEncoder)))

@app.get("/api/download-info/{task_id}")
async def get_download_info(task_id: str):
    """获取下载信息"""
    if task_id not in processing_tasks:
        return JSONResponse(content={"error": "任务不存在"}, status_code=404)
    
    task_info = processing_tasks[task_id]
    if task_info["status"] != "completed":
        return JSONResponse(content={"error": "任务尚未完成"}, status_code=400)
    
    # 获取Excel文件列表
    excel_files = task_info.get("excel_files", [])
    
    if not excel_files:
        return JSONResponse(content={"error": "没有可下载的文件"}, status_code=404)
    
    # 返回文件信息
    return JSONResponse(content={
        "task_id": task_id,
        "files": [os.path.basename(f) for f in excel_files],
        "count": len(excel_files)
    })

@app.get("/api/download-file/{task_id}/{filename}")
async def download_file(task_id: str, filename: str):
    """下载单个处理后的文件"""
    if task_id not in processing_tasks:
        return JSONResponse(content={"error": "任务不存在"}, status_code=404)
    
    task_info = processing_tasks[task_id]
    if task_info["status"] != "completed":
        return JSONResponse(content={"error": "任务尚未完成"}, status_code=400)
    
    excel_files = task_info.get("excel_files", [])
    
    # 查找匹配的文件
    for file_path in excel_files:
        if os.path.basename(file_path) == filename:
            if os.path.exists(file_path):
                return FileResponse(
                    path=file_path,
                    filename=filename,
                    media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
            else:
                return JSONResponse(content={"error": "文件不存在"}, status_code=404)
    
    return JSONResponse(content={"error": "文件不存在"}, status_code=404)

@app.get("/api/llm-status")
async def get_llm_status():
    """获取LLM服务状态"""
    global llm_status
    return JSONResponse(content=llm_status)

async def check_llm_health():
    """定期检查LLM服务健康状态"""
    global llm_status
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    EmbeddingConfig.api_url,
                    json={"input": "health check", "model": EmbeddingConfig.model_name},
                    timeout=aiohttp.ClientTimeout(total=15)  # 15秒超时
                ) as response:
                    if response.status == 200:
                        llm_status["status"] = "normal"
                    else:
                        llm_status["status"] = "error"
        except asyncio.TimeoutError:
            llm_status["status"] = "overload"
        except Exception:
            llm_status["status"] = "error"
        finally:
            llm_status["last_check"] = datetime.now().isoformat()
            
        # 每15秒检查一次
        await asyncio.sleep(15)

@app.on_event("startup")
async def startup_event():
    """应用启动时执行"""
    # 启动LLM健康检查
    asyncio.create_task(check_llm_health())
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Great Attractor system')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind')
    parser.add_argument('--ssl-keyfile', help='SSL key file')
    parser.add_argument('--ssl-certfile', help='SSL certificate file')
    
    args = parser.parse_args()
    
    uvicorn.run(
        "main:app",
        host=args.host,
        port=args.port,
        ssl_keyfile=args.ssl_keyfile,
        ssl_certfile=args.ssl_certfile,
    )
    