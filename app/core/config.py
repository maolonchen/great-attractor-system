from dataclasses import dataclass
import os
from typing import ClassVar, Dict, Any, List

class MinerUConfig:
    url: ClassVar[str] = "http://???:8003/file_parse"
    output_dir: ClassVar[str] = "./processed"
    lang_list: ClassVar[List[str]] = ["ch", "en"]
    backend: ClassVar[str] = "pipeline"
    parse_method: ClassVar[str] = "auto"
    formula_enable: ClassVar[bool] = False
    table_enable: ClassVar[bool] = True
    return_images: ClassVar[bool] = False
    return_md: ClassVar[bool] = True
    return_content_list: ClassVar[bool] = False
    return_middle_json: ClassVar[bool] = False

    @classmethod
    def get_request_data(cls) -> Dict[str, Any]:
        """获取完整的请求参数字典"""
        return {
            "output_dir": cls.output_dir,
            "lang_list": cls.lang_list,
            "backend": cls.backend,
            "parse_method": cls.parse_method,
            "formula_enable": cls.formula_enable,
            "table_enable": cls.table_enable,
            "return_images": cls.return_images,
            "return_md": cls.return_md,
            "return_content_list": cls.return_content_list,
            "return_middle_json": cls.return_middle_json,
        }


class ChatLLMConfig:
    url: ClassVar[str] = "https://api.siliconflow.cn/v1/chat/completions"

    headers: ClassVar[Dict[str, Any]] = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer xxx"
    }

    model_name: ClassVar[str] = "Qwen/Qwen3-14B"

    temperature: ClassVar[float] = 0.2
    top_p: ClassVar[float] = 0.8
    top_k: ClassVar[int] = 20
    max_tokens: ClassVar[int] = 4096
    presence_penalty: ClassVar[float] = 1.6
    chat_template_kwargs: ClassVar[Dict[str, Any]] = {"enable_thinking": False}
    
    # 异步调用相关配置
    max_concurrent_requests: ClassVar[int] = 1

    @classmethod
    def get_request_data(cls, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """获取完整的请求数据"""
        return {
            "model": cls.model_name,
            "messages": messages,
            "temperature": cls.temperature,
            "top_p": cls.top_p,
            "top_k": cls.top_k,
            "max_tokens": cls.max_tokens,
            "presence_penalty": cls.presence_penalty,
            "chat_template_kwargs": cls.chat_template_kwargs,
            "stream": False,
        }

class ClassificationConfig:
    # 分类缓存大小限制
    cache_size_limit: ClassVar[int] = 1000
    
    # 分类结果存储方式
    # temporary: 仅临时存储在内存中
    # persistent: 持久化存储到文件或数据库中
    storage_mode: ClassVar[str] = "persistent"
    
    # 临时存储过期时间（秒）
    cache_expire_time: ClassVar[int] = 3600  # 1小时
    
    # 文档内容最大长度（字符数）
    max_content_length: ClassVar[int] = 3000


class AsyncLLMConfig:
    # 异步LLM配置
    # 最大并发请求数
    max_concurrent_requests: ClassVar[int] = 8
    
    # 请求超时时间（秒）
    request_timeout: ClassVar[int] = 300  # 5分钟
    
    # 重试次数
    max_retries: ClassVar[int] = 3
    
    # 重试间隔（秒）
    retry_interval: ClassVar[float] = 1.0
    
    
class AsyncEmbeddingConfig:
    # 异步嵌入模型配置
    # 最大并发请求数
    max_concurrent_requests: ClassVar[int] = 6
    
    # 请求超时时间（秒）
    request_timeout: ClassVar[int] = 120  # 2分钟
    
    # 重试次数
    max_retries: ClassVar[int] = 3
    
    # 重试间隔（秒）
    retry_interval: ClassVar[float] = 1.0


class DatabaseConfig:
    # 数据库配置
    path: ClassVar[str] = "./data/db/milvus_data.db"
    collection_name: ClassVar[str] = "knowledge_base"
    
class EmbeddingConfig:
    # 嵌入模型配置
    api_url: ClassVar[str] = "http://xxx:9998/v1/embeddings"
    model_name: ClassVar[str] = "qwen3-embedding-8b"
    embedding_dim: ClassVar[int] = 4096
    # 文本最大长度（字符数）
    max_content_length: ClassVar[int] = 3000


class ChunkingConfig:
    # 分块配置
    # 文档内容最大长度（字符数）
    max_content_length: ClassVar[int] = 3000  # 只是对md_content，不是整个提示词


class KafkaConfig:
    # Kafka配置
    enable: ClassVar[bool] = False  # 默认禁用 Kafka，用于测试环境
    bootstrap_servers: ClassVar[List[str]] = ["xxx:9092"]
    sasl_plain_username: ClassVar[str] = "xxx"
    sasl_plain_password: ClassVar[str] = "xxx"
    security_protocol: ClassVar[str] = "xxx"
    sasl_mechanism: ClassVar[str] = "xxx"
    topic: ClassVar[str] = "xxx"
    partition: ClassVar[int] = 0
