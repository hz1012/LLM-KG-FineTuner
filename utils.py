# coding:utf-8
"""
工具函数模块 - 提供各种辅助功能
"""
import re
import os
import json
import time
import requests
import logging
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from openai import OpenAI
import openai
import threading
from functools import wraps

logger = logging.getLogger(__name__)


def timeout_handler(func):
    """超时装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        timeout = kwargs.pop('timeout', 90)  # 默认60秒超时

        result = [None]
        exception = [None]

        def target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                exception[0] = e

        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            logger.error(f"函数 {func.__name__} 执行超时 ({timeout}秒)")
            raise TimeoutError(f"API调用超时: {timeout}秒")

        if exception[0]:
            raise exception[0]

        return result[0]

    return wrapper


class OpenAIAPIManager:
    """OpenAI API管理器，负责处理重试、超时等机制"""

    def __init__(self, config: Dict[str, Any]):
        # 提取配置参数
        self.api_key = config.get('api_key')
        self.base_url = config.get('base_url')
        self.model = config.get('model', 'qwen-plus')
        self.timeout = config.get('timeout', 90)
        self.max_tokens = config.get('max_tokens', 4000)
        self.max_retries = config.get('max_retries', 3)
        self.temperature = config.get('temperature', 0.7)
        self.top_p = config.get('top_p', 1)
        self.frequency_penalty = config.get('frequency_penalty', 0)
        self.presence_penalty = config.get('presence_penalty', 0)

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )

    def fix_json_call_api(self, broken_json: str) -> str:
        """使用GPT修复损坏的JSON格式"""
        try:
            prompt = """你是JSON修复专家。

    任务：修复损坏的JSON，确保语法完全正确。

    输出格式：{"entities":[],"relationships":[]}

    实体格式：{"labels":"EntityType","id":"entity-id","name":"Entity Name","description":"描述"}
    关系格式：{"type":"RELATION_TYPE","source":"source-id","target":"target-id","confidence":0.95,"evidence":"证据"}

    核心要求：
    1. 所有属性名和字符串值必须用双引号包围
    2. 移除多余空格和隐藏字符
    3. 确保括号匹配和逗号正确，json结尾必须是"}]}"
    4. 必须包含entities和relationships字段
    5. 实体结构扁平化：直接包含labels,id,name,description字段，不使用properties嵌套

    直接输出修复后的JSON，无其他内容。

    待修复JSON："""

            messages = [
                {"role": "system", "content": "你是JSON语法修复专家。严格遵守JSON规范，只输出修复后的JSON。确保实体结构扁平化，不使用properties嵌套。"},
                {"role": "user", "content": f"{prompt}\n\n{broken_json}"}
            ]

            response = self.call_api(
                messages=messages,
                model=self.model,
                temperature=0.1,
                max_tokens=min(8000, len(broken_json) + 1000)
            )

            if not response:
                logger.error("❌ GPT返回空响应")
                return broken_json

            logger.debug(f"📋 修复后JSON长度: {len(response)}")
            return response

        except Exception as e:
            logger.error(f"❌ JSON修复异常: {e}")
            return broken_json

    def _should_retry(self, exception: Exception, retry_count: int) -> bool:
        """判断是否应该重试"""
        if retry_count >= self.max_retries:
            return False

        # OpenAI API特定的重试条件
        if isinstance(exception, openai.RateLimitError):
            logger.warning(f"遇到速率限制，第{retry_count + 1}次重试...")
            return True

        if isinstance(exception, openai.APITimeoutError):
            logger.warning(f"API超时，第{retry_count + 1}次重试...")
            return True

        if isinstance(exception, openai.InternalServerError):
            logger.warning(f"服务器内部错误，第{retry_count + 1}次重试...")
            return True

        if isinstance(exception, openai.APIConnectionError):
            logger.warning(f"连接错误，第{retry_count + 1}次重试...")
            return True

        # 🔥 新增：BadRequestError的特定处理
        if isinstance(exception, openai.BadRequestError):
            error_str = str(exception)
            # 对于内容审查失败，不重试（因为重试也会失败）
            if 'data_inspection_failed' in error_str:
                logger.warning(f"内容审查失败，跳过重试: {error_str}")
                return False
            # 对于其他400错误，可以考虑重试（可能是临时问题）
            elif retry_count < 2:  # 最多重试2次
                logger.warning(f"BadRequest错误，第{retry_count + 1}次重试...")
                return True
            return False

        if isinstance(exception, (requests.exceptions.ConnectionError,
                                  requests.exceptions.Timeout,
                                  requests.exceptions.RequestException)):
            logger.warning(f"网络错误，第{retry_count + 1}次重试...")
            return True

        # 其他可重试的异常
        if "timeout" in str(exception).lower():
            logger.warning(f"超时错误，第{retry_count + 1}次重试...")
            return True

        return False

    def _get_retry_delay(self, retry_count: int) -> float:
        """获取重试延迟时间（指数退避）"""
        if retry_count < len(self.retry_delays):
            return self.retry_delays[retry_count]
        return self.retry_delays[-1]

    @timeout_handler
    def call_api(
        self,
        messages: List[Dict[str, str]],
        response_format: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> str:
        """
        API 调用函数，返回响应内容字符串。
        除response_format 外，其余 LLM 超参均复用 OpenAIAPIManager
        初始化值（model、max_tokens、timeout、max_retries）。
        """
        retry_count = 0
        last_exception = None
        start_time = time.time()

        logger.info(
            f"🚀 开始 API 调用 - 模型: {self.model}, "
            f"消息数: {len(messages)}, 超时: {self.timeout}s"
        )

        while retry_count <= self.max_retries:
            try:
                # 🔥 修复：简化参数处理逻辑
                api_params = {
                    "model": kwargs.get("model", self.model),
                    "messages": messages,
                    "temperature": kwargs.get("temperature", self.temperature),
                    "max_tokens": kwargs.get("max_tokens", self.max_tokens),
                    "timeout": kwargs.get("timeout", self.timeout),
                }

                if response_format:
                    api_params["response_format"] = response_format

                # 排除已经明确处理的参数
                excluded_keys = {"model", "messages", "temperature",
                                 "max_tokens", "timeout", "response_format"}
                for key, value in kwargs.items():
                    if key not in excluded_keys:
                        api_params[key] = value

                # 记录重试信息
                if retry_count > 0:
                    logger.info(f"🔄 第{retry_count}次重试API调用...")

                # 执行API调用
                call_start_time = time.time()
                response = self.client.chat.completions.create(**api_params)
                call_duration = time.time() - call_start_time

                # 检查响应是否有效
                if not response or not response.choices:
                    raise APICallError("API返回空响应或无choices")

                # 🔥 确保返回字符串类型
                content = response.choices[0].message.content
                if content is None:
                    raise APICallError("API返回空内容")

                # 确保内容是字符串类型
                if not isinstance(content, str):
                    content = str(content)

                # 记录成功信息
                total_duration = time.time() - start_time
                usage = response.usage if hasattr(response, 'usage') else None

                logger.info(f"✅ API调用成功!")
                logger.info(f"   - 本次调用耗时: {call_duration:.2f}秒")
                logger.info(f"   - 总耗时: {total_duration:.2f}秒")
                logger.info(f"   - 重试次数: {retry_count}")
                if usage:
                    logger.info(
                        f"   - Token使用: {usage.prompt_tokens}+{usage.completion_tokens}={usage.total_tokens}")

                return content

            except Exception as e:
                last_exception = e

                # 记录错误详情
                error_type = type(e).__name__
                error_msg = str(e)
                logger.error(
                    f"❌ API调用失败 (第{retry_count + 1}次): {error_type}: {error_msg}")

                # 检查是否应该重试
                if not self._should_retry(e, retry_count):
                    logger.error(f"🚫 不可重试的错误或达到最大重试次数，停止重试")
                    break

                # 等待后重试
                if retry_count < self.max_retries:
                    retry_delay = self._get_retry_delay(retry_count)
                    logger.info(f"⏳ 等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)

                retry_count += 1

        # 所有重试都失败了
        total_duration = time.time() - start_time
        error_msg = f"API调用最终失败，重试{retry_count}次，总耗时{total_duration:.2f}秒"
        logger.error(f"💥 {error_msg}")

        if last_exception:
            logger.error(
                f"最后一次错误: {type(last_exception).__name__}: {last_exception}")

        raise APICallError(
            message=error_msg,
            status_code=getattr(last_exception, 'status_code', None),
            retry_count=retry_count
        ) from last_exception


class GPTResponseParser:
    """GPT响应解析工具类"""

    @staticmethod
    def post_process_json(response: str) -> str:
        """基于规则的后处理，从GPT响应中提取纯JSON内容"""
        try:
            response = response.strip()

            # 🔥 步骤1：移除代码块标记
            if '```' in response:
                lines = response.split('\n')
                json_content = []
                in_code_block = False

                for line in lines:
                    line_stripped = line.strip()
                    # 检测代码块开始/结束
                    if line_stripped.startswith('```'):
                        in_code_block = not in_code_block
                        continue
                    # 在代码块内或不在代码块时收集内容
                    if in_code_block or (not in_code_block and line_stripped):
                        json_content.append(line)

                response = '\n'.join(json_content).strip()

            # 🔥 步骤2：查找JSON对象边界（更精确）
            if '{' in response and '}' in response:
                # 找到第一个{
                start = response.find('{')

                # 从start位置开始，正确匹配大括号
                brace_count = 0
                end_pos = -1

                for i, char in enumerate(response[start:], start):
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_pos = i
                            break

                if end_pos != -1:
                    response = response[start:end_pos + 1]
                else:
                    # 如果没找到匹配的}，取到最后一个}
                    last_brace = response.rfind('}')
                    if last_brace > start:
                        response = response[start:last_brace + 1]

             # 🔥 步骤3：增强清理 - 移除隐藏字符和规范化
            # 移除各种隐藏的Unicode字符
            response = re.sub(
                r'[\u200b\u200c\u200d\ufeff\u00a0\u2028\u2029]', '', response)

            # 移除换行和多余空白
            response = re.sub(r'\n\s*', '', response)
            response = re.sub(r'\s+', ' ', response)
            response = response.strip()

            # 🔥 步骤4：引号验证和修复
            response = GPTResponseParser._fix_quote_issues(response)

            # 🔥 步骤5：基础语法检查和修复
            response = GPTResponseParser._basic_syntax_fix(response)

            return response

        except Exception as e:
            logger.warning(f"⚠️ JSON提取失败: {e}")
            return response

    @staticmethod
    def _fix_quote_issues(json_str: str) -> str:
        """修复引号相关问题"""
        try:
            # 🔥 修复字符串内部的嵌套引号
            # 查找 "text"inner"text" 模式并修复为 "text inner text"
            json_str = re.sub(
                r'"([^"]*)"([^"{}[\],]*)"([^"]*)"', r'"\1 \2 \3"', json_str)

            # 🔥 修复单引号为双引号（在JSON中必须是双引号）
            # 但要小心不要替换字符串内容中的单引号
            json_str = re.sub(r"'([^']*)':", r'"\1":', json_str)  # 属性名

            return json_str
        except Exception as e:
            logger.warning(f"⚠️ 引号修复失败: {e}")
            return json_str

    @staticmethod
    def _basic_syntax_fix(json_str: str) -> str:
        """基础语法修复"""
        try:
            # 🔥 修复常见的语法问题
            # 移除对象/数组末尾的多余逗号
            json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)

            # 🔥 修复缺少逗号的问题 - 在}{ 或 ]} 之间添加逗号
            json_str = re.sub(r'}(\s*)([{\[])', r'},\1\2', json_str)
            json_str = re.sub(r'](\s*)([{\[])', r'],\1\2', json_str)

            # 🔥 检查并修复末尾问题
            if json_str.endswith('}]'):
                # 验证是否需要移除末尾的]
                open_braces = json_str.count('{')
                close_braces = json_str.count('}')
                open_brackets = json_str.count('[')
                close_brackets = json_str.count(']')

                if close_brackets > open_brackets:
                    test_json = json_str[:-1]
                    try:
                        import json
                        json.loads(test_json)
                        json_str = test_json
                        logger.debug("🔧 移除末尾多余的']'")
                    except json.JSONDecodeError:
                        pass

            return json_str
        except Exception as e:
            logger.warning(f"⚠️ 基础语法修复失败: {e}")
            return json_str

    @staticmethod
    def parse_json_response(response: str, expected_key: str = None, api_manager=None) -> Dict[str, Any]:
        """通用的GPT JSON响应解析方法"""
        try:
            # 类型检查和转换
            if not isinstance(response, str):
                if isinstance(response, dict):
                    if expected_key and expected_key in response:
                        return {expected_key: response[expected_key]}
                    return response
                response = str(response)

            response = response.strip()
            if not response:
                logger.warning("⚠️ 收到空响应")
                return {}

            # 首先尝试直接解析
            try:
                result = json.loads(response)
                logger.info("✅ 直接JSON解析成功")
                if expected_key and expected_key in result:
                    return {expected_key: result[expected_key]}
                return result
            except json.JSONDecodeError as e:
                logger.info(f"❌ 直接JSON解析失败: {e}")
                logger.debug(
                    f"📋 响应内容: {response}")

                # 使用大模型修复JSON（如果提供了api_manager）
                if api_manager is not None:
                    logger.info("🤖 使用GPT修复JSON格式...")
                    fixed_response = api_manager.fix_json_call_api(
                        response)
                    # 验证修复结果
                    try:
                        result = json.loads(fixed_response)
                        logger.info("✅ GPT修复JSON成功")
                        if expected_key and expected_key in result:
                            return {expected_key: result[expected_key]}
                        return result
                    except json.JSONDecodeError as repair_error:
                        logger.warning(f"⚠️ GPT修复后仍解析失败: {repair_error}")
                        logger.warning(f"⚠️ GPT修复后响应内容: {fixed_response}")
                        logger.info("尝试基于规则的修复")
                        # 🔥 新增：基于规则的后处理
                        fixed_response = GPTResponseParser.post_process_json(
                            fixed_response)
                        # 验证修复结果
                        try:
                            result = json.loads(fixed_response)
                            logger.info("✅ GPT修复JSON成功")
                            if expected_key and expected_key in result:
                                return {expected_key: result[expected_key]}
                            return result
                        except json.JSONDecodeError as repair_error:
                            logger.error(f"❌ 所有修复方式均失败，JSON解析失败")
                            return {}
                else:
                    logger.error("❌ 未配置api_manager，JSON解析失败")
                    return {}

        except Exception as e:
            logger.error(f"❌ JSON解析发生异常: {e}")
            logger.debug(
                f"📋 响应内容: {response}")
            return {}

    @staticmethod
    def parse_knowledge_graph_result(response: str, api_manager=None) -> Dict[str, Any]:
        """解析知识图谱提取结果"""

        def get_parsed_data(resp):
            if isinstance(resp, dict):
                return resp
            return GPTResponseParser.parse_json_response(response=str(resp), api_manager=api_manager)

        def validate_entity(entity):
            """验证实体格式 - 适配新的labels字符串格式"""
            if not isinstance(entity, dict):
                return None

            # 🔥 新格式：属性直接在实体对象上
            if 'labels' in entity and 'name' in entity and 'id' in entity:
                return {
                    'name': entity.get('name', ''),
                    'type': entity.get('labels', ''),  # 🔥 直接使用字符串
                    'id': entity.get('id', ''),
                    'description': entity.get('description', ''),
                    'labels': entity.get('labels', ''),
                }

            # 🔥 🔥 向后兼容：旧格式（labels为字符串 + properties）
            if 'labels' in entity and 'properties' in entity:
                props = entity.get('properties', {})
                if 'name' in props and 'id' in props:
                    return {
                        'name': props['name'],
                        'type': entity.get('labels', ''),  # 🔥 直接使用字符串
                        'id': props['id'],
                        'description': props.get('description', ''),
                        'labels': entity.get('labels', ''),
                        'properties': props
                    }
            return None

        def validate_relationship(rel):
            """验证关系格式"""
            if not isinstance(rel, dict) or not all(k in rel for k in ['source', 'target', 'type']):
                return None

            # 🔥 简化置信度处理
            confidence = rel.get('confidence', 0.7)
            if isinstance(confidence, (int, float)) and 0 <= confidence <= 1:
                rel['confidence'] = round(float(confidence), 2)
            else:
                rel['confidence'] = 0.7

            return rel

        # 主处理逻辑
        try:
            result = get_parsed_data(response)
            entities = result.get('entities', [])
            relationships = result.get('relationships', [])

            # 验证并过滤数据
            valid_entities = [e for e in map(
                validate_entity, entities) if e is not None]
            valid_relationships = [r for r in map(
                validate_relationship, relationships) if r is not None]

            logger.info(
                f"✅ 解析完成: 实体 {len(valid_entities)}个, 关系 {len(valid_relationships)}个")

            return {
                "entities": valid_entities,
                "relationships": valid_relationships
            }

        except Exception as e:
            logger.error(f"❌ 解析知识图谱结果失败: {e}")
            return {"entities": [], "relationships": []}

    @staticmethod
    def parse_merge_groups(response: str, api_manager: OpenAIAPIManager) -> List[List[int]]:
        """解析实体对齐的合并组"""
        try:
            # 🔥 新增：处理带有代码块标识符的响应
            if response.startswith("```"):
                # 提取代码块中的JSON内容
                import re
                json_match = re.search(
                    r"```(?:json)?\s*({.*?})\s*```", response, re.DOTALL)
                if json_match:
                    response = json_match.group(1)
                else:
                    # 如果无法提取JSON，尝试直接去除代码块标记
                    response = re.sub(r"^```(?:json)?\s*|\s*```$",
                                      "", response, flags=re.DOTALL).strip()

            result = GPTResponseParser.parse_json_response(
                response, 'merge_groups', api_manager)
            merge_groups = result.get('merge_groups', [])

            # 验证格式
            valid_groups = []
            for group in merge_groups:
                if isinstance(group, list) and len(group) >= 2:
                    try:
                        int_group = [int(idx) for idx in group]
                        if all(idx >= 0 for idx in int_group):
                            valid_groups.append(int_group)
                    except (ValueError, TypeError):
                        continue

            return valid_groups

        except Exception as e:
            logger.error(f"❌ 解析合并组失败: {e}")
            # 🔥 新增：记录原始响应内容以便调试
            logger.error(f"📋 响应前500字符: {response[:500]}")
            logger.error(f"📋 响应后500字符: {response[-500:]}")
            return []

    @staticmethod
    def parse_qa_json(response: str, api_manager: OpenAIAPIManager) -> List[Dict[str, Any]]:
        """解析QA生成的JSON响应"""
        def validate_qa_pair(qa: Dict[str, Any]) -> bool:
            """验证QA对格式"""
            required_fields = ['question', 'answer']

            # 检查必需字段
            for field in required_fields:
                if field not in qa or not qa[field] or not isinstance(qa[field], str):
                    return False

            # 检查内容长度
            if len(qa['question'].strip()) < 5 or len(qa['answer'].strip()) < 10:
                return False

            # 设置默认值
            if 'type' not in qa:
                qa['type'] = 'factual'
            if 'confidence' not in qa:
                qa['confidence'] = 0.8

            return True
        try:
            # 🔥 新增：处理带有代码块标识符的响应
            if response.startswith("```"):
                # 提取代码块中的JSON内容
                import re
                json_match = re.search(
                    r"```(?:json)?\s*({.*?})\s*```", response, re.DOTALL)
                if json_match:
                    response = json_match.group(1)
                else:
                    # 如果无法提取JSON，尝试直接去除代码块标记
                    response = re.sub(r"^```(?:json)?\s*|\s*```$",
                                      "", response, flags=re.DOTALL).strip()

            result = GPTResponseParser.parse_json_response(
                response, 'qa_pairs', api_manager)
            qa_pairs = result.get('qa_pairs', [])

            # 验证QA对格式
            validated_pairs = []
            for qa in qa_pairs:
                if validate_qa_pair(qa):
                    validated_pairs.append(qa)

            return validated_pairs

        except Exception as e:
            logger.error(f"❌ 解析QA对JSON失败: {e}")
            # 🔥 新增：记录原始响应内容以便调试
            logger.error(f"📋 响应前500字符: {response[:500]}")
            if len(response) > 500:
                logger.error(f"📋 响应后500字符: {response[-500:]}")
            return []


class APICallStats:
    """API调用统计"""

    def __init__(self):
        self.reset()

    def reset(self):
        self.total_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
        self.total_retries = 0
        self.total_tokens = 0
        self.total_duration = 0.0

    def record_call(self, success: bool, retry_count: int = 0, tokens: int = 0, duration: float = 0.0):
        self.total_calls += 1
        self.total_retries += retry_count
        self.total_tokens += tokens
        self.total_duration += duration

        if success:
            self.successful_calls += 1
        else:
            self.failed_calls += 1

    def get_stats(self) -> Dict[str, Any]:
        success_rate = (self.successful_calls /
                        self.total_calls * 100) if self.total_calls > 0 else 0
        avg_retries = (self.total_retries /
                       self.total_calls) if self.total_calls > 0 else 0
        avg_duration = (self.total_duration /
                        self.total_calls) if self.total_calls > 0 else 0

        return {
            'total_calls': self.total_calls,
            'successful_calls': self.successful_calls,
            'failed_calls': self.failed_calls,
            'success_rate': f"{success_rate:.1f}%",
            'total_retries': self.total_retries,
            'avg_retries_per_call': f"{avg_retries:.1f}",
            'total_tokens': self.total_tokens,
            'total_duration': f"{self.total_duration:.2f}s",
            'avg_duration_per_call': f"{avg_duration:.2f}s"
        }


api_stats = APICallStats()


class ConfigManager:
    """配置管理器"""

    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def load_config(cls, config_path: str = None) -> Dict[str, Any]:
        """
        加载配置文件

        Args:
            config_path: 配置文件路径，如果为None则使用默认路径

        Returns:
            配置字典
        """
        if cls._config is not None:
            return cls._config

        if config_path is None:
            # 获取当前文件所在目录（pdf2md目录）
            current_dir = Path(__file__).parent
            config_path = current_dir / "config.json"

        config_path = Path(config_path)

        # 如果配置文件不存在，创建默认配置
        if not config_path.exists():
            logger.warning(f"配置文件不存在: {config_path}，创建默认配置...")
            default_config = cls._get_default_config()
            cls._save_config(default_config, config_path)
            cls._config = default_config
            return default_config

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # 验证和补充配置
            config = cls._validate_and_complete_config(config)
            cls._config = config

            logger.info(f"✅ 配置文件加载成功: {config_path}")
            return config

        except json.JSONDecodeError as e:
            logger.error(f"❌ 配置文件JSON格式错误: {e}")
            logger.info("使用默认配置...")
            default_config = cls._get_default_config()
            cls._config = default_config
            return default_config

        except Exception as e:
            logger.error(f"❌ 配置文件加载失败: {e}")
            logger.info("使用默认配置...")
            default_config = cls._get_default_config()
            cls._config = default_config
            return default_config

    @classmethod
    def _get_default_config(cls) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "openai": {
                "api_key": "",
                "base_url": "https://api.openai.com/v1",
                "model": "gpt-3.5-turbo",
                "timeout": 90,
                "max_retries": 5
            },
            "knowledge_extractor": {
                "min_quality_score": 65,
                "batch_size": 5,
                "enable_api_health_check": True,
                "entity_types": {
                    "ThreatActor": "威胁行为者/攻击组织/APT组织",
                    "Tactics": "战术层面的攻击策略",
                    "Techniques": "具体的攻击技术方法",
                    "Procedures": "详细的执行步骤过程",
                    "Tools": "使用的工具软件"
                },
                "relationship_types": {
                    "USE": "威胁行为者使用战术",
                    "IMPLEMENT": "战术实现技术",
                    "EXECUTE": "技术执行过程",
                    "APPLY": "过程应用工具",
                    "ABSTRACT": "将过程抽象为技术/战术"
                },
                "entity_examples": {
                    "ThreatActor": "APT29, Lazarus Group",
                    "Tactics": "Initial Access, Defense Evasion",
                    "Techniques": "Spear Phishing, DLL Injection",
                    "Procedures": "Create scheduled task, Execute PowerShell",
                    "Tools": "Cobalt Strike, Mimikatz"
                }
            },
            "pdf_converter": {
                "artifacts_path": "./docling-models",
                "do_ocr": False
            },
            "html_converter": {
                "extract_images": True,
                "timeout": 30,
                "max_image_size": 5242880
            },
            "chunk_splitter": {
                "max_chunk_size": 2000,
                "chunk_overlap": 200,
                "separators": ["\n\n", "\n", " ", ""]
            },
            "graph_processor": {
                "entity_alignment": {
                    "similarity_threshold": 0.75,
                    "enable_acronym_match": True,
                    "enable_contains_match": True
                },
                "quality_filters": {
                    "min_entity_name_length": 2,
                    "min_relationship_confidence": 0.6
                }
            },
            "output": {
                "save_intermediate": True,
                "create_timestamp_dirs": False,
                "compression": False
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "file_output": False
            }
        }

    @classmethod
    def _validate_and_complete_config(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证和补充配置"""
        default_config = cls._get_default_config()

        # 递归合并配置，确保所有必需的键都存在
        def merge_configs(default: Dict[str, Any], user: Dict[str, Any]) -> Dict[str, Any]:
            merged = default.copy()
            for key, value in user.items():
                if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                    merged[key] = merge_configs(merged[key], value)
                else:
                    merged[key] = value
            return merged

        complete_config = merge_configs(default_config, config)

        # 验证关键配置
        cls._validate_critical_settings(complete_config)

        return complete_config

    @classmethod
    def _validate_critical_settings(cls, config: Dict[str, Any]):
        """验证关键配置项"""
        # 检查OpenAI API密钥
        openai_config = config.get('openai', {})
        api_key = openai_config.get('api_key', '')

        if not api_key or api_key == "your-api-key-here":
            logger.warning("⚠️ OpenAI API密钥未设置，请在config.json中设置api_key")

        # 检查超时设置
        timeout = openai_config.get('timeout', 90)
        if timeout < 10:
            logger.warning(f"⚠️ API超时设置过低: {timeout}秒，建议至少30秒")

        # 检查chunk大小设置
        chunk_config = config.get('chunk_splitter', {})
        max_chunk_size = chunk_config.get('max_chunk_size', 2000)
        if max_chunk_size < 500:
            logger.warning(f"⚠️ chunk大小设置过小: {max_chunk_size}，可能影响信息提取效果")

        # 检查质量评分设置
        kg_config = config.get('knowledge_extractor', {})
        min_quality_score = kg_config.get('min_quality_score', 65)
        if min_quality_score < 30:
            logger.warning(f"⚠️ 质量评分阈值过低: {min_quality_score}，可能包含低质量数据")

    @classmethod
    def _save_config(cls, config: Dict[str, Any], config_path: Path):
        """保存配置文件"""
        try:
            # 确保目录存在
            config_path.parent.mkdir(parents=True, exist_ok=True)

            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)

            logger.info(f"✅ 配置文件已保存: {config_path}")

        except Exception as e:
            logger.error(f"❌ 配置文件保存失败: {e}")

    @classmethod
    def update_config(cls, key_path: str, value: Any) -> bool:
        """
        更新配置项

        Args:
            key_path: 配置键路径，如 "openai.api_key"
            value: 新值

        Returns:
            是否更新成功
        """
        if cls._config is None:
            cls.load_config()

        try:
            # 分解键路径
            keys = key_path.split('.')
            current = cls._config

            # 导航到父级
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]

            # 设置值
            current[keys[-1]] = value

            # 保存配置
            current_dir = Path(__file__).parent
            config_path = current_dir / "config.json"
            cls._save_config(cls._config, config_path)

            logger.info(f"✅ 配置已更新: {key_path} = {value}")
            return True

        except Exception as e:
            logger.error(f"❌ 配置更新失败: {e}")
            return False

    @classmethod
    def get_config_value(cls, key_path: str, default_value: Any = None) -> Any:
        """
        获取配置值

        Args:
            key_path: 配置键路径，如 "openai.api_key"
            default_value: 默认值

        Returns:
            配置值
        """
        if cls._config is None:
            cls.load_config()

        try:
            keys = key_path.split('.')
            current = cls._config

            for key in keys:
                if key in current:
                    current = current[key]
                else:
                    return default_value

            return current

        except Exception as e:
            logger.error(f"❌ 获取配置值失败: {e}")
            return default_value

    @classmethod
    def reload_config(cls, config_path: str = None) -> Dict[str, Any]:
        """重新加载配置文件"""
        cls._config = None
        return cls.load_config(config_path)

    @classmethod
    def print_config_summary(cls):
        """打印配置摘要"""
        if cls._config is None:
            cls.load_config()

        logger.info("📋 当前配置摘要:")
        logger.info(
            f"   - OpenAI模型: {cls._config.get('openai', {}).get('model', 'N/A')}")
        logger.info(
            f"   - API超时: {cls._config.get('openai', {}).get('timeout', 'N/A')}秒")
        logger.info(
            f"   - 最大重试: {cls._config.get('openai', {}).get('max_retries', 'N/A')}次")
        logger.info(
            f"   - chunk大小: {cls._config.get('chunk_splitter', {}).get('max_chunk_size', 'N/A')}")
        logger.info(
            f"   - 质量阈值: {cls._config.get('quality_filter', {}).get('min_quality_score', 'N/A')}")
        logger.info(
            f"   - 保存中间结果: {cls._config.get('output', {}).get('save_intermediate', 'N/A')}")


class APICallError(Exception):
    """API调用异常"""

    def __init__(self, message: str, status_code: Optional[int] = None, retry_count: int = 0):
        super().__init__(message)
        self.status_code = status_code
        self.retry_count = retry_count


class TokenCounter:
    """Token计数器"""

    @staticmethod
    def get_tokens_num(text_list: List[str]) -> int:
        """
        直接调用EAS接口，基于LLM模型分词，计算token数量
        注意：此函数不支持批量处理，批量需自己改造为异步模式
        """
        api_key = "MTM0NWE3NDBhYzEyZjE4YmUwNTU3OTg3MjM5ZGIyOGJkMTAzOTM0YQ=="
        api_base = "http://jianyu.aliyun-inc.com/agent/pai/api/predict/yundun_prehost"

        def _headers(api_key: str) -> dict:
            return {"Authorization": f"Bearer {api_key}"}

        try:
            response = requests.post(
                url=api_base + '/count_token',
                headers=_headers(api_key),
                json={"prompt": text_list[0]}
            )

            if response.text.strip():
                time.sleep(0.1)
                rst = response.json()
                logger.info("token_num: %d", rst['count'])
                return rst['count']
            else:
                logger.info("Empty or whitespace-only response received.")
                return 0

        except json.JSONDecodeError as e:
            logger.exception("Invalid JSON format: %s", e)
            logger.exception("text_list: %s", text_list)
            logger.exception("response: %s", response.text)
            return 0
        except Exception as e:
            logger.error(f"Token计算请求失败: {e}")
            return 0

    @staticmethod
    def count_tokens(text: str) -> int:
        """计算文本的token数量 - 适配函数"""
        try:
            return TokenCounter.get_tokens_num([text])
        except Exception as e:
            logger.error(f"Token计算失败: {e}")
            # 简单估算作为备选方案
            chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
            english_words = len(re.findall(r'[a-zA-Z]+', text))
            return chinese_chars + english_words


class FileManager:
    """文件管理器"""

    @staticmethod
    def ensure_directory(file_path: str):
        """确保目录存在"""
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def save_json(data: Dict[str, Any], file_path: str):
        """保存JSON数据到文件"""
        try:
            FileManager.ensure_directory(file_path)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"数据已保存到: {file_path}")
        except Exception as e:
            logger.error(f"保存JSON文件失败: {e}")
            raise

    @staticmethod
    def load_json(file_path: str) -> Dict[str, Any]:
        """从文件加载JSON数据"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载JSON文件失败: {e}")
            raise

    @staticmethod
    def save_text(content: str, file_path: str):
        """保存文本内容到文件"""
        try:
            FileManager.ensure_directory(file_path)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f"文本已保存到: {file_path}")
        except Exception as e:
            logger.error(f"保存文本文件失败: {e}")
            raise


class StatisticsReporter:
    """统计报告器 - 支持聚合统计格式的图数据"""

    def analyze_aggregated_graph_data(self, graph_list: List[Dict]) -> Dict[str, Any]:
        """分析聚合统计格式的图数据"""
        if not graph_list:
            return {"unique_nodes": 0, "unique_edges": 0, "node_type_counts": {}, "edge_type_counts": {}}

        logger.info(f"🔍 开始分析聚合图数据，共{len(graph_list)}个统计块")

        # 统计实体种类数和出现次数
        node_type_counts = {}       # 每种节点类型有多少个不同实体
        edge_type_counts = {}       # 每种关系类型有多少个不同关系
        node_occurrences = {}       # 节点类型总出现次数
        edge_occurrences = {}       # 关系类型总出现次数
        unique_nodes = {}           # 记录每种类型的唯一实体
        unique_edges = {}           # 记录每种类型的唯一关系

        for i, data_block in enumerate(graph_list):
            # 处理nodes统计
            if 'nodes' in data_block:
                nodes_data = data_block['nodes']
                logger.info(f"   块[{i}] - 节点统计项: {len(nodes_data)}")

                for node_key, count in nodes_data.items():
                    try:
                        import json
                        node_info = json.loads(node_key)
                        node_type = node_info.get('entity_type', 'Unknown')
                        node_label = node_info.get('label', 'Unknown')

                        # 统计出现次数
                        node_occurrences[node_type] = node_occurrences.get(
                            node_type, 0) + count

                        # 统计实体种类数
                        if node_type not in unique_nodes:
                            node_type_counts[node_type] = 0
                            unique_nodes[node_type] = set()

                        # 用label作为唯一标识
                        if node_label not in unique_nodes[node_type]:
                            unique_nodes[node_type].add(node_label)
                            node_type_counts[node_type] += 1

                    except json.JSONDecodeError:
                        node_occurrences['Unknown'] = node_occurrences.get(
                            'Unknown', 0) + count
                        if 'Unknown' not in unique_nodes:
                            node_type_counts['Unknown'] = 0
                            unique_nodes['Unknown'] = set()

            # 处理edges统计
            if 'edges' in data_block:
                edges_data = data_block['edges']
                logger.info(f"   块[{i}] - 边统计项: {len(edges_data)}")

                for edge_key, count in edges_data.items():
                    try:
                        import json
                        edge_info = json.loads(edge_key)
                        edge_type = edge_info.get('label', 'Unknown')

                        # 统计出现次数
                        edge_occurrences[edge_type] = edge_occurrences.get(
                            edge_type, 0) + count

                        # 统计关系种类数
                        if edge_type not in unique_edges:
                            edge_type_counts[edge_type] = 0
                            unique_edges[edge_type] = set()

                        # 用完整edge_key作为唯一标识
                        if edge_key not in unique_edges[edge_type]:
                            unique_edges[edge_type].add(edge_key)
                            edge_type_counts[edge_type] += 1

                    except json.JSONDecodeError:
                        edge_occurrences['Unknown'] = edge_occurrences.get(
                            'Unknown', 0) + count
                        if 'Unknown' not in unique_edges:
                            edge_type_counts['Unknown'] = 0
                            unique_edges['Unknown'] = set()

        # 计算总数
        total_unique_nodes = sum(node_type_counts.values())
        total_unique_edges = sum(edge_type_counts.values())
        total_node_occurrences = sum(node_occurrences.values())
        total_edge_occurrences = sum(edge_occurrences.values())

        logger.info(
            f"🔍 聚合分析完成: {total_unique_nodes}个不同节点(出现{total_node_occurrences}次), {total_unique_edges}个不同边(出现{total_edge_occurrences}次)")

        return {
            "unique_nodes": total_unique_nodes,
            "unique_edges": total_unique_edges,
            "total_node_occurrences": total_node_occurrences,
            "total_edge_occurrences": total_edge_occurrences,
            "node_type_counts": node_type_counts,
            "edge_type_counts": edge_type_counts,
            "node_occurrences": node_occurrences,
            "edge_occurrences": edge_occurrences
        }

    def print_graph_summary(self, graph_list: List[Dict], chunk_count: int, data_source: str = ""):
        """打印图数据摘要"""
        logger.info(f"🔍 开始统计图数据，数据源: {data_source}")

        stats = self.analyze_aggregated_graph_data(graph_list)

        logger.info("=" * 50)
        logger.info(f"📊 图谱统计({data_source})")
        logger.info("=" * 50)
        logger.info(
            f"🏷️  节点总数: {stats['unique_nodes']} (出现次数: {stats['total_node_occurrences']})")
        logger.info(
            f"🔗 关系总数: {stats['unique_edges']} (出现次数: {stats['total_edge_occurrences']})")
        logger.info(f"📄 处理块数: {chunk_count}")

        # 显示完整的节点类型统计
        logger.info("📋 完整节点类型统计:")
        for node_type, unique_count in sorted(stats['node_type_counts'].items(), key=lambda x: x[1], reverse=True):
            occurrence_count = stats['node_occurrences'].get(node_type, 0)
            logger.info(
                f"   {node_type}: {unique_count}种 (出现{occurrence_count}次)")

        # 显示完整的关系类型统计
        logger.info("🔗 完整关系类型统计:")
        for edge_type, unique_count in sorted(stats['edge_type_counts'].items(), key=lambda x: x[1], reverse=True):
            occurrence_count = stats['edge_occurrences'].get(edge_type, 0)
            logger.info(
                f"   {edge_type}: {unique_count}种 (出现{occurrence_count}次)")

        # 主要类型显示（前5名）
        top_node_types = sorted(
            stats['node_type_counts'].items(), key=lambda x: x[1], reverse=True)[:5]
        top_edge_types = sorted(
            stats['edge_type_counts'].items(), key=lambda x: x[1], reverse=True)[:5]

        logger.info(
            f"🎯 主要节点类型: {', '.join([f'{k}({v})' for k, v in top_node_types])}")
        logger.info(
            f"🔄 主要关系类型: {', '.join([f'{k}({v})' for k, v in top_edge_types])}")
        logger.info("=" * 50)


class ContentAnalyzer:
    """内容分析器 - 统一处理文档内容分析和保存"""

    def analyze_and_optionally_save(
        self,
        docs: List,
        selected_docs: List,
        selection_info: Dict,
        output_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        分析文档内容，可选择保存到文件

        Args:
            docs: 所有文档块
            selected_docs: 选中的文档块
            selection_info: 选择信息
            output_dir: 输出目录，None表示不保存文件
        """
        # 基础统计分析
        content_analysis = self.analyze_content_distribution(docs)

        # 添加选择相关的统计
        content_analysis.update({
            'selected_chunks_count': len(selected_docs),
            'selection_info': selection_info,
            'selection_ratio': len(selected_docs) / len(docs) if docs else 0
        })

        # 保存详细信息到文件
        if output_dir:
            self._save_chunks_analysis(
                docs, selected_docs, selection_info, output_dir)

        return content_analysis

    def _save_chunks_analysis(
        self,
        docs: List,
        selected_docs: List,
        selection_info: Dict,
        output_dir: str
    ):
        """保存chunks分析信息到文件"""
        try:
            # 保存选中的chunks
            selected_chunks_path = os.path.join(
                output_dir, "03_selected_chunks.json")
            chunks_data = {
                'selected_chunks': [
                    {
                        'content': doc.page_content,
                        'metadata': doc.metadata,
                        'length': doc.metadata.get('token_length', len(doc.page_content))
                    }
                    for doc in selected_docs
                ],
                'selection_info': selection_info,
                'total_chunks': len(docs),
                'selected_count': len(selected_docs)
            }

            file_manager = FileManager()
            file_manager.save_json(chunks_data, selected_chunks_path)
            logger.info(f"✅ 已保存选中chunks信息到: {selected_chunks_path}")

            # 🔥 新增：保存所有chunks到02.5_total_chunks.json
            total_chunks_path = os.path.join(
                output_dir, "02.5_total_chunks.json")
            total_chunks_data = [
                {
                    'content': doc.page_content,
                    'metadata': doc.metadata,
                    'length': doc.metadata.get('token_length', len(doc.page_content))
                }
                for doc in docs
            ]

            file_manager.save_json(total_chunks_data, total_chunks_path)
            logger.info(f"✅ 已保存所有chunks信息到: {total_chunks_path}")

        except Exception as e:
            logger.error(f"❌ 保存chunks分析信息失败: {e}")

    def analyze_content_distribution(self, docs: List) -> Dict[str, Any]:
        """分析文档内容分布（保持原有功能）"""
        if not docs:
            return {
                'total_chunks': 0,
                'avg_length': 0,
                'length_distribution': {},
                'content_types': {}
            }

        # 长度统计
        lengths = [len(doc.page_content) for doc in docs]

        # 长度分布
        length_ranges = {
            '0-500': 0,
            '500-1000': 0,
            '1000-2000': 0,
            '2000+': 0
        }

        for length in lengths:
            if length <= 500:
                length_ranges['0-500'] += 1
            elif length <= 1000:
                length_ranges['500-1000'] += 1
            elif length <= 2000:
                length_ranges['1000-2000'] += 1
            else:
                length_ranges['2000+'] += 1

        # 内容类型简单分析
        content_types = {
            'text_heavy': 0,
            'code_like': 0,
            'mixed': 0
        }

        for doc in docs:
            content = doc.page_content.lower()
            if any(keyword in content for keyword in ['def ', 'class ', 'import ', '{', '}', ';']):
                content_types['code_like'] += 1
            elif len(content.split()) > 50:
                content_types['text_heavy'] += 1
            else:
                content_types['mixed'] += 1

        return {
            'total_chunks': len(docs),
            'avg_length': sum(lengths) / len(lengths) if lengths else 0,
            'min_length': min(lengths) if lengths else 0,
            'max_length': max(lengths) if lengths else 0,
            'length_distribution': length_ranges,
            'content_types': content_types
        }


class ProgressTracker:
    """进度追踪器"""

    def __init__(self, total_steps: int):
        self.total_steps = total_steps
        self.current_step = 0
        self.start_time = time.time()

    def update(self, description: str = ""):
        """更新进度"""
        self.current_step += 1
        progress = (self.current_step / self.total_steps) * 100

        elapsed_time = time.time() - self.start_time
        if self.current_step > 0:
            estimated_total_time = elapsed_time * self.total_steps / self.current_step
            remaining_time = estimated_total_time - elapsed_time
        else:
            remaining_time = 0

        logger.info(
            f"进度: {progress:.1f}% ({self.current_step}/{self.total_steps}) - "
            f"{description} - 预计剩余: {remaining_time:.1f}秒"
        )
