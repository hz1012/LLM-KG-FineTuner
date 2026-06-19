# coding:utf-8
"""
API 管理模块 - OpenAI API 调用、响应解析、统计与异常
"""
import re
import json
import time
import requests
import logging
from typing import List, Dict, Any, Optional
from functools import wraps
from openai import OpenAI
import openai

logger = logging.getLogger(__name__)


# ── 装饰器 ──────────────────────────────────────────────

def timeout_handler(func):
    """超时装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 允许函数自己处理超时，不额外添加超时控制
        return func(*args, **kwargs)

    return wrapper


# ── 异常 ────────────────────────────────────────────────

class APICallError(Exception):
    """API调用异常"""

    def __init__(self, message: str, status_code: Optional[int] = None, retry_count: int = 0):
        super().__init__(message)
        self.status_code = status_code
        self.retry_count = retry_count


# ── API 调用统计 ────────────────────────────────────────

class APICallStats:
    """API调用统计"""

    def __init__(self):
        self.reset()

    def reset(self):
        self.total_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
        self.total_retries = 0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_duration = 0.0

    def record_call(self, success: bool, retry_count: int = 0,
                    input_tokens: int = 0, output_tokens: int = 0,
                    duration: float = 0.0):
        self.total_calls += 1
        self.total_retries += retry_count
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        self.total_duration += duration

        if success:
            self.successful_calls += 1
        else:
            self.failed_calls += 1

    @property
    def total_tokens(self) -> int:
        """总 token 数（input + output）"""
        return self.total_input_tokens + self.total_output_tokens

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
            'total_input_tokens': self.total_input_tokens,
            'total_output_tokens': self.total_output_tokens,
            'total_tokens': self.total_tokens,
            'total_duration': f"{self.total_duration:.2f}s",
            'avg_duration_per_call': f"{avg_duration:.2f}s"
        }


api_stats = APICallStats()


# ── GPT 响应解析 ────────────────────────────────────────

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

            # 🔥 新增步骤6：修复转义序列问题
            response = GPTResponseParser._fix_escape_sequences(response)

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
    def _fix_escape_sequences(json_str: str) -> str:
        """修复JSON中的非法转义序列"""
        try:
            # 修复常见的非法转义字符，特别是路径中的反斜杠
            # 查找并修复类似 HKEY_CURRENT_USER\Console\0 这样的路径

            # 使用正则表达式匹配可能存在问题的转义序列
            # 匹配那些不是合法JSON转义字符的反斜杠
            json_str = re.sub(r'(?<!\\)\\(?![\\/bfnrt"])(?![0-9]{3})', r'\\\\', json_str)

            return json_str
        except Exception as e:
            logger.warning(f"⚠️ 转义序列修复失败: {e}")
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
                            logger.info("✅ 基于规则的修复JSON成功")
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
    def parse_merge_groups(response: str, api_manager) -> List[List[int]]:
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
    def parse_qa_json(response: str, api_manager) -> List[Dict[str, Any]]:
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


# ── OpenAI API 管理器 ───────────────────────────────────

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
        self.last_usage = None  # 记录最近一次 API 调用的 token 使用情况

        # 初始化retry_delays属性，实现指数退避算法
        self.retry_delays = [
            min(2 ** i * 1000, 60000) / 1000.0 for i in range(self.max_retries)]

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )

    def fix_json_call_api(self, broken_json: str) -> str:
        """使用GPT修复损坏的JSON格式"""
        try:
            system_prompt = """你是JSON语法修复专家。严格遵守JSON规范，只输出修复后的JSON。确保实体结构扁平化，不使用properties嵌套。

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
6. 修复非法转义字符，特别是路径中的反斜杠，如"HKEY_CURRENT_USER\\Console\\0"应正确转义为"HKEY_CURRENT_USER\\\\Console\\\\0"

直接输出修复后的JSON，无其他内容。"""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": broken_json}
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
        # 增加对retry_delays属性存在性的检查
        if hasattr(self, 'retry_delays') and retry_count < len(self.retry_delays):
            return self.retry_delays[retry_count]
        # 提供默认的指数退避算法
        return min(2 ** retry_count * 1000, 60000) / 1000.0  # 最大延迟60秒

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

                # 保存最近一次调用的 token 信息，供外部获取
                self.last_usage = usage

                # 记录成功调用统计
                call_duration_for_stats = time.time() - start_time
                api_stats.record_call(
                    success=True,
                    retry_count=retry_count,
                    input_tokens=usage.prompt_tokens if usage else 0,
                    output_tokens=usage.completion_tokens if usage else 0,
                    duration=call_duration_for_stats
                )

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

        # 记录失败调用统计
        api_stats.record_call(
            success=False,
            retry_count=retry_count,
            duration=total_duration
        )

        if last_exception:
            logger.error(
                f"最后一次错误: {type(last_exception).__name__}: {last_exception}")

        raise APICallError(
            message=error_msg,
            status_code=getattr(last_exception, 'status_code', None),
            retry_count=retry_count
        ) from last_exception
