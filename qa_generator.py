# coding:utf-8
"""
QA对生成模块 - 基于文档chunks生成问答对
"""
import logging
import json
import time
from typing import List, Dict, Any, Optional
from langchain.docstore.document import Document
from utils import OpenAIAPIManager, GPTResponseParser
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


class QAGenerator:
    """QA对生成器 - 为每个chunk生成问答对"""

    def __init__(self, config: Dict[str, Any], api_manager: OpenAIAPIManager):
        """
        初始化QA生成器

        Args:
            config: QA生成配置
            api_manager: API管理器实例
        """
        self.config = config
        self.api_manager = api_manager

        # 从配置中读取参数
        self.qa_per_chunk = config.get('qa_per_chunk', 3)
        self.batch_size = config.get('batch_size', 5)
        self.max_workers = config.get('max_workers', 3)
        self.enable_threading = config.get('enable_threading', True)
        self.answer_language = config.get('answer_language', 'chinese')
        self.question_types = config.get(
            'question_types', ['factual', 'analytical', 'inferential'])

        logger.info(f"🤖 QA生成器初始化完成 - 每chunk生成{self.qa_per_chunk}个QA对")

    def generate_qa_for_chunks(self, chunks: List[Document], max_chunks: int = None) -> List[Dict[str, Any]]:
        """
        为所有chunks生成QA对

        Args:
            chunks: 文档块列表
            max_chunks: 最大处理数量

        Returns:
            包含QA对的结果列表
        """
        logger.info(f"🚀 开始为{len(chunks)}个chunks生成QA对")

        if max_chunks:
            chunks = chunks[:max_chunks]
            logger.info(f"📝 限制处理数量为{max_chunks}个chunks")

        results = []
        start_time = time.time()

        if self.enable_threading and len(chunks) > 1:
            results = self._generate_qa_parallel(chunks)
        else:
            results = self._generate_qa_sequential(chunks)

        total_time = time.time() - start_time
        total_qa_pairs = sum(len(result.get('qa_pairs', []))
                             for result in results)

        logger.info(f"✅ QA生成完成！")
        logger.info(f"   - 处理chunks: {len(chunks)}个")
        logger.info(f"   - 生成QA对: {total_qa_pairs}个")
        logger.info(f"   - 总耗时: {total_time:.2f}秒")
        logger.info(f"   - 平均速度: {total_qa_pairs/total_time:.2f}个QA对/秒")

        return results

    def _generate_qa_sequential(self, chunks: List[Document]) -> List[Dict[str, Any]]:
        """顺序生成QA对"""
        results = []

        for i, chunk in enumerate(chunks, 1):
            logger.info(f"📝 处理第{i}/{len(chunks)}个chunk")
            result = self._generate_qa_for_single_chunk(chunk, i)
            results.append(result)

        return results

    def _generate_qa_parallel(self, chunks: List[Document]) -> List[Dict[str, Any]]:
        """并行生成QA对"""
        results = [None] * len(chunks)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_index = {
                executor.submit(self._generate_qa_for_single_chunk, chunk, i+1): i
                for i, chunk in enumerate(chunks)
            }

            # 收集结果
            completed = 0
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                completed += 1

                try:
                    result = future.result()
                    results[index] = result
                    logger.info(
                        f"✅ 完成第{index+1}个chunk QA生成 ({completed}/{len(chunks)})")

                except Exception as e:
                    logger.error(f"❌ 第{index+1}个chunk QA生成失败: {e}")
                    results[index] = {
                        'chunk_index': index + 1,
                        'status': 'failed',
                        'error': str(e),
                        'qa_pairs': []
                    }

        return results

    def _generate_qa_for_single_chunk(self, chunk: Document, chunk_index: int) -> Dict[str, Any]:
        """为单个chunk生成QA对"""
        try:
            content = chunk.page_content
            metadata = chunk.metadata

            # 检查内容长度
            if len(content.strip()) < 50:
                logger.warning(f"⚠️ 第{chunk_index}个chunk内容过短，跳过QA生成")
                return {
                    'chunk_index': chunk_index,
                    'status': 'skipped',
                    'reason': 'content_too_short',
                    'qa_pairs': [],
                    'chunk_metadata': metadata
                }

            # 构建提示词
            prompt = self._build_qa_generation_prompt(content)

            # 调用API生成QA对
            messages = [
                {"role": "system", "content": self._get_system_prompt()},
                {"role": "user", "content": prompt}
            ]

            response = self.api_manager.call_api(
                messages=messages,
                temperature=0.7,
                max_tokens=2000
            )

            # 解析响应
            qa_pairs = self._parse_qa_response(response, chunk_index)

            return {
                'chunk_index': chunk_index,
                'status': 'success',
                'qa_pairs': qa_pairs,
                'chunk_content': content,
                'chunk_metadata': metadata,
                'api_response': response
            }

        except Exception as e:
            logger.error(f"❌ 第{chunk_index}个chunk QA生成异常: {e}")
            return {
                'chunk_index': chunk_index,
                'status': 'failed',
                'error': str(e),
                'qa_pairs': [],
                'chunk_metadata': metadata
            }

    def _get_system_prompt(self) -> str:
        """获取系统提示词"""
        language_instruction = {
            'chinese': '请用中文回答',
            'english': 'Please answer in English',
            'bilingual': '请用中英文双语回答'
        }.get(self.answer_language, '请用中文回答')

        return f"""你是一个专业的问答对生成专家。

任务：根据给定的文档内容，生成高质量的问答对。

要求：
1. 生成{self.qa_per_chunk}个不同类型的问答对
2. 问题类型包括：事实性问题、分析性问题、推理性问题
3. 问题要具体、清晰，避免过于宽泛
4. 答案要准确、完整，基于文档内容
5. {language_instruction}
6. 严格按照JSON格式输出

输出格式：
{{
    "qa_pairs": [
        {{
            "question": "具体问题",
            "answer": "详细答案",
            "type": "factual|analytical|inferential",
            "confidence": 0.85
        }}
    ]
}}"""

    def _build_qa_generation_prompt(self, content: str) -> str:
        """构建QA生成提示词"""
        return f"""请基于以下文档内容生成{self.qa_per_chunk}个高质量问答对：

【文档内容】
{content}

【生成要求】
- 生成{self.qa_per_chunk}个问答对
- 包含不同类型的问题（事实性、分析性、推理性）
- 问题要具体、有针对性
- 答案要基于文档内容，准确完整
- 按JSON格式输出

请直接输出JSON结果："""

    def _parse_qa_response(self, response: str, chunk_index: int) -> List[Dict[str, Any]]:
        """解析API响应中的QA对"""
        try:
            # 🔥 使用GPTResponseParser进行解析
            qa_pairs = GPTResponseParser.parse_qa_json(
                response, self.api_manager)

            # 为每个QA对添加chunk相关的ID
            validated_pairs = []
            for i, qa in enumerate(qa_pairs):
                qa['id'] = f"chunk_{chunk_index}_qa_{i+1}"
                validated_pairs.append(qa)

            if validated_pairs:
                logger.info(
                    f"✅ 第{chunk_index}个chunk成功生成{len(validated_pairs)}个QA对")
                return validated_pairs
            else:
                logger.warning(f"⚠️ 第{chunk_index}个chunk未生成有效QA对")
                return []

        except Exception as e:
            logger.error(f"❌ 第{chunk_index}个chunk QA解析异常: {e}")
            return []

    def generate_qa_summary(self, qa_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """生成QA对统计摘要"""
        total_chunks = len(qa_results)
        successful_chunks = len(
            [r for r in qa_results if r.get('status') == 'success'])
        total_qa_pairs = sum(len(r.get('qa_pairs', [])) for r in qa_results)

        # 按类型统计
        type_counts = {}
        for result in qa_results:
            for qa in result.get('qa_pairs', []):
                qa_type = qa.get('type', 'unknown')
                type_counts[qa_type] = type_counts.get(qa_type, 0) + 1

        # 平均置信度
        confidences = []
        for result in qa_results:
            for qa in result.get('qa_pairs', []):
                if 'confidence' in qa:
                    confidences.append(qa['confidence'])

        avg_confidence = sum(confidences) / \
            len(confidences) if confidences else 0

        return {
            'total_chunks_processed': total_chunks,
            'successful_chunks': successful_chunks,
            'failed_chunks': total_chunks - successful_chunks,
            'total_qa_pairs': total_qa_pairs,
            'avg_qa_per_chunk': total_qa_pairs / successful_chunks if successful_chunks > 0 else 0,
            'qa_types_distribution': type_counts,
            'average_confidence': round(avg_confidence, 3),
            'success_rate': round(successful_chunks / total_chunks * 100, 2) if total_chunks > 0 else 0
        }
