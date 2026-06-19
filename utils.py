# coding:utf-8
"""
工具函数模块 - 兼容性 re-export 层

本文件保留用于向后兼容。所有类已拆分到独立模块，
通过本文件 re-export 确保现有 `from utils import ...` 代码无需修改。

模块拆分如下：
  - api_manager    : OpenAIAPIManager, GPTResponseParser, APICallStats, APICallError, api_stats
  - config_manager : ConfigManager
  - file_manager   : FileManager
  - stats_reporter : StatisticsReporter

本文件保留：ContentAnalyzer, ProgressTracker
"""
import os
import time
import logging
from typing import List, Dict, Any, Optional

# ── 向后兼容 re-export ──────────────────────────────────
from api_manager import (          # noqa: F401
    timeout_handler,
    APICallError,
    APICallStats,
    api_stats,
    GPTResponseParser,
    OpenAIAPIManager,
)
from config_manager import ConfigManager   # noqa: F401
from file_manager import FileManager       # noqa: F401
from stats_reporter import StatisticsReporter  # noqa: F401

logger = logging.getLogger(__name__)


# ── 内容分析器 ─────────────────────────────────────────

class ContentAnalyzer:
    """内容分析器 - 统一处理文档内容分析和保存"""

    def analyze_and_optionally_save(
        self,
        docs: List,
        selected_docs: List,
        selection_info: Dict,
        output_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """分析文档内容，可选择保存到文件"""
        content_analysis = self.analyze_content_distribution(docs)
        content_analysis.update({
            'selected_chunks_count': len(selected_docs),
            'selection_info': selection_info,
            'selection_ratio': len(selected_docs) / len(docs) if docs else 0
        })
        if output_dir:
            self._save_chunks_analysis(
                docs, selected_docs, selection_info, output_dir)
        return content_analysis

    def _save_chunks_analysis(self, docs, selected_docs, selection_info, output_dir):
        """保存chunks分析信息到文件"""
        try:
            selected_chunks_path = os.path.join(output_dir, "03_selected_chunks.json")
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

            total_chunks_path = os.path.join(output_dir, "02.5_total_chunks.json")
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
        """分析文档内容分布"""
        if not docs:
            return {
                'total_chunks': 0, 'avg_length': 0,
                'length_distribution': {}, 'content_types': {}
            }
        lengths = [len(doc.page_content) for doc in docs]
        length_ranges = {'0-500': 0, '500-1000': 0, '1000-2000': 0, '2000+': 0}
        for length in lengths:
            if length <= 500:
                length_ranges['0-500'] += 1
            elif length <= 1000:
                length_ranges['500-1000'] += 1
            elif length <= 2000:
                length_ranges['1000-2000'] += 1
            else:
                length_ranges['2000+'] += 1
        content_types = {'text_heavy': 0, 'code_like': 0, 'mixed': 0}
        for doc in docs:
            content = doc.page_content.lower()
            if any(kw in content for kw in ['def ', 'class ', 'import ', '{', '}', ';']):
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


# ── 进度追踪器 ─────────────────────────────────────────

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
