# coding:utf-8
"""
主模块 - 协调各个功能模块完成PDF到知识图谱的完整流程
"""
import os
import logging
import random
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from langchain.docstore.document import Document
# 导入各个功能模块
from document_converter import DocumentConverter
from markdown_processor import MarkdownProcessor

from chunk_splitter import ChunkSplitter
from knowledge_graph_extractor import KnowledgeGraphExtractor
from utils import FileManager, StatisticsReporter, ContentAnalyzer, ConfigManager, ProgressTracker, OpenAIAPIManager
from graph_data_processor import EnhancedGraphDataProcessor
from quality_filter import QualityFilter
from qa_generator import QAGenerator
from graph_enhancer import GraphEnhancer


# 设置日志
# 先读配置，再配日志
config = ConfigManager.load_config()
log_cfg = config.get("logging", {})

log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO),
    format=log_cfg.get(
        "format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
    handlers=[
        logging.StreamHandler(),
        *([logging.FileHandler(log_dir/log_cfg["filename"])] if log_cfg.get("file_output") and log_cfg.get("filename") else [])
    ]
)
logger = logging.getLogger(__name__)


class Document2KnowledgeGraphPipeline:  #
    """文档到知识图谱的完整处理流水线（支持PDF和HTML）"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化处理流水线

        Args:
            config: 配置字典，如果为None则使用默认配置
        """
        # 🔥 修改：从config.json加载配置
        if config is None:
            self.config = ConfigManager.load_config()
            # 打印配置摘要
            ConfigManager.print_config_summary()
        else:
            self.config = config

        self.document_converter = DocumentConverter(self.config)

        self.markdown_processor = MarkdownProcessor()

        chunk_config = self.config.get('chunk_splitter', {})
        # 处理document_title，如果是"None"字符串则转换为None
        document_title = chunk_config.get('document_title', None)
        if document_title == "None":
            document_title = None
        self.chunk_splitter = ChunkSplitter(
            max_chunk_size=chunk_config.get('max_chunk_size', 2000),
            chunk_overlap=chunk_config.get('chunk_overlap', 200),
            document_title=document_title
        )

        openai_config = self.config.get('openai', {})
        api_manager = OpenAIAPIManager(openai_config)

        # 🔥 新增：QA生成器初始化
        qa_config = self.config.get('qa_generator', {})
        self.qa_generator = QAGenerator(qa_config, api_manager)

        # 🔥 传入完整配置给知识图谱提取器
        kg_config = self.config.get('knowledge_extractor', {})
        self.kg_extractor = KnowledgeGraphExtractor(
            kg_config=kg_config,
            api_manager=api_manager
        )

        # 🔥 新增：图谱增强器初始化
        enhancer_config = self.config.get('graph_enhancer', {})
        self.graph_enhancer = GraphEnhancer(enhancer_config, api_manager)

        # 工具类
        self.file_manager = FileManager()
        self.stats_reporter = StatisticsReporter()
        self.content_analyzer = ContentAnalyzer()
        self.quality_filter = QualityFilter(self.config)

        # 使用增强版图数据处理器
        self.graph_processor = EnhancedGraphDataProcessor(config=self.config)

        # 🔥 添加：显示加载的配置文件位置
        logger.info(f"📁 配置文件位置: {Path(__file__).parent / 'config.json'}")

    def process_document_file(
        self,
        file_path: str,
        output_dir: str = "./output",
        save_intermediate: bool = True,
        max_chunks: int = None,
        chunk_selection_strategy: str = "quality",
        enable_qa_generation: bool = False  # 是否生成qa对
    ) -> Dict[str, Any]:
        """
        处理文档文件的完整流程（支持PDF和HTML）

        Args:
            file_path: 文件路径（支持本地PDF/HTML文件和远程URL）
            output_dir: 输出目录
            save_intermediate: 是否保存中间结果
            max_chunks: 最大处理chunk数，None表示处理所有chunk
            chunk_selection_strategy: chunk选择策略
            enable_qa_generation： 是否生成qa对

        Returns:
            包含最终结果和统计信息的字典
        """

        # 🔥 动态计算总步骤数
        total_steps = 8  # 基础步骤
        if enable_qa_generation:
            total_steps += 1

        progress = ProgressTracker(total_steps)

        try:
            logger.info(f"开始处理文档文件: {file_path}")

            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)

            # 🔥 简化：一步完成检测和转换
            progress.update("文档类型检测和转换")
            file_type, raw_markdown, extracted_images = self.document_converter.detect_and_convert(
                file_path)

            if save_intermediate:
                raw_md_path = os.path.join(output_dir, "01_raw_markdown.md")
                self.file_manager.save_text(raw_markdown, raw_md_path)

                # 保存提取的图片信息
                if extracted_images:
                    images_info_path = os.path.join(
                        output_dir, "00_extracted_images.json")
                    self.file_manager.save_json(
                        extracted_images, images_info_path)
                    logger.info(f"提取了{len(extracted_images)}张图片信息")

            # 2. Markdown后处理
            progress.update("Markdown后处理")
            processed_markdown = self.markdown_processor.post_process_markdown(
                raw_markdown)

            if save_intermediate:
                processed_md_path = os.path.join(
                    output_dir, "02_processed_markdown.md")
                self.file_manager.save_text(
                    processed_markdown, processed_md_path)

            # 3. 文档分块 chunk选择逻辑
            progress.update("文档分块")
            docs = self.chunk_splitter.process_document(processed_markdown)
            if save_intermediate:
                total_chunks_path = os.path.join(
                    output_dir, "02.5_total_chunks.json")
                # 将Document对象转换为可序列化的字典格式
                docs_data = [
                    {
                        'content': doc.page_content,
                        'metadata': doc.metadata,
                        'length': doc.metadata.get('token_length', len(doc.page_content))
                    }
                    for doc in docs
                ]
                self.file_manager.save_json(
                    docs_data, total_chunks_path)

            selected_docs, selection_info = self._select_chunks_for_processing(
                docs, max_chunks, chunk_selection_strategy
            )

            # 4. chunk内容分析
            progress.update("chunk内容分析")

            # 🔥 统一使用ContentAnalyzer，传入保存选项
            content_analysis = self.content_analyzer.analyze_and_optionally_save(
                docs=docs,
                selected_docs=selected_docs,
                selection_info=selection_info,
                output_dir=output_dir if save_intermediate else None
            )
            # 5. QA对生成
            qa_results = None
            qa_summary = None
            if enable_qa_generation:
                progress.update("QA对生成")
                qa_results = self.qa_generator.generate_qa_for_chunks(
                    selected_docs,
                    max_chunks=len(selected_docs)
                )

                qa_summary = self.qa_generator.generate_qa_summary(qa_results)

                # 保存QA结果
                if save_intermediate:
                    qa_path = os.path.join(output_dir, "03.5_qa_pairs.json")
                    self.file_manager.save_json({
                        'qa_results': qa_results,
                        'qa_summary': qa_summary
                    }, qa_path)

                    logger.info(
                        f"✅ QA生成完成: {qa_summary['total_qa_pairs']}个QA对")

            # 6. 知识图谱抽取
            progress.update("知识图谱抽取")
            kg_results = self.kg_extractor.extract_from_chunks(
                selected_docs)

            # 保存对齐前的原始点边数据
            if save_intermediate:
                raw_graph_data = self.graph_processor.extract_raw_graph_data(
                    kg_results)
                raw_graph_path = os.path.join(
                    output_dir, "04_raw_graph_data.json")
                self.file_manager.save_json(raw_graph_data, raw_graph_path)

            # 7. 生成对齐后的知识图谱数据
            full_kg_data, simple_kg_data = self.graph_processor.extract_pure_graph_data(
                kg_results)

            # 🔥 紧跟在步骤7后保存完整记录数据
            if save_intermediate:
                full_kg_path = os.path.join(
                    output_dir, "05_knowledge_graph_full.json")
                self.file_manager.save_json(full_kg_data, full_kg_path)

                # 保存简化图数据
                simple_kg_path = os.path.join(
                    output_dir, "06_knowledge_graph_simple.json")
                self.file_manager.save_json(simple_kg_data, simple_kg_path)

            # 8. 图谱增强
            # 🔥 添加可控参数enable_graph_enhancement
            enable_graph_enhancement = self.config.get(
                'graph_enhancer', {}).get('enable', True)
            enhanced_kg_data = None
            enhancement_stats = None

            if enable_graph_enhancement:
                progress.update("图谱ES增强")
                enhanced_kg_data, enhancement_stats = self.graph_enhancer.enhance_knowledge_graph(
                    simple_kg_data)

                # 🔥 紧跟在步骤8后保存增强后的图数据
                if save_intermediate:
                    enhanced_kg_path = os.path.join(
                        output_dir, "07_enhanced_knowledge_graph.json")
                    self.file_manager.save_json(
                        enhanced_kg_data, enhanced_kg_path)

                    # 保存增强统计信息
                    enhancement_stats_path = os.path.join(
                        output_dir, "08_enhancement_stats.json")
                    self.file_manager.save_json(
                        enhancement_stats, enhancement_stats_path)
            else:
                logger.info("⏭️  跳过图谱增强步骤")
                enhanced_kg_data = simple_kg_data
                enhancement_stats = {}

            # 9. 生成统计报告
            progress.update("生成统计报告")

            # 🔥 精简：直接使用StatisticsReporter处理聚合格式数据
            if enhanced_kg_data and len(enhanced_kg_data) > 0:
                self.stats_reporter.print_graph_summary(
                    enhanced_kg_data, len(selected_docs), "增强后")
            elif simple_kg_data and len(simple_kg_data) > 0:
                self.stats_reporter.print_graph_summary(
                    simple_kg_data, len(selected_docs), "简化")
            else:
                logger.info("=" * 60)
                logger.info("🎯 知识图谱处理完成")
                logger.info("=" * 60)
                logger.info("📊 图谱统计: 0个实体, 0个关系")
                logger.info(f"📄 处理文档块: {len(selected_docs)}个")
                logger.info("=" * 60)

            # 10. 保存最终结果
            progress.update("保存结果")
            final_results = {
                'source_file': file_path,
                'file_type': file_type,
                'extracted_images_count': len(extracted_images) if extracted_images else 0,
                'processing_config': self.config,
                'content_analysis': content_analysis,
                'chunk_selection_info': selection_info,
                'qa_generation_results': qa_results,
                'qa_summary': qa_summary,
                'knowledge_graph_results': kg_results,
                'aligned_knowledge_graph': full_kg_data,
                'enhanced_knowledge_graph': enhanced_kg_data,
                'enhancement_stats': enhancement_stats
            }

            results_path = os.path.join(output_dir, "09_final_results.json")
            self.file_manager.save_json(final_results, results_path)

            return final_results

        except Exception as e:
            logger.error(f"❌ 处理失败: {e}", exc_info=True)
            raise

    def _select_chunks_for_processing(
        self,
        docs: List[Document],
        max_chunks: int = None,
        strategy: str = "quality"
    ) -> Tuple[List[Document], Dict[str, Any]]:
        """
        选择要处理的chunks，支持多种选择策略

        Args:
            docs: 所有文档块
            max_chunks: 最大处理数量
            strategy: 选择策略 - 'quality'(质量优先)、'first'(前N个)、'random'(随机选择)

        Returns:
            (选中的文档块, 选择信息)
        """

        logger.info(f"📋 开始chunk选择，总计{len(docs)}个chunk，策略: {strategy}")

        # 步骤1: 质量过滤（所有策略都需要）
        filtered_docs_result = self.quality_filter.filter_chunks(docs)
        # 正确解包filter_chunks的返回值（元组）
        if isinstance(filtered_docs_result, tuple):
            filtered_docs, filtered_chunks_info = filtered_docs_result
        else:
            filtered_docs = filtered_docs_result
            filtered_chunks_info = None

        if not filtered_docs:
            logger.warning("⚠️ 质量过滤后无可用chunk")
            return [], {
                'strategy': strategy,
                'original_count': len(docs),
                'filtered_count': 0,
                'selected_count': 0,
                'quality_threshold': self.quality_filter.min_quality_score,
                'message': '质量过滤后无可用chunk'
            }

        # 步骤2: 根据策略选择chunk
        if strategy == "first":
            # 策略1: 选择前N个（按原顺序）
            selected_docs = filtered_docs[:max_chunks] if max_chunks else filtered_docs

        elif strategy == "quality":
            # 策略2: 按质量分数排序，选择得分最高的N个
            scored_docs = []
            for doc in filtered_docs:
                score, _ = self.quality_filter.calculate_quality_score(
                    doc.page_content,
                    doc.metadata.get('type', 'text')
                )
                scored_docs.append((doc, score))

            # 按质量分数降序排序
            scored_docs.sort(key=lambda x: x[1], reverse=True)
            selected_docs = [doc for doc, score in scored_docs[:max_chunks]] if max_chunks else [
                doc for doc, score in scored_docs]

        elif strategy == "random":
            # 策略3: 随机选择N个
            if max_chunks and max_chunks < len(filtered_docs):
                selected_docs = random.sample(filtered_docs, max_chunks)
            else:
                selected_docs = filtered_docs

        else:
            logger.warning(f"⚠️ 未知策略'{strategy}'，使用默认'quality'策略")
            # 默认使用quality策略
            return self._select_chunks_for_processing(docs, max_chunks, "quality")

        # 记录选择结果
        selection_info = {
            'strategy': strategy,
            'original_count': len(docs),
            'filtered_count': len(filtered_docs),
            'selected_count': len(selected_docs),
            'quality_threshold': self.quality_filter.min_quality_score
        }

        # 为quality策略添加额外统计信息
        if strategy == "quality" and selected_docs:
            quality_scores = []
            for doc in selected_docs:
                score, _ = self.quality_filter.calculate_quality_score(
                    doc.page_content,
                    doc.metadata.get('type', 'text')
                )
                quality_scores.append(score)

            selection_info.update({
                'avg_quality_score': round(sum(quality_scores) / len(quality_scores), 1),
                'min_quality_score': min(quality_scores),
                'max_quality_score': max(quality_scores)
            })

        logger.info(
            f"✅ 策略'{strategy}'选择完成: {len(filtered_docs)} -> {len(selected_docs)}个chunk")

        return selected_docs, selection_info


def main():
    """主函数 - 处理文档并生成知识图谱"""

    # 测试文件列表
    test_files = [
        {
            'path': "test_samples/sample_1.html",
            'type': 'HTML',
            'output_dir': "html_output"
        },
        {
            'path': "https://www.example.com/research/sample-report.html",
            'type': 'HTML',
            'output_dir': "html_output_2"
        },
        {
            'path': "test_samples/sample_document.pdf",
            'type': 'PDF',
            'output_dir': "pdf_output"
        }
    ]

    # 选择要测试的文件（默认测试HTML文件）
    test_file = test_files[1]  # 修改索引来选择不同的测试文件

    logger.info(f"🚀 开始测试{test_file['type']}文件处理流程")
    logger.info(f"📁 输入文件: {test_file['path']}")
    logger.info(f"📂 输出目录: {test_file['output_dir']}")

    # 确保输出目录存在
    os.makedirs(test_file['output_dir'], exist_ok=True)

    # 初始化处理流水线
    pipeline = Document2KnowledgeGraphPipeline()

    try:
        # 处理文档
        results = pipeline.process_document_file(
            file_path=test_file['path'],
            output_dir=test_file['output_dir'],
            save_intermediate=True,
            max_chunks=None,
            chunk_selection_strategy="quality",
            enable_qa_generation=False  # 禁用QA生成
        )

        # 打印QA生成摘要
        qa_summary = results.get('qa_summary')
        if qa_summary:
            logger.info("📊 QA生成摘要:")
            logger.info(f"   - 总QA对数: {qa_summary['total_qa_pairs']}")
            logger.info(f"   - 成功率: {qa_summary['success_rate']}%")
            logger.info(f"   - 平均置信度: {qa_summary['average_confidence']}")
            logger.info(f"   - 问题类型分布: {qa_summary['qa_types_distribution']}")
            logger.info("✅ QA生成示例完成")

        # 打印处理结果摘要
        logger.info(f"🎉 处理完成！")
        logger.info(f"   - 源文件: {results['source_file']}")
        logger.info(f"   - 文件类型: {results['file_type']}")
        logger.info(
            f"   - 总chunk数: {results['chunk_selection_info']['original_count']}")
        logger.info(
            f"   - 过滤后chunk数: {results['chunk_selection_info']['filtered_count']}")
        # 🔥 修复：使用正确的字段名
        # 或 'selected_chunks'
        logger.info(
            f"   - 选中chunk数: {results['chunk_selection_info']['selected_count']}")
        logger.info(
            f"   - 选择策略: {results['chunk_selection_info']['strategy']}")

        # 特殊处理HTML文件的图片信息
        if results['file_type'] == 'html' and results.get('extracted_images_count', 0) > 0:
            logger.info(f"🖼️  已提取{results['extracted_images_count']}张图片信息")

            # 下载图片到本地，传入正确的base_url
            try:
                image_output_dir = os.path.join(
                    test_file['output_dir'], "images")

                # 确保base_url正确
                if test_file['path'].startswith('http'):
                    base_url = test_file['path']
                else:
                    # 本地文件，使用文件所在目录作为base_url
                    base_url = f"file://{os.path.dirname(os.path.abspath(test_file['path']))}/"

                downloaded_images = pipeline.document_converter.html_converter.download_images(
                    image_output_dir,
                    base_url=base_url
                )
                logger.info(
                    f"📥 成功下载{len(downloaded_images)}张图片到: {image_output_dir}")

            except Exception as e:
                logger.warning(f"图片下载失败: {e}")

        logger.info(f"📁 详细结果请查看: {test_file['output_dir']}")

    except Exception as e:
        logger.error(f"❌ 处理失败: {e}")
        raise


if __name__ == "__main__":
    main()
