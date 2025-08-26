# coding:utf-8
"""
文档分块模块 - 负责将文档切分成合适大小的块
"""
import re
from typing import List, Dict, Any, Tuple
from langchain.text_splitter import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from langchain.docstore.document import Document
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)

# 初始化OpenAI客户端用于计算token长度
embedding_client = OpenAI(
    api_key="sk-d8626ac601d843d1800a0e349f7c3c8b",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)


def get_token_length(text: str) -> int:
    """
    使用OpenAI embedding模型计算文本的token长度

    Args:
        text: 输入文本

    Returns:
        int: 文本的token数量
    """
    try:
        # 使用OpenAI的embedding接口计算token数量
        response = embedding_client.embeddings.create(
            input=text,
            model="text-embedding-v2"
        )
        # 从response中获取token使用情况
        if hasattr(response, 'usage') and hasattr(response.usage, 'prompt_tokens'):
            return response.usage.prompt_tokens
        else:
            # 如果无法获取usage信息，则使用一个估算方法
            # 根据DashScope文档，text-embedding-v2模型的token计算方法
            logger.warning("无法获取usage信息，使用粗略的估算方法")
            return len(text) // 4  # 一个粗略的估算，一般中文每个token约4个字符
    except Exception as e:
        logger.warning(f"无法使用OpenAI模型计算token长度，使用字符长度估算: {e}")
        # 出错时回退到字符长度估算
        return len(text) // 4


class ChunkSplitter:
    """文档分块器"""

    def __init__(self, max_chunk_size: int = 2000, chunk_overlap: int = 200, document_title: str = None):
        """
        初始化分块器

        Args:
            max_chunk_size: 最大块大小
            chunk_overlap: 块重叠大小
        """
        self.max_chunk_size = max_chunk_size
        self.chunk_overlap = chunk_overlap
        self.document_title = document_title

        # 设置标题层级，包含到4级标题
        self.headers_to_split_on = [
            ("#", "Header 1"),
            ("##", "Header 2"),
            ("###", "Header 3"),
            ("####", "Header 4"),
        ]

        self.markdown_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=self.headers_to_split_on,
            strip_headers=False
        )

        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=max_chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=get_token_length,
            separators=["\n\n", "\n", " ", ""]
        )

    def split_markdown_by_headers(self, markdown_content: str) -> List[Document]:
        """使用标题分割markdown文档"""
        try:
            # 🔥 新增：提取并缓存文档主标题 如果没有给标题
            if not self.document_title:
                self.document_title = self._extract_main_title(
                    markdown_content)

            # 获取分割后的文档块
            docs = self.markdown_splitter.split_text(markdown_content)

            # 为每个文档块添加完整的标题层次结构信息
            enhanced_docs = []
            for doc in docs:
                # 提取当前块的完整标题层次结构
                header_hierarchy = self._extract_header_hierarchy(
                    markdown_content, doc)
                # 将标题层次结构存储在metadata中
                doc.metadata['header_hierarchy'] = header_hierarchy
                enhanced_docs.append(doc)

            return enhanced_docs
        except Exception as e:
            logger.error(f"标题分割失败: {e}")
            # 降级到普通文本分割
            return [Document(page_content=markdown_content, metadata={})]

    def _extract_header_hierarchy(self, markdown_content: str, doc: Document) -> List[str]:
        """提取文档块的完整标题层次结构"""
        # 获取文档的所有标题行
        lines = markdown_content.split('\n')
        all_headers = []

        for line in lines:
            line_stripped = line.strip()
            # 检测各级标题
            if (line_stripped.startswith('# ') or
                line_stripped.startswith('## ') or
                line_stripped.startswith('### ') or
                    line_stripped.startswith('#### ')) and not line_stripped.startswith('#####'):
                all_headers.append(line_stripped)

        # 获取当前文档块的内容行
        doc_lines = doc.page_content.split('\n')
        # 找到当前块中的标题
        current_headers = []
        for line in doc_lines:
            line_stripped = line.strip()
            if (line_stripped.startswith('# ') or
                line_stripped.startswith('## ') or
                line_stripped.startswith('### ') or
                    line_stripped.startswith('#### ')) and not line_stripped.startswith('#####'):
                current_headers.append(line_stripped)

        # 如果当前块没有标题，则返回空列表
        if not current_headers:
            return []

        # 获取当前块的主要标题（最后一个标题）
        target_header = current_headers[-1] if current_headers else None
        if not target_header:
            return []

        # 构建从文档标题到目标标题的完整路径
        header_hierarchy = []
        if self.document_title:
            header_hierarchy.append(f"# {self.document_title}")

        # 跟踪当前的标题层次结构
        current_hierarchy = [
            f"# {self.document_title}"] if self.document_title else []

        # 遍历所有标题，构建层次结构
        for header in all_headers:
            # 跳过文档主标题，因为我们已经添加了
            if header.startswith('# ') and self.document_title and self.document_title in header:
                continue

            # 确定标题级别
            level = header.count('#')

            # 调整当前层次结构的深度
            if level > len(current_hierarchy):
                current_hierarchy.append(header)
            else:
                current_hierarchy = current_hierarchy[:level-1]
                current_hierarchy.append(header)

            # 如果找到了目标标题，则保存当前层次结构
            if header == target_header:
                # 只保留从文档标题开始的完整路径
                header_hierarchy = current_hierarchy[:]
                break

        return header_hierarchy

    def extract_markdown_table_and_text(self, docs: List[Document]) -> List[Document]:
        """区分表格和文本内容，将混合内容拆分成不同的doc"""
        result_docs = []

        for doc in docs:
            content = doc.page_content
            lines = content.split('\n')

            current_segment = []
            current_type = None

            for line in lines:
                line_stripped = line.strip()

                # 判断是否为表格行
                is_table = (line_stripped.startswith('|') and
                            line_stripped.endswith('|') and
                            line_stripped.count('|') >= 2)

                # 判断是否为标题行
                is_header = (line_stripped.startswith('#') and 
                           not line_stripped.startswith('#####'))

                if is_table:
                    # 如果之前是文本，先保存文本段
                    if current_type == 'text' and current_segment:
                        text_content = '\n'.join(current_segment).strip()
                        # 🔥 修改：过滤掉只包含标题和空行的文本段
                        if text_content and not self._is_only_headers_and_empty_lines(current_segment):
                            new_doc = Document(
                                page_content=text_content,
                                metadata={**doc.metadata,
                                          'content_type': 'text'}
                            )
                            result_docs.append(new_doc)
                        current_segment = []

                    current_type = 'table'
                    current_segment.append(line)

                else:
                    # 如果之前是表格，先保存表格段
                    if current_type == 'table' and current_segment:
                        table_content = '\n'.join(current_segment).strip()
                        if table_content:
                            new_doc = Document(
                                page_content=table_content,
                                metadata={**doc.metadata,
                                          'content_type': 'table'}
                            )
                            result_docs.append(new_doc)
                        current_segment = []

                    current_type = 'text'
                    current_segment.append(line)

            # 处理最后一段
            if current_segment:
                final_content = '\n'.join(current_segment).strip()
                # 🔥 修改：过滤掉只包含标题和空行的文本段
                if final_content and not self._is_only_headers_and_empty_lines(current_segment):
                    new_doc = Document(
                        page_content=final_content,
                        metadata={**doc.metadata,
                                  'content_type': current_type or 'text'}
                    )
                    result_docs.append(new_doc)

        return result_docs

    def _is_only_headers_and_empty_lines(self, lines: List[str]) -> bool:
        """
        🔥 新增：判断文本段是否只包含标题和空行，没有实质性内容
        
        Args:
            lines: 文本行列表
            
        Returns:
            bool: 如果只包含标题和空行返回True，否则返回False
        """
        substantive_content_count = 0
        
        for line in lines:
            line_stripped = line.strip()
            
            # 跳过空行和标题行
            if not line_stripped or (line_stripped.startswith('#') and not line_stripped.startswith('#####')):
                continue
                
            # 检查是否为分隔符行（如 ---）
            if re.match(r'^\s*[-=]{3,}\s*$', line_stripped):
                continue
                
            # 其他行都算作实质性内容
            substantive_content_count += 1
            
        # 如果没有实质性内容，返回True
        return substantive_content_count == 0

    def _extract_header_context(self, content: str) -> List[str]:
        """🔥 新增：提取完整的标题上下文（支持1-4级标题）"""
        lines = content.split('\n')
        header_context = []

        # 用于跟踪当前各级标题的状态
        current_headers = ["", "", "", ""]  # 对应 #, ##, ###, ####

        for line in lines:
            line_stripped = line.strip()

            # 检测各级标题并更新状态
            if line_stripped.startswith('#### ') and not line_stripped.startswith('#####'):
                current_headers[3] = line_stripped
            elif line_stripped.startswith('### ') and not line_stripped.startswith('####'):
                current_headers[2] = line_stripped
                current_headers[3] = ""  # 清空更低级别的标题状态
            elif line_stripped.startswith('## ') and not line_stripped.startswith('###'):
                current_headers[1] = line_stripped
                current_headers[2] = ""  # 清空更低级别的标题状态
                current_headers[3] = ""
            elif line_stripped.startswith('# ') and not line_stripped.startswith('##'):
                current_headers[0] = line_stripped
                current_headers[1] = ""  # 清空更低级别的标题状态
                current_headers[2] = ""
                current_headers[3] = ""

        # 构建当前有效的标题层级（按照层级顺序）
        for header in current_headers:
            if header:
                header_context.append(header)

        return header_context

    def _extract_main_title(self, markdown_content: str) -> str:
        """🔥 新增：提取文档主标题（第一个一级标题）"""
        lines = markdown_content.split('\n')

        for line in lines:
            line_stripped = line.strip()
            # 检测一级标题
            if line_stripped.startswith('# ') and not line_stripped.startswith('## '):
                title = line_stripped[2:].strip()
                logger.info(f"📋 提取到文档主标题: {title}")
                return title
            # 也支持使用 = 符号的一级标题
            elif line_stripped and len(line_stripped) > 0:
                # 检查下一行是否是 === 分隔符
                next_line_idx = lines.index(line) + 1
                if (next_line_idx < len(lines) and
                        lines[next_line_idx].strip().startswith('==')):
                    title = line_stripped
                    logger.info(f"📋 提取到文档主标题（=格式）: {title}")
                    return title

        logger.error("未解析出主标题，在config.json里面添加document_title")
        raise ValueError("未解析出主标题，在config.json里面添加document_title")

    def _split_text_with_header_context(self, doc: Document) -> List[Document]:
        """🔥 新增：切分文本时保持标题上下文"""
        content = doc.page_content

        # 提取完整的标题上下文
        header_context = doc.metadata.get('header_hierarchy', [])
        if not header_context:
            header_context = self._extract_header_context(content)

        # 使用RecursiveCharacterTextSplitter切分
        sub_docs = self.text_splitter.split_documents([doc])

        # 为每个子chunk补充标题上下文
        enhanced_sub_docs = []
        for i, sub_doc in enumerate(sub_docs):
            # 检查子chunk是否已包含标题
            sub_content = sub_doc.page_content.strip()

            # 🔥 新增：构建完整的标题上下文
            full_header_context = self._build_full_header_context(
                sub_content, header_context)

            # 如果有完整标题上下文，添加到chunk开头
            if full_header_context:
                enhanced_content = f"{full_header_context}\n\n{sub_content}"
                enhanced_metadata = {
                    **sub_doc.metadata,
                    'has_full_header_context': True,
                    'document_title': self.document_title,
                    'section_header': '\n'.join(header_context) if header_context else "",
                    'sub_chunk_index': i
                }

                # 添加Header层级字段
                self._add_header_fields(enhanced_metadata, header_context)
            else:
                enhanced_content = sub_content
                enhanced_metadata = {
                    **sub_doc.metadata,
                    'has_full_header_context': False,
                    'sub_chunk_index': i
                }

                # 即使没有完整标题上下文，也尝试添加Header层级字段
                self._add_header_fields(enhanced_metadata, header_context)

            enhanced_doc = Document(
                page_content=enhanced_content,
                metadata=enhanced_metadata
            )
            enhanced_sub_docs.append(enhanced_doc)

        logger.debug(
            f"🔗 文本切分: {len(sub_docs)}个子chunk，{len([d for d in enhanced_sub_docs if d.metadata.get('has_full_header_context')])}个补充了完整标题上下文")

        return enhanced_sub_docs

    def _add_header_fields(self, metadata: dict, header_context: List[str]):
        """添加Header层级字段到metadata"""
        # 初始化所有Header字段
        for i in range(1, 5):
            metadata[f'Header {i}'] = ""

        # 根据header_context按层级顺序填充Header字段
        current_headers = ["", "", "", ""]  # 对应 #, ##, ###, ####

        # 从header_context中提取当前有效的标题
        for header in header_context:
            if header.startswith('# ') and not header.startswith('##'):
                current_headers[0] = header
            elif header.startswith('## ') and not header.startswith('###'):
                current_headers[1] = header
            elif header.startswith('### ') and not header.startswith('####'):
                current_headers[2] = header
            elif header.startswith('#### ') and not header.startswith('#####'):
                current_headers[3] = header

        # 按层级顺序设置Header字段
        for i in range(4):
            if current_headers[i]:
                metadata[f'Header {i+1}'] = current_headers[i].strip()

    def _build_full_header_context(self, content: str, section_headers: List[str]) -> str:
        """🔥 新增：构建完整的标题上下文（文档标题+完整的层级标题）"""
        context_parts = []

        # 添加文档主标题（如果还没有）
        if self.document_title and not any(self.document_title in line for line in content.split('\n') if line.strip().startswith('#')):
            context_parts.append(f"# {self.document_title}")

        # 添加层级标题（如果内容中还没有）
        # 先解析当前内容中的标题层级
        existing_levels = {}
        content_lines = content.split('\n')
        for line in content_lines:
            line_stripped = line.strip()
            if line_stripped.startswith('#'):
                # 计算标题级别
                level = line_stripped.split(' ')[0]
                level_num = level.count('#')
                existing_levels[level_num] = line_stripped

        # 按照标题层级顺序添加缺失的标题
        current_headers = ["", "", "", ""]  # 对应 #, ##, ###, ####

        # 从section_headers中提取当前有效的标题
        for header in section_headers:
            if header.startswith('# ') and not header.startswith('##'):
                current_headers[0] = header
            elif header.startswith('## ') and not header.startswith('###'):
                current_headers[1] = header
            elif header.startswith('### ') and not header.startswith('####'):
                current_headers[2] = header
            elif header.startswith('#### ') and not header.startswith('#####'):
                current_headers[3] = header

        # 按层级顺序添加缺失的标题
        for i in range(4):
            header = current_headers[i]
            if header and (i+1) not in existing_levels:
                # 特殊处理：不添加文档主标题（已经单独处理过了）
                header_stripped = header.strip()
                if not (header_stripped.startswith('# ') and self.document_title and self.document_title in header_stripped):
                    context_parts.append(header_stripped)
                    existing_levels[i+1] = header_stripped  # 添加到已存在级别中，防止重复

        return '\n'.join(context_parts) if context_parts else ""

    def further_split_large_chunks(self, docs: List[Document]) -> List[Document]:
        """进一步切分过大的块，对表格进行特殊处理"""
        result_docs = []

        for doc in docs:
            # 在metadata中添加token_length字段，避免后续重复计算
            if 'token_length' not in doc.metadata:
                doc.metadata['token_length'] = get_token_length(
                    doc.page_content)

            if doc.metadata['token_length'] > self.max_chunk_size:
                content_type = doc.metadata.get('content_type', 'text')

                if content_type == 'table':
                    # 🔥 表格类型：特殊处理，确保每个子chunk都包含标题行
                    sub_docs = self._split_large_table(doc)
                    result_docs.extend(sub_docs)
                else:
                    # 普通文本：使用默认切分器
                    sub_docs = self._split_text_with_header_context(doc)
                    result_docs.extend(sub_docs)
            else:
                # 🔥 对于不需要进一步切分的小块，也要检查并添加文档主标题
                enhanced_doc = self._ensure_document_title_context(doc)
                result_docs.append(enhanced_doc)

        return result_docs

    def _ensure_document_title_context(self, doc: Document) -> Document:
        """🔥 新增：确保文档块包含文档主标题上下文"""
        content = doc.page_content.strip()

        # 检查内容是否已包含文档主标题
        has_main_title = False
        if self.document_title:
            content_lines = content.split('\n')
            for line in content_lines:
                if line.strip().startswith('# ') and self.document_title in line:
                    has_main_title = True
                    break

        # 如果没有文档主标题且存在文档主标题，则添加
        if self.document_title and not has_main_title:
            # 提取当前内容的标题上下文
            header_context = doc.metadata.get('header_hierarchy', [])
            if not header_context:
                header_context = self._extract_header_context(content)

            # 构建完整的标题上下文
            full_header_context = self._build_full_header_context(
                content, header_context)

            if full_header_context:
                enhanced_content = f"{full_header_context}\n\n{content}"
            else:
                enhanced_content = f"# {self.document_title}\n\n{content}"

            enhanced_metadata = {
                **doc.metadata,
                'has_full_header_context': True,
                'document_title': self.document_title
            }

            # 添加Header层级字段
            self._add_header_fields(enhanced_metadata, header_context)

            logger.debug(f"📋 为小块添加文档主标题: {self.document_title}")

            return Document(
                page_content=enhanced_content,
                metadata=enhanced_metadata
            )
        else:
            # 确保metadata中包含document_title信息
            enhanced_metadata = {
                **doc.metadata,
                'document_title': self.document_title
            }
            if self.document_title and has_main_title:
                enhanced_metadata['has_full_header_context'] = True

            # 添加Header层级字段
            header_context = doc.metadata.get('header_hierarchy', [])
            if not header_context:
                header_context = self._extract_header_context(doc.page_content)
            self._add_header_fields(enhanced_metadata, header_context)

            return Document(
                page_content=content,
                metadata=enhanced_metadata
            )

    def _split_large_table(self, doc: Document) -> List[Document]:
        """
        🔥 新增：切分大表格，确保每个子chunk都包含标题行
        """
        content = doc.page_content
        lines = content.split('\n')

        # 识别表格结构
        table_lines = []
        header_lines = []
        separator_line = None
        data_lines = []

        for i, line in enumerate(lines):
            line_stripped = line.strip()
            if not line_stripped:
                continue

            if line_stripped.startswith('|') and line_stripped.endswith('|'):
                table_lines.append(line)

                # 检查是否是分隔行 (|---|---|)
                if re.match(r'^\|\s*:?-+:?\s*(\|\s*:?-+:?\s*)*\|$', line_stripped):
                    separator_line = line

                    # 🔥 修复：处理分隔行在第一行的情况
                    if i == 0 or len(table_lines) == 1:
                        # 分隔行在第一行，下一行应该是标题行
                        header_lines = []  # 暂时为空，后面从数据行中提取
                    else:
                        # 标准格式：分隔行之前的都是标题行
                        header_lines = table_lines[:-1]  # 不包括分隔行本身

                elif separator_line is None:
                    # 还没遇到分隔行，当前行可能是标题行
                    continue
                else:
                    # 分隔行之后的行
                    data_lines.append(line)

        # 处理分隔行在第一行的特殊情况
        if separator_line and not header_lines and data_lines:
            # 如果分隔行在第一行且没有标题行，将第一个数据行作为标题行
            if data_lines:
                header_lines = [data_lines[0]]
                data_lines = data_lines[1:]
                logger.info(f"检测到分隔行在第一行，将 '{header_lines[0].strip()}' 作为标题行")

        # 如果没有找到分隔行，将第一行作为标题行
        if separator_line is None and table_lines:
            header_lines = [table_lines[0]]
            # 生成分隔行
            columns = table_lines[0].split('|')[1:-1]  # 去掉首尾空元素
            separator_line = '|' + '|'.join(['---' for _ in columns]) + '|'
            data_lines = table_lines[1:]

        if not header_lines or not data_lines:
            # 如果无法解析表格结构，将第一行作为标题行
            logger.warning("无法解析表格结构，使用默认文本切分")
            logger.info(f"原始文本:\n{content}")
            return self.text_splitter.split_documents([doc])

        # 计算标题部分的大小（使用token长度）
        header_content = '\n'.join(header_lines + [separator_line])
        header_size = get_token_length(header_content)

        # 计算每个子chunk可以容纳多少数据行
        available_size = self.max_chunk_size - header_size - 20  # 预留20 token缓冲

        if available_size < 50:
            # 如果标题太长，无法合理切分，使用默认切分
            logger.warning(f"表格标题过长({header_size} tokens)，无法合理切分")
            return self.text_splitter.split_documents([doc])

        # 按照可用空间切分数据行
        result_docs = []
        chunk_data_lines = []
        current_size = 0

        for data_line in data_lines:
            line_size = get_token_length(data_line) + 1  # +1 for newline

            if current_size + line_size > available_size and chunk_data_lines:
                # 当前chunk已满，创建新的chunk
                chunk_content = '# '+self.document_title + '\n' + header_content + \
                    '\n' + '\n'.join(chunk_data_lines)

                chunk_doc = Document(
                    page_content=chunk_content,
                    metadata={
                        **doc.metadata,
                        'is_table_chunk': True,
                        'has_header': True,
                        'chunk_data_rows': len(chunk_data_lines),
                        # 添加token_length到metadata
                        'token_length': get_token_length(chunk_content)
                    }
                )
                result_docs.append(chunk_doc)

                # 重置当前chunk
                chunk_data_lines = []
                current_size = 0

            chunk_data_lines.append(data_line)
            current_size += line_size

        # 处理最后一个chunk
        if chunk_data_lines:
            chunk_content = '# ' + self.document_title + '\n' + \
                header_content + '\n' + '\n'.join(chunk_data_lines)
            # 计算chunk的token长度并添加到metadata
            chunk_token_length = get_token_length(chunk_content)
            chunk_doc = Document(
                page_content=chunk_content,
                metadata={
                    **doc.metadata,
                    'is_table_chunk': True,
                    'has_header': True,
                    'chunk_data_rows': len(chunk_data_lines),
                    'token_length': chunk_token_length  # 添加token_length到metadata
                }
            )
            result_docs.append(chunk_doc)

        logger.info(f"✅ 表格切分完成: {len(result_docs)}个chunk，每个chunk包含完整标题行")

        return result_docs

    def process_document(self, markdown_content: str) -> List[Document]:
        """完整的文档处理流程"""
        logger.info("开始文档分块处理")

        # 1. 按标题分割
        docs = self.split_markdown_by_headers(markdown_content)
        logger.info(f"标题分割后得到 {len(docs)} 个文档块")

        # 2. 区分表格和文本
        docs = self.extract_markdown_table_and_text(docs)
        logger.info(f"表格文本分离后得到 {len(docs)} 个文档块")

        # 3. 进一步分割大块
        docs = self.further_split_large_chunks(docs)
        logger.info(f"最终得到 {len(docs)} 个文档块")

        # 4. 添加索引信息和token长度
        for i, doc in enumerate(docs):
            doc.metadata['chunk_id'] = i
            doc.metadata['total_chunks'] = len(docs)
            # 确保每个chunk都有token_length字段
            if 'token_length' not in doc.metadata:
                doc.metadata['token_length'] = get_token_length(
                    doc.page_content)

        return docs
