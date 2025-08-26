# coding:utf-8
"""
PDF转换模块 - 负责将PDF文档转换为Markdown格式
"""
from typing import Tuple, List, Dict, Any
from docling.datamodel.base_models import InputFormat
from docling.document_converter import DocumentConverter as DoclingDocumentConverter
from docling.document_converter import PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any
import re

# 新增HTML转换相关导入
from markdownify import MarkdownConverter
from bs4 import BeautifulSoup
import requests

logger = logging.getLogger(__name__)

# coding:utf-8
"""
统一文档转换器 - 负责各种文档格式的类型检测和转换
"""

logger = logging.getLogger(__name__)


class DocumentConverter(DoclingDocumentConverter):
    """统一文档转换器 - 支持PDF和HTML"""

    def __init__(self, config: Dict[str, Any]):
        """初始化转换器"""
        self.config = config

        # 初始化各种转换器
        pdf_config = config.get('pdf_converter', {})
        self.pdf_converter = PDFConverter(
            artifacts_path=pdf_config.get(
                'artifacts_path', './docling-models'),
            do_ocr=pdf_config.get('do_ocr', False)
        )

        html_config = config.get('html_converter', {})
        self.html_converter = HTMLConverter(**html_config)

        # 支持的文件类型映射
        self.type_handlers = {
            'pdf': self._convert_pdf,
            'html': self._convert_html
        }

        logger.info(f"📄 文档转换器初始化完成，支持类型: {list(self.type_handlers.keys())}")

    def detect_and_convert(self, file_path: str) -> Tuple[str, str, List[Dict[str, Any]]]:
        """
        检测文件类型并转换为Markdown

        Args:
            file_path: 文件路径（本地文件或URL）

        Returns:
            Tuple[文件类型, Markdown内容, 提取的图片信息]
        """
        # 检测文件类型
        file_type = self.detect_file_type(file_path)
        logger.info(f"📋 检测到文件类型: {file_type}")

        # 转换为Markdown
        markdown_content, images = self.convert_to_markdown(
            file_path, file_type)

        return file_type, markdown_content, images

    def detect_file_type(self, file_path: str) -> str:
        """检测文件类型"""
        if file_path.startswith(('http://', 'https://')):
            # URL类型判断
            if file_path.lower().endswith('.pdf'):
                return 'pdf'
            else:
                return 'html'  # 默认认为是HTML页面
        else:
            # 本地文件，根据扩展名判断
            file_ext = Path(file_path).suffix.lower()
            if file_ext == '.pdf':
                return 'pdf'
            elif file_ext in ['.html', '.htm']:
                return 'html'
            else:
                raise ValueError(f"不支持的文件类型: {file_ext}")

    def convert_to_markdown(self, file_path: str, file_type: str) -> Tuple[str, List[Dict[str, Any]]]:
        """根据文件类型转换为Markdown"""
        if file_type not in self.type_handlers:
            raise ValueError(f"不支持的文件类型: {file_type}")

        handler = self.type_handlers[file_type]
        return handler(file_path)

    def _convert_pdf(self, file_path: str) -> Tuple[str, List[Dict[str, Any]]]:
        """PDF转换"""
        markdown_content = self.pdf_converter.convert_pdf_to_markdown(
            file_path)
        return markdown_content, []  # PDF转换器不提取图片信息

    def _convert_html(self, file_path: str) -> Tuple[str, List[Dict[str, Any]]]:
        """HTML转换"""
        if file_path.startswith(('http://', 'https://')):
            # URL
            markdown_content, images = self.html_converter.convert_html_url_to_markdown(
                file_path)
        else:
            # 本地文件
            markdown_content, images = self.html_converter.convert_html_file_to_markdown(
                file_path)
        return markdown_content, images

    def get_supported_types(self) -> List[str]:
        """获取支持的文件类型列表"""
        return list(self.type_handlers.keys())

    def add_converter(self, file_type: str, converter_func):
        """动态添加新的转换器（便于扩展）"""
        self.type_handlers[file_type] = converter_func
        logger.info(f"✅ 添加了新的转换器: {file_type}")


class PDFConverter:
    """PDF转换器"""

    def __init__(self, artifacts_path: str = "./docling-models", do_ocr: bool = False):
        """
        初始化PDF转换器

        Args:
            artifacts_path: 模型路径
            do_ocr: 是否启用OCR
        """
        self.artifacts_path = artifacts_path
        self.pipeline_options = PdfPipelineOptions(
            artifacts_path=artifacts_path,
            do_ocr=do_ocr
        )

        self.doc_converter = DoclingDocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=self.pipeline_options)
            }
        )

    def convert_pdf_to_markdown(self, file_path: str) -> str:
        """
        将PDF转换为Markdown格式

        Args:
            file_path: PDF文件路径（支持本地文件和远程URL）

        Returns:
            转换后的markdown内容
        """
        try:
            logger.info(f"开始转换PDF: {file_path}")
            result = self.doc_converter.convert(source=file_path)
            markdown_content = result.document.export_to_markdown()
            logger.info("PDF转换完成")
            return markdown_content
        except Exception as e:
            logger.error(f"PDF转换失败: {e}")
            raise


class HTMLConverter:
    """HTML转换器 - 将HTML转换为Markdown并单独提取图片信息"""

    def __init__(self, extract_images: bool = True, **options):
        """
        初始化HTML转换器

        Args:
            extract_images : 是否提取并下载图片
            **other_options: 其余 markdownify 选项
        """
        self.extract_images = extract_images
        self.options = options
        self.extracted_images = []  # 存储提取的图片信息

    def convert_html_to_markdown(self, html_content: str) -> Tuple[str, List[Dict[str, Any]]]:
        """
        将HTML转换为Markdown格式，并单独提取图片信息

        Args:
            html_content: HTML内容字符串

        Returns:
            Tuple[markdown内容, 图片信息列表]
        """
        try:
            logger.info("开始转换HTML到Markdown")

            # 重置图片提取列表
            self.extracted_images = []

            # 🔥 新增：HTML预处理，移除无关内容
            cleaned_html = self._preprocess_html_content(html_content)

            # 使用自定义转换器
            converter = self._create_custom_converter(**self.options)
            markdown_content = converter.convert(cleaned_html)

            logger.info(f"HTML转换完成，提取了{len(self.extracted_images)}张图片")

            return markdown_content, self.extracted_images.copy()

        except Exception as e:
            logger.error(f"HTML转换失败: {e}")
            raise

    def convert_html_file_to_markdown(self, file_path: str, encoding: str = 'utf-8') -> Tuple[str, List[Dict[str, Any]]]:
        """
        从HTML文件转换为Markdown格式

        Args:
            file_path: HTML文件路径
            encoding: 文件编码

        Returns:
            Tuple[markdown内容, 图片信息列表]
        """
        try:
            logger.info(f"开始转换HTML文件: {file_path}")

            with open(file_path, 'r', encoding=encoding) as f:
                html_content = f.read()

            return self.convert_html_to_markdown(html_content)

        except Exception as e:
            logger.error(f"HTML文件转换失败: {e}")
            raise

    def convert_html_url_to_markdown(self, url: str, timeout: int = 30) -> Tuple[str, List[Dict[str, Any]]]:
        """
        从URL获取HTML并转换为Markdown格式

        Args:
            url: HTML页面URL
            timeout: 请求超时时间

        Returns:
            Tuple[markdown内容, 图片信息列表]
        """
        try:
            logger.info(f"开始从URL获取HTML: {url}")

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            response.encoding = response.apparent_encoding

            return self.convert_html_to_markdown(response.text)

        except Exception as e:
            logger.error(f"从URL转换HTML失败: {e}")
            raise

    def _preprocess_html_content(self, html_content: str) -> str:
        """HTML内容预处理：移除无关内容，保留核心内容"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # 🔥 新增：提取文档标题
            document_title = self._extract_document_title(soup)

            # 1. 移除无关元素
            unwanted_selectors = [
                'nav', '.nav', '.navigation', '.navbar', '.menu',
                'header', '.header', '.site-header',
                'footer', '.footer', '.site-footer',
                '.sidebar', '.widget', '.advertisement', '.ads',
                '.breadcrumb', '.pagination', '.social-share',
                'script', 'style', 'noscript'
            ]

            for selector in unwanted_selectors:
                for element in soup.select(selector):
                    element.decompose()

            # 2. 查找主要内容区域
            main_content = self._extract_main_content_area(soup)

            # 3. 清理伪表格
            self._clean_fake_tables(main_content)

            # 🔥 新增：如果提取到标题，将其插入到内容开头
            content_html = str(main_content)
            if document_title:
                # 创建标题标签并插入到内容开头
                title_tag = soup.new_tag('h1')
                title_tag.string = document_title

                # 将标题插入到main_content的开头
                if main_content.find():
                    main_content.insert(0, title_tag)
                else:
                    main_content.append(title_tag)

                logger.info(f"📋 提取到文档标题: {document_title}")

            return str(main_content)

        except Exception as e:
            logger.warning(f"HTML预处理失败，使用原始内容: {e}")
            return html_content

    def _extract_document_title(self, soup) -> str:
        """提取文档标题"""
        try:
            # 🔥 按优先级尝试多种标题来源
            title_sources = [
                # 1. Meta标签标题（最可靠）
                lambda: soup.find('meta', property='og:title'),
                lambda: soup.find('meta', attrs={'name': 'title'}),
                lambda: soup.find('meta', attrs={'name': 'dc.title'}),

                # 2. 主内容区域的第一个h1
                lambda: self._find_main_h1(soup),

                # 3. 页面第一个h1
                lambda: soup.find('h1'),

                # 4. title标签（但需要清理）
                lambda: soup.find('title'),

                # 5. 特定class的标题元素
                lambda: soup.find(
                    class_=['article-title', 'post-title', 'entry-title', 'title']),
            ]

            for source_func in title_sources:
                try:
                    element = source_func()
                    if element:
                        title_text = None

                        if element.name == 'meta':
                            title_text = element.get('content', '').strip()
                        else:
                            title_text = element.get_text().strip()

                        if title_text and len(title_text) > 5:  # 标题至少5个字符
                            # 清理标题
                            cleaned_title = self._clean_title_text(title_text)
                            if cleaned_title:
                                return cleaned_title
                except Exception as e:
                    logger.debug(f"标题提取失败: {e}")
                    continue

            return ""

        except Exception as e:
            logger.warning(f"标题提取过程出错: {e}")
            return ""

    def _find_main_h1(self, soup):
        """在主内容区域查找h1标题"""
        # 先尝试在主内容区域查找
        main_selectors = [
            'main', '[role="main"]', 'article', '.article',
            '.post', '.entry', '.main-content', '.content',
            '.post-content', '#content', '#main-content'
        ]

        for selector in main_selectors:
            main_element = soup.select_one(selector)
            if main_element:
                h1 = main_element.find('h1')
                if h1:
                    return h1
        return None

    def _clean_title_text(self, title_text: str) -> str:
        """清理标题文本"""
        if not title_text:
            return ""

        # 移除常见的网站后缀
        title_text = re.sub(r'\s*[\|\-–—]\s*.+$', '', title_text)

        # 移除多余空白
        title_text = ' '.join(title_text.split())

        # 验证标题质量
        if len(title_text) < 5 or len(title_text) > 200:
            return ""

        # 过滤无意义标题
        meaningless_patterns = [
            r'^(home|index|main|default)$',
            r'^(页面|page)\s*\d*$',
            r'^(untitled|无标题)$'
        ]

        for pattern in meaningless_patterns:
            if re.match(pattern, title_text, re.IGNORECASE):
                return ""

        return title_text

    def _extract_main_content_area(self, soup):
        """提取主要内容区域"""
        # 按优先级查找主内容容器
        main_selectors = [
            'main', '[role="main"]',
            'article', '.article', '.post', '.entry',
            '.main-content', '.content', '.post-content',
            '#content', '#main-content'
        ]

        for selector in main_selectors:
            main_element = soup.select_one(selector)
            if main_element:
                logger.info(f"找到主内容区域: {selector}")
                return main_element

        # 如果找不到明确的主内容区域，使用body
        body = soup.find('body')
        return body if body else soup

    def _clean_fake_tables(self, soup):
        """清理伪表格：移除明显用于布局的表格"""
        tables = soup.find_all('table')

        for table in tables:
            if self._is_layout_table(table):
                # 将布局表格转换为div，保留内容
                table.name = 'div'
                table.attrs = {}
                logger.debug("清理布局表格")

    def _is_layout_table(self, table) -> bool:
        """判断是否为布局表格（而非数据表格）"""
        # 1. 检查是否有数据表格的典型特征
        has_th = table.find('th') is not None
        has_thead = table.find('thead') is not None

        if has_th or has_thead:
            return False  # 有表头，可能是数据表格

        # 2. 检查行列结构
        rows = table.find_all('tr')
        if len(rows) < 2:
            return True  # 行数太少

        # 3. 检查单元格内容
        cells = table.find_all(['td', 'th'])
        if len(cells) < 4:
            return True  # 单元格太少

        # 4. 检查是否包含大量链接（导航特征）
        links = table.find_all('a')
        if len(links) > len(cells) * 0.6:
            return True  # 链接密度过高

        # 5. 检查单元格内容长度
        cell_texts = [cell.get_text().strip() for cell in cells]
        avg_length = sum(len(text) for text in cell_texts) / \
            len(cell_texts) if cell_texts else 0
        if avg_length < 15:
            return True  # 平均内容过短

        return False

    def _create_custom_converter(self, **options) -> 'MarkdownConverter':
        """创建自定义的Markdown转换器"""

        class CustomMarkdownConverter(MarkdownConverter):
            """自定义Markdown转换器 - 提取图片信息但不在markdown中显示"""

            def __init__(self, parent_html_converter, **options):
                super().__init__(**options)
                self.parent = parent_html_converter
                self.image_counter = 0

            def convert_img(self, el, text, parent_tags):
                # 如果全局开关关闭，直接忽略
                if not self.parent.extract_images:
                    return ''

                """重写图片转换方法 - 提取图片信息但不显示在markdown中"""
                # 获取图片属性
                placement = el.attrs.get('placement', None)
                alt = el.attrs.get('alt', '') or ''
                src = el.attrs.get('src', '') or ''
                title = el.attrs.get('title', '') or ''
                width = el.attrs.get('width', '')
                height = el.attrs.get('height', '')

                # 如果是inline图标，直接忽略
                if placement == 'inline':
                    return ''

                # 构建图片信息
                self.image_counter += 1
                image_info = {
                    'id': f"image_{self.image_counter}",
                    'src': src,
                    'alt': alt,
                    'title': title,
                    'width': width,
                    'height': height,
                    'placement': placement,
                    'attributes': dict(el.attrs)  # 保存所有属性
                }

                # 将 parent_converter 改为 parent
                self.parent.extracted_images.append(image_info)

                # 在markdown中不显示图片，返回空字符串或者图片占位符
                return ''  # 完全不显示图片

            def convert_a(self, el, text, parent_tags):
                """重写链接转换方法 - 处理链接中的图片"""
                # 检查链接是否指向图片文件
                href = el.attrs.get('href', '')
                # 增强图片链接识别能力，支持更多情况
                image_extensions = ['.png', '.jpg',
                                    '.jpeg', '.gif', '.bmp', '.svg', '.webp']
                is_image_link = href and any(
                    href.lower().endswith(ext) for ext in image_extensions)

                # 特殊处理：如果链接文本是"download"且链接指向图片，则认为是图片链接
                is_download_image = (text.lower().strip(
                ) == 'download' or '下载' in text) and is_image_link

                if is_image_link or is_download_image:
                    # 如果是图片链接，提取图片信息
                    if self.parent.extract_images:
                        self.image_counter += 1
                        image_info = {
                            'id': f"image_{self.image_counter}",
                            'src': href,
                            'alt': el.attrs.get('title', '') or text or '',
                            'title': el.attrs.get('title', '') or text or '',
                            'width': '',
                            'height': '',
                            'placement': 'link',
                            'attributes': dict(el.attrs)
                        }
                        self.parent.extracted_images.append(image_info)

                    # 返回空字符串，不在markdown中显示图片链接
                    return ''

                # 非图片链接使用默认处理
                return super().convert_a(el, text, parent_tags)

            def convert_table(self, el, text, parent_tags):
                """处理表格转换 - 增强伪表格检测"""
                # 🔥 修复：确保使用正确的parent引用
                if hasattr(self.parent, '_is_valid_data_table') and not self.parent._is_valid_data_table(el):
                    # 如果是布局表格，转换为普通文本
                    return self.process_tag(el, convert_children_only=True)

                # 对于有效的数据表格，使用默认转换
                return super().convert_table(el, text, parent_tags)

        return CustomMarkdownConverter(self, **options)

    def _is_valid_data_table(self, table_element) -> bool:
        """验证是否为有效的数据表格"""
        # 检查表格结构
        rows = table_element.find_all('tr')
        if len(rows) < 2:
            return False

        # 检查是否有表头
        has_headers = table_element.find(
            'th') is not None or table_element.find('thead') is not None

        # 检查单元格数量和内容质量
        cells = table_element.find_all(['td', 'th'])
        if len(cells) < 4:
            return False

        # 检查内容质量（非导航链接）
        text_content = table_element.get_text().strip()
        if len(text_content) < 50:  # 内容太少
            return False

        return True

    def get_extracted_images(self) -> List[Dict[str, Any]]:
        """获取最后一次转换提取的图片信息"""
        return self.extracted_images.copy()

    def save_extracted_images(self, output_path: str) -> None:
        """保存提取的图片信息到JSON文件"""
        try:
            import json
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self.extracted_images, f,
                          ensure_ascii=False, indent=2)
            logger.info(f"图片信息已保存到: {output_path}")
        except Exception as e:
            logger.error(f"保存图片信息失败: {e}")
            raise

    def download_images(self, output_dir: str, base_url: str = None) -> Dict[str, str]:
        """
        下载提取的图片到指定目录

        Args:
            output_dir: 输出目录
            base_url: 基础URL（用于相对路径的图片）

        Returns:
            Dict[原始URL, 本地文件路径]
        """
        import os
        import urllib.parse
        from pathlib import Path
        from urllib.parse import urlparse, urljoin

        downloaded_files = {}
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        for i, img_info in enumerate(self.extracted_images):
            src = img_info['src']
            if not src:
                continue

            try:
                # 跳过data URL - 改进提示信息
                if src.startswith('data:'):
                    logger.info(f"跳过data URL图片: {img_info['id']} (数据已嵌入HTML)")
                    continue

                # 改进的URL处理逻辑
                final_url = self._resolve_image_url(src, base_url)

                if not final_url:
                    logger.warning(f"无法解析图片URL: {src}")
                    continue

                # 验证URL格式
                parsed = urlparse(final_url)
                if not parsed.scheme or not parsed.netloc:
                    logger.warning(f"无效的图片URL: {final_url}")
                    continue

                # 生成文件名
                file_name = self._generate_image_filename(final_url, i + 1)
                file_path = output_path / file_name

                # 下载图片
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }

                response = requests.get(final_url, headers=headers, timeout=30)
                response.raise_for_status()

                with open(file_path, 'wb') as f:
                    f.write(response.content)

                downloaded_files[img_info['src']] = str(file_path)
                logger.debug(f"✅ 下载成功: {final_url} -> {file_path}")

            except Exception as e:
                logger.warning(f"❌ 下载图片失败 {src}: {e}")

        logger.info(f"📥 成功下载{len(downloaded_files)}张图片到: {output_dir}")
        return downloaded_files

    def _resolve_image_url(self, src: str, base_url: str = None) -> str:
        """
        解析图片URL，处理相对路径和绝对路径

        Args:
            src: 原始图片URL
            base_url: 基础URL

        Returns:
            解析后的完整URL
        """
        from urllib.parse import urlparse, urljoin

        # 如果已经是完整URL，直接返回
        if src.startswith(('http://', 'https://')):
            return src

        # 如果没有base_url，无法处理相对路径
        if not base_url:
            logger.warning(f"缺少base_url，无法处理相对路径: {src}")
            return None

        # 解析base_url
        parsed_base = urlparse(base_url)
        if not parsed_base.scheme or not parsed_base.netloc:
            logger.warning(f"无效的base_url: {base_url}")
            return None

        # 处理不同类型的相对路径
        if src.startswith('/'):
            # 绝对路径（相对于域名根目录）
            # 例如：/wp-content/themes/... -> https://example.com/wp-content/themes/...
            final_url = f"{parsed_base.scheme}://{parsed_base.netloc}{src}"
        else:
            # 相对路径（相对于当前页面）
            # 例如：images/pic.jpg -> https://example.com/page/images/pic.jpg
            final_url = urljoin(base_url, src)

        return final_url

    def _generate_image_filename(self, url: str, index: int) -> str:
        """
        生成图片文件名

        Args:
            url: 图片URL
            index: 图片序号

        Returns:
            生成的文件名
        """
        from pathlib import Path
        import re

        try:
            # 从URL中提取文件名
            path = Path(url)
            original_name = path.name

            # 如果没有扩展名，添加默认扩展名
            if not path.suffix:
                # 根据URL或内容类型推断扩展名
                if 'svg' in url.lower():
                    original_name += '.svg'
                elif any(ext in url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                    pass  # 已有扩展名
                else:
                    original_name += '.jpg'  # 默认扩展名

            # 清理文件名，移除非法字符
            clean_name = re.sub(r'[<>:"/\\|?*]', '_', original_name)

            # 生成最终文件名
            final_name = f"image_{index:03d}_{clean_name}"

            return final_name

        except Exception as e:
            logger.warning(f"生成文件名失败: {e}")
            return f"image_{index:03d}.jpg"


def markdownify_with_custom_converter(html: str, **options) -> Tuple[str, List[Dict[str, Any]]]:
    """
    便捷函数：使用自定义转换器将HTML转换为Markdown并提取图片

    Args:
        html: HTML内容
        **options: markdownify选项

    Returns:
        Tuple[markdown内容, 图片信息列表]
    """
    converter = HTMLConverter(**options)
    return converter.convert_html_to_markdown(html)
