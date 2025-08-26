# coding:utf-8
"""
Markdown预处理模块 - 负责清理和优化markdown内容
"""
import re
from typing import List
import logging

logger = logging.getLogger(__name__)


class MarkdownProcessor:
    """Markdown预处理器"""

    @staticmethod
    def post_process_markdown(markdown_content: str) -> str:
        """对PDF转换的markdown进行后处理"""
        lines = markdown_content.split('\n')
        processed_lines = []
        i = 0

        while i < len(lines):
            line = lines[i].strip()

            # 1. 删除"<!-- image -->"行
            if line == "<!-- image -->":
                i += 1
                continue

            # 2. 删除上下行都是空行且本行内容只有一个字符的行
            if len(line) == 1 and line.isalnum():
                prev_empty = (i == 0) or (i > 0 and lines[i-1].strip() == "")
                next_empty = (i == len(lines)-1) or (i <
                                                     len(lines) - 1 and lines[i+1].strip() == "")

                if prev_empty and next_empty:
                    i += 1
                    continue

            # 3. 删除图标题行
            if (line.startswith('图') and '.' in line and len(line.split()) >= 2) or \
               (line.startswith('## 图') and '.' in line):
                if re.match(r'^(##\s*)?图\d+\.\d+\s+.+', line):
                    i += 1
                    continue

            # 4. 删除表标题行
            if (line.startswith('表') and '.' in line and len(line.split()) >= 2) or \
               (line.startswith('## 表') and '.' in line):
                if re.match(r'^(##\s*)?表\d+\.\d+\s+.+', line):
                    i += 1
                    continue

            # 5. 处理误识别为标题的正文
            if line.startswith('## '):
                content = line[3:].strip()
                has_chinese_numbers = bool(re.search(r'[一二三四五六七八九十]', content))
                has_arabic_numbers = bool(re.search(r'[0-9]', content))
                ends_with_period = content.endswith('。')

                if not (has_chinese_numbers or has_arabic_numbers) or ends_with_period:
                    processed_lines.append(content)
                    i += 1
                    continue

            # 6. 将特定格式的行转换为标题
            title_pattern = r'^[（(][一二三四五六七八九十\d]+[）)]\s*.+'
            if re.match(title_pattern, line) and not line.startswith('#'):
                processed_lines.append(f"## {line}")
                i += 1
                continue

            # 7. 删除所有代码块
            if line == "```":
                j = i + 1
                while j < len(lines):
                    if lines[j].strip() == "```":
                        i = j + 1
                        break
                    j += 1
                else:
                    i += 1
                continue

            # 8. 删除表格标题行 (如 "Table 1. Sample of files dropped by *LetsPro.msi*")
            if re.match(r'^Table\s+\d+\.\s+.+', line):
                i += 1
                continue

            # 9. 删除图表标题行 (如 "Figure 1. The Void Arachne campaign attack diagram")
            if re.match(r'^Figure\s+\d+\.\s+.+', line):
                i += 1
                continue

            processed_lines.append(lines[i])
            i += 1

        # 处理使用====和----分隔符的标题格式
        processed_lines = MarkdownProcessor._convert_setext_headers(processed_lines)
        
        # 应用其他处理步骤
        processed_lines = TableProcessor.merge_split_tables(processed_lines)
        processed_lines = MarkdownProcessor._remove_excessive_blank_lines(
            processed_lines)
        processed_lines = MarkdownProcessor._remove_content_after_appendix4(
            processed_lines)

        return '\n'.join(processed_lines)

    @staticmethod
    def _convert_setext_headers(lines: List[str]) -> List[str]:
        """将使用====和----分隔符的标题转换为标准的#格式"""
        result = []
        i = 0
        
        while i < len(lines):
            # 检查是否为标题行（下一行是=====或-----）
            if (i + 1 < len(lines) and 
                lines[i + 1].strip() and 
                all(c == '=' for c in lines[i + 1].strip())):
                # 一级标题 (使用 =====)
                title = lines[i].strip()
                if title:
                    result.append(f"# {title}")
                i += 2  # 跳过标题行和分隔行
                continue
            elif (i + 1 < len(lines) and 
                  lines[i + 1].strip() and 
                  all(c == '-' for c in lines[i + 1].strip()) and
                  len(lines[i + 1].strip()) >= 3):
                # 二级标题 (使用 -----)
                title = lines[i].strip()
                if title:
                    result.append(f"## {title}")
                i += 2  # 跳过标题行和分隔行
                continue
            else:
                result.append(lines[i])
                i += 1
                
        return result

    @staticmethod
    def _remove_excessive_blank_lines(lines: List[str]) -> List[str]:
        """删除多余的空行，最多保留两个连续空行"""
        result = []
        blank_count = 0

        for line in lines:
            if line.strip() == '':
                blank_count += 1
                if blank_count <= 2:
                    result.append(line)
            else:
                blank_count = 0
                result.append(line)

        return result

    @staticmethod
    def _remove_content_after_appendix4(lines: List[str]) -> List[str]:
        """删除'附录4 参考链接'之后的所有内容"""
        result = []

        for line in lines:
            if line.strip() == "## 附录4  参考链接" or line.strip() == "## 附录4 参考链接":
                break
            result.append(line)

        return result


class TableProcessor:
    """表格处理器"""

    @staticmethod
    def is_table_row(line: str) -> bool:
        """判断是否为表格行"""
        line = line.strip()
        return line.startswith('|') and line.endswith('|') and line.count('|') >= 2

    @staticmethod
    def merge_split_tables(lines: List[str]) -> List[str]:
        """合并被分割的表格并清理分割线"""
        merged_lines = []
        i = 0

        while i < len(lines):
            line = lines[i]

            if TableProcessor.is_table_row(line):
                table_lines = []

                # 收集当前表格的所有行
                while i < len(lines) and (TableProcessor.is_table_row(lines[i]) or lines[i].strip() == ''):
                    if lines[i].strip() != '':
                        table_lines.append(lines[i])
                    i += 1

                # 检查后续是否还有表格（跨页表格）
                j = i
                while j < len(lines):
                    if lines[j].strip() == '':
                        j += 1
                        continue
                    elif TableProcessor.is_table_row(lines[j]):
                        if TableProcessor._should_merge_tables(table_lines, lines[j:]):
                            while j < len(lines) and (TableProcessor.is_table_row(lines[j]) or lines[j].strip() == ''):
                                if lines[j].strip() != '':
                                    table_lines.append(lines[j])
                                j += 1
                            i = j
                        else:
                            break
                    else:
                        break

                cleaned_table = TableProcessor._clean_table_separators(
                    table_lines)
                merged_lines.extend(cleaned_table)
            else:
                merged_lines.append(line)
                i += 1

        return merged_lines

    @staticmethod
    def _should_merge_tables(table1_lines: List[str], table2_lines: List[str]) -> bool:
        """判断两个表格是否应该合并"""
        if not table1_lines or not table2_lines:
            return False

        table1_cols = table1_lines[0].count('|') - 1

        first_table2_row = None
        for line in table2_lines:
            if TableProcessor.is_table_row(line):
                first_table2_row = line
                break

        if not first_table2_row:
            return False

        table2_cols = first_table2_row.count('|') - 1
        return table1_cols == table2_cols

    @staticmethod
    def _clean_table_separators(table_lines: List[str]) -> List[str]:
        """清理表格中的分割线并统一格式"""
        if not table_lines:
            return table_lines

        cleaned_lines = []
        separator_added = False

        for i, line in enumerate(table_lines):
            if TableProcessor._is_table_separator(line):
                if not separator_added and i > 0:
                    cleaned_lines.append(line)
                    separator_added = True
                continue
            else:
                cleaned_lines.append(line)

        # 如果没有分割线，在第一行后添加标准分割线
        if not separator_added and len(cleaned_lines) > 0:
            first_row = cleaned_lines[0]
            col_count = first_row.count('|') - 1
            separator = '|' + '------|' * col_count
            cleaned_lines.insert(1, separator)

        return cleaned_lines

    @staticmethod
    def _is_table_separator(line: str) -> bool:
        """判断是否为表格分割线"""
        line = line.strip()
        if not (line.startswith('|') and line.endswith('|')):
            return False

        content = line[1:-1]
        parts = content.split('|')

        for part in parts:
            part = part.strip()
            if part and not all(c in '-:' for c in part):
                return False

        return True
