import re
import logging
from langchain.docstore.document import Document
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

# 安全领域关键词
SECURITY_KEYWORDS = {
    # 中文关键词
    'chinese': [
        'APT', '攻击', '威胁', '恶意', '漏洞', '病毒', '木马', '后门',
        '钓鱼', '间谍', '渗透', '入侵', '组织', '活动', '技术', '战术',
        '工具', '流程', '样本', '分析', '检测', '防御', '响应', '情报',
        '黑客', '网络', '安全', '数据', '泄露', '勒索', '软件', '恶意代码',
        '僵尸网络', '命令控制', '横向移动', '权限提升', '持久化', '数据收集',
        '数据窃取', '破坏', '影响', '目标', '受害者', '载荷', '投递',
        '执行', '通信', '隐蔽', '伪装', '绕过', '规避', '监控', '日志'
    ],
    # 英文关键词
    'english': [
        'attack', 'threat', 'malicious', 'malware', 'vulnerability', 'exploit',
        'backdoor', 'trojan', 'virus', 'phishing', 'spear', 'espionage',
        'infiltration', 'penetration', 'intrusion', 'breach', 'compromise',
        'payload', 'C2', 'command', 'control', 'RAT', 'botnet', 'dropper',
        'loader', 'downloader', 'implant', 'backdoor', 'rootkit', 'keylogger',
        'stealer', 'ransomware', 'wiper', 'cryptor', 'packer', 'obfuscator',
        'reconnaissance', 'initial', 'access', 'execution', 'persistence',
        'privilege', 'escalation', 'defense', 'evasion', 'credential',
        'discovery', 'lateral', 'movement', 'collection', 'exfiltration',
        'impact', 'command', 'control', 'communication',
        'actor', 'group', 'campaign', 'operation', 'mission', 'target',
        'victim', 'infrastructure', 'domain', 'IP', 'hash', 'indicator',
        'IOC', 'TTPs', 'tactics', 'techniques', 'procedures', 'tools',
        'sample', 'binary', 'executable', 'script', 'shellcode', 'injection',
        'hooking', 'hijacking', 'spoofing', 'masquerading', 'living',
        'fileless', 'memory', 'registry', 'process', 'service', 'task',
        'scheduled', 'startup', 'autostart', 'persistence', 'steganography',
        'network', 'protocol', 'traffic', 'packet', 'connection', 'session',
        'tunnel', 'proxy', 'gateway', 'firewall', 'IDS', 'IPS', 'SIEM',
        'endpoint', 'host', 'server', 'client', 'browser', 'email',
        'analysis', 'forensic', 'investigation', 'detection', 'hunting',
        'monitoring', 'logging', 'signature', 'rule', 'alert', 'event',
        'intelligence', 'attribution', 'clustering', 'correlation'
    ]
}


class QualityFilter:
    """内容质量过滤器 - 过滤导航菜单、营销内容等低质量chunks"""

    def __init__(self, config: Dict[str, Any]):
        """初始化质量过滤器"""
        quality_config = config.get('quality_filter', {})
        self.min_quality_score = quality_config.get('min_quality_score', 60)
        self.enable_quality_filter = quality_config.get(
            'enable_quality_filter', True)

    def filter_chunks(self, chunks: List[Document]) -> List[Document]:
        """过滤低质量chunks"""
        if not self.enable_quality_filter:
            return chunks, None

        filtered_chunks = []
        navigation_count = 0
        marketing_count = 0
        low_quality_count = 0

        # 存储被过滤的chunks信息
        filtered_chunks_info = []

        for chunk in chunks:
            content = getattr(chunk, 'page_content', str(chunk))

            # 🔥 优先进行快速过滤检查
            if self._is_navigation_menu(content):
                navigation_count += 1
                logger.debug(f"❌ 导航菜单内容被过滤")
                # 记录被过滤的chunk信息
                filtered_chunks_info.append({
                    'type': 'navigation_menu',
                    'content': content[:200] + "..." if len(content) > 200 else content,
                    'metadata': getattr(chunk, 'metadata', {}),
                    'reason': '导航菜单内容'
                })
                continue

            if self._is_marketing_content(content):
                marketing_count += 1
                logger.debug(f"❌ 营销内容被过滤")
                # 记录被过滤的chunk信息
                filtered_chunks_info.append({
                    'type': 'marketing_content',
                    'content': content[:200] + "..." if len(content) > 200 else content,
                    'metadata': getattr(chunk, 'metadata', {}),
                    'reason': '营销推广内容'
                })
                continue

            # 详细质量评分
            quality_score, issues = self.calculate_quality_score(content)

            if quality_score >= self.min_quality_score:
                filtered_chunks.append(chunk)
            else:
                low_quality_count += 1
                # 获取内容的前100个字符作为预览
                content_preview = content[:100] + \
                    "..." if len(content) > 100 else content
                logger.info(
                    f"❌ 低质量内容被过滤 (分数: {quality_score}, 问题: {issues})")
                logger.debug(
                    f"   内容预览: {content_preview}")

                # 记录被过滤的chunk信息
                filtered_chunks_info.append({
                    'type': 'low_quality',
                    'content': content,
                    'metadata': getattr(chunk, 'metadata', {}),
                    'quality_score': quality_score,
                    'issues': issues
                })

        logger.info(f"🔍 质量过滤完成: {len(chunks)} -> {len(filtered_chunks)}个chunk")
        logger.info(
            f"   过滤统计: 导航菜单{navigation_count}个, 营销内容{marketing_count}个, 低质量{low_quality_count}个")

        return filtered_chunks, filtered_chunks_info

    def calculate_quality_score(self, content: str, content_type: str = 'text') -> Tuple[int, List[str]]:
        """计算内容质量分数"""
        issues = []
        score = 100

        # 基础检查
        if len(content.strip()) < 50:
            score -= 40
            issues.append("内容过短")
            return max(0, score), issues

        # 乱码检测
        if self._is_garbled_text(content):
            score -= 80
            issues.append("检测到乱码内容")
            return max(0, score), issues

        # 纯标题检测
        if self._is_title_only(content):
            score -= 40
            issues.append("仅包含标题")

        # 语言内容检查
        lang_score, lang_issues = self._check_language_content(content)
        score += lang_score
        issues.extend(lang_issues)

        # 安全关键词检查
        keyword_score, keyword_issues = self._check_security_keywords(content)
        score += keyword_score
        issues.extend(keyword_issues)

        # 实质内容检查
        substance_score, substance_issues = self._check_content_substance(
            content)
        score += substance_score
        issues.extend(substance_issues)

        # 表格特殊检查
        if content_type == 'table':
            table_score, table_issues = self._check_table_quality(content)
            score += table_score
            issues.extend(table_issues)

        # 技术指标加分
        tech_bonus = self._get_technical_bonus(content)
        score += tech_bonus
        if tech_bonus > 0:
            issues.append(f"包含技术指标 (+{tech_bonus}分)")

        return max(0, min(100, score)), issues

    def _is_navigation_menu(self, content: str) -> bool:
        """🔥 增强：检测导航菜单内容"""
        # 检测链接密度
        link_pattern = r'\[([^\]]+)\]\([^)]+\)'
        links = re.findall(link_pattern, content)
        lines = [line.strip() for line in content.split('\n') if line.strip()]

        if not lines:
            return False

        # 链接密度超过60%判定为导航
        link_density = len(links) / len(lines) if lines else 0
        if link_density > 0.6:
            return True

        # 检测重复导航模式
        nav_patterns = [
            r'^[+*-]\s*\[([^\]]+)\]\([^)]+\)',  # + [产品](/路径/)
            r'^\s*\[([^\]]+)\]\([^)]+\)\s*$',   # [产品](/路径/)
        ]

        nav_line_count = 0
        for line in lines:
            for pattern in nav_patterns:
                if re.match(pattern, line):
                    nav_line_count += 1
                    break

        # 导航项超过8个判定为导航菜单
        if nav_line_count > 8:
            return True

        # 检测产品/服务列表模式
        product_indicators = [
            r'Products?\s*$', r'Services?\s*$', r'Solutions?\s*$',
            r'Learn more', r'Explore solutions', r'Free Tools'
        ]

        indicator_count = sum(1 for pattern in product_indicators
                              for line in lines
                              if re.search(pattern, line, re.IGNORECASE))

        # 有产品指标且链接多的内容
        if indicator_count >= 2 and len(links) > 5:
            return True

        return False

    def _is_marketing_content(self, content: str) -> bool:
        """检测营销推广内容"""
        marketing_indicators = [
            r'Contact.*sales', r'Talk to sales', r'Get.*assistance',
            r'Join.*Club', r'Subscribe', r'fill.*form', r'representative.*contact',
            r'24/7.*response', r'Learn more', r'Explore.*solutions'
        ]

        marketing_count = sum(1 for pattern in marketing_indicators
                              if re.search(pattern, content, re.IGNORECASE))

        # 营销指标超过2个判定为营销内容
        return marketing_count >= 2

    def _is_garbled_text(self, content: str) -> bool:
        """检测乱码内容"""
        # 检测长编码字符串
        if re.search(r'[a-zA-Z0-9+/=]{50,}', content):
            return True

        # 检测字符分布异常
        total_chars = len(content)
        if total_chars > 100:
            spaces = content.count(' ')
            punctuation = len(re.findall(r'[.,;:!?()[\]{}"]', content))

            space_ratio = spaces / total_chars
            punct_ratio = punctuation / total_chars

            # 缺乏正常的空格和标点分布
            if space_ratio < 0.05 and punct_ratio < 0.02:
                return True

        return False

    def _is_title_only(self, content: str) -> bool:
        """检测仅包含标题的内容"""
        lines = [line.strip() for line in content.split('\n') if line.strip()]

        if len(lines) <= 2:
            content_clean = content.strip()
            # 检测标题模式
            title_patterns = [
                r'^#+\s+', r'^.*\n[-=]+$', r'^\s*[A-Z][^.!?]*$'
            ]

            for pattern in title_patterns:
                if re.search(pattern, content_clean, re.MULTILINE):
                    return True

            # 短内容且无句号
            if len(content_clean) < 100 and '.' not in content_clean:
                return True

        return False

    def _check_language_content(self, content: str) -> Tuple[int, List[str]]:
        """检查语言内容质量"""
        chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', content))
        english_chars = len(re.findall(r'[a-zA-Z]', content))
        total_chars = len(content)

        if total_chars == 0:
            return -30, ["内容为空"]

        chinese_ratio = chinese_chars / total_chars
        english_ratio = english_chars / total_chars

        # 缺乏有效文字内容
        if chinese_ratio < 0.1 and english_ratio < 0.3:
            return -25, ["缺乏有效文字内容"]

        # 数字符号比例过高
        digit_symbol_chars = len(re.findall(r'[\d\|\-\+\.\s]', content))
        digit_ratio = digit_symbol_chars / total_chars

        if digit_ratio > 0.6:
            return -15, ["数字符号比例过高"]

        return 0, []

    def _check_security_keywords(self, content: str) -> Tuple[int, List[str]]:
        """检查安全关键词"""
        content_lower = content.lower()

        # 统计关键词
        chinese_keywords = sum(1 for kw in SECURITY_KEYWORDS['chinese']
                               if kw.lower() in content_lower)
        english_keywords = sum(1 for kw in SECURITY_KEYWORDS['english']
                               if re.search(r'\b' + re.escape(kw.lower()) + r'\b', content_lower))

        total_keywords = chinese_keywords + english_keywords

        # 评分策略
        if total_keywords < 2:
            return -15, [f"安全关键词不足 (总计: {total_keywords})"]
        elif total_keywords < 4:
            return -5, [f"安全关键词偏少 (总计: {total_keywords})"]
        else:
            return 5, [f"安全关键词充足 (总计: {total_keywords})"]

    def _check_content_substance(self, content: str) -> Tuple[int, List[str]]:
        """检查内容实质性"""
        # 去除链接后的纯文本
        text_without_links = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', content)
        pure_text = re.sub(r'[+*-]\s*', '', text_without_links)
        pure_text_length = len(
            [c for c in pure_text if c.isalnum() or c.isspace()])

        # 实质内容长度检查
        if pure_text_length < 100:
            return -20, [f"实质内容不足 ({pure_text_length}字符)"]
        elif pure_text_length < 200:
            return -10, [f"实质内容较少 ({pure_text_length}字符)"]
        else:
            return 5, [f"实质内容充足 ({pure_text_length}字符)"]

    def _check_table_quality(self, content: str) -> Tuple[int, List[str]]:
        """检查表格质量"""
        issues = []
        score = 0

        # 表格实体关键词
        table_keywords = [
            'APT', '组织', '攻击', '技术', '工具', '样本', '家族', 'group',
            'actor', 'family', 'campaign', 'hash', 'domain', 'IP'
        ]

        keyword_count = sum(1 for kw in table_keywords
                            if kw.lower() in content.lower())

        if keyword_count < 2:
            score -= 15
            issues.append(f"表格实体关键词不足 ({keyword_count})")

        # 表格结构检查
        table_lines = [line for line in content.split('\n')
                       if line.strip().startswith('|')]

        if len(table_lines) < 3:
            score -= 10
            issues.append("表格结构不完整")
        else:
            # 列数检查
            avg_columns = sum(len(line.split('|'))
                              for line in table_lines) / len(table_lines)
            if avg_columns < 3:
                score -= 5
                issues.append("表格信息密度低")

        return score, issues

    def _get_technical_bonus(self, content: str) -> int:
        """获取技术指标加分"""
        technical_patterns = [
            r'\b[a-fA-F0-9]{32}\b',            # MD5
            r'\b[a-fA-F0-9]{40}\b',            # SHA1
            r'\b[a-fA-F0-9]{64}\b',            # SHA256
            r'\b(?:\d{1,3}\.){3}\d{1,3}\b',    # IP地址
            r'\b[a-zA-Z0-9-]+\.[a-zA-Z]{2,}\b',  # 域名
            r'\bCVE-\d{4}-\d{4,}\b',           # CVE编号
            r'\bT\d{4}(?:\.\d{3})?\b',         # ATT&CK技术编号
            r'\b0x[a-fA-F0-9]+\b',             # 十六进制地址
        ]

        technical_count = sum(1 for pattern in technical_patterns
                              if re.search(pattern, content))

        return min(technical_count * 2, 10)  # 最多加10分
