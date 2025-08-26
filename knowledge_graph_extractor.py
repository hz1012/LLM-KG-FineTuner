# coding:utf-8
"""
知识图谱提取模块 - 负责从文档块中提取知识图谱
"""
import re
import json
from typing import List, Dict, Any, Tuple, Optional
from langchain.docstore.document import Document
import logging
from utils import OpenAIAPIManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import GPTResponseParser
import threading
logger = logging.getLogger(__name__)

# ---------- 1. 静态模板 ----------
_BASE_PROMPT_TMPL = """\
你是一个专业的网络安全知识图谱提取专家，根据user的content信息，提取对应的实体和关系，请严格按照要求的json格式返回

## 实体类型
{entity_desc}

## 关系类型
{rel_desc}


## 提取要求
从威胁情报文档中提取安全相关的实体和关系，重点关注：
- 威胁组织及其发起的攻击事件
- 攻击事件的目标、战术、技术和程序
- 程序使用的工具和资产
- 报告对攻击事件的记录

## 🚨 重要：实体-关系一致性规则
1. **关系必须引用已存在的实体**：relationships中的source和target必须是entities中某个实体的id
2. **避免孤立实体**：每个entities中的实体都应该至少参与一个关系
3. **实体ID唯一性**：每个实体的id必须唯一，不能重复
4. **关系完整性**：如果提取了关系，必须同时提取关系两端的实体
5. **报告实体统一性**：若chunk开头为"# 报告\n"，则提取成report

## 输出格式
严格按此JSON格式输出，不要添加代码块标记：
{{"entities":[],"relationships":[]}}

## few_shot示例
{few_shot_examples}

## 额外要求
{guidance_text}

"""
_FEW_SHOT = {
    "english": '''
### 示例1：威胁组织攻击事件
**输入文本：**
Behind the Great Wall: Void Arachne Targets Chinese-Speaking Users. Void Arachne group launched a campaign targeting Chinese users using SEO poisoning techniques.

**输出：**
{{"entities":[{{"labels":"Report","id":"report--great-wall","name":"Behind the Great Wall: Void Arachne Targets Chinese-Speaking Users","description":"威胁情报报告"}},{{"labels":"ThreatOrganization","id":"threat-org--void-arachne","name":"Void Arachne","description":"威胁组织"}},{{"labels":"AttackEvent","id":"attack-event--seo-campaign","name":"SEO Poisoning Campaign","description":"针对中文用户的SEO投毒攻击活动"}},{{"labels":"Target","id":"target--chinese-users","name":"Chinese-Speaking Users","description":"中文用户"}},{{"labels":"Technique","id":"technique--seo-poisoning","name":"SEO Poisoning","description":"搜索引擎优化投毒技术"}}],
    "relationships":[{{"type":"BELONG","source":"report--great-wall","target":"attack-event--seo-campaign","confidence":0.95,"evidence":"报告记录了SEO投毒攻击活动"}},{{"type":"LAUNCH","source":"threat-org--void-arachne","target":"attack-event--seo-campaign","confidence":0.95,"evidence":"Void Arachne组织发起了攻击活动"}},{{"type":"ATTACK","source":"attack-event--seo-campaign","target":"target--chinese-users","confidence":0.95,"evidence":"攻击活动针对中文用户"}},{{"type":"ATTACK","source":"attack-event--seo-campaign","target":"technique--seo-poisoning","confidence":0.9,"evidence":"攻击活动使用SEO投毒技术"}}]}}

### 示例2：工具和程序关系
**输入文本：**
The malicious MSI file uses Dynamic Link Libraries during the installation process. The MSI installer deploys backdoor components to the system.

**输出：**
{{"entities":[{{"labels":"Tool","id":"tool--msi-file","name":"Malicious MSI File","description":"恶意MSI安装文件"}},{{"labels":"Procedure","id":"procedure--dll-installation","name":"DLL Installation Process","description":"使用动态链接库的安装过程"}},{{"labels":"Tool","id":"tool--dll","name":"Dynamic Link Libraries",
    "description":"动态链接库"}}],"relationships":[{{"type":"USE","source":"procedure--dll-installation","target":"tool--msi-file","confidence":0.9,"evidence":"安装过程使用MSI文件"}},{{"type":"USE","source":"procedure--dll-installation","target":"tool--dll","confidence":0.95,"evidence":"安装过程使用动态链接库"}}]}}

### 示例3：战术技术层级关系
**输入文本：**
Initial Access (TA0001) includes Spearphishing Link (T1566.002) technique. The attacker implemented email-based social engineering procedures.

**输出：**
{{"entities":[{{"labels":"Tactic","id":"tactic--initial-access","name":"Initial Access","description":"TA0001: 初始访问战术"}},{{"labels":"Technique","id":"technique--spearphishing","name":"Spearphishing Link","description":"T1566.002: 钓鱼链接技术"}},{{"labels":"Procedure","id":"procedure--email-social-eng","name":"Email Social Engineering",
    "description":"基于邮件的社会工程学程序"}}],"relationships":[{{"type":"HAS","source":"tactic--initial-access","target":"technique--spearphishing","confidence":0.95,"evidence":"初始访问战术包含钓鱼链接技术"}},{{"type":"LAUNCH","source":"technique--spearphishing","target":"procedure--email-social-eng","confidence":0.9,"evidence":"钓鱼技术启动邮件社工程序"}}]}}

### 示例4：资产利用关系
**输入文本：**
Attackers used compromised web servers to host malicious payloads. The infrastructure served as distribution points for malware.

**输出：**
{{"entities":[{{"labels":"Asset","id":"asset--web-servers","name":"Compromised Web Servers","description":"被攻陷的Web服务器"}},{{"labels":"Procedure","id":"procedure--payload-hosting","name":"Malicious Payload Hosting",
    "description":"恶意载荷托管程序"}}],"relationships":[{{"type":"USE","source":"asset--web-servers","target":"procedure--payload-hosting","confidence":0.95,"evidence":"被攻陷的Web服务器用于托管恶意载荷"}}]}}

### 示例5：空结果示例
**输入文本：**
The system was running normally without any suspicious activities detected during the monitoring period.

**输出：**
{{"entities":[],"relationships":[]}}
''',
    "chinese": """
### 示例1：中文威胁组织攻击事件
**输入文本：**
海莲花组织是由奇安信威胁情报中心最早披露并命名的一个APT组织，该组织针对中国政府、科研院所、海事机构展开了有组织、有计划、有针对性的长时间不间断攻击。

**输出：**
{{"entities":[{{"labels":"Report","id":"report--qianxin-apt","name":"奇安信威胁情报报告","description":"威胁情报报告"}},{{"labels":"ThreatOrganization","id":"threat-org--ocean-lotus","name":"海莲花组织","description":"APT威胁组织"}},{{"labels":"AttackEvent","id":"attack-event--targeted-campaign","name":"针对性攻击活动","description":"有组织有计划的攻击活动"}},{{"labels":"Target","id":"target--cn-gov","name":"中国政府机构","description":"攻击目标"}},{{"labels":"Target","id":"target--research-inst","name":"科研院所","description":"攻击目标"}},{{"labels":"Target","id":"target--maritime","name":"海事机构","description":"攻击目标"}}],"relationships":[{{"type":"BELONG","source":"report--qianxin-apt","target":"attack-event--targeted-campaign","confidence":0.95,"evidence":"报告披露了针对性攻击活动"}},{{"type":"LAUNCH","source":"threat-org--ocean-lotus","target":"attack-event--targeted-campaign","confidence":0.95,"evidence":"海莲花组织发起攻击活动"}},{{"type":"ATTACK","source":"attack-event--targeted-campaign","target":"target--cn-gov","confidence":0.9,"evidence":"攻击活动针对中国政府"}},{{"type":"ATTACK","source":"attack-event--targeted-campaign","target":"target--research-inst","confidence":0.9,"evidence":"攻击活动针对科研院所"}},{{"type":"ATTACK","source":"attack-event--targeted-campaign","target":"target--maritime","confidence":0.9,"evidence":"攻击活动针对海事机构"}}]}}

### 示例2：中文攻击技术和工具
**输入文本：**
攻击者使用鱼叉式钓鱼邮件作为初始访问手段，邮件中包含恶意附件，利用0day漏洞执行恶意代码。

**输出：**
{{"entities":[{{"labels":"Technique","id":"technique--spearphishing","name":"鱼叉式钓鱼邮件","description":"钓鱼攻击技术"}},{{"labels":"Tool","id":"tool--malicious-attachment","name":"恶意附件","description":"攻击工具"}},{{"labels":"Tool","id":"tool--zero-day","name":"0day漏洞","description":"零日漏洞利用工具"}},{{"labels":"Procedure","id":"procedure--code-execution","name":"恶意代码执行","description":"代码执行程序"}}],"relationships":[{{"type":"USE","source":"technique--spearphishing","target":"tool--malicious-attachment","confidence":0.95,"evidence":"钓鱼邮件使用恶意附件"}},{{"type":"USE","source":"procedure--code-execution","target":"tool--zero-day","confidence":0.9,"evidence":"代码执行利用0day漏洞"}},{{"type":"LAUNCH","source":"technique--spearphishing","target":"procedure--code-execution","confidence":0.85,"evidence":"钓鱼技术启动代码执行"}}]}}
"""
}

_GUIDANCE_TEMPLATES = {
    "table": "\n## 📊 表格内容提取\n- 每行为实体，列为属性\n- 重点提取技术ID和工具名称",

    "en": "\n## 🌍 English Content\n- Keep entity names in English\n- Extract all T1XXX patterns\n- Maintain tool name capitalization",

    "zh": "\n## 🇨🇳 中文内容\n- 实体名称使用中文，保留技术ID\n- 工具名称保持英文原名",

    "mixed": "\n## 🔄 混合语言\n- 保持原文语言，不要翻译\n- 优先提取技术ID模式",

    "long": "\n## 📄 长文本：重点提取技术ID、工具、威胁组织",
    "short": "\n## 📝 短文本：精确提取，避免遗漏关键实体",

    "attck": "\n## ⚔️ ATT&CK结构：按战术-技术层级提取，建立IMPLEMENT关系"
}


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


class KnowledgeGraphExtractor:
    """知识图谱提取器 - 这是唯一的实现"""

    def __init__(self, kg_config: Dict[str, Any] = None, api_manager: Optional[OpenAIAPIManager] = None):
        """
        初始化提取器

        Args:
            config: 配置字典
        """
        self.api_manager = api_manager

        # 🔥 直接使用dict格式的实体和关系定义
        self.entity_types = kg_config.get('entity_types', {})
        self.relationship_types = kg_config.get('relationship_types', {})

        # 其他配置参数
        self.batch_size = kg_config.get('batch_size', 5)
        self.max_workers = kg_config.get('max_workers', 3)
        self.enable_threading = kg_config.get('enable_threading', True)
        self.filter_isolated_nodes = kg_config.get(
            'filter_isolated_nodes', True)

        # 线程锁
        self._lock = threading.Lock()

        logger.info(f"🔧 知识图谱提取器初始化完成")
        logger.info(f"   实体类型: {list(self.entity_types.keys())}")
        logger.info(f"   关系类型: {list(self.relationship_types.keys())}")
        logger.info(f"   批处理大小: {self.batch_size}")
        logger.info(f"   最大工作线程: {self.max_workers}")
        logger.info(f"   多线程处理: {self.enable_threading}")

    def get_entity_description(self, entity_type: str) -> str:
        """获取实体类型的描述"""
        return self.entity_types.get(entity_type, f"未知实体类型: {entity_type}")

    def get_relationship_description(self, relationship_type: str) -> str:
        """获取关系类型的描述"""
        return self.relationship_types.get(relationship_type, f"未知关系类型: {relationship_type}")

    def get_valid_types_summary(self) -> Dict[str, Any]:
        """获取有效类型的摘要信息"""
        return {
            "valid_entity_types": list(self.entity_types.keys()),
            "valid_relationship_types": list(self.relationship_types.keys()),
            "entity_type_count": len(self.entity_types),
            "relationship_type_count": len(self.relationship_types),
            "entity_descriptions": self.entity_types,
            "relationship_descriptions": self.relationship_types
        }

    def _build_system_prompt(self, content: str, content_type: str = 'text') -> str:
        """构建系统提示词"""
        # 🔥 移除质量检查，专注于构建提示词
        guidance = []

        # 简化的内容类型检测和指导
        if content_type == 'table':
            guidance.append(_GUIDANCE_TEMPLATES["table"])

        # 简化的语言检测
        chinese_ratio = len(re.findall(
            r'[\u4e00-\u9fff]', content)) / len(content) if content else 0
        english_ratio = len(re.findall(
            r'[a-zA-Z]', content)) / len(content) if content else 0

        if chinese_ratio > 0.6:
            guidance.append(_GUIDANCE_TEMPLATES["zh"])
        elif english_ratio > 0.6:
            guidance.append(_GUIDANCE_TEMPLATES["en"])
        else:
            guidance.append(_GUIDANCE_TEMPLATES["mixed"])

        # 简化的长度指导
        if len(content) > 3000:
            guidance.append(_GUIDANCE_TEMPLATES["long"])
        elif len(content) < 500:
            guidance.append(_GUIDANCE_TEMPLATES["short"])

        # ATT&CK结构检测
        if re.search(r'###\s+[^#\n]+?\s*\n.*?\*\*[^–]+?–\s*T\.?\d+', content, re.DOTALL):
            guidance.append(_GUIDANCE_TEMPLATES["attck"])

        guidance_text = '\n'.join(guidance)

        # 根据语言选择不同的few shot示例
        if chinese_ratio > 0.5:
            few_shot_examples = _FEW_SHOT['chinese']
        else:
            few_shot_examples = _FEW_SHOT['english']

        return _BASE_PROMPT_TMPL.format(
            entity_desc=self.entity_types,
            rel_desc=self.relationship_types,
            few_shot_examples=few_shot_examples,
            guidance_text=guidance_text
        )

    def _filter_invalid_types(self, kg_data: Dict[str, Any]) -> Dict[str, Any]:
        """🔥 修改：使用配置的dict格式类型进行过滤"""
        filtered_entities = []
        filtered_relationships = []

        # 获取有效的实体和关系类型
        valid_entity_types = set(self.entity_types.keys())
        valid_relationship_types = set(self.relationship_types.keys())

        # 过滤实体
        for entity in kg_data.get('entities', []):
            entity_type = entity.get('labels', '')  # 直接获取字符串
            if entity_type in valid_entity_types:
                filtered_entities.append(entity)
            else:
                logger.debug(
                    f"过滤无效实体类型: {entity_type} (实体: {entity.get('name', 'Unknown')})")

        # 过滤关系
        for relationship in kg_data.get('relationships', []):
            rel_type = relationship.get('type', '')
            if rel_type in valid_relationship_types:
                filtered_relationships.append(relationship)
            else:
                logger.debug(f"过滤无效关系类型: {rel_type}")

        # 统计过滤结果
        original_entity_count = len(kg_data.get('entities', []))
        original_relationship_count = len(kg_data.get('relationships', []))
        filtered_entity_count = len(filtered_entities)
        filtered_relationship_count = len(filtered_relationships)

        if original_entity_count > filtered_entity_count:
            logger.info(
                f"📊 实体过滤: {original_entity_count} -> {filtered_entity_count} (过滤了{original_entity_count - filtered_entity_count}个)")

        if original_relationship_count > filtered_relationship_count:
            logger.info(
                f"📊 关系过滤: {original_relationship_count} -> {filtered_relationship_count} (过滤了{original_relationship_count - filtered_relationship_count}个)")

        # 🔥 新增：过滤孤立实体和悬挂关系
        final_entities, final_relationships = self._filter_isolated_nodes_and_edges(
            filtered_entities, filtered_relationships)

        return {
            'entities': final_entities,
            'relationships': final_relationships
        }

    def _filter_isolated_nodes_and_edges(self, entities: List[Dict], relationships: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        过滤孤立实体和悬挂关系

        Args:
            entities: 实体列表
            relationships: 关系列表

        Returns:
            (有连接的实体列表, 有效的关系列表)
        """
        if not self.filter_isolated_nodes:
            logger.info("🔧 孤立节点过滤已禁用，跳过")
            return entities, relationships
        logger.info(
            f"🧹 开始过滤孤立节点和悬挂边，实体数: {len(entities)}, 关系数: {len(relationships)}")

        # 第一步：收集所有实体ID
        all_entity_ids = set()
        entity_id_to_entity = {}

        for entity in entities:
            entity_id = entity.get('id')
            if entity_id:
                all_entity_ids.add(entity_id)
                entity_id_to_entity[entity_id] = entity

        # 第二步：收集关系中引用的实体ID
        connected_entity_ids = set()
        valid_relationships = []

        for relationship in relationships:
            source_id = relationship.get('source')
            target_id = relationship.get('target')

            # 检查关系的两端实体是否都存在
            if source_id in all_entity_ids and target_id in all_entity_ids:
                valid_relationships.append(relationship)
                connected_entity_ids.add(source_id)
                connected_entity_ids.add(target_id)
            else:
                logger.debug(f"过滤悬挂关系: {source_id} -> {target_id} (实体不存在)")

        # 第三步：过滤出有连接的实体
        connected_entities = []
        isolated_entities = []

        for entity_id in all_entity_ids:
            if entity_id in connected_entity_ids:
                connected_entities.append(entity_id_to_entity[entity_id])
            else:
                isolated_entities.append(entity_id_to_entity[entity_id])
                entity_name = entity_id_to_entity[entity_id].get(
                    'name', 'Unknown')
                logger.debug(f"过滤孤立实体: {entity_name} (ID: {entity_id})")

        # 统计过滤结果
        original_entity_count = len(entities)
        original_relationship_count = len(relationships)
        final_entity_count = len(connected_entities)
        final_relationship_count = len(valid_relationships)

        isolated_count = len(isolated_entities)
        dangling_rel_count = original_relationship_count - final_relationship_count

        if isolated_count > 0:
            logger.info(f"🗑️  过滤孤立实体: {isolated_count}个")
            if isolated_count <= 5:  # 少量时显示具体名称
                isolated_names = [e.get('name', 'Unknown')
                                  for e in isolated_entities]
                logger.info(f"   孤立实体列表: {isolated_names}")

        if dangling_rel_count > 0:
            logger.info(f"🗑️  过滤悬挂关系: {dangling_rel_count}个")

        logger.info(
            f"✅ 孤立节点/边过滤完成: 实体 {original_entity_count}→{final_entity_count}, 关系 {original_relationship_count}→{final_relationship_count}")

        return connected_entities, valid_relationships

    def extract_from_chunks(
        self,
        docs: List[Document],
    ) -> List[Dict[str, Any]]:
        """提取知识图谱 - 支持多线程处理"""
        # ... chunk选择逻辑保持不变 ...

        if self.enable_threading and len(docs) > 1:
            return self._extract_with_threading(docs)
        else:
            return self._extract_sequential(docs)

    def _extract_with_threading(self, docs: List[Document]) -> List[Dict[str, Any]]:
        """多线程提取知识图谱"""
        logger.info(
            f"🔥 启用多线程处理 - 线程数: {self.max_workers}, chunk数: {len(docs)}")

        results = [None] * len(docs)  # 预分配结果列表，保持顺序
        successful_extractions = 0
        failed_extractions = 0

        # 创建线程池
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务，保持chunk索引
            future_to_index = {
                executor.submit(self._thread_safe_extract_single_chunk, doc, i): i
                for i, doc in enumerate(docs)
            }

            # 收集结果
            completed_count = 0
            for future in as_completed(future_to_index):
                chunk_index = future_to_index[future]
                completed_count += 1

                try:
                    result = future.result()

                    # 🔥 修复：使用索引赋值而不是append
                    results[chunk_index] = result

                    # 线程安全的统计更新
                    with self._lock:
                        if result and (len(result.get('entities', [])) > 0 or len(result.get('relationships', [])) > 0):
                            successful_extractions += 1
                            logger.debug(
                                f"✅ Chunk {chunk_index+1} 提取成功 ({completed_count}/{len(docs)})")
                        else:
                            failed_extractions += 1
                            logger.debug(
                                f"⚠️ Chunk {chunk_index+1} 提取为空 ({completed_count}/{len(docs)})")

                except Exception as e:
                    logger.error(f"❌ Chunk {chunk_index+1} 处理异常: {e}")
                    results[chunk_index] = {
                        "entities": [], "relationships": []}
                    with self._lock:
                        failed_extractions += 1

                # 进度提示
                if completed_count % 5 == 0 or completed_count == len(docs):
                    logger.info(
                        f"🔄 多线程进度: {completed_count}/{len(docs)} ({completed_count/len(docs)*100:.1f}%)")

        # 🔥 确保没有None值
        results = [r if r is not None else {
            "entities": [], "relationships": []} for r in results]

        # 按chunk索引排序
        results.sort(key=lambda x: x.get('chunk_index', 0))

        success_rate = (successful_extractions /
                        len(docs) * 100) if docs else 0

        logger.info(f"🎯 多线程知识图谱提取完成:")
        logger.info(f"   - 总chunk数: {len(docs)}")
        logger.info(f"   - 成功提取: {successful_extractions}")
        logger.info(f"   - 失败/跳过: {failed_extractions}")
        logger.info(f"   - 成功率: {success_rate:.1f}%")
        logger.info(f"   - 使用线程数: {self.max_workers}")

        return results

    def _thread_safe_extract_single_chunk(self, chunk: Document, chunk_index: int) -> Dict[str, Any]:
        """线程安全的单chunk提取"""
        try:
            # 添加线程ID到日志
            thread_id = threading.current_thread().ident
            logger.debug(f"🧵 线程{thread_id} 开始处理 Chunk {chunk_index+1}")
            result = self._extract_from_single_chunk(
                chunk, chunk_index)

            logger.debug(f"🧵 线程{thread_id} 完成 Chunk {chunk_index+1}")
            return result

        except Exception as e:
            logger.error(f"🧵 线程异常 Chunk {chunk_index+1}: {e}")
            return {"entities": [], "relationships": []}

    def _extract_sequential(self, docs: List[Document]) -> List[Dict[str, Any]]:
        """原有的串行处理方法"""
        logger.info(f"📝 使用串行处理 - chunk数: {len(docs)}")

        results = []
        successful_extractions = 0
        failed_extractions = 0

        for i, doc in enumerate(docs):
            try:
                result = self._extract_from_single_chunk(doc, i)
                results.append(result)

                if result and (len(result.get('entities', [])) > 0 or len(result.get('relationships', [])) > 0):
                    successful_extractions += 1
                    logger.debug(f"✅ Chunk {i+1} 提取成功")
                else:
                    failed_extractions += 1
                    logger.debug(f"⚠️ Chunk {i+1} 提取为空")

            except Exception as e:
                logger.error(f"❌ Chunk {i+1} 处理异常: {e}")
                results.append({"entities": [], "relationships": []})
                failed_extractions += 1

        success_rate = (successful_extractions /
                        len(docs) * 100) if docs else 0

        logger.info(f"📊 串行知识图谱提取完成:")
        logger.info(f"   - 总chunk数: {len(docs)}")
        logger.info(f"   - 成功提取: {successful_extractions}")
        logger.info(f"   - 失败/跳过: {failed_extractions}")
        logger.info(f"   - 成功率: {success_rate:.1f}%")

        return results

    def _extract_from_single_chunk(self, chunk: Document, chunk_index: int) -> Dict[str, Any]:
        """从单个chunk提取知识图谱"""
        chunk_content = chunk.page_content
        chunk_type = chunk.metadata.get('content_type', 'unknown')
        chunk_id = chunk.metadata.get('chunk_id', f'chunk_{chunk_index}')
        logger.info(
            f"🔍 开始处理 Chunk {chunk_id} \n 内容摘要: {chunk_content[:200]}...")

        try:
            if not chunk_content or not chunk_content.strip():
                logger.warning(f"⚠️ Chunk {chunk_id} 内容为空，跳过处理")
                return {"entities": [], "relationships": []}

            # 构建提示词
            prompt = self._build_system_prompt(chunk_content, chunk_type)

            # 调用API
            messages = [
                {"role": "system",
                    "content": prompt},
                {"role": "user", "content": chunk_content}
            ]

            # 直接使用api_manager的属性
            response = self.api_manager.call_api(
                messages=messages,
                model=self.api_manager.model,  # 直接使用api_manager的属性
                temperature=self.api_manager.temperature,  # 固定温度参数
                max_tokens=self.api_manager.max_tokens,
                timeout=self.api_manager.timeout,
                top_p=self.api_manager.top_p,
                frequency_penalty=self.api_manager.frequency_penalty,
                presence_penalty=self.api_manager.presence_penalty
            )

            # 类型检查和转换
            if not isinstance(response, str):
                logger.warning(
                    f"⚠️ Chunk {chunk_id} API返回非字符串类型: {type(response)}")
                response = str(response)

            # 清理和空检查
            response = response.strip()
            if not response:
                logger.warning(f"⚠️ Chunk {chunk_id} GPT响应为空")
                extracted_data = {"entities": [], "relationships": []}
            else:
                logger.debug(f"🤖 Chunk {chunk_id} 响应长度: {len(response)}")

                # 直接调用解析方法
                extracted_data = GPTResponseParser.parse_knowledge_graph_result(
                    response=response,
                    api_manager=self.api_manager
                )

            logger.debug(
                f"📊 Chunk {chunk_id} 解析结果: 实体{len(extracted_data.get('entities', []))}, 关系{len(extracted_data.get('relationships', []))}")

            # 过滤无效类型 孤立节点和悬挂关系
            filtered_data = self._filter_invalid_types(extracted_data)

            # 🔥 新增：为每个实体和关系添加chunk信息
            chunk_info = {
                'chunk_id': chunk_id,
                'chunk_type': chunk_type,
                'chunk_content': chunk_content,
                'chunk_length': len(chunk_content),
                'source_metadata': chunk.metadata if hasattr(chunk, 'metadata') else {}
            }

            # 为实体添加chunk信息
            for entity in filtered_data.get('entities', []):
                if 'chunks_info' not in entity:
                    entity['chunks_info'] = []
                entity['chunks_info'].append(chunk_info)

            # 为关系添加chunk信息
            for relationship in filtered_data.get('relationships', []):
                if 'chunks_info' not in relationship:
                    relationship['chunks_info'] = []
                relationship['chunks_info'].append(chunk_info)

            # 详细的结果对比
            original_entities = len(extracted_data.get('entities', []))
            original_relationships = len(
                extracted_data.get('relationships', []))
            filtered_entities = len(filtered_data.get('entities', []))
            filtered_relationships = len(
                filtered_data.get('relationships', []))

            logger.debug(f"🎯 Chunk {chunk_id} 过滤结果:")
            logger.debug(f"   实体: {original_entities} -> {filtered_entities}")
            logger.debug(
                f"   关系: {original_relationships} -> {filtered_relationships}")

            # 如果过滤后为空，记录原因
            if filtered_entities == 0 and filtered_relationships == 0:
                if original_entities > 0 or original_relationships > 0:
                    logger.warning(f"⚠️ Chunk {chunk_id} 所有内容被过滤掉了！")
                    logger.warning(f"   原始GPT响应前500字符: {response[:500]}...")
                else:
                    logger.warning(f"⚠️ Chunk {chunk_id} GPT未提取到任何内容")
                    logger.warning(f"   内容前500字符: {chunk_content[:500]}...")

                    # 🔥 添加：分析为什么没有提取到内容
                    self._analyze_extraction_failure(
                        chunk_content, response)

            logger.info(
                f"✅ Chunk {chunk_id} 处理完成，已从chunk中抽取 {filtered_entities} 个实体和 {filtered_relationships} 个关系")
            return filtered_data

        except Exception as e:
            logger.error(f"❌ Chunk {chunk_id} 处理失败: {e}")
            logger.error(f"❌ 错误详情: {type(e).__name__}: {str(e)}")
            return {"entities": [], "relationships": []}

    def _analyze_extraction_failure(self, chunk_content: str, gpt_response: str) -> str:
        """简化版失败原因分析"""
        try:
            # 快速检查内容质量
            if len(chunk_content.strip()) < 100:
                return "内容过短"

            # 检查关键词密度
            content_lower = chunk_content.lower()
            keyword_count = sum(1 for keyword in SECURITY_KEYWORDS['chinese'] + SECURITY_KEYWORDS['english']
                                if keyword.lower() in content_lower)

            if keyword_count < 2:
                return "缺乏安全关键词"

            # 检查是否为导航内容
            nav_indicators = ['首页', '登录', '注册', 'home', 'about', 'contact']
            if sum(1 for nav in nav_indicators if nav in content_lower) >= 2:
                return "疑似导航内容"

            # 检查GPT响应
            if not gpt_response or len(gpt_response.strip()) < 10:
                return "GPT无响应"

            if not ('"entities"' in gpt_response and '"relationships"' in gpt_response):
                return "响应格式错误"

            # 如果都正常但仍失败
            return "解析失败"

        except Exception:
            return "分析异常"
