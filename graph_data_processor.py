import json
import logging
import os
from typing import Dict, Any, List, Tuple, Optional, Set
from utils import OpenAIAPIManager, GPTResponseParser

logger = logging.getLogger(__name__)
# ---------- 实体对齐提示词模板 ----------
_ENTITY_ALIGNMENT_PROMPT_TMPL = """合并以下{entity_type}实体中的相同概念。

实体信息：
{entity_details}

示例：
输入: [
  {{0: "name": "APT1", "description": "中国高级持续威胁组织，又称注释小组"}},
  {{1: "name": "Advanced Persistent Threat 1", "description": "中国网络间谍组织，专注于窃取知识产权"}},
  {{2: "name": "Comment Crew", "description": "APT1的别名，因在恶意软件中留下注释而得名"}},
  {{3: "name": "SSH", "description": "安全外壳协议，用于加密网络连接"}},
  {{4: "name": "Secure Shell", "description": "SSH协议的全称，提供安全的远程访问"}},
  {{5: "name": "ssh", "description": "SSH协议的小写形式"}}
]
输出: {{"merge_groups": [[0,1,2], [3,4,5]]}}

合并规则：
- 缩写与全称：SSH ↔ Secure Shell
- 同义词/别名：APT1 ↔ Comment Crew
- 大小写变体：SSH ↔ ssh
- 描述相似的同一概念合并
- 基于name和description综合判断，相同概念不同表述合并，完全不同概念分开

⚠️ 重要约束：
- 每个索引只能出现在一个组中，不允许重复
- 只合并真正相似的概念，不确定时宁可不合并
- 输出必须是完整有效的JSON格式

注意：所有索引必须 ≤ {entity_count}

直接返回JSON格式的string，不能包含代码块提示：
{{"merge_groups": [[索引组1], [索引组2], ...]}}"""


class EntityAligner:
    """实体对齐器 - 专门负责实体对齐"""

    def __init__(self, config: Dict[str, Any], api_manager: Optional[OpenAIAPIManager] = None):
        self.config = config
        self.api_manager = api_manager

        # 对齐参数
        alignment_config = config.get(
            'graph_processor', {}).get('entity_alignment', {})
        self.similarity_threshold = alignment_config.get(
            'similarity_threshold', 0.75)
        self.enable_contains_match = alignment_config.get(
            'enable_contains_match', True)
        self.enable_acronym_match = alignment_config.get(
            'enable_acronym_match', True)

    def align_entities(self, entities: List[Dict]) -> Tuple[List[Dict], Dict[str, str], Dict[str, str]]:
        """
        对齐实体，返回三元组：(对齐后实体列表, 名称映射, ID映射)

        Returns:
            Tuple[对齐后实体列表, 原始名称->对齐后名称的映射, 原始ID->对齐后ID的映射]
        """
        logger.info(f"🔄 开始实体对齐，原始实体数: {len(entities)}")

        # 优先尝试大模型对齐
        if self.api_manager:
            try:
                return self._gpt_align(entities)
            except Exception as e:
                logger.warning(f"⚠️ 大模型对齐失败: {e}，降级到规则对齐")

        # 备选方案：规则对齐
        return self._rule_align(entities)

    def _gpt_align(self, entities: List[Dict]) -> Tuple[List[Dict], Dict[str, str], Dict[str, str]]:
        """大模型实体对齐，返回三元组"""
        # 按类型分组
        entities_by_type = {}
        for entity in entities:
            entity_type = entity.get('type', 'Unknown')
            if entity_type not in entities_by_type:
                entities_by_type[entity_type] = []
            entities_by_type[entity_type].append(entity)

        aligned_entities = []
        name_mapping = {}
        id_mapping = {}  # 🔥 修复：确保总是有ID映射

        for entity_type, type_entities in entities_by_type.items():
            if len(type_entities) <= 1:
                aligned_entities.extend(type_entities)
                for entity in type_entities:
                    name = entity.get('name', '')
                    entity_id = entity.get('id', '')
                    if name:
                        name_mapping[name] = name
                    if entity_id:
                        id_mapping[entity_id] = entity_id
                        self._add_id_variants(entity_id, entity_id, id_mapping)
                continue

            # 确保返回三个值
            aligned_type_entities, type_name_mapping, type_id_mapping = self._gpt_align_by_type(
                entity_type, type_entities)
            aligned_entities.extend(aligned_type_entities)
            name_mapping.update(type_name_mapping)
            id_mapping.update(type_id_mapping)  # 更新ID映射

        # 🔥 修复：返回三个值
        return aligned_entities, name_mapping, id_mapping

    def _gpt_align_by_type(self, entity_type: str, entities: List[Dict]) -> Tuple[List[Dict], Dict[str, str], Dict[str, str]]:
        """对特定类型的实体进行大模型对齐，返回三元组"""
        if len(entities) <= 1:
            entity = entities[0] if entities else {}
            name = entity.get('name', '')
            entity_id = entity.get('id', '')
            name_mapping = {name: name} if name else {}
            id_mapping = {entity_id: entity_id} if entity_id else {}
            if entity_id:
                self._add_id_variants(entity_id, entity_id, id_mapping)
            return entities, name_mapping, id_mapping

        # 🔥 构建实体信息时记录索引映射
        entity_details = []
        entity_count = len(entities)  # 记录实际实体数量
        for i, entity in enumerate(entities):
            name = entity.get('name', '')
            description = entity.get('description', '')
            entity_id = entity.get('id', f'entity_{i}')

            # 🔥 关键优化：截断过长的描述以减少prompt大小
            if len(description) > 100:  # 限制描述长度
                description = description[:97] + "..."
            if len(name) > 50:  # 限制名称长度
                name = name[:47] + "..."

            entity_info = {
                "name": name,
                "description": description or "无描述",
                "id": entity_id
            }

            # 格式化为字符串
            entity_detail = f"{{{i}: \"name\": \"{entity_info['name']}\", \"description\": \"{entity_info['description']}\"}}"
            entity_details.append(entity_detail)

        # 🔥 使用改进的模板
        entity_info_str = ",\n  ".join(entity_details)

        # 🔥 系统消息优化 - 更加具体
        system_content = f"""你是专业的实体对齐专家，能够根据实体的名称和描述准确识别同义实体。重点关注：
1) 名称的缩写关系
2) 描述的语义相似性  
3) 同一概念的不同表述方式

合并规则：
- 缩写与全称：SSH ↔ Secure Shell
- 同义词/别名：APT1 ↔ Comment Crew
- 大小写变体：SSH ↔ ssh
- 描述相似的同一概念合并
- 基于name和description综合判断，相同概念不同表述合并，完全不同概念分开

⚠️ 重要约束：
- 每个索引只能出现在一个组中，不允许重复
- 只合并真正相似的概念，不确定时宁可不合并
- 输出必须是完整有效的JSON格式

注意：所有索引必须 ≤ {entity_count-1}

实体类型：{entity_type}"""

        messages = [
            {
                "role": "system",
                "content": system_content
            },
            {"role": "user",
             "content": f"""请合并以下实体中的相同概念：

实体信息：
{entity_info_str}

直接返回JSON格式的string，不能包含代码块提示：
{{"merge_groups": [[索引组1], [索引组2], ...]}}"""}
        ]

        logger.info(f"🚀 调用大模型进行对齐，实体类型: {entity_type}，实体数量: {len(entities)}")

        # API调用保持不变
        response = self.api_manager.call_api(
            messages=messages,
            model=self.api_manager.model,
            temperature=self.api_manager.temperature,
            max_tokens=self.api_manager.max_tokens,
            timeout=self.api_manager.timeout,
            top_p=self.api_manager.top_p,
            frequency_penalty=self.api_manager.frequency_penalty,
            presence_penalty=self.api_manager.presence_penalty
        )

        # 解析对齐结果
        return self._parse_align_response(entities, response)

    def _parse_align_response(self, entities: List[Dict], response: str) -> Tuple[List[Dict], Dict[str, str], Dict[str, str]]:
        """解析大模型的对齐响应，返回三元组：(对齐实体, 名称映射, ID映射)"""
        try:
            merge_groups = GPTResponseParser.parse_merge_groups(
                response, self.api_manager)

            if not merge_groups:
                entity_names = [e.get('name', 'Unknown') for e in entities]
                entity_type = entities[0].get('type', 'Unknown')
                logger.warning(f"⚠️ 未解析到有效的合并组，使用原始实体")
                logger.warning(
                    f"📋 原始实体类型: {entity_type}, 实体列表 ({len(entity_names)}个): {entity_names}")

                # 🔥 修复：创建完整的三元组映射关系
                name_mapping = {}
                id_mapping = {}
                for entity in entities:
                    name = entity.get('name', '')
                    entity_id = entity.get('id', '')
                    if name:
                        name_mapping[name] = name
                    if entity_id:
                        id_mapping[entity_id] = entity_id
                        self._add_id_variants(entity_id, entity_id, id_mapping)

                return entities, name_mapping, id_mapping

            # 执行合并
            aligned_entities = []
            name_mapping = {}
            id_mapping = {}  # 🔥 修复：确保总是创建ID映射
            processed_indices = set()

            for group in merge_groups:
                if any(idx in processed_indices for idx in group):
                    continue

                group_entities = []
                for idx in group:
                    if 0 <= idx < len(entities):
                        group_entities.append(entities[idx])
                        processed_indices.add(idx)

                if group_entities:
                    merged_entity = self._merge_entities(group_entities)
                    aligned_entities.append(merged_entity)

                    merged_name = merged_entity.get('name', '')
                    merged_id = merged_entity.get('id', '')

                    for entity in group_entities:
                        original_name = entity.get('name', '')
                        original_id = entity.get('id', '')

                        if original_name:
                            name_mapping[original_name] = merged_name

                        if original_id and merged_id:
                            id_mapping[original_id] = merged_id
                            self._add_id_variants(
                                original_id, merged_id, id_mapping)

            # 处理未分组的实体
            for idx, entity in enumerate(entities):
                if idx not in processed_indices:
                    aligned_entities.append(entity)
                    name = entity.get('name', '')
                    entity_id = entity.get('id', '')

                    if name:
                        name_mapping[name] = name
                    if entity_id:
                        id_mapping[entity_id] = entity_id
                        self._add_id_variants(entity_id, entity_id, id_mapping)

            self._validate_id_mapping(entities, id_mapping)

            return aligned_entities, name_mapping, id_mapping

        except Exception as e:
            logger.error(f"❌ 解析GPT对齐响应失败: {e}")
            return self._rule_align(entities)

    def _add_id_variants(self, original_id: str, mapped_id: str, id_mapping: Dict[str, str]):
        """为ID添加各种变体映射"""
        if not original_id or not mapped_id:
            return

        # 添加大小写变体
        variants = [
            original_id.lower(),
            original_id.upper(),
            original_id.capitalize()
        ]

        # 处理格式变体 (如: Techniques--T1059 <-> techniques--t1059)
        if '--' in original_id:
            parts = original_id.split('--', 1)
            if len(parts) == 2:
                prefix, suffix = parts
                variants.extend([
                    f"{prefix.lower()}--{suffix.lower()}",
                    f"{prefix.upper()}--{suffix.upper()}",
                    f"{prefix.capitalize()}--{suffix}",
                    f"{prefix.capitalize()}--{suffix.lower()}",
                    f"{prefix.capitalize()}--{suffix.upper()}"
                ])

        # 添加所有变体到映射中
        for variant in variants:
            if variant not in id_mapping:  # 避免覆盖已有映射
                id_mapping[variant] = mapped_id

    def _validate_id_mapping(self, entities: List[Dict], id_mapping: Dict[str, str]):
        """验证ID映射完整性"""
        # 收集所有原始ID
        original_ids = {e.get('id') for e in entities if e.get('id')}

        # 检查哪些ID没有映射
        unmapped_ids = []
        for entity_id in original_ids:
            if entity_id not in id_mapping:
                unmapped_ids.append(entity_id)

        if unmapped_ids:
            logger.error(f"❌ 发现{len(unmapped_ids)}个未映射的实体ID:")
            for uid in unmapped_ids[:5]:  # 只显示前5个
                logger.error(f"  - {uid}")
            if len(unmapped_ids) > 5:
                logger.error(f"  ... 还有{len(unmapped_ids) - 5}个")

        logger.info(f"📊 ID映射统计: 原始实体{len(entities)}个, 映射项{len(id_mapping)}个")

    def _rule_align(self, entities: List[Dict]) -> Tuple[List[Dict], Dict[str, str], Dict[str, str]]:
        """基于规则的实体对齐，返回三元组"""
        logger.info("📐 使用规则对齐")

        aligned_entities = []
        name_mapping = {}
        id_mapping = {}
        processed_names = set()

        for entity in entities:
            name = entity.get('name', '')
            entity_id = entity.get('id', '')

            if name in processed_names:
                continue

            # 找到相似的实体
            similar_entities = self._find_similar_entities(entity, entities)

            # 创建合并后的实体
            merged_entity = self._merge_entities(similar_entities)
            aligned_entities.append(merged_entity)

            # 建立映射关系
            aligned_name = merged_entity.get('name', '')
            aligned_id = merged_entity.get('id', '')

            for similar_entity in similar_entities:
                original_name = similar_entity.get('name', '')
                original_id = similar_entity.get('id', '')

                if original_name:
                    name_mapping[original_name] = aligned_name
                    processed_names.add(original_name)

                if original_id and aligned_id:
                    id_mapping[original_id] = aligned_id
                    # 添加变体映射
                    self._add_id_variants(original_id, aligned_id, id_mapping)

        logger.info(f"✅ 规则对齐完成: {len(entities)} -> {len(aligned_entities)}")
        return aligned_entities, name_mapping, id_mapping

    def _find_similar_entities(self, target_entity: Dict, all_entities: List[Dict]) -> List[Dict]:
        """找到与目标实体相似的所有实体"""
        target_name = target_entity.get('name', '').lower()
        target_type = target_entity.get('type', '')

        similar_entities = [target_entity]

        for entity in all_entities:
            if entity == target_entity:
                continue

            name = entity.get('name', '').lower()
            entity_type = entity.get('type', '')

            # 同类型且相似
            if entity_type == target_type and self._are_names_similar(target_name, name):
                similar_entities.append(entity)

        return similar_entities

    def _are_names_similar(self, name1: str, name2: str) -> bool:
        """判断两个名称是否相似"""
        if name1 == name2:
            return True

        if self.enable_contains_match:
            if name1 in name2 or name2 in name1:
                return True

        if self.enable_acronym_match:
            # 检查缩写匹配
            if self._is_acronym_match(name1, name2):
                return True

        # 简单的字符相似度
        common_chars = len(set(name1) & set(name2))
        total_chars = len(set(name1) | set(name2))
        similarity = common_chars / total_chars if total_chars > 0 else 0.0

        return similarity >= self.similarity_threshold

    def _is_acronym_match(self, name1: str, name2: str) -> bool:
        """检查是否为缩写匹配"""
        words1 = name1.split()
        words2 = name2.split()

        # 一个是单词，一个是多个单词的首字母
        if len(words1) == 1 and len(words2) > 1:
            acronym = ''.join(w[0].lower() for w in words2 if w)
            return words1[0].lower() == acronym

        if len(words2) == 1 and len(words1) > 1:
            acronym = ''.join(w[0].lower() for w in words1 if w)
            return words2[0].lower() == acronym

        return False

    def _merge_entities(self, entities: List[Dict]) -> Dict:
        """合并多个实体为一个实体，保留chunk信息"""
        if not entities:
            return {}

        if len(entities) == 1:
            entity = entities[0].copy()
            # 确保实体有正确的ID格式
            if 'id' in entity:
                entity_id = entity['id']
                # 修复ID格式，确保使用正确的前缀
                if '--' not in entity_id or entity_id.startswith('unknown--'):
                    entity_type = entity.get('type', '').lower()
                    entity_name = entity.get(
                        'name', '').lower().replace(' ', '')
                    entity['id'] = f"{entity_type}--{entity_name}"
            return entity

        # 选择最佳名称（最长的非空名称）
        best_name = max((e.get('name', '') for e in entities),
                        key=len) or entities[0].get('name', '')

        # 选择最佳类型
        best_type = entities[0].get('type', '')

        # 合并描述
        descriptions = [e.get('description', '')
                        for e in entities if e.get('description')]
        merged_description = '; '.join(
            set(descriptions)) if descriptions else ''

        # 收集所有原始名称
        original_names = []
        for entity in entities:
            name = entity.get('name', '')
            if name and name not in original_names:
                original_names.append(name)

        # 收集所有chunk信息
        all_chunks_info = []
        seen_chunk_indices = set()

        for entity in entities:
            chunks_info = entity.get('chunks_info', [])
            for chunk_info in chunks_info:
                chunk_index = chunk_info.get('chunk_index')
                if chunk_index is not None and chunk_index not in seen_chunk_indices:
                    all_chunks_info.append(chunk_info)
                    seen_chunk_indices.add(chunk_index)

        # 生成正确的实体ID
        entity_type_lower = best_type.lower()
        entity_name_normalized = best_name.lower().replace(' ', '').replace('-', '')
        merged_id = f"{entity_type_lower}--{entity_name_normalized}"

        merged_entity = {
            'name': best_name,
            'type': best_type,
            'id': merged_id,
            'description': merged_description,
            'labels': best_type,
            'properties': {
                'id': merged_id,
                'name': best_name,
                'entity_type': entity_type_lower,
                'merge_count': len(entities),
                'description': merged_description,
                'chunks_info': all_chunks_info
            },
            'chunks_info': all_chunks_info
        }

        # 如果有多个原始名称，记录它们
        if len(original_names) > 1:
            merged_entity['properties']['original_names'] = original_names

        return merged_entity


class EnhancedGraphDataProcessor:
    """简化的图数据处理器 - 统一协调各组件"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

    @property
    def entity_aligner(self):
        """懒加载的实体对齐器"""
        if not hasattr(self, '_entity_aligner'):
            graph_config = self.config.get('graph_processor', {})
            if graph_config.get('enable_entity_alignment', False):
                try:
                    from utils import OpenAIAPIManager
                    openai_config = self.config.get('openai', {})
                    api_manager = OpenAIAPIManager(openai_config)
                    self._entity_aligner = EntityAligner(
                        self.config, api_manager)
                    logger.info("✅ 实体对齐器已启用")
                except Exception as e:
                    logger.warning(f"⚠️ 实体对齐器初始化失败: {e}")
                    self._entity_aligner = None
            else:
                self._entity_aligner = None
                logger.info("📋 实体对齐器已禁用")
        return self._entity_aligner

    def extract_raw_graph_data(self, kg_results: List[Dict]) -> Dict[str, Any]:
        """提取原始图数据（对齐前）"""
        logger.info("📊 提取原始图数据")

        # 合并所有实体和关系
        all_entities = []
        all_relationships = []

        for result in kg_results:
            all_entities.extend(result.get('entities', []))
            all_relationships.extend(result.get('relationships', []))

        return {
            'entities': all_entities,
            'relationships': all_relationships,
            'statistics': {
                'total_entities': len(all_entities),
                'total_relationships': len(all_relationships),
                'chunks_processed': len(kg_results)
            }
        }

    def extract_pure_graph_data(self, kg_results: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        从知识图谱结果中提取纯净的图数据（带实体对齐）

        Returns:
            Tuple[完整记录数据, 简化图数据]
            完整记录数据: 包含nodes和edges统计的列表，nodes中包含all_chunks_info
            简化图数据: 用于生成图的pkey-skey格式，不包含无关属性
        """
        logger.info("🔄 开始提取实体对齐图数据")

        # 1. 收集所有原始实体和关系
        all_entities = []
        all_relationships = []

        for result in kg_results:
            all_entities.extend(result.get("entities", []))
            all_relationships.extend(result.get("relationships", []))

        logger.info(
            f"收集到原始数据: {len(all_entities)}个实体, {len(all_relationships)}个关系")

        entity_aligner = self.entity_aligner  # 触发懒加载
        # 实体对齐
        if entity_aligner:
            try:
                # 🔥 接收三个返回值
                aligned_entities, name_mapping, id_mapping = self.entity_aligner.align_entities(
                    all_entities)
                logger.info(
                    f"✅ 大模型对齐完成: {len(all_entities)}个原始节点 -> {len(aligned_entities)}个对齐节点")
                logger.info(f"创建了{len(id_mapping)}个ID映射")
            except Exception as e:
                logger.error(f"❌ 实体对齐失败: {e}")
                aligned_entities = all_entities
                name_mapping = {}
                # 🔥 创建基础映射避免关系丢失
                id_mapping = {}
                for entity in all_entities:
                    entity_id = entity.get('id', '')
                    if entity_id:
                        id_mapping[entity_id] = entity_id
                        # 添加变体映射
                        self._add_id_variants_fallback(entity_id, id_mapping)
        else:
            aligned_entities = all_entities
            name_mapping = {}
            id_mapping = {}
            for entity in all_entities:
                entity_id = entity.get('id', '')
                if entity_id:
                    id_mapping[entity_id] = entity_id
                    self._add_id_variants_fallback(entity_id, id_mapping)

        # 处理关系（使用完整的ID映射）
        processed_relationships = self._process_relationships_with_mapping(
            all_relationships, id_mapping)

        # 5-8. 生成结果数据（使用新的抽象函数）
        full_result = self._build_full_graph_data(
            aligned_entities, processed_relationships)
        simple_result = self._build_simple_graph_data(
            aligned_entities, processed_relationships)

        logger.info(
            f"✅ 点对齐完成: {len(all_entities)}个原始点 -> {len(aligned_entities)}个对齐点")
        logger.info(
            f"✅ 边简化完成: {len(processed_relationships)}个完整边 -> {len(simple_result[0]['edges'])}个简化边")

        return full_result, simple_result

    def _add_id_variants_fallback(self, entity_id: str, id_mapping: Dict[str, str]):
        """后备方案：为单个实体ID添加变体映射"""
        if not entity_id:
            return

        variants = [
            entity_id.lower(),
            entity_id.upper(),
            entity_id.capitalize()
        ]

        if '--' in entity_id:
            parts = entity_id.split('--', 1)
            if len(parts) == 2:
                prefix, suffix = parts
                variants.extend([
                    f"{prefix.lower()}--{suffix.lower()}",
                    f"{prefix.upper()}--{suffix.upper()}",
                    f"{prefix.capitalize()}--{suffix.lower()}",
                    f"{prefix.capitalize()}--{suffix.upper()}"
                ])

        for variant in variants:
            if variant not in id_mapping:
                id_mapping[variant] = entity_id

    def _process_relationships_with_mapping(self, relationships: List[Dict], id_mapping: Dict[str, str]) -> List[Dict]:
        """使用ID映射处理关系"""
        processed_relationships = []
        skipped_count = 0

        for rel in relationships:
            source_id = rel.get('source', '')
            target_id = rel.get('target', '')

            # 查找映射
            mapped_source = id_mapping.get(source_id)
            mapped_target = id_mapping.get(target_id)

            if not mapped_source or not mapped_target:
                logger.warning(
                    f"跳过关系：无法解析实体引用 {source_id} -> {mapped_source} 或 {target_id} -> {mapped_target}")
                skipped_count += 1
                continue

            # 创建处理后的关系
            processed_rel = rel.copy()
            processed_rel['source'] = mapped_source
            processed_rel['target'] = mapped_target
            processed_relationships.append(processed_rel)

        if skipped_count > 0:
            logger.warning(f"⚠️ 跳过了{skipped_count}个无法解析的关系")
        else:
            logger.info(f"✅ 所有{len(relationships)}个关系都成功解析")

        return processed_relationships

    def _build_full_graph_data(self, aligned_entities: List[Dict], processed_relationships: List[Dict]) -> List[Dict]:
        """构建完整的图数据（包含所有详细信息）"""
        full_entities = self._build_full_entities(aligned_entities)
        full_relationships = self._build_full_relationships(
            processed_relationships)

        return [{
            "entities": full_entities,
            "relationships": full_relationships
        }]

    def _build_full_entities(self, aligned_entities: List[Dict]) -> List[Dict]:
        """构建完整的实体数据"""
        full_entities = []
        for entity in aligned_entities:
            entity_id = entity.get('id', '')
            entity_type = entity.get('type', '')
            name = entity.get('name', '')
            description = entity.get('description', '')
            properties = entity.get('properties', {})

            # 确保包含chunks_info
            chunks_info = entity.get('chunks_info', [])
            if chunks_info:
                properties['chunks_info'] = chunks_info

            # 按照标准格式构造实体
            full_entity = {
                "name": name,
                "type": entity_type,
                "id": entity_id,
                "description": description,
                "labels": entity_type,
                "properties": properties,
                "chunks_info": chunks_info
            }
            full_entities.append(full_entity)

        return full_entities

    def _build_full_relationships(self, processed_relationships: List[Dict]) -> List[Dict]:
        """构建完整的关系数据"""
        full_relationships = []
        for rel in processed_relationships:
            source_id = rel.get('source', '')
            target_id = rel.get('target', '')
            rel_type = rel.get('type', '')
            description = rel.get('description', '')
            confidence = rel.get('confidence', 0.7)
            chunks_info = rel.get('chunks_info', [])

            # 按照标准格式构造关系
            full_relationship = {
                "source": source_id,
                "target": target_id,
                "relationship": rel_type,
                "description": description,
                "confidence": confidence,
                "properties": {
                    "source": source_id,
                    "target": target_id,
                    "relationship": rel_type,
                    "description": description,
                    "chunks_info": chunks_info
                },
                "chunks_info": chunks_info
            }
            full_relationships.append(full_relationship)

        return full_relationships

    def _build_simple_graph_data(self, aligned_entities: List[Dict], processed_relationships: List[Dict]) -> List[Dict]:
        """构建简化的图数据（仅包含必要属性）"""
        simple_nodes = self._build_simple_nodes(aligned_entities)
        simple_edges = self._build_simple_edges(processed_relationships)

        return [{
            "nodes": simple_nodes,
            "edges": simple_edges
        }]

    def _build_simple_nodes(self, aligned_entities: List[Dict]) -> Dict[str, int]:
        """构建简化的节点数据"""
        simple_nodes = {}
        for entity in aligned_entities:
            entity_id = entity.get('id', '')
            entity_type = entity.get('type', '').lower()
            name = entity.get('name', '')

            # 简化节点格式，仅包含必要属性
            node_key = json.dumps({
                "pkey": entity_id,
                "label": name,
                "entity_type": entity_type
            }, ensure_ascii=False, sort_keys=True)

            merge_count = entity.get('properties', {}).get('merge_count', 1)
            simple_nodes[node_key] = merge_count

        return simple_nodes

    def _build_simple_edges(self, processed_relationships: List[Dict]) -> Dict[str, int]:
        """构建简化的边数据"""
        simple_edges = {}
        for rel in processed_relationships:
            source_id = rel.get('source', '')
            target_id = rel.get('target', '')
            rel_type = rel.get('type', '')

            # 简化边格式，仅包含必要属性
            edge_key = json.dumps({
                "pkey": source_id,
                "skey": target_id,
                "label": rel_type
            }, ensure_ascii=False, sort_keys=True)

            # 累加相同边的计数
            simple_edges[edge_key] = simple_edges.get(edge_key, 0) + 1

        return simple_edges
