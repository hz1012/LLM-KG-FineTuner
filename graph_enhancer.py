# coding:utf-8
"""
图谱增强模块 - 通过ES向量检索增强知识图谱
"""
import logging
import json
import time
import re
from typing import List, Dict, Any, Optional, Tuple
from elasticsearch import Elasticsearch
from openai import OpenAI
from utils import OpenAIAPIManager

logger = logging.getLogger(__name__)


class GraphEnhancer:
    """图谱增强器 - 通过ES向量检索增强已有知识图谱"""

    def __init__(self, config: Dict[str, Any], api_manager: Optional[OpenAIAPIManager] = None):
        """
        初始化图谱增强器

        Args:
            config: 图谱增强配置
            api_manager: API管理器实例
        """
        self.config = config
        self.api_manager = api_manager

        # 检查是否启用图谱增强
        self.enable = config.get('enable', True)
        if not self.enable:
            logger.info("⏭️  图谱增强器已被禁用，跳过初始化")
            return

        # ES配置
        es_config = config.get('elasticsearch', {})
        self.es_hosts = es_config.get('hosts', ["http://localhost:9200"])
        self.es_auth = es_config.get('auth', None)  # 从配置文件读取，不提供默认值
        self.index_name = es_config.get('index_name', 'test_ttp_embedding_index')

        # 增强配置
        self.top_k = config.get('top_k', 3)
        self.similarity_threshold = config.get('similarity_threshold', 0.7)
        self.max_enhance_per_procedure = config.get(
            'max_enhance_per_procedure', 2)
        self.enable_deduplication = config.get('enable_deduplication', True)
        self.embedding_model = config.get(
            'embedding_model', 'text-embedding-v2')

        # 初始化ES客户端
        self.es_client = None
        self._init_es_client()

        # 初始化OpenAI客户端（用于生成embedding）
        # 从 api_manager 获取配置，如果没有则从 config 中查找
        if self.api_manager:
            self.openai_client = self.api_manager.client
        else:
            # 备用方案：尝试从全局配置读取（不推荐）
            logger.warning("⚠️ 未提供 api_manager，图谱增强可能无法正常工作")
            self.openai_client = None

        logger.info(f"🔧 图谱增强器初始化完成")
        logger.info(f"   ES索引: {self.index_name}")
        logger.info(f"   TopK: {self.top_k}")
        logger.info(f"   相似度阈值: {self.similarity_threshold}")

    def _init_es_client(self):
        """初始化ES客户端"""
        try:
            self.es_client = Elasticsearch(
                hosts=self.es_hosts,
                basic_auth=self.es_auth,
                verify_certs=False,
                ssl_show_warn=False,
                request_timeout=30,
                max_retries=3,
                retry_on_timeout=True,
            )

            if self.es_client.ping():
                logger.info("✅ Elasticsearch 连接成功")
            else:
                logger.error("❌ Elasticsearch 连接失败")
                self.es_client = None

        except Exception as e:
            logger.error(f"❌ 连接 Elasticsearch 时发生异常: {e}")
            self.es_client = None

    def get_embedding(self, text: str) -> Optional[List[float]]:
        """生成文本向量"""
        if not self.openai_client:
            logger.error("❌ OpenAI客户端未初始化，无法生成embedding")
            return None

        max_retries = 2
        base_wait_time = 1200  # 基础等待时间：20min

        for attempt in range(max_retries + 1):
            try:
                # 🔥 简单粗暴：每次调用前等待足够长时间
                if attempt > 0:  # 重试时等待更久
                    wait_time = base_wait_time * (attempt + 1)
                    logger.warning(f"⏳ 第{attempt}次重试，等待 {wait_time} 秒...")
                    time.sleep(wait_time)

                logger.info(
                    f"🔍 生成embedding (尝试 {attempt + 1}/{max_retries + 1})")

                response = self.openai_client.embeddings.create(
                    input=text,
                    model=self.embedding_model
                )

                if response.data and len(response.data) > 0:
                    embedding = response.data[0].embedding
                    logger.info(f"✅ Embedding生成成功，维度: {len(embedding)}")
                    return embedding
                else:
                    raise ValueError("API返回空embedding")

            except Exception as e:
                error_msg = str(e)

                # 检查是否为限流错误
                if any(indicator in error_msg.lower() for indicator in
                       ['irc-001', '资源限制策略', '10次/60.0分钟', 'rate limit']):
                    if attempt < max_retries:
                        logger.warning(f"⚠️  触发API限流: {error_msg}")
                        continue  # 继续重试
                    else:
                        logger.error(f"❌ 达到最大重试次数，放弃embedding生成")
                        break
                else:
                    logger.error(f"❌ 生成embedding失败: {error_msg}")
                    if attempt < max_retries:
                        time.sleep(30)  # 非限流错误，短暂等待
                        continue
                    else:
                        break

        logger.error("💥 Embedding生成最终失败")

    def enhance_knowledge_graph(self, kg_data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        增强知识图谱

        Args:
            kg_data: 原始知识图谱数据 (06_knowledge_graph_simple.json格式)

        Returns:
            (增强后的知识图谱, 增强统计信息)
        """
        logger.info("🚀 开始图谱增强...")

        if not self.es_client:
            logger.error("❌ ES客户端未初始化，跳过图谱增强")
            return kg_data, {"status": "failed", "reason": "ES客户端未初始化"}

        # 🔥 新增：全局级别跟踪被增强的procedure
        global_enhanced_procedures = set()

        start_time = time.time()
        enhancement_stats = {
            "original_graph_count": len(kg_data),
            "procedures_found": 0,
            "procedures_enhanced": 0,  # 🔥 重命名：实际被增强的procedure数量
            "es_queries": 0,
            "matched_results": 0,
            "new_entities_added": 0,
            "new_relationships_added": 0,
            "processing_time": 0
        }

        enhanced_kg_data = []

        # 处理每个知识图谱数据
        for graph_idx, graph_data in enumerate(kg_data):
            logger.info(f"📝 处理第{graph_idx + 1}/{len(kg_data)}个知识图谱的增强")

            enhanced_graph = self._enhance_single_knowledge(
                graph_data, enhancement_stats, global_enhanced_procedures)
            enhanced_kg_data.append(enhanced_graph)

        # 🔥 修复：使用全局去重后的数量
        enhancement_stats["procedures_enhanced"] = len(
            global_enhanced_procedures)
        enhancement_stats["processing_time"] = time.time() - start_time
        logger.info("✅ 图谱增强完成!")
        logger.info(
            f"   - 处理知识图谱: {enhancement_stats['original_graph_count']}")
        logger.info(
            f"   - 发现procedures: {enhancement_stats['procedures_found']}")
        logger.info(
            f"   - 被增强procedures: {enhancement_stats['procedures_enhanced']}")  # 🔥 修复日志
        logger.info(f"   - ES查询次数: {enhancement_stats['es_queries']}")
        logger.info(f"   - 匹配结果: {enhancement_stats['matched_results']}")
        logger.info(f"   - 新增实体: {enhancement_stats['new_entities_added']}")
        logger.info(
            f"   - 新增关系: {enhancement_stats['new_relationships_added']}")
        logger.info(f"   - 处理耗时: {enhancement_stats['processing_time']:.2f}秒")
        return enhanced_kg_data, enhancement_stats

    def _enhance_single_knowledge(self, chunk_data: Dict[str, Any], stats: Dict[str, Any],
                                  global_enhanced_procedures: set) -> Dict[str, Any]:
        """增强单个知识图谱的图谱数据"""
        try:
            # 深拷贝原始数据
            enhanced_chunk = json.loads(json.dumps(chunk_data))

            nodes = enhanced_chunk.get("nodes", {})

            # 🔥 新增：记录被增强的原始实体
            current_enhanced_procedures = set()

            procedure_entities = []
            for node_key, node_count in nodes.items():
                try:
                    # 解析JSON格式的节点键
                    node_info = json.loads(node_key)
                    entity_type = node_info.get("entity_type", "")

                    # 查找Procedures类型的实体
                    if entity_type.lower() in ["procedures", "procedure"]:
                        procedure_entities.append(
                            (node_key, node_info, node_count))
                        stats["procedures_found"] += 1

                except json.JSONDecodeError:
                    # 如果不能解析为JSON，跳过
                    logger.debug(f"   跳过无法解析的节点键: {node_key}")
                    continue

            if not procedure_entities:
                return enhanced_chunk

            # 为每个procedure实体进行ES查询和增强
            for node_key, node_info, node_count in procedure_entities:
                # 🔥 修改：检查是否成功增强了该procedure
                was_enhanced = self._enhance_procedure_entity(
                    node_key, node_info, node_count, enhanced_chunk, stats, global_enhanced_procedures
                )

                # 🔥 修复：如果成功增强，添加到全局集合（自动去重）
                if was_enhanced:
                    global_enhanced_procedures.add(node_key)

            return enhanced_chunk

        except Exception as e:
            logger.error(f"❌ 增强单个chunk失败: {e}")
            return chunk_data

    def _enhance_procedure_entity(self, node_key: str, node_info: Dict[str, Any], node_count: int,
                                  enhanced_chunk: Dict[str, Any], stats: Dict[str, Any],
                                  global_enhanced_procedures: set) -> bool:
        """为单个procedure实体进行增强"""
        try:
            proc_name = node_info.get("label", "")
            proc_desc = node_info.get("description", "") or proc_name
            query_text = proc_desc if proc_desc and len(
                proc_desc) > 10 else proc_name

            if not query_text or len(query_text.strip()) < 5:
                return False

            similar_ttps = self._query_similar_ttps(query_text)
            stats["es_queries"] += 1

            if not similar_ttps:
                return False

            stats["matched_results"] += len(similar_ttps)

            # 添加相似TTP到图谱
            actually_enhanced = self._add_similar_ttps_to_graph(
                node_key, node_info, similar_ttps, enhanced_chunk, stats, global_enhanced_procedures
            )

            return actually_enhanced  # 返回是否真正进行了增强

        except Exception as e:
            logger.error(f"❌ 增强procedure实体失败: {e}")
            return False

    def _add_similar_ttps_to_graph(self, original_node_key: str, original_node_info: Dict[str, Any],
                                   similar_ttps: List[Dict[str, Any]],
                                   enhanced_chunk: Dict[str, Any], stats: Dict[str, Any],
                                   enhanced_original_entities: set):
        """将相似的TTP数据添加到图谱中"""
        try:
            nodes = enhanced_chunk["nodes"]
            edges = enhanced_chunk["edges"]

            # 记录是否真正添加了新的实体或关系
            actually_enhanced = False

            # 收集similar procedures信息
            similar_procedures_info = []

            # 收集所有相关的节点键用于关系创建
            all_new_node_keys = []

            for i, ttp in enumerate(similar_ttps):
                procedure_text = ttp["procedure"]
                tactics_text = ttp["tactics"]
                techniques_text = ttp["techniques"]
                similarity_score = ttp["score"]

                # 收集similar procedure信息用于保存到原始节点
                if procedure_text and procedure_text.strip():
                    similar_procedures_info.append({
                        "text": procedure_text[:100] + "..." if len(procedure_text) > 100 else procedure_text,
                        "similarity_score": similarity_score,
                        "source": "ES_enhancement"
                    })

                # 🔥 对于procedure，直接增强原有实体（因为原节点肯定是procedure类型）
                if procedure_text and procedure_text.strip():
                    # 增强原有procedure实体，增加计数和相似信息
                    nodes[original_node_key] += 1
                    enhanced_original_entities.add(
                        original_node_key)  # 记录被增强的原始实体

                    # 将原procedure节点加入关系创建列表
                    all_new_node_keys.append(original_node_key)

                # 创建新的Tactics实体（保持原逻辑，因为tactics可能确实不同）
                if tactics_text and tactics_text.strip():
                    pkey = self._generate_simple_pkey("tactic", tactics_text)
                    new_tactic_key = json.dumps({
                        "entity_type": "tactic",
                        "label": tactics_text,
                        "pkey": pkey,
                        "source": "ES_enhancement",
                        "similarity_score": similarity_score,
                        "enhanced_from": original_node_key
                    }, ensure_ascii=False)

                    # 即使是重复节点也要添加到all_new_node_keys列表中用于关系创建
                    if not self._is_duplicate_node_key(nodes, new_tactic_key):
                        nodes[new_tactic_key] = 1
                        stats["new_entities_added"] += 1
                        actually_enhanced = True  # 标记为真正增强

                    # 无论是否是重复节点，都要添加到all_new_node_keys用于关系创建
                    all_new_node_keys.append(new_tactic_key)

                # 创建新的Techniques实体（保持原逻辑，因为techniques可能确实不同）
                if techniques_text and techniques_text.strip():
                    pkey = self._generate_simple_pkey(
                        "technique", techniques_text)
                    new_technique_key = json.dumps({
                        "entity_type": "technique",
                        "label": techniques_text,
                        "pkey": pkey,
                        "source": "ES_enhancement",
                        "similarity_score": similarity_score,
                        "enhanced_from": original_node_key
                    }, ensure_ascii=False)

                    # 即使是重复节点也要添加到all_new_node_keys列表中用于关系创建
                    if not self._is_duplicate_node_key(nodes, new_technique_key):
                        nodes[new_technique_key] = 1
                        stats["new_entities_added"] += 1
                        actually_enhanced = True  # 标记为真正增强

                    # 无论是否是重复节点，都要添加到all_new_node_keys用于关系创建
                    all_new_node_keys.append(new_technique_key)

            # 创建实体间的关系
            relationships_added = self._create_ttp_relationships(
                all_new_node_keys, original_node_key, edges, max([ttp.get("score", 0) for ttp in similar_ttps], default=0), stats
            )

            # 如果添加了关系，则标记为真正增强
            if relationships_added > 0:
                actually_enhanced = True

            # 更新原始节点信息，添加similar_procedures字段
            if similar_procedures_info:
                original_node_updated = original_node_info.copy()
                original_node_updated["similar_procedures"] = similar_procedures_info
                updated_node_key = json.dumps(
                    original_node_updated, ensure_ascii=False)

                # 更新节点键
                if original_node_key in nodes:
                    node_count = nodes.pop(original_node_key)
                    nodes[updated_node_key] = node_count

            # 只有真正添加了实体或关系才返回True
            return actually_enhanced

        except Exception as e:
            logger.error(f"添加相似TTP到图谱时发生错误: {e}")
            return False

    def _generate_simple_pkey(self, entity_type: str, label: str) -> str:
        """生成简洁的实体pkey"""
        # 清理标签，保留字母数字和点号
        clean_label = re.sub(r'[^\w\.]', '', label.lower())
        return f"{entity_type}--{clean_label}"

    def _is_duplicate_node_key(self, nodes: Dict[str, Any], new_node_key: str) -> bool:
        """检查是否为重复节点"""
        if not self.enable_deduplication:
            return False

        try:
            new_node_info = json.loads(new_node_key)
            new_label = new_node_info.get("label", "").lower().strip()
            new_type = new_node_info.get("entity_type", "").lower()

            for existing_key in nodes.keys():
                try:
                    existing_info = json.loads(existing_key)
                    existing_label = existing_info.get(
                        "label", "").lower().strip()
                    existing_type = existing_info.get(
                        "entity_type", "").lower()

                    if existing_type == new_type and existing_label == new_label:
                        return True
                except json.JSONDecodeError:
                    continue

            return False
        except json.JSONDecodeError:
            return False

    def _create_ttp_relationships(self, new_node_keys: List[str],
                                  original_node_key: str, edges: Dict[str, Any],
                                  similarity_score: float, stats: Dict[str, Any]):
        """创建TTP实体间的关系"""
        relationships_added = 0  # 记录添加的关系数量
        try:
            original_node_info = json.loads(original_node_key)
            original_pkey = original_node_info.get("pkey", "")

            # 收集新增节点的pkey信息
            tactics_pkeys = []  # 支持多个tactic
            techniques_pkeys = []  # 支持多个technique

            # 从当前new_node_keys中收集tactic和technique
            for new_node_key in new_node_keys:
                try:
                    new_node_info = json.loads(new_node_key)
                    entity_type = new_node_info.get("entity_type", "")

                    if entity_type == "tactic":
                        tactics_pkeys.append(new_node_info.get("pkey", ""))
                    elif entity_type == "technique":
                        techniques_pkeys.append(new_node_info.get("pkey", ""))
                except json.JSONDecodeError:
                    continue

            # 只创建两种关系类型:
            # 1. tactics HAS techniques：战术包含技术
            # 2. techniques LAUNCH procedure：技术启动具体的过程

            # 创建 tactics HAS techniques 关系 - 支持多个tactic和technique的组合
            for tactic_pkey in tactics_pkeys:
                for technique_pkey in techniques_pkeys:
                    rel_key = json.dumps({
                        "label": "HAS",
                        "pkey": tactic_pkey,      # tactics作为主体
                        "skey": technique_pkey,   # techniques作为客体
                        "confidence": similarity_score,
                        "source": "ES_enhancement"
                    }, ensure_ascii=False)

                    if rel_key not in edges:
                        edges[rel_key] = 1
                        stats["new_relationships_added"] += 1
                        relationships_added += 1

            # 创建 techniques LAUNCH procedure 关系 - 支持多个technique
            for technique_pkey in techniques_pkeys:
                if technique_pkey and original_pkey:
                    rel_key = json.dumps({
                        "label": "LAUNCH",
                        "pkey": technique_pkey,   # techniques作为主体
                        "skey": original_pkey,     # procedure作为客体
                        "confidence": similarity_score,
                        "source": "ES_enhancement"
                    }, ensure_ascii=False)

                    if rel_key not in edges:
                        edges[rel_key] = 1
                        stats["new_relationships_added"] += 1
                        relationships_added += 1

        except Exception as e:
            logger.error(f"❌ 创建TTP关系失败: {e}")

        return relationships_added  # 返回添加的关系数量

    def _query_similar_ttps(self, query_text: str) -> List[Dict[str, Any]]:
        """查询相似的TTP数据"""
        try:
            logger.info(f"🔍 开始查询相似TTP: {query_text[:50]}...")

            # 🔥 预检查：避免无效调用
            if not query_text or len(query_text.strip()) < 5:
                logger.warning("⚠️  查询文本过短，跳过TTP查询")
                return []

            # 生成查询向量（带限流控制）
            query_embedding = self.get_embedding(query_text)
            if not query_embedding:
                logger.warning("⚠️  无法生成查询向量，跳过TTP查询")
                return []

            # ES k-NN查询
            search_body = {
                "knn": {
                    "field": "dense_embedding",
                    "query_vector": query_embedding,
                    "k": self.top_k,
                    "num_candidates": 100
                },
                "fields": ["procedure", "tactics", "techniques"],
                "_source": False
            }

            response = self.es_client.search(
                index=self.index_name,
                body=search_body
            )

            similar_ttps = []
            for hit in response.get("hits", {}).get("hits", []):
                score = hit.get("_score", 0)
                fields = hit.get("fields", {})

                # 应用相似度阈值过滤
                if score >= self.similarity_threshold:
                    ttp_data = {
                        "score": score,
                        "procedure": fields.get("procedure", [""])[0],
                        "tactics": fields.get("tactics", [""])[0],
                        "techniques": fields.get("techniques", [""])[0]
                    }
                    similar_ttps.append(ttp_data)

            # 限制每个procedure的增强数量
            return similar_ttps[:self.max_enhance_per_procedure]

        except Exception as e:
            logger.error(f"❌ ES查询失败: {e}")
            return []
