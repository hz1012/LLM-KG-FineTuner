# coding:utf-8
"""
统计报告模块 - 知识图谱数据分析与统计报告
"""
import json
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class StatisticsReporter:
    """统计报告器 - 支持聚合统计格式的图数据"""

    def analyze_aggregated_graph_data(self, graph_list: List[Dict]) -> Dict[str, Any]:
        """分析聚合统计格式的图数据"""
        if not graph_list:
            return {"unique_nodes": 0, "unique_edges": 0, "node_type_counts": {}, "edge_type_counts": {}}

        logger.info(f"🔍 开始分析聚合图数据，共{len(graph_list)}个统计块")

        # 统计实体种类数和出现次数
        node_type_counts = {}       # 每种节点类型有多少个不同实体
        edge_type_counts = {}       # 每种关系类型有多少个不同关系
        node_occurrences = {}       # 节点类型总出现次数
        edge_occurrences = {}       # 关系类型总出现次数
        unique_nodes = {}           # 记录每种类型的唯一实体
        unique_edges = {}           # 记录每种类型的唯一关系

        for i, data_block in enumerate(graph_list):
            # 处理nodes统计
            if 'nodes' in data_block:
                nodes_data = data_block['nodes']
                logger.info(f"   块[{i}] - 节点统计项: {len(nodes_data)}")

                for node_key, count in nodes_data.items():
                    try:
                        node_info = json.loads(node_key)
                        node_type = node_info.get('entity_type', 'Unknown')
                        node_label = node_info.get('label', 'Unknown')

                        # 统计出现次数
                        node_occurrences[node_type] = node_occurrences.get(
                            node_type, 0) + count

                        # 统计实体种类数
                        if node_type not in unique_nodes:
                            node_type_counts[node_type] = 0
                            unique_nodes[node_type] = set()

                        # 用label作为唯一标识
                        if node_label not in unique_nodes[node_type]:
                            unique_nodes[node_type].add(node_label)
                            node_type_counts[node_type] += 1

                    except json.JSONDecodeError:
                        node_occurrences['Unknown'] = node_occurrences.get(
                            'Unknown', 0) + count
                        if 'Unknown' not in unique_nodes:
                            node_type_counts['Unknown'] = 0
                            unique_nodes['Unknown'] = set()

            # 处理edges统计
            if 'edges' in data_block:
                edges_data = data_block['edges']
                logger.info(f"   块[{i}] - 边统计项: {len(edges_data)}")

                for edge_key, count in edges_data.items():
                    try:
                        edge_info = json.loads(edge_key)
                        edge_type = edge_info.get('label', 'Unknown')

                        # 统计出现次数
                        edge_occurrences[edge_type] = edge_occurrences.get(
                            edge_type, 0) + count

                        # 统计关系种类数
                        if edge_type not in unique_edges:
                            edge_type_counts[edge_type] = 0
                            unique_edges[edge_type] = set()

                        # 用完整edge_key作为唯一标识
                        if edge_key not in unique_edges[edge_type]:
                            unique_edges[edge_type].add(edge_key)
                            edge_type_counts[edge_type] += 1

                    except json.JSONDecodeError:
                        edge_occurrences['Unknown'] = edge_occurrences.get(
                            'Unknown', 0) + count
                        if 'Unknown' not in unique_edges:
                            edge_type_counts['Unknown'] = 0
                            unique_edges['Unknown'] = set()

        # 计算总数
        total_unique_nodes = sum(node_type_counts.values())
        total_unique_edges = sum(edge_type_counts.values())
        total_node_occurrences = sum(node_occurrences.values())
        total_edge_occurrences = sum(edge_occurrences.values())

        logger.info(
            f"🔍 聚合分析完成: {total_unique_nodes}个不同节点(出现{total_node_occurrences}次), {total_unique_edges}个不同边(出现{total_edge_occurrences}次)")

        return {
            "unique_nodes": total_unique_nodes,
            "unique_edges": total_unique_edges,
            "total_node_occurrences": total_node_occurrences,
            "total_edge_occurrences": total_edge_occurrences,
            "node_type_counts": node_type_counts,
            "edge_type_counts": edge_type_counts,
            "node_occurrences": node_occurrences,
            "edge_occurrences": edge_occurrences
        }

    def print_graph_summary(self, graph_list: List[Dict], chunk_count: int, data_source: str = ""):
        """打印图数据摘要"""
        logger.info(f"🔍 开始统计图数据，数据源: {data_source}")

        stats = self.analyze_aggregated_graph_data(graph_list)

        logger.info("=" * 50)
        logger.info(f"📊 图谱统计({data_source})")
        logger.info("=" * 50)
        logger.info(
            f"🏷️  节点总数: {stats['unique_nodes']} (出现次数: {stats['total_node_occurrences']})")
        logger.info(
            f"🔗 关系总数: {stats['unique_edges']} (出现次数: {stats['total_edge_occurrences']})")
        logger.info(f"📄 处理块数: {chunk_count}")

        # 显示完整的节点类型统计
        logger.info("📋 完整节点类型统计:")
        for node_type, unique_count in sorted(stats['node_type_counts'].items(), key=lambda x: x[1], reverse=True):
            occurrence_count = stats['node_occurrences'].get(node_type, 0)
            logger.info(
                f"   {node_type}: {unique_count}种 (出现{occurrence_count}次)")

        # 显示完整的关系类型统计
        logger.info("🔗 完整关系类型统计:")
        for edge_type, unique_count in sorted(stats['edge_type_counts'].items(), key=lambda x: x[1], reverse=True):
            occurrence_count = stats['edge_occurrences'].get(edge_type, 0)
            logger.info(
                f"   {edge_type}: {unique_count}种 (出现{occurrence_count}次)")

        # 主要类型显示（前5名）
        top_node_types = sorted(
            stats['node_type_counts'].items(), key=lambda x: x[1], reverse=True)[:5]
        top_edge_types = sorted(
            stats['edge_type_counts'].items(), key=lambda x: x[1], reverse=True)[:5]

        logger.info(
            f"🎯 主要节点类型: {', '.join([f'{k}({v})' for k, v in top_node_types])}")
        logger.info(
            f"🔄 主要关系类型: {', '.join([f'{k}({v})' for k, v in top_edge_types])}")
        logger.info("=" * 50)
