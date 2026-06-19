# coding:utf-8
"""
文件管理模块 - 统一的文件读写操作
"""
import os
import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class FileManager:
    """文件管理器 - 处理文件读写操作"""

    @staticmethod
    def save_text(content: str, file_path: str) -> None:
        """保存文本内容到文件"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.debug(f"✅ 文本已保存: {file_path}")
        except Exception as e:
            logger.error(f"❌ 保存文本失败: {file_path}, 错误: {e}")
            raise

    @staticmethod
    def save_json(data: Any, file_path: str, ensure_ascii: bool = False) -> None:
        """保存JSON数据到文件"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=ensure_ascii, indent=2)
            logger.debug(f"✅ JSON已保存: {file_path}")
        except Exception as e:
            logger.error(f"❌ 保存JSON失败: {file_path}, 错误: {e}")
            raise

    @staticmethod
    def load_json(file_path: str) -> Any:
        """从文件加载JSON数据"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"❌ 加载JSON失败: {file_path}, 错误: {e}")
            raise

    @staticmethod
    def convert_graph_format(graph_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换图数据格式"""
        try:
            # 初始化转换后的数据结构
            converted_data = {
                "nodes": [],
                "links": []
            }

            # 如果传入的是列表，取第一个元素
            if isinstance(graph_data, list) and len(graph_data) > 0:
                graph_data = graph_data[0]

            # 确保graph_data是字典类型
            if not isinstance(graph_data, dict):
                logger.error(f"❌ 图数据格式不正确: 期望dict类型，实际{type(graph_data)}")
                return {"nodes": [], "links": []}

            # 创建节点名称到数字ID的映射
            node_name_to_id = {}
            node_id_counter = 0

            # 处理节点数据
            nodes_data = graph_data.get("nodes", {})
            if isinstance(nodes_data, dict):
                for node_key, count in nodes_data.items():
                    try:
                        # 解析节点JSON字符串
                        node_obj = json.loads(node_key) if isinstance(node_key, str) else node_key

                        # 提取节点属性
                        node_name = node_obj.get("pkey", "")
                        if not node_name:
                            node_name = node_obj.get("name", "")

                        # 如果节点名称还未映射，则分配新的数字ID
                        if node_name not in node_name_to_id:
                            node_name_to_id[node_name] = node_id_counter
                            node_id_counter += 1

                        # 构建新节点
                        new_node = {
                            "id": node_name_to_id[node_name],
                            "name": node_name,
                        }

                        # 保留其他属性
                        if "label" in node_obj:
                            new_node["label"] = node_obj["label"]
                        if "entity_type" in node_obj:
                            new_node["entity_type"] = node_obj["entity_type"]
                        if "image" in node_obj:
                            new_node["image"] = node_obj["image"]

                        converted_data["nodes"].append(new_node)
                    except json.JSONDecodeError:
                        # 如果不是JSON字符串，直接使用
                        if node_key not in node_name_to_id:
                            node_name_to_id[node_key] = node_id_counter
                            node_id_counter += 1

                        new_node = {
                            "id": node_name_to_id[node_key],
                            "name": node_key,
                        }
                        converted_data["nodes"].append(new_node)

            # 处理关系数据
            edges_data = graph_data.get("edges", {})
            if isinstance(edges_data, dict):
                for edge_key, count in edges_data.items():
                    try:
                        # 解析关系JSON字符串
                        edge_obj = json.loads(edge_key) if isinstance(edge_key, str) else edge_key

                        # 提取源节点和目标节点 - 根据用户要求修改映射关系
                        # source_name 对应原始的 pkey
                        # target_name 对应原始的 skey
                        source_name = edge_obj.get("pkey", "")
                        target_name = edge_obj.get("skey", "")

                        # 确保节点存在，如果不存在则创建
                        if source_name not in node_name_to_id:
                            node_name_to_id[source_name] = node_id_counter
                            # 添加缺失的节点
                            converted_data["nodes"].append({
                                "id": node_id_counter,
                                "name": source_name
                            })
                            node_id_counter += 1

                        if target_name not in node_name_to_id:
                            node_name_to_id[target_name] = node_id_counter
                            # 添加缺失的节点
                            converted_data["nodes"].append({
                                "id": node_id_counter,
                                "name": target_name
                            })
                            node_id_counter += 1

                        # 构建新关系 - 确保方向正确
                        new_link = {
                            "source": node_name_to_id[source_name],
                            "target": node_name_to_id[target_name],
                            "relation": edge_obj.get("label", "")
                        }

                        converted_data["links"].append(new_link)
                    except json.JSONDecodeError:
                        # 如果解析失败，跳过该关系
                        continue

            logger.info(f"✅ 图数据格式转换完成: {len(converted_data['nodes'])}个节点, {len(converted_data['links'])}个关系")
            return converted_data

        except Exception as e:
            logger.error(f"❌ 图数据格式转换失败: {e}")
            # 返回空的转换结果而不是抛出异常
            return {"nodes": [], "links": []}
