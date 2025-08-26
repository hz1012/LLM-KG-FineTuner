# coding:utf-8
import json
from typing import Dict, Any, List


def extract_apt41_subgraph(json_file_path: str = "html_output/06_knowledge_graph_simple.json") -> List[Dict[str, Any]]:
    """
    从06_knowledge_graph_simple.json中提取与threatactor--apt41相关的多跳子图

    Args:
        json_file_path: 06_knowledge_graph_simple.json文件路径

    Returns:
        与原始结构相同的数据，但只包含与APT41相关的节点和边（包括多跳关系）
    """

    # 加载原始数据
    with open(json_file_path, 'r', encoding='utf-8') as f:
        original_data = json.load(f)

    # 检查数据结构
    if not isinstance(original_data, list) or len(original_data) == 0:
        print("❌ 数据格式错误或为空")
        return []

    graph_data = original_data[0]
    all_nodes = graph_data.get("nodes", {})
    all_edges = graph_data.get("edges", {})

    print(f"📊 原始数据统计:")
    print(f"   总节点数: {len(all_nodes)}")
    print(f"   总边数: {len(all_edges)}")

    # 🔥 新增：构建边的索引结构，方便快速查找
    # pkey -> [(edge_key, edge_info, skey)]
    outgoing_edges = {}
    for edge_key, count in all_edges.items():
        try:
            edge_info = json.loads(edge_key)
            pkey = edge_info.get("pkey", "")
            skey = edge_info.get("skey", "")

            if pkey not in outgoing_edges:
                outgoing_edges[pkey] = []
            outgoing_edges[pkey].append((edge_key, edge_info, skey, count))
        except json.JSONDecodeError:
            continue

    print(f"🔗 构建边索引完成，共有{len(outgoing_edges)}个节点有出边")

    # 🔥 新增：多跳遍历逻辑
    visited_nodes = set()  # 已访问的节点pkey
    visited_edges = set()  # 已访问的边key
    to_visit = []  # 待访问的节点队列

    # 步骤1: 找到APT41起始节点
    apt41_start_nodes = []
    for node_key, count in all_nodes.items():
        try:
            node_info = json.loads(node_key)
            pkey = node_info.get("pkey", "")

            if "threatactor--apt41" in pkey:
                apt41_start_nodes.append(pkey)
                visited_nodes.add(pkey)
                to_visit.append(pkey)
                print(
                    f"🎯 找到APT41起始节点: {node_info.get('label', 'Unknown')} (pkey: {pkey})")
        except json.JSONDecodeError:
            continue

    if not apt41_start_nodes:
        print("❌ 未找到APT41相关节点")
        return []

    # 步骤2: 广度优先搜索(BFS)遍历多跳关系
    hop_count = 0
    while to_visit:
        hop_count += 1
        print(f"\n🔍 第{hop_count}跳遍历，待处理节点: {len(to_visit)}个")

        current_level = to_visit.copy()
        to_visit.clear()

        new_nodes_this_hop = 0
        new_edges_this_hop = 0

        for current_pkey in current_level:
            # 查找当前节点的所有出边
            if current_pkey in outgoing_edges:
                for edge_key, edge_info, skey, count in outgoing_edges[current_pkey]:
                    # 添加边
                    if edge_key not in visited_edges:
                        visited_edges.add(edge_key)
                        new_edges_this_hop += 1
                        print(
                            f"   📎 添加边: {edge_info.get('label', 'Unknown')} ({current_pkey} -> {skey})")

                    # 添加目标节点
                    if skey not in visited_nodes:
                        visited_nodes.add(skey)
                        to_visit.append(skey)
                        new_nodes_this_hop += 1

                        # 找到目标节点的名称
                        target_name = "Unknown"
                        for node_key, node_count in all_nodes.items():
                            try:
                                node_info = json.loads(node_key)
                                if node_info.get("pkey") == skey:
                                    target_name = node_info.get(
                                        "label", "Unknown")
                                    break
                            except json.JSONDecodeError:
                                continue

                        print(f"   ➕ 添加节点: {target_name} (pkey: {skey})")

        print(
            f"   📊 第{hop_count}跳统计: 新增{new_nodes_this_hop}个节点, {new_edges_this_hop}条边")

        # 防止无限循环
        if hop_count > 10:
            print("⚠️ 达到最大跳数限制(10跳)，停止遍历")
            break

    print(f"\n✅ 多跳遍历完成，共进行{hop_count}跳")
    print(f"   最终访问节点数: {len(visited_nodes)}")
    print(f"   最终访问边数: {len(visited_edges)}")

    # 步骤3: 收集最终的节点和边数据
    final_nodes = {}
    final_edges = {}

    # 收集访问过的节点
    for node_key, count in all_nodes.items():
        try:
            node_info = json.loads(node_key)
            pkey = node_info.get("pkey", "")

            if pkey in visited_nodes:
                final_nodes[node_key] = count
        except json.JSONDecodeError:
            continue

    # 收集访问过的边
    for edge_key, count in all_edges.items():
        if edge_key in visited_edges:
            final_edges[edge_key] = count

    # 构建返回结果
    result = [{
        "nodes": final_nodes,
        "edges": final_edges
    }]

    print(f"\n🎉 APT41多跳子图提取完成:")
    print(f"   节点数: {len(final_nodes)}")
    print(f"   边数: {len(final_edges)}")
    print(f"   跳数: {hop_count}")

    return result


def print_apt41_subgraph_summary(subgraph_data: List[Dict[str, Any]]):
    """
    打印APT41子图的摘要信息（增强版，显示层级结构）
    """
    if not subgraph_data:
        print("❌ 子图数据为空")
        return

    graph = subgraph_data[0]
    nodes = graph.get("nodes", {})
    edges = graph.get("edges", {})

    print(f"\n📈 APT41多跳子图详细信息:")
    print(f"=" * 60)

    # 🔥 新增：按跳数分层显示
    # 重建图结构
    node_pkey_to_info = {}
    for node_key, count in nodes.items():
        try:
            node_info = json.loads(node_key)
            pkey = node_info.get("pkey", "")
            node_pkey_to_info[pkey] = {
                'label': node_info.get('label', 'Unknown'),
                'entity_type': node_info.get('entity_type', 'unknown'),
                'count': count
            }
        except json.JSONDecodeError:
            continue

    # 构建边结构
    adjacency_list = {}
    for edge_key, count in edges.items():
        try:
            edge_info = json.loads(edge_key)
            pkey = edge_info.get("pkey", "")
            skey = edge_info.get("skey", "")
            label = edge_info.get("label", "unknown")

            if pkey not in adjacency_list:
                adjacency_list[pkey] = []
            adjacency_list[pkey].append((skey, label, count))
        except json.JSONDecodeError:
            continue

    # 🔥 新增：按层级显示连接路径
    print(f"🌳 层级结构展示:")

    # 找到APT41起始节点
    apt41_nodes = [pkey for pkey in node_pkey_to_info.keys()
                   if "threatactor--apt41" in pkey]

    def print_level(current_nodes, visited, level=0, max_level=5):
        if level > max_level or not current_nodes:
            return

        indent = "  " * level
        level_symbol = "🎯" if level == 0 else "📍"

        print(f"{indent}{level_symbol} 第{level}层:")

        next_level_nodes = set()
        for pkey in current_nodes:
            if pkey in visited:
                continue
            visited.add(pkey)

            node_info = node_pkey_to_info.get(pkey, {})
            label = node_info.get('label', 'Unknown')
            entity_type = node_info.get('entity_type', 'unknown')
            count = node_info.get('count', 0)

            print(f"{indent}  - {label} ({entity_type}) [出现{count}次]")

            # 显示出边
            if pkey in adjacency_list:
                for target_pkey, edge_label, edge_count in adjacency_list[pkey]:
                    target_info = node_pkey_to_info.get(target_pkey, {})
                    target_label = target_info.get('label', 'Unknown')
                    print(f"{indent}    └─ {edge_label} → {target_label}")
                    next_level_nodes.add(target_pkey)

        # 递归处理下一层
        if next_level_nodes:
            print_level(next_level_nodes, visited, level + 1, max_level)

    visited_in_tree = set()
    print_level(apt41_nodes, visited_in_tree)

    # 原有的统计信息
    print(f"\n📊 统计信息:")

    # 按实体类型分组
    node_types = {}
    for pkey, info in node_pkey_to_info.items():
        entity_type = info['entity_type']
        if entity_type not in node_types:
            node_types[entity_type] = []
        node_types[entity_type].append((info['label'], info['count']))

    print(f"🎯 节点统计 (按类型):")
    for entity_type, entities in node_types.items():
        print(f"   {entity_type}: {len(entities)}个")
        for label, count in entities:
            print(f"      - {label} (出现{count}次)")

    # 按关系类型分组
    edge_types = {}
    for edge_key, count in edges.items():
        try:
            edge_info = json.loads(edge_key)
            label = edge_info.get("label", "unknown")

            if label not in edge_types:
                edge_types[label] = 0
            edge_types[label] += count
        except json.JSONDecodeError:
            continue

    print(f"\n🔗 边统计 (按关系类型):")
    for rel_type, total_count in edge_types.items():
        print(f"   {rel_type}: {total_count}次")


def save_apt41_subgraph(subgraph_data: List[Dict[str, Any]], output_path: str = "html_output/apt41_subgraph.json"):
    """
    保存APT41子图到文件
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(subgraph_data, f, ensure_ascii=False, indent=2)

    print(f"💾 APT41子图已保存到: {output_path}")


if __name__ == "__main__":
    # 测试提取功能
    print("🚀 开始提取APT41子图...")

    # 提取APT41相关的节点和边
    apt41_subgraph = extract_apt41_subgraph()

    # 打印摘要信息
    print_apt41_subgraph_summary(apt41_subgraph)

    # 保存到文件
    save_apt41_subgraph(apt41_subgraph)

    print("\n🎉 APT41子图提取完成!")
