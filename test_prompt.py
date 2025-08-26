#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知识图谱抽取模块测试文件
用于测试给定文本的实体和关系抽取结果
"""

from langchain.docstore.document import Document
from knowledge_graph_extractor import KnowledgeGraphExtractor
from utils import OpenAIAPIManager, ConfigManager
import os
import json
from typing import Dict, Any, List


def create_test_extractor() -> KnowledgeGraphExtractor:
    """
    创建用于测试的知识图谱抽取器实例

    Returns:
        KnowledgeGraphExtractor: 测试用的抽取器实例
    """
    # 从config.json加载配置
    config = ConfigManager.load_config()

    # 获取知识图谱提取器配置
    kg_config = config.get('knowledge_extractor', {})

    # 获取OpenAI配置
    openai_config = config.get('openai', {})

    # 创建API管理器
    api_manager = OpenAIAPIManager(openai_config)

    # 创建抽取器实例
    extractor = KnowledgeGraphExtractor(
        kg_config=kg_config,
        api_manager=api_manager
    )

    return extractor


def test_kg_extraction(text: str, content_type: str = "text", metadata: Dict = None) -> Dict[str, Any]:
    """
    测试知识图谱抽取功能

    Args:
        text (str): 待抽取的文本内容
        content_type (str): 内容类型，默认为"text"
        metadata (Dict): 文档元数据，默认为None

    Returns:
        Dict[str, Any]: 抽取结果
    """
    if metadata is None:
        metadata = {}

    # 创建测试抽取器
    extractor = create_test_extractor()

    # 创建测试文档
    doc_metadata = {
        "content_type": content_type,
        "chunk_id": metadata.get("chunk_id", "test_chunk_001"),
        "source": "test_prompt"
    }

    # 合并传入的metadata
    doc_metadata.update(metadata)

    doc = Document(page_content=text, metadata=doc_metadata)

    # 执行抽取
    result = extractor._extract_from_single_chunk(doc, 0)

    return result


def test_from_json_file(json_path: str, chunk_ids: List[str] = None) -> List[Dict[str, Any]]:
    """
    从JSON文件中读取内容进行测试

    Args:
        json_path (str): JSON文件路径
        chunk_ids (List[str]): 要测试的chunk_id列表，默认为None

    Returns:
        List[Dict[str, Any]]: 抽取结果列表
    """
    # 读取JSON文件
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    chunks = data.get('selected_chunks', [])

    # 如果指定了chunk_ids，则根据chunk_id查找
    if chunk_ids:
        selected_chunks = []
        for chunk in chunks:
            chunk_id = str(chunk.get('metadata', {}).get('chunk_id', ''))
            if chunk_id in [str(cid) for cid in chunk_ids]:
                selected_chunks.append(chunk)
        chunks = selected_chunks
    else:
        # 如果没有指定chunk_id，则测试所有chunk
        chunk_ids = [str(chunk.get('metadata', {}).get('chunk_id', ''))
                     for chunk in chunks]

    results = []

    # 创建测试抽取器
    extractor = create_test_extractor()

    # 测试指定的chunk
    for chunk in chunks:
        content = chunk.get('content', '')
        metadata = chunk.get('metadata', {})
        chunk_id = metadata.get('chunk_id', 'unknown')

        print(f"\n{'='*50}")
        print(f"测试 Chunk ID: {chunk_id}")
        print(f"{'='*50}")

        # 打印内容预览
        preview_content = content[:500] + \
            "..." if len(content) > 500 else content
        print(f"内容预览:\n{preview_content}\n")

        # 创建文档
        doc = Document(page_content=content, metadata=metadata)

        # 执行抽取
        # 使用chunk_id在原始chunks中的索引作为位置参数
        original_index = next((i for i, c in enumerate(data.get('selected_chunks', []))
                              if str(c.get('metadata', {}).get('chunk_id', '')) == str(chunk_id)), 0)
        result = extractor._extract_from_single_chunk(doc, original_index)
        results.append({
            'chunk_id': chunk_id,
            'result': result
        })

        # 打印结果
        print_result(result)

    return results


def print_result(result: Dict[str, Any]):
    """
    打印抽取结果

    Args:
        result (Dict[str, Any]): 抽取结果
    """
    print(f"\n实体数量: {len(result.get('entities', []))}")
    print("实体列表:")
    for i, entity in enumerate(result.get('entities', []), 1):
        print(f"  {i}. {entity.get('name', 'N/A')} ({entity.get('labels', 'N/A')})")
        print(f"     ID: {entity.get('id', 'N/A')}")
        print(f"     描述: {entity.get('description', 'N/A')}")

    print(f"\n关系数量: {len(result.get('relationships', []))}")
    print("关系列表:")
    for i, relation in enumerate(result.get('relationships', []), 1):
        print(f"  {i}. {relation.get('source', 'N/A')} --{relation.get('type', 'N/A')}--> {relation.get('target', 'N/A')}")
        print(f"     置信度: {relation.get('confidence', 'N/A')}")
        print(f"     证据: {relation.get('evidence', 'N/A')}")


def main():
    """
    主函数 - 提供测试示例
    """
    print("知识图谱抽取模块测试工具")
    print("1. 直接输入文本测试")
    print("2. 从JSON文件测试")

    choice = input("请选择测试方式 (1 或 2): ").strip()

    if choice == "1":
        # 测试示例文本，使用字符串连接避免注释问题
        test_text = (
            "# Behind the Great Wall Void Arachne Targets Chinese\n"
            "## Technical analysis\n"
            "### *Letvpn* MSI analysis\n\n"
            "This section discusses our analysis of the malicious files associated with Void Arachne's campaign, "
            "starting with the *letvpn.msi* file.  \n"
            "The malicious MSI file uses Dynamic Link Libraries (DLLs) during the installation process. "
            "These DLLs play a pivotal role during runtime, facilitating various essential operations including "
            "property management within MSI packages, scheduling tasks, and configuring firewall rules.  \n"
        )

        print("测试知识图谱抽取功能")
        print("测试文本预览:")
        # 显示前300个字符，避免输出过长
        preview_text = test_text[:300] + \
            "..." if len(test_text) > 300 else test_text
        print(preview_text)

        # 执行测试
        result = test_kg_extraction(test_text, "text")

        # 打印结果
        print_result(result)

        # 以JSON格式输出完整结果
        print("\n完整结果 (JSON格式):")
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif choice == "2":
        json_path = input(
            "请输入JSON文件路径 (默认: ./html_output_2/03_selected_chunks.json): ").strip()
        if not json_path:
            json_path = "./html_output_2/03_selected_chunks.json"

        chunk_ids_input = input("请输入要测试的chunk_id，用逗号分隔: ").strip()
        if chunk_ids_input:
            try:
                chunk_ids = [cid.strip() for cid in chunk_ids_input.split(",")]
            except ValueError:
                print("chunk_id格式错误")
                return
        else:
            print("未输入chunk_id")
            return

        # 检查文件是否存在
        if not os.path.exists(json_path):
            print(f"文件不存在: {json_path}")
            return

        # 执行测试
        test_from_json_file(json_path, chunk_ids)

    else:
        print("无效选择")


if __name__ == "__main__":
    main()
