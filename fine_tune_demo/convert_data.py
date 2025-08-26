import json
import os
from pathlib import Path

def convert_raw_graph_to_training_data(input_file: str, output_file: str, append_mode: bool = True):
    """
    将raw_graph.json转换为微调训练数据
    
    Args:
        input_file: raw_graph.json文件路径
        output_file: 输出的训练数据文件路径
        append_mode: 是否追加模式，默认为False
    """
    # 读取原始知识图谱数据
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    entities = data.get('entities', [])
    relationships = data.get('relationships', [])
    
    # 创建训练样本列表
    training_samples = []
    
    # 处理实体
    chunk_entity_map = {}  # 用于按chunk_index分组实体
    
    for entity in entities:
        # 获取实体的chunk信息
        chunks_info = entity.get('chunks_info', [])
        for chunk_info in chunks_info:
            chunk_index = chunk_info.get('chunk_index')
            chunk_content = chunk_info.get('chunk_content', '')
            
            if chunk_index not in chunk_entity_map:
                chunk_entity_map[chunk_index] = {
                    'content': chunk_content,
                    'entities': [],
                    'relationships': []
                }
            
            # 添加实体到对应chunk
            chunk_entity_map[chunk_index]['entities'].append({
                'labels': entity.get('labels', ''),
                'id': entity.get('id', ''),
                'name': entity.get('name', ''),
                'description': entity.get('description', '')
            })
    
    # 处理关系
    for relationship in relationships:
        # 获取关系的chunk信息
        chunks_info = relationship.get('chunks_info', [])
        for chunk_info in chunks_info:
            chunk_index = chunk_info.get('chunk_index')
            chunk_content = chunk_info.get('chunk_content', '')
            
            if chunk_index not in chunk_entity_map:
                chunk_entity_map[chunk_index] = {
                    'content': chunk_content,
                    'entities': [],
                    'relationships': []
                }
            elif not chunk_entity_map[chunk_index]['content'] and chunk_content:
                # 如果之前没有设置内容或内容为空，则设置
                chunk_entity_map[chunk_index]['content'] = chunk_content
                
            # 添加关系到对应chunk
            chunk_entity_map[chunk_index]['relationships'].append({
                'type': relationship.get('type', ''),
                'source': relationship.get('source', ''),
                'target': relationship.get('target', ''),
                'confidence': relationship.get('confidence', 0.0),
                'evidence': relationship.get('evidence', '')
            })
    
    # 构建训练样本
    for chunk_index, chunk_data in sorted(chunk_entity_map.items()):
        if not chunk_data['content']:
            continue
            
        # 构建输出JSON
        output_json = {
            'entities': chunk_data['entities'],
            'relationships': chunk_data['relationships']
        }
        
        # 创建训练样本
        training_sample = {
            'input': chunk_data['content'],
            'output': json.dumps(output_json, ensure_ascii=False)
        }
        
        training_samples.append(training_sample)
    
    # 保存训练数据
    mode = 'a' if append_mode and os.path.exists(output_file) else 'w'
    if append_mode and os.path.exists(output_file):
        # 如果是追加模式且文件存在，需要特殊处理JSON格式
        if len(training_samples) > 0:
            # 读取现有文件内容
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            
            # 合并数据
            existing_data.extend(training_samples)
            
            # 写回文件
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
    else:
        # 直接写入文件
        with open(output_file, mode, encoding='utf-8') as f:
            json.dump(training_samples, f, ensure_ascii=False, indent=2)
    
    print(f"转换完成！共生成 {len(training_samples)} 个训练样本")
    print(f"训练数据已保存到: {output_file}")
    print(f"处理的chunk索引: {sorted(chunk_entity_map.keys())}")

def main():
    # 创建输出目录
    output_dir = Path("./fine_tune_input")
    output_dir.mkdir(exist_ok=True)
    
    # 转换数据
    input_files = ["./fine_tune_input/raw_graph_chinese.json", "./fine_tune_input/raw_graph_english.json"]  # 可以添加更多输入文件
    output_file = "./fine_tune_input/training_data.json"
    
    # 第一个文件使用写入模式，后续文件使用追加模式
    for i, input_file in enumerate(input_files):
        append_mode = i > 0  # 第一个文件不追加，后续文件追加
        convert_raw_graph_to_training_data(input_file, output_file, append_mode)

if __name__ == "__main__":
    main()