import sys
import os
import json

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import time
import hashlib
from openai import OpenAI

def load_config():
    """加载项目配置"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"配置文件未找到: {config_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"配置文件格式错误: {e}")
        return None

def get_embedding_client():
    """获取嵌入模型客户端"""
    config = load_config()
    if not config:
        return None

    openai_config = config.get('openai', {})
    embedding_model = config.get('graph_enhancer', {}).get('embedding_model', 'text-embedding-v2')

    # 使用配置文件中的 API Key
    client = OpenAI(
        api_key=openai_config.get('api_key'),
        base_url=openai_config.get('base_url')
    )

    return client, embedding_model

def get_embedding(text):
    """获取文本的向量嵌入"""
    client, model = get_embedding_client()
    if not client:
        return None

    try:
        response = client.embeddings.create(
            input=text,
            model=model
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"获取嵌入向量失败: {e}")
        return None

def connect_elasticsearch():
    """连接Elasticsearch"""
    config = load_config()
    if not config:
        return None

    # 从graph_enhancer配置获取ES配置
    graph_enhancer_config = config.get('graph_enhancer', {})
    es_config = graph_enhancer_config.get('elasticsearch', {})

    # 检查是否启用
    if not graph_enhancer_config.get('enable', True):
        print("⏭️  Elasticsearch功能未启用")
        return None

    try:
        es = Elasticsearch(
            hosts=es_config.get('hosts', ['http://localhost:9200']),
            basic_auth=es_config.get('auth', None),
            verify_certs=es_config.get('verify_certs', True),
            ca_certs=es_config.get('ca_certs', None),
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )

        if es.ping():
            print("✅ Elasticsearch 连接成功")
            info = es.info()
            print(f"🔍 集群名称: {info['cluster_name']}, 版本: {info['version']['number']}")
            return es
        else:
            print("❌ Elasticsearch 连接失败")
            return None

    except Exception as e:
        print(f"❌ 连接 Elasticsearch 时发生异常: {str(e)}")
        return None

def delete_index(es, index_name):
    """删除索引"""
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            print(f"✅ 索引 {index_name} 已删除")
            return True
        else:
            print(f"⚠️  索引 {index_name} 不存在")
            return True
    except Exception as e:
        print(f"❌ 删除索引时出错: {e}")
        return False

def create_index(es, index_name):
    """创建索引"""
    try:
        # 定义索引映射
        mapping = {
            "mappings": {
                "properties": {
                    "dense_embedding": {
                        "type": "dense_vector",
                        "dims": 1536,  # 根据实际使用的嵌入模型调整维度
                        "index": True,
                        "similarity": "cosine"
                    },
                    "procedure": {
                        "type": "text"
                    },
                    "tactics": {
                        "type": "keyword"
                    },
                    "techniques": {
                        "type": "keyword"
                    }
                }
            }
        }

        es.indices.create(index=index_name, body=mapping)
        print(f"✅ 索引 {index_name} 创建成功")
        return True
    except Exception as e:
        print(f"❌ 创建索引时出错: {e}")
        return False

def reindex(es, index_name):
    """重新创建索引"""
    print(f"🔄 重新创建索引: {index_name}")

    # 删除现有索引
    if not delete_index(es, index_name):
        return False

    # 创建新索引
    if not create_index(es, index_name):
        return False

    print(f"✅ 索引 {index_name} 重新创建完成")
    return True

def generate_doc_id(tactics, techniques, procedure):
    """基于内容生成唯一文档ID"""
    content_string = f"{tactics}_{techniques}_{procedure}"
    return hashlib.md5(content_string.encode('utf-8')).hexdigest()

def insert_csv_to_es(csv_file_path, index_name="test_ttp_embedding_index", batch_size=50):
    """从CSV文件读取数据并插入到Elasticsearch"""

    # 连接ES
    es = connect_elasticsearch()
    if not es:
        return False

    try:
        # 读取CSV文件
        print(f"🔄 正在读取CSV文件: {csv_file_path}")

        # 直接使用utf-8编码读取文件，避免编码检测问题
        df = pd.read_csv(csv_file_path, encoding='utf-8')

        # 显示CSV文件的列名和前几行数据
        print(f"📊 CSV文件列名: {list(df.columns)}")
        print(f"📊 数据总行数: {len(df)}")
        print("📊 前3行数据预览:")
        print(df.head(3))

        # 确认列名
        if len(df.columns) < 3:
            print("❌ CSV文件列数不足，至少需要3列: tactics, techniques, procedure")
            return False

        tactics_col = df.columns[0]     # 第一列：tactics
        techniques_col = df.columns[1]  # 第二列：techniques
        procedure_col = df.columns[2]   # 第三列：procedure

        # 存储批量插入数据
        bulk_data = []
        success_count = 0
        error_count = 0
        skipped_count = 0  # 新增：记录跳过的重复文档数
        existing_count = 0  # 新增：记录已存在的文档数

        print(f"🔄 开始处理数据并生成向量嵌入...")

        for index, row in df.iterrows():
            try:
                # 获取行数据
                tactics = str(row[tactics_col]) if pd.notna(row[tactics_col]) else ""
                techniques = str(row[techniques_col]) if pd.notna(row[techniques_col]) else ""
                procedure = str(row[procedure_col]) if pd.notna(row[procedure_col]) else ""

                # 跳过空的procedure
                if not procedure.strip():
                    print(f"⚠️  跳过第{index+1}行，procedure为空")
                    skipped_count += 1
                    continue

                # 生成唯一ID
                doc_id = generate_doc_id(tactics, techniques, procedure)

                # 检查文档是否已存在，避免重复插入
                try:
                    if es.exists(index=index_name, id=doc_id):
                        print(f"⏭️  第{index+1}行已存在，跳过插入: {procedure[:50]}...")
                        existing_count += 1
                        skipped_count += 1
                        continue
                except Exception as exist_error:
                    print(f"⚠️  检查文档存在性时出错: {exist_error}")

                print(f"🔄 处理第{index+1}行: {procedure[:50]}...")

                # 获取向量嵌入
                embedding = get_embedding(procedure)
                if not embedding:
                    print(f"❌ 获取第{index+1}行的嵌入向量失败，跳过该行")
                    error_count += 1
                    continue

                # 构造插入数据
                doc = {
                    "_op_type": "index",
                    "_index": index_name,
                    "_id": doc_id,
                    "_source": {
                        "dense_embedding": embedding,
                        "procedure": procedure,
                        "tactics": tactics,
                        "techniques": techniques
                    }
                }

                bulk_data.append(doc)

                # 批量插入
                if len(bulk_data) >= batch_size:
                    try:
                        success, failed = bulk(es, bulk_data)
                        success_count += success
                        error_count += len(failed) if failed else 0
                        print(f"📝 批量插入完成：成功 {success} 条")
                        if failed:
                            print(f"❌ 失败 {len(failed)} 条")
                    except Exception as bulk_error:
                        print(f"❌ 批量插入失败: {bulk_error}")
                        error_count += len(bulk_data)

                    bulk_data = []  # 清空缓存

                # 添加延时避免API限制
                time.sleep(1)

            except Exception as row_error:
                print(f"❌ 处理第{index+1}行时出错: {row_error}")
                error_count += 1
                continue

        # 插入剩余数据
        if bulk_data:
            try:
                success, failed = bulk(es, bulk_data)
                success_count += success
                error_count += len(failed) if failed else 0
                print(f"📝 最后批次插入完成：成功 {success} 条")
                if failed:
                    print(f"❌ 失败 {len(failed)} 条")
            except Exception as bulk_error:
                print(f"❌ 最后批次插入失败: {bulk_error}")
                error_count += len(bulk_data)

        # 显示最终统计信息
        total_processed = success_count + existing_count + error_count
        print(f"🎉 数据处理完成！")
        print(f"📈 总处理行数: {len(df)}")
        print(f"✅ 成功插入: {success_count} 条")
        print(f"⏭️  已存在跳过: {existing_count} 条")
        print(f"❌ 处理失败: {error_count} 条")
        print(f"⚠️  其他跳过: {skipped_count - existing_count} 条")  # 减去已存在的情况

        # 查询并显示索引中的文档总数
        try:
            doc_count = es.count(index=index_name)['count']
            print(f"📂 索引 {index_name} 中当前文档总数: {doc_count}")
        except Exception as count_error:
            print(f"⚠️  获取索引文档数时出错: {count_error}")

        return True

    except FileNotFoundError:
        print(f"❌ 文件未找到: {csv_file_path}")
    except UnicodeDecodeError as e:
        print(f"❌ 文件编码错误: {e}")
        print("💡 提示: 尝试使用不同的编码方式，如 'utf-8', 'gbk', 'latin1' 等")
    except Exception as e:
        print(f"❌ 读取CSV文件时出错: {e}")
    return False

def main():
    print("🚀 开始从CSV文件插入TTP数据到Elasticsearch...")

    # CSV文件路径
    csv_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "attck_procedures", "test_ttp_v1.csv")

    # 从配置获取索引名称
    config = load_config()
    index_name = "test_ttp_embedding_index"
    if config:
        index_name = config.get('graph_enhancer', {}).get('elasticsearch', {}).get('index_name', index_name)

    # 插入数据
    insert_csv_to_es(csv_file_path, index_name, 50)

if __name__ == "__main__":
    main()