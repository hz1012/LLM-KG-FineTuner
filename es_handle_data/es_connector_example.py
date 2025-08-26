from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import json
from openai import OpenAI
import time
import hashlib

# 示例OpenAI客户端配置
client = OpenAI(
    api_key="your_openai_api_key_here",  # 替换为你的实际API密钥
    base_url="https://api.openai.com/v1"  # 或其他兼容的API端点
)


def get_embedding(input_str):
    """获取文本的向量嵌入"""
    response = client.embeddings.create(
        input=input_str,
        model="text-embedding-3-small"  # 或其他嵌入模型
    )
    return response.data[0].embedding


def connect_elasticsearch():
    """连接Elasticsearch"""
    try:
        es = Elasticsearch(
            hosts=["https://localhost:9200"],  # 替换为你的Elasticsearch主机地址
            basic_auth=('elastic', 'your_elastic_password'),  # 替换为你的实际用户名和密码
            verify_certs=False,  # ⚠️ 测试环境可设为 False；生产环境应设为 True 并提供 CA 证书
            ssl_show_warn=False,
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True,
            ssl_assert_hostname=False,
            ssl_assert_fingerprint=False,
            connections_per_node=10,
        )

        if es.ping():
            print("✅ Elasticsearch 连接成功")
            info = es.info()
            print(
                f"🔍 集群名称: {info['cluster_name']}, 版本: {info['version']['number']}")
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
            print(f"⚠️ 索引 {index_name} 不存在")
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
                        "dims": 1536,
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


def insert_csv_to_es(csv_file_path, index_name="example_ttp_embedding_index", batch_size=100):
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

        # 假设CSV列结构为：tactics(列0), techniques(列1), procedure(列2)
        # 根据实际CSV文件结构调整列索引
        tactics_col = df.columns[0]     # 第一列：tactics
        techniques_col = df.columns[1]  # 第二列：techniques
        procedure_col = df.columns[2]   # 第三列：procedure

        # 存储批量插入数据
        bulk_data = []
        success_count = 0
        error_count = 0

        print(f"🔄 开始处理数据并生成向量嵌入...")

        for index, row in df.iterrows():
            try:
                # 获取行数据
                tactics = str(row[tactics_col]) if pd.notna(
                    row[tactics_col]) else ""
                techniques = str(row[techniques_col]) if pd.notna(
                    row[techniques_col]) else ""
                procedure = str(row[procedure_col]) if pd.notna(
                    row[procedure_col]) else ""

                # 跳过空的procedure
                if not procedure.strip():
                    print(f"⚠️  跳过第{index+1}行，procedure为空")
                    continue

                # 生成唯一ID
                doc_id = generate_doc_id(tactics, techniques, procedure)
                print(f"🔄 处理第{index+1}行: {procedure[:100]}...")

                # 获取向量嵌入
                embedding = get_embedding(procedure)

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

        print(f"🎉 数据插入完成！")
        print(f"✅ 成功插入: {success_count} 条")
        print(f"❌ 失败: {error_count} 条")
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
    print("🚀 开始从CSV文件插入数据到Elasticsearch...")

    # 插入数据
    insert_csv_to_es("./example_ttp_data.csv", "example_ttp_embedding_index", 50)


if __name__ == "__main__":
    main()