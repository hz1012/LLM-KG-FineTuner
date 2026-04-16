import json
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch

def load_config():
    """
    从config.json加载配置
    """
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

def test_es_connection():
    """
    测试Elasticsearch连接
    """
    config = load_config()
    if not config:
        return False

    # 从graph_enhancer节点获取es_config
    graph_enhancer_config = config.get('graph_enhancer', {})
    es_config = graph_enhancer_config.get('elasticsearch', {})
    
    # 检查是否启用
    if not graph_enhancer_config.get('enable', False):
        print("Elasticsearch功能未启用")
        return False
    
    print(f"使用的ES配置: {es_config}")

    try:
        # 创建Elasticsearch客户端
        es_client = Elasticsearch(
            hosts=es_config.get('hosts', ['http://localhost:9200']),
            basic_auth=es_config.get('auth', None),
            verify_certs=es_config.get('verify_certs', True),
            ca_certs=es_config.get('ca_certs', None),
            request_timeout=30
        )

        # 测试连接
        if es_client.ping():
            print("Elasticsearch连接成功!")
            info = es_client.info()
            print(f"Elasticsearch版本: {info['version']['number']}")
            print(f"集群名称: {info['cluster_name']}")
            return True
        else:
            print("Elasticsearch连接失败!")
            return False

    except Exception as e:
        print(f"Elasticsearch连接出错: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_es_connection()