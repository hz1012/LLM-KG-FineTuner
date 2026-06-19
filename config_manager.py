# coding:utf-8
"""
配置管理模块 - 统一加载、校验和更新 config.json
"""
import json
import logging
from typing import Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigManager:
    """配置管理器"""

    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def load_config(cls, config_path: str = None) -> Dict[str, Any]:
        """
        加载配置文件

        Args:
            config_path: 配置文件路径，如果为None则使用默认路径

        Returns:
            配置字典
        """
        if cls._config is not None:
            return cls._config

        if config_path is None:
            # 获取当前文件所在目录（项目根目录）
            current_dir = Path(__file__).parent
            config_path = current_dir / "config.json"

        config_path = Path(config_path)

        # 如果配置文件不存在，则报错
        if not config_path.exists():
            logger.error(f"配置文件不存在: {config_path}")
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # 验证关键配置
            cls._validate_critical_settings(config)

            cls._config = config

            logger.info(f"✅ 配置文件加载成功: {config_path}")
            return config

        except json.JSONDecodeError as e:
            logger.error(f"❌ 配置文件JSON格式错误: {e}")
            raise e

        except Exception as e:
            logger.error(f"❌ 配置文件加载失败: {e}")
            raise e

    @classmethod
    def _validate_critical_settings(cls, config: Dict[str, Any]):
        """验证关键配置项"""
        # 检查OpenAI API密钥
        openai_config = config.get('openai', {})
        api_key = openai_config.get('api_key', '')

        if not api_key or api_key == "your-api-key-here":
            logger.warning("⚠️ OpenAI API密钥未设置，请在config.json中设置api_key")

        # 检查超时设置
        timeout = openai_config.get('timeout', 90)
        if timeout < 10:
            logger.warning(f"⚠️ API超时设置过低: {timeout}秒，建议至少30秒")

        # 检查chunk大小设置
        chunk_config = config.get('chunk_splitter', {})
        max_chunk_size = chunk_config.get('max_chunk_size', 2000)
        if max_chunk_size < 500:
            logger.warning(f"⚠️ chunk大小设置过小: {max_chunk_size}，可能影响信息提取效果")

        # 检查质量评分设置
        kg_config = config.get('knowledge_extractor', {})

    @classmethod
    def _save_config(cls, config: Dict[str, Any], config_path: Path):
        """保存配置文件"""
        try:
            # 确保目录存在
            config_path.parent.mkdir(parents=True, exist_ok=True)

            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)

            logger.info(f"✅ 配置文件已保存: {config_path}")

        except Exception as e:
            logger.error(f"❌ 配置文件保存失败: {e}")

    @classmethod
    def update_config(cls, key_path: str, value: Any) -> bool:
        """
        更新配置项

        Args:
            key_path: 配置键路径，如 "openai.api_key"
            value: 新值

        Returns:
            是否更新成功
        """
        if cls._config is None:
            cls.load_config()

        try:
            # 分解键路径
            keys = key_path.split('.')
            current = cls._config

            # 导航到父级
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]

            # 设置值
            current[keys[-1]] = value

            # 保存配置
            current_dir = Path(__file__).parent
            config_path = current_dir / "config.json"
            cls._save_config(cls._config, config_path)

            logger.info(f"✅ 配置已更新: {key_path} = {value}")
            return True

        except Exception as e:
            logger.error(f"❌ 配置更新失败: {e}")
            return False

    @classmethod
    def get_config_value(cls, key_path: str, default_value: Any = None) -> Any:
        """
        获取配置值

        Args:
            key_path: 配置键路径，如 "openai.api_key"
            default_value: 默认值

        Returns:
            配置值
        """
        if cls._config is None:
            cls.load_config()

        try:
            keys = key_path.split('.')
            current = cls._config

            for key in keys:
                if key in current:
                    current = current[key]
                else:
                    return default_value

            return current

        except Exception as e:
            logger.error(f"❌ 获取配置值失败: {e}")
            return default_value

    @classmethod
    def reload_config(cls, config_path: str = None) -> Dict[str, Any]:
        """重新加载配置文件"""
        cls._config = None
        return cls.load_config(config_path)

    @classmethod
    def print_config_summary(cls):
        """打印配置摘要"""
        if cls._config is None:
            cls.load_config()

        logger.info("📋 当前配置摘要:")
        logger.info(
            f"   - OpenAI模型: {cls._config.get('openai', {}).get('model', 'N/A')}")
        logger.info(
            f"   - API超时: {cls._config.get('openai', {}).get('timeout', 'N/A')}秒")
        logger.info(
            f"   - 最大重试: {cls._config.get('openai', {}).get('max_retries', 'N/A')}次")
        logger.info(
            f"   - chunk大小: {cls._config.get('chunk_splitter', {}).get('max_chunk_size', 'N/A')}")
        logger.info(
            f"   - 质量阈值: {cls._config.get('quality_filter', {}).get('min_quality_score', 'N/A')}")
        logger.info(
            f"   - 保存中间结果: {cls._config.get('output', {}).get('save_intermediate', 'N/A')}")
