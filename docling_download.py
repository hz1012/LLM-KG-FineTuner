#!/usr/bin/env python3
"""
Docling 模型下载脚本
用于下载 PDF 解析所需的 Docling 模型（约 1-2GB）

使用方法：
    python docling_download.py

说明：
    - 模型将下载到 ./docling-models 目录
    - 如果目录已存在，会跳过下载
    - 使用 ModelScope 下载（国内更快）
"""

import os
from pathlib import Path

def check_model_exists():
    """检查模型是否已经下载"""
    model_dir = Path("./docling-models")
    if model_dir.exists() and any(model_dir.iterdir()):
        print(f"✅ 模型目录已存在: {model_dir}")
        print("   如果需要重新下载，请先删除该目录")
        return True
    return False

def download_model():
    """下载 Docling 模型"""
    try:
        from modelscope import snapshot_download

        print("🚀 开始下载 Docling 模型...")
        print("   模型大小：约 1-2GB")
        print("   下载源：ModelScope（国内镜像）")
        print()

        # 使用local_dir下载模型到指定的本地目录中
        model_dir = snapshot_download(
            model_id='ds4sd/docling-models',
            local_dir='./docling-models'
        )

        print()
        print(f"✅ 模型已成功下载到: {model_dir}")
        print("   你现在可以处理 PDF 文件了！")

    except ImportError as e:
        print(f"❌ 无法导入 modelscope 库: {e}")
        print()
        print("请安装 ModelScope：")
        print("   pip install modelscope")

    except Exception as e:
        print(f"❌ 下载模型时出错: {e}")
        print()
        print("可能的解决方案：")
        print("1. 检查网络连接")
        print("2. 安装 CPU 版本的 PyTorch：")
        print("   pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu")
        print("3. 确保已安装 modelscope：")
        print("   pip install modelscope")

if __name__ == "__main__":
    print("="*60)
    print("📦 Docling 模型下载工具")
    print("="*60)
    print()

    # 检查模型是否已存在
    if check_model_exists():
        exit(0)

    # 下载模型
    download_model()

    print()
    print("="*60)