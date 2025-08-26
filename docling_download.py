# 如果不存在docling-models目录，则运行该文件下载·
# 模型下载
from modelscope import snapshot_download
# 使用local_dir下载模型到指定的本地目录中
model_dir = snapshot_download(model_id='ds4sd/docling-models',
                              local_dir='./docling-models')
print(model_dir)
