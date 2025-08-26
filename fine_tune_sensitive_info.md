# fine_tune_demo目录文件处理建议

## 敏感信息分析

在`fine_tune_demo`目录中发现以下包含敏感信息的文件：

### 需要删除的文件

1. **训练数据文件**：
   - [fine_tune_input/raw_graph_chinese.json](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_input/raw_graph_chinese.json) - 包含中文威胁情报原始数据
   - [fine_tune_input/raw_graph_english.json](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_input/raw_graph_english.json) - 包含英文威胁情报原始数据
   - [fine_tune_input/training_data.json](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_input/training_data.json) - 由原始数据转换而来的训练数据

2. **模型输出文件**：
   - [fine_tune_output/](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_output/) 目录下的所有文件 - 包含训练过程中的模型检查点和最终模型
   - [fine_tune_output/checkpoint-100/](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_output/checkpoint-100/) - 训练过程中的检查点
   - [fine_tune_output/checkpoint-50/](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_output/checkpoint-50/) - 训练过程中的检查点
   - [fine_tune_output/final_model/](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_output/final_model/) - 最终训练好的模型
   - [fine_tune_output/training_samples.json](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_output/training_samples.json) - 训练样本数据
   - [fine_tune_output/evaluation_results.json](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_output/evaluation_results.json) - 评估结果数据
   - [fine_tune_output/automatic_evaluation_report.txt](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune_output/automatic_evaluation_report.txt) - 自动生成的评估报告

### 可以保留的文件

1. **代码逻辑文件**：
   - [README.md](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/README.md) - 项目说明文档
   - [convert_data.py](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/convert_data.py) - 数据转换脚本
   - [evaluate_results.py](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/evaluate_results.py) - 结果评估脚本
   - [fine_tune.py](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/fine_tune.py) - 微调训练脚本
   - [inference.py](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/inference.py) - 推理测试脚本
   - [test.py](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/test.py) - 模型测试脚本
   - [requirements.txt](file:///Users/huzhe/Documents/python/RAG/LLM-KG-FineTuner-public/fine_tune_demo/requirements.txt) - 依赖包列表

## 建议处理方式

1. **删除敏感数据文件**：
   - 删除所有原始数据文件（raw_graph_*.json）
   - 删除训练数据文件（training_data.json）
   - 删除模型输出文件和目录

2. **创建示例数据文件**：
   - 创建示例的原始数据文件（raw_graph_example.json）
   - 创建示例的训练数据文件（training_data_example.json）

3. **更新README或文档**：
   - 添加说明文档，指导用户如何准备自己的训练数据
   - 提供示例数据格式说明

4. **更新.gitignore文件**：
   - 添加规则忽略敏感数据文件和模型输出文件