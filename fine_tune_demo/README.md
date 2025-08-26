# Fine-tune Demo 项目说明

Fine-tune Demo 是一个专门用于微调大型语言模型以提升知识图谱提取准确性的项目。该项目通过优化模型对JSON格式输出的理解能力，显著提高从威胁情报文本中提取实体和关系的准确性。

## 1. Fine-tune 流程

本项目的微调流程包括以下几个关键步骤：

### 1.1 数据准备

- 从 `fine_tune_input/training_data.json` 文件中读取训练样本
- 对训练数据进行增强处理，包括：
  - 同义词替换：增加模型对不同表达方式的鲁棒性
  - 句子顺序调整：提高模型对文本顺序变化的适应性
  - 添加前缀/后缀扰动：增强模型对上下文变化的处理能力
  - 添加空结果样本：帮助模型识别无相关信息的文本

### 1.2 模型设置

- 使用 Qwen2.5-7B-Instruct 作为基础模型
- 应用 LoRA (Low-Rank Adaptation) 技术进行参数高效微调
- 配置量化和内存优化以适应有限的GPU资源

### 1.3 模型训练

- 使用 SFTTrainer (Supervised Fine-tuning Trainer) 进行监督式微调
- 采用指令微调方式，将输入文本和系统提示封装为完整的对话格式
- 通过梯度累积等技术优化训练过程

### 1.4 效果评估

- 对比微调前后的JSON解析成功率
- 使用测试集验证模型在实际应用场景中的表现
- 生成详细的评估报告

## 2. 核心参数说明与修改建议

### 2.1 模型配置参数

| 参数 | 默认值 | 说明 | 修改建议 |
|------|--------|------|----------|
| `model_id` | "qwen/Qwen2.5-7B-Instruct" | 基础模型路径 | 根据实际部署环境调整 |
| `max_seq_length` | 2048 | 最大序列长度 | 根据GPU显存调整，A10(24GB)可保持默认 |
| `load_in_4bit` | True | 是否使用4bit量化 | A10资源充足可考虑关闭以提高精度 |

### 2.2 LoRA配置参数

| 参数 | 默认值 | 说明 | 修改建议 |
|------|--------|------|----------|
| `lora_r` | 4 | LoRA秩(rank) | A10资源下建议保持4-8，资源更充裕可增至16-32 |
| `lora_alpha` | 4 | LoRA alpha值 | 通常设为lora_r的1-2倍 |
| `lora_dropout` | 0.1 | LoRA dropout率 | 保持默认或根据过拟合情况调整 |
| `target_modules` | ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"] | 应用LoRA的目标模块 | 保持默认以获得最佳效果 |

### 2.3 训练配置参数

| 参数 | 默认值 | 说明 | 修改建议 |
|------|--------|------|----------|
| `per_device_train_batch_size` | 1 | 每设备训练批次大小 | A10上保持1，资源更充裕可增至2-4 |
| `gradient_accumulation_steps` | 32 | 梯度累积步数 | 与batch_size成反比调整，保持总batch size |
| `max_steps` | 100 | 最大训练步数 | 根据数据集大小和效果调整，通常50-200 |
| `learning_rate` | 2e-4 | 学习率 | LoRA微调推荐值，可微调至1e-4~5e-4 |
| `fp16/bf16` | 根据硬件自动选择 | 混合精度训练 | A10支持bf16，保持自动选择 |

### 2.4 资源调整建议

#### 资源更多时（如32GB+显存）

- 增加 `per_device_train_batch_size` 至 2-4
- 减少 `gradient_accumulation_steps` 至 16-8
- 增加 `lora_r` 至 8-16
- 增加 `max_seq_length` 至 4096
- 考虑关闭 `load_in_4bit` 量化

#### 资源更少时（如16GB显存）

- 保持 `per_device_train_batch_size` 为 1
- 增加 `gradient_accumulation_steps` 至 64+
- 减少 `lora_r` 至 2-4
- 减少 `max_seq_length` 至 1024-1536
- 确保开启 `load_in_4bit` 量化

## 3. 集成到当前Pipeline的建议

### 3.1 模型部署方式

建议将微调后的模型封装为独立的API服务，理由如下：

1. **模块化设计**：保持训练和推理环境分离，便于维护和扩展
2. **资源优化**：微调模型通常需要更高规格的GPU，而文档处理可以使用较低配置
3. **灵活性**：可以独立扩展推理服务，不影响其他模块
4. **解耦合**：上层代码无需大幅修改即可使用新模型

### 3.2 集成步骤

1. **模型服务化**：
   - 将训练好的模型部署为RESTful API服务
   - 推荐使用FastAPI或Flask框架
   - 服务应接收文本输入并返回结构化JSON输出

2. **配置更新**：
   - 修改主配置文件，添加微调模型API的访问地址
   - 配置重试机制和超时设置

3. **代码修改**：
   - 在 `src/llm/openai_api_manager.py` 中添加对微调模型API的支持
   - 更新 `KnowledgeGraphExtractor` 类以使用新模型

### 3.3 接口设计建议

微调模型API应提供以下核心接口：

1. **知识图谱提取接口** `/api/kg-extract`
   - **方法**: POST
   - **请求体**:

     ```json
     {
       "text": "输入的文本内容"
     }
     ```

   - **响应体**:

     ```json
     {
       "entities": [
         {
           "labels": "实体类型",
           "id": "实体唯一标识符",
           "name": "实体名称",
           "description": "实体描述"
         }
       ],
       "relationships": [
         {
           "type": "关系类型",
           "source": "源实体ID",
           "target": "目标实体ID",
           "confidence": "置信度",
           "evidence": "证据"
         }
       ]
     }
     ```

   - **错误响应**:

     ```json
     {
       "error": "错误信息"
     }
     ```

2. **健康检查接口** `/api/health`
   - **方法**: GET
   - **响应**:

     ```json
     {
       "status": "healthy"
     }
     ```

这些接口设计简洁明了，能够满足知识图谱提取的核心需求，同时保持与现有系统的兼容性。

## 4. 项目脱敏说明

为了保护敏感数据和知识产权，本项目在开源时已对以下内容进行了脱敏处理：

1. **训练数据**：
   - 删除了原始的中文和英文威胁情报数据文件
   - 删除了从原始数据生成的训练数据文件
   - 提供了示例数据文件用于参考和测试

2. **模型输出**：
   - 删除了训练过程中生成的所有模型检查点
   - 删除了最终训练完成的模型文件
   - 删除了训练样本和评估结果文件

3. **使用方法**：
   - 用户需要准备自己的训练数据，格式可参考示例文件
   - 用户需要从头开始训练模型
   - 用户可以基于自己的数据和需求调整训练参数

### 4.1 准备训练数据

要使用此项目进行模型微调，您需要准备自己的训练数据：

1. 准备原始知识图谱数据：
   - 创建 `fine_tune_input/raw_graph_custom.json` 文件
   - 文件格式可参考 `fine_tune_input/raw_graph_example.json`
   - 确保包含实体(entities)和关系(relationships)信息

2. 转换数据格式：
   - 运行 `python convert_data.py` 脚本
   - 脚本会将原始数据转换为训练所需的格式
   - 输出文件为 `fine_tune_input/training_data.json`

### 4.2 执行模型训练

准备好训练数据后，可以执行模型训练：

```bash
python fine_tune.py
```

训练过程中会生成以下内容：
- 模型检查点(checkpoints)
- 最终模型(final_model)
- 训练样本(training_samples.json)
- 评估结果(evaluation_results.json)

### 4.3 部署微调模型

训练完成后，可以将模型部署为API服务：

1. 使用 `inference.py` 中的代码作为基础创建API服务
2. 将训练好的模型文件部署到服务器
3. 配置适当的API接口供其他模块调用

### 4.4 集成到主流程

要将微调模型集成到主流程中：

1. 修改主项目的配置文件，添加微调模型API的访问地址
2. 更新 `KnowledgeGraphExtractor` 类以使用新模型API
3. 根据需要调整推理参数和超时设置