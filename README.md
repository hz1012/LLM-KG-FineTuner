# LLM-KG-FineTuner

LLM-KG-FineTuner是一个先进的端到端系统，专门用于将非结构化文档（PDF/HTML）转换为结构化知识图谱。该系统结合了大型语言模型的强大能力与知识图谱的结构化优势，能够从复杂的技术文档、威胁情报报告、研究论文等文本资源中自动提取实体、关系和属性，并构建出语义丰富的知识网络。

通过集成文档处理、自然语言处理、知识抽取和图数据库技术，LLM-KG-FineTuner能够帮助企业、研究机构和安全团队快速将海量文档转化为可查询、可分析、可可视化的知识资产，为决策支持、情报分析、语义搜索等应用场景提供强大支撑。

## 核心优势

- **高精度抽取**：基于微调的大型语言模型，实现对复杂文档中实体和关系的高精度识别
- **多格式支持**：全面支持PDF、HTML等多种文档格式的处理
- **自动化流程**：端到端自动化处理，从原始文档到知识图谱一键完成
- **灵活扩展**：模块化设计，可根据具体需求定制和扩展功能
- **安全可靠**：完善的敏感信息处理机制，确保数据安全

## 功能特性

- 文档转换（PDF/HTML转Markdown）
- Markdown后处理
- 文档分割为文本块
- 质量过滤
- 问答对生成
- 知识图谱抽取
- 图数据清洗与格式化
- 图谱增强（Elasticsearch + 嵌入模型）

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置

在运行系统前，需要配置相关参数。请复制示例配置文件并配置其中的敏感信息：

```bash
cp config.example.json config.json
```

然后编辑 `config.json` 文件，填入你的API密钥和其他配置信息。

## Elasticsearch数据处理

项目中的 `es_handle_data` 目录包含用于处理Elasticsearch数据的相关脚本：

1. **TTP数据插入示例**：`es_connector_example.py` 提供了如何将TTP数据插入Elasticsearch的示例代码
2. **数据格式**：需要准备包含 tactics、techniques 和 procedure 三列的CSV文件
3. **使用方法**：
   - 配置好Elasticsearch连接信息
   - 准备好CSV格式的TTP数据文件
   - 运行脚本插入数据

## 知识图谱微调 (fine-tune)

项目中的 `fine_tune_demo` 目录包含用于微调大型语言模型以提高知识图谱提取准确性的相关脚本：

1. **数据准备**：
   - `convert_data.py`：将原始知识图谱数据转换为训练数据
   - 示例数据文件：`raw_graph_example.json` 和 `training_data_example.json`

2. **模型训练**：
   - `fine_tune.py`：执行模型微调的主脚本

3. **模型推理**：
   - `inference.py`：使用微调后的模型进行推理测试

4. **结果评估**：
   - `evaluate_results.py`：评估模型推理结果质量

注意：实际的训练数据和模型输出文件由于包含敏感信息，已被添加到 `.gitignore` 中，不会被提交到版本控制系统。

## 使用方法

```bash
python main.py
```

## 项目结构

- `main.py`: 主程序入口
- `config.json`: 配置文件（需手动创建）
- `document_converter.py`: 文档转换模块
- `markdown_processor.py`: Markdown处理模块
- `chunk_splitter.py`: 文档分割模块
- `quality_filter.py`: 质量过滤模块
- `qa_generator.py`: 问答生成模块
- `knowledge_graph_extractor.py`: 知识图谱抽取模块
- `graph_data_processor.py`: 图数据处理模块
- `graph_enhancer.py`: 图谱增强模块
- `utils.py`: 工具函数模块

## 输出结果

处理后的结果将保存在以下目录：

- `pdf_output/`: PDF文档处理结果
- `html_output/`: HTML文档处理结果
- `html_output_2/`: 另一批HTML文档处理结果

## 注意事项

1. 请确保配置文件中的API密钥正确
2. Elasticsearch服务需要提前配置并运行
3. pdf 解析模型文件需要提前通过download.py下载到 `docling-models/` 目录
