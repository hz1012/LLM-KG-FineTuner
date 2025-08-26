from modelscope import AutoModelForCausalLM, AutoTokenizer

try:
    # 尝试加载模型和tokenizer
    model_name = "qwen/Qwen2.5-7B-Instruct"
    print("正在尝试加载模型...")

    tokenizer = AutoTokenizer.from_pretrained(
        model_name, trust_remote_code=True)
    print("Tokenizer加载成功")

    model = AutoModelForCausalLM.from_pretrained(
        model_name, trust_remote_code=True)
    print("模型加载成功")

except Exception as e:
    print(f"加载失败: {e}")
