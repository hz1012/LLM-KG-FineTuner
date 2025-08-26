import json
import logging
import torch
from pathlib import Path
from peft import PeftModel
import os
from modelscope import AutoModelForCausalLM, AutoTokenizer

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InferenceDemo:
    """简化版推理演示"""

    def __init__(self, base_model_path=None, lora_model_path=None):
        """初始化推理器"""
        self.model = None
        self.tokenizer = None
        # 替换成自己的基础模型路径
        self.base_model_path = base_model_path or "/root/.cache/modelscope/hub/models/qwen/Qwen2___5-7B-Instruct"
        # 替换成自己的LoRA模型路径
        self.lora_model_path = lora_model_path or "./fine_tune_output/final_model"

    def load_model(self):
        """加载模型"""
        try:
            logger.info("正在加载基础模型...")

            # 检查基础模型路径是否存在
            if not os.path.exists(self.base_model_path):
                logger.warning(
                    f"基础模型路径不存在: {self.base_model_path}，将从ModelScope下载")
                self.base_model_path = "qwen/Qwen2.5-7B-Instruct"

            # 加载tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.base_model_path,
                trust_remote_code=True,
                padding_side="right",
                truncation_side="right"
            )

            # 设置pad_token
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token

            if self.tokenizer.pad_token_id is None:
                self.tokenizer.pad_token_id = self.tokenizer.eos_token_id

            # 加载基础模型
            logger.info(f"正在加载基础模型: {self.base_model_path}")
            self.model = AutoModelForCausalLM.from_pretrained(
                self.base_model_path,
                device_map="auto",
                trust_remote_code=True,
                torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
            )

            # 检查LoRA模型路径是否存在并加载
            if os.path.exists(self.lora_model_path):
                logger.info(f"正在加载LoRA权重: {self.lora_model_path}")
                try:
                    # 加载LoRA适配器
                    self.model = PeftModel.from_pretrained(
                        self.model,
                        self.lora_model_path,
                        device_map="auto",
                    )
                    logger.info("✅ LoRA权重加载成功")
                except Exception as e:
                    logger.error(f"❌ LoRA权重加载失败: {e}")
                    logger.info("💡 将使用基础模型进行推理")
            else:
                logger.warning(f"LoRA模型路径不存在: {self.lora_model_path}")
                logger.info("💡 将使用基础模型进行推理")

            # 设置为评估模式
            self.model.eval()

            logger.info("✅ 模型加载完成")
            return True

        except Exception as e:
            logger.error(f"❌ 模型加载失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def inference(self, input_text):
        """执行推理"""
        if self.model is None or self.tokenizer is None:
            logger.error("❌ 模型未加载")
            return None

        try:
            # 构建提示词
            system_prompt = '''你是网络安全知识图谱提取专家。从威胁情报文档中提取实体和关系，严格按照JSON格式输出。

实体类型包括：ThreatOrganization(威胁组织), AttackEvent(攻击事件), Tool(工具), Technique(技术), Target(目标), Report(报告), Tactic(战术), Procedure(程序), Asset(资产)
关系类型包括：LAUNCH(发起), ATTACK(攻击), USE(使用), BELONG(属于), HAS(拥有), IMPLEMENT(实现)

输出格式要求：
{"entities":[{"labels":"类型","id":"唯一ID","name":"名称","description":"描述"}],"relationships":[{"type":"关系类型","source":"源实体ID","target":"目标实体ID","confidence":0.95,"evidence":"证据"}]}

注意事项：
1. 只提取与网络安全相关的信息
2. 如果文本中没有安全相关内容，可以返回空数组
3. 关系中的source和target必须是实体中的id
4. 实体ID应该有意义且唯一
5. 输出必须是严格的JSON格式，不要添加其他文字'''

            prompt = f"<|im_start|>system\n{system_prompt}<|im_end|>\n<|im_start|>user\n{input_text}<|im_end|>\n<|im_start|>assistant\n"

            # 编码输入
            inputs = self.tokenizer(
                prompt, return_tensors="pt", truncation=True, max_length=2048)

            # 移动到模型所在设备
            inputs = {k: v.to(self.model.device) for k, v in inputs.items()}

            # 生成输出
            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=1024,
                    temperature=0.1,
                    do_sample=True,
                    pad_token_id=self.tokenizer.eos_token_id
                )

            # 解码输出
            response = self.tokenizer.decode(
                outputs[0][len(inputs['input_ids'][0]):], skip_special_tokens=True)

            return response

        except Exception as e:
            logger.error(f"❌ 推理失败: {e}")
            return None


def main():
    """主函数"""
    logger.info("🚀 启动推理演示")

    # 初始化推理器
    inference_demo = InferenceDemo()

    # 加载模型
    if not inference_demo.load_model():
        return

    # 测试输入
    test_inputs = [
        "APT41 World Tour 2021 on a tight schedule\n=========================================  \nShare this article  \nFound it interesting? Don't hesitate to share it to wow your friends or colleagues  \n[← Blog](/blog/)  \nShare this article  \nFound it interesting? Don't hesitate to share it to wow your friends or colleagues  \n[Nikita Rostovcev  \nCyber Intelligence Researcher](https://www.group-ib.com/author/nikita-rostovtsev/)  \nAPT41 World Tour 2021 on a tight schedule\n=========================================  \n4 malicious campaigns, 13 confirmed victims, and a new wave of Cobalt Strike infections  \nAugust 18, 2022 ·  min to read · Advanced Persistent Threats  \nAPT41  \nThreat Intelligence",
        "# APT41 World Tour 2021 on a tight schedule\n\n4 malicious campaigns, 13 confirmed victims, and a new wave of Cobalt Strike infections  \nAugust 18, 2022 ·  min to read · Advanced Persistent Threats  \nAPT41  \nThreat Intelligence  \nIn March 2022 one of the oldest state-sponsored hacker groups, **APT41**, breached government networks in six US states, including by exploiting a vulnerability in a livestock management system, Mandiant investigators [have reported](https://www.mandiant.com/resources/apt41-initiates-global-intrusion-campaign-using-multiple-exploits).",
        "# APT41 World Tour 2021 on a tight schedule\n\nInterestingly, according to sqlmap logs, the threat actors breached only half of the websites they were interested in. This suggests that even hackers like APT41 do not always go out of their way to ensure that a breach is successful.  \nThis blog post also uncovers subnets from which the threat actors connected to their C&C servers, which is further evidence confirming the threat’s country of origin.  \nFor the first time, we were able to identify the group’s working hours in 2021, which are similar to regular office business hours.  \nIT directors, heads of cybersecurity teams, SOC analysts and incident response specialists are likely to find this material useful. Our goal is to reduce financial losses and infrastructure downtime as well as to help take preventive measures to fend off APT41 attacks.",
        "# APT41 World Tour 2021 on a tight schedule\n\nKey findings\n------------  \n* We estimate that in 2021 APT41 compromised and gained various levels of access to at least 13 organizations worldwide.\n* The group’s targets include government and private organizations based in the US, Taiwan, India, Thailand, China, Hong Kong, Mongolia, Indonesia, Vietnam, Bangladesh, Ireland, Brunei, and the UK.\n* In the campaigns that we analyzed, APT41 targeted the following industries: the government sector, manufacturing, healthcare, logistics, hospitality, finance, education, telecommunications, consulting, sports, media, and travel. The targets also included a political group, military organizations, and airlines.\n* To conduct reconnaissance, the threat actors use tools such as Acunetix, Nmap, Sqlmap, OneForAll, subdomain3, subDomainsBrute, and Sublist3r.\n* As an initial vector, the group uses web applications vulnerable to SQL injection attacks."
    ]

    # 执行推理
    for i, input_text in enumerate(test_inputs, 1):
        logger.info(f"\n🔍 测试 {i}: {input_text}")
        result = inference_demo.inference(input_text)
        if result:
            logger.info(f"✅ 输出: {result}")
        else:
            logger.error("❌ 推理失败")


if __name__ == "__main__":
    main()
