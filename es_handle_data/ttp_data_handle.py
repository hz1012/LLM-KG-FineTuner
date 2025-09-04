#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TTP数据提取脚本
从业务方提供的CSV文件中提取指定技术的TTP记录，追加到test_ttp_v2.csv
符合历史格式：tactics,techniques,procedure
"""

import os
import pandas as pd
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TTPExtractor:
    def __init__(self):
        """初始化TTP提取器"""
        self.source_folder = "./es_handle_data/attck_procedures"
        self.target_file = "./es_handle_data/attck_procedures/test_ttp_v1.csv"

        # 指定要提取的技术列表
        self.target_techniques = ["T1608.006", "T1566.002", "T1204.002", "T1055.001",
                                  "T1562.004", "T1053.005", "T1055.004", "T1543.003", "T1059.005"]

        # 输出CSV的列名（符合历史格式）
        self.output_columns = ["tactics", "techniques", "procedure"]

        # 统计信息
        self.stats = {
            "files_processed": 0,
            "total_records": 0,
            "matched_records": 0,
            "written_records": 0
        }

    def get_source_files(self) -> List[str]:
        """获取所有源CSV文件"""
        try:
            source_path = Path(self.source_folder)
            if not source_path.exists():
                logger.error(f"源文件夹不存在: {self.source_folder}")
                return []

            # 查找所有以"attck_procedures_"开头的CSV文件
            csv_files = []
            for file_path in source_path.glob("attck_procedures_*.csv"):
                csv_files.append(str(file_path))

            logger.info(f"发现CSV文件: {len(csv_files)}个")
            for file in csv_files:
                logger.info(f"  - {file}")

            return csv_files

        except Exception as e:
            logger.error(f"获取源文件失败: {e}")
            return []

    def clean_text(self, text: str) -> str:
        """清理文本内容"""
        if pd.isna(text) or text is None:
            return ""

        # 转换为字符串并清理
        text = str(text).strip()

        # 移除多余的空白字符
        text = re.sub(r'\s+', ' ', text)

        # 移除换行符
        text = text.replace('\n', ' ').replace('\r', ' ')

        return text

    def extract_techniques(self, technique_text: str) -> List[str]:
        """从技术文本中提取所有技术ID"""
        if pd.isna(technique_text) or not technique_text:
            return []

        # 使用正则表达式提取所有T开头的技术ID
        technique_pattern = r'T\d{4}(?:\.\d{3})?'
        matches = re.findall(technique_pattern, str(technique_text))
        return matches if matches else [str(technique_text).strip()]

    def extract_tactics(self, tactics_text: str) -> List[str]:
        """从tactics文本中提取所有战术名称"""
        if pd.isna(tactics_text) or not tactics_text:
            return []

        tactics_clean = self.clean_text(tactics_text)
        
        # 处理格式如 "Tactics: Persistence, Privilege Escalation"
        # 首先去掉开头的 "Tactics: " 前缀
        if tactics_clean.startswith("Tactics:"):
            tactics_clean = tactics_clean[len("Tactics:"):].strip()
        
        # 按逗号分割战术名称
        tactic_names = [tactic.strip() for tactic in tactics_clean.split(',')]
        
        # 过滤掉空的战术名称
        tactic_names = [tactic for tactic in tactic_names if tactic]
        
        return tactic_names if tactic_names else [tactics_clean]

    def parse_csv_file(self, file_path: str) -> List[Dict[str, Any]]:
        """解析单个CSV文件"""
        try:
            logger.info(f"正在处理文件: {file_path}")

            # 尝试不同的编码
            encodings = ['utf-8', 'gbk', 'gb2312', 'latin-1']
            df = None

            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"使用编码 {encoding} 成功读取文件")
                    break
                except UnicodeDecodeError:
                    continue

            if df is None:
                logger.error(f"无法读取文件 {file_path}")
                return []

            # 输出文件信息
            logger.info(f"文件包含 {len(df)} 条记录")
            logger.info(f"列名: {list(df.columns)}")

            self.stats["files_processed"] += 1
            self.stats["total_records"] += len(df)

            # 解析每一行记录
            matched_records = []
            for index, row in df.iterrows():
                try:
                    # 提取技术ID
                    technique_raw = row.get('Techniques', '')
                    technique_ids = self.extract_techniques(technique_raw)

                    # 遍历所有技术ID
                    for technique_id in technique_ids:
                        # 检查是否在目标技术列表中
                        if technique_id in self.target_techniques:
                            # 提取战术ID（可能有多个）
                            tactics_raw = row.get('Tactics', '')
                            tactic_ids = self.extract_tactics(tactics_raw)

                            # 提取过程描述
                            procedure = self.clean_text(
                                row.get('Description', ''))

                            # 为每个战术创建一条记录
                            for tactic_id in tactic_ids:
                                record = {
                                    "tactics": tactic_id,
                                    "techniques": technique_id,
                                    "procedure": procedure
                                }

                                matched_records.append(record)
                                self.stats["matched_records"] += 1

                                logger.info(
                                    f"匹配记录: {row.get('ID', 'N/A')} - {technique_id} - {tactic_id}")

                except Exception as e:
                    logger.warning(f"处理第{index+1}行时出错: {e}")
                    continue

            logger.info(f"文件 {file_path} 匹配到 {len(matched_records)} 条记录")
            return matched_records

        except Exception as e:
            logger.error(f"解析文件 {file_path} 失败: {e}")
            return []

    def deduplicate_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """去除重复记录"""
        seen = set()
        unique_records = []

        for record in records:
            # 使用tactics, techniques, procedure的组合作为唯一标识
            key = (record["tactics"], record["techniques"],
                   record["procedure"])
            if key not in seen:
                seen.add(key)
                unique_records.append(record)
            else:
                logger.debug(f"跳过重复记录: {record['techniques']}")

        logger.info(f"去重前: {len(records)} 条记录，去重后: {len(unique_records)} 条记录")
        return unique_records

    def write_to_target_file(self, records: List[Dict[str, Any]]):
        """将记录写入目标文件（新建文件）"""
        try:
            # 去重
            unique_records = self.deduplicate_records(records)

            # 使用写入模式'w'，会覆盖现有文件
            with open(self.target_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(
                    csvfile, fieldnames=self.output_columns)

                # 总是写入表头（因为是新文件）
                writer.writeheader()
                logger.info(f"创建新文件: {self.target_file}")

                # 写入记录
                for record in unique_records:
                    writer.writerow(record)
                    self.stats["written_records"] += 1

                logger.info(
                    f"成功写入 {len(unique_records)} 条记录到 {self.target_file}")

        except Exception as e:
            logger.error(f"写入文件失败: {e}")

    def run(self):
        """运行主处理流程"""
        logger.info("开始TTP数据提取...")
        logger.info(f"目标技术: {self.target_techniques}")
        logger.info(f"输出格式: {self.output_columns}")

        # 获取源文件列表
        source_files = self.get_source_files()
        if not source_files:
            logger.error("未找到源文件，退出处理")
            return

        # 处理每个文件
        all_records = []
        for file_path in source_files:
            records = self.parse_csv_file(file_path)
            all_records.extend(records)

        # 写入目标文件
        if all_records:
            self.write_to_target_file(all_records)
        else:
            logger.warning("未找到匹配的记录")

        # 输出统计信息
        self.print_statistics()

    def print_statistics(self):
        """打印统计信息"""
        logger.info("=" * 50)
        logger.info("处理完成，统计信息:")
        logger.info(f"处理文件数: {self.stats['files_processed']}")
        logger.info(f"总记录数: {self.stats['total_records']}")
        logger.info(f"匹配记录数: {self.stats['matched_records']}")
        logger.info(f"写入记录数: {self.stats['written_records']}")

        if self.stats['total_records'] > 0:
            match_rate = (self.stats['matched_records'] /
                          self.stats['total_records']) * 100
            logger.info(f"匹配率: {match_rate:.2f}%")

        logger.info("=" * 50)


def main():
    """主函数"""
    try:
        extractor = TTPExtractor()
        extractor.run()

    except KeyboardInterrupt:
        logger.info("用户中断程序")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")


if __name__ == "__main__":
    main()
