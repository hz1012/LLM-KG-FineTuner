#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评估知识图谱提取模型的推理结果质量
"""

import json
import logging
from typing import Dict, List, Any
from pathlib import Path

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KGEvaluation:
    """知识图谱推理结果评估器"""

    def __init__(self, results_file: str):
        """
        初始化评估器

        Args:
            results_file: 评估结果文件路径
        """
        self.results_file = Path(results_file)
        self.evaluation_data = None
        self.load_results()

    def load_results(self):
        """加载评估结果数据"""
        try:
            with open(self.results_file, 'r', encoding='utf-8') as f:
                self.evaluation_data = json.load(f)
            logger.info(f"✅ 成功加载评估结果: {self.results_file}")
        except Exception as e:
            logger.error(f"❌ 加载评估结果失败: {e}")
            raise

    def evaluate_entity_extraction(self, parsed_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        评估实体提取质量

        Args:
            parsed_json: 解析后的JSON数据

        Returns:
            评估结果
        """
        entities = parsed_json.get('entities', [])
        
        # 统计各类实体数量
        entity_types = {}
        for entity in entities:
            label = entity.get('labels', 'Unknown')
            entity_types[label] = entity_types.get(label, 0) + 1
        
        return {
            'total_entities': len(entities),
            'entity_types': entity_types
        }

    def evaluate_relationship_extraction(self, parsed_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        评估关系提取质量

        Args:
            parsed_json: 解析后的JSON数据

        Returns:
            评估结果
        """
        relationships = parsed_json.get('relationships', [])
        
        # 统计各类关系数量
        relationship_types = {}
        for rel in relationships:
            rel_type = rel.get('type', 'Unknown')
            relationship_types[rel_type] = relationship_types.get(rel_type, 0) + 1
        
        return {
            'total_relationships': len(relationships),
            'relationship_types': relationship_types
        }

    def evaluate_json_structure(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        评估JSON结构质量

        Args:
            result: 单个测试结果

        Returns:
            评估结果
        """
        return {
            'json_parse_success': result.get('json_parse_success', False),
            'has_entities': 'entities' in result.get('parsed_json', {}),
            'has_relationships': 'relationships' in result.get('parsed_json', {}),
            'entities_not_empty': len(result.get('parsed_json', {}).get('entities', [])) > 0,
            'relationships_not_empty': len(result.get('parsed_json', {}).get('relationships', [])) > 0
        }

    def comprehensive_evaluation(self) -> Dict[str, Any]:
        """
        综合评估所有测试结果

        Returns:
            综合评估结果
        """
        if not self.evaluation_data:
            logger.error("❌ 评估数据未加载")
            return {}

        detailed_results = self.evaluation_data.get('detailed_results', [])
        
        # 总体统计
        total_tests = len(detailed_results)
        json_success_count = 0
        entity_count = 0
        relationship_count = 0
        
        # 类型统计
        all_entity_types = {}
        all_relationship_types = {}
        
        # 逐项评估
        evaluation_details = []
        for i, result in enumerate(detailed_results, 1):
            logger.info(f"🔍 评估测试项 {i}/{total_tests}")
            
            # JSON结构评估
            json_eval = self.evaluate_json_structure(result)
            
            if json_eval['json_parse_success']:
                json_success_count += 1
                
                parsed_json = result.get('parsed_json', {})
                
                # 实体提取评估
                entity_eval = self.evaluate_entity_extraction(parsed_json)
                entity_count += entity_eval['total_entities']
                
                # 关系提取评估
                rel_eval = self.evaluate_relationship_extraction(parsed_json)
                relationship_count += rel_eval['total_relationships']
                
                # 合并类型统计
                for etype, count in entity_eval['entity_types'].items():
                    all_entity_types[etype] = all_entity_types.get(etype, 0) + count
                    
                for rtype, count in rel_eval['relationship_types'].items():
                    all_relationship_types[rtype] = all_relationship_types.get(rtype, 0) + count
                
                evaluation_details.append({
                    'test_index': i,
                    'json_structure': json_eval,
                    'entity_extraction': entity_eval,
                    'relationship_extraction': rel_eval
                })
        
        # 计算成功率
        json_success_rate = json_success_count / total_tests if total_tests > 0 else 0
        
        # 综合评估结果
        comprehensive_result = {
            'summary': {
                'total_tests': total_tests,
                'json_parse_success_count': json_success_count,
                'json_parse_success_rate': json_success_rate,
                'total_entities_extracted': entity_count,
                'total_relationships_extracted': relationship_count,
                'average_entities_per_test': entity_count / total_tests if total_tests > 0 else 0,
                'average_relationships_per_test': relationship_count / total_tests if total_tests > 0 else 0
            },
            'entity_types_distribution': all_entity_types,
            'relationship_types_distribution': all_relationship_types,
            'detailed_evaluations': evaluation_details
        }
        
        return comprehensive_result

    def generate_report(self, output_file: str = None) -> str:
        """
        生成评估报告

        Args:
            output_file: 输出文件路径（可选）

        Returns:
            评估报告内容
        """
        evaluation_result = self.comprehensive_evaluation()
        
        if not evaluation_result:
            return "无法生成评估报告"
        
        summary = evaluation_result['summary']
        entity_types = evaluation_result['entity_types_distribution']
        rel_types = evaluation_result['relationship_types_distribution']
        
        # 生成报告内容
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("知识图谱提取模型评估报告")
        report_lines.append("=" * 60)
        report_lines.append("")
        
        # 总体统计
        report_lines.append("总体统计:")
        report_lines.append(f"  - 测试样本数: {summary['total_tests']}")
        report_lines.append(f"  - JSON解析成功率: {summary['json_parse_success_rate']:.2%} ({summary['json_parse_success_count']}/{summary['total_tests']})")
        report_lines.append(f"  - 总实体数: {summary['total_entities_extracted']}")
        report_lines.append(f"  - 总关系数: {summary['total_relationships_extracted']}")
        report_lines.append(f"  - 平均每样本实体数: {summary['average_entities_per_test']:.2f}")
        report_lines.append(f"  - 平均每样本关系数: {summary['average_relationships_per_test']:.2f}")
        report_lines.append("")
        
        # 实体类型分布
        report_lines.append("实体类型分布:")
        for etype, count in sorted(entity_types.items(), key=lambda x: x[1], reverse=True):
            report_lines.append(f"  - {etype}: {count}")
        report_lines.append("")
        
        # 关系类型分布
        report_lines.append("关系类型分布:")
        for rtype, count in sorted(rel_types.items(), key=lambda x: x[1], reverse=True):
            report_lines.append(f"  - {rtype}: {count}")
        report_lines.append("")
        
        report_content = "\n".join(report_lines)
        
        # 输出到文件（如果指定）
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                logger.info(f"✅ 评估报告已保存到: {output_file}")
            except Exception as e:
                logger.error(f"❌ 保存评估报告失败: {e}")
        
        return report_content


def main():
    """主函数"""
    logger.info("🚀 开始评估知识图谱提取模型推理结果")
    
    # 评估结果文件路径
    results_file = "./fine_tune_output/evaluation_results.json"
    
    try:
        # 初始化评估器
        evaluator = KGEvaluation(results_file)
        
        # 生成评估报告
        report = evaluator.generate_report("./fine_tune_output/automatic_evaluation_report.txt")
        print(report)
        
        logger.info("✅ 评估完成")
        
    except Exception as e:
        logger.error(f"❌ 评估过程中发生错误: {e}")
        raise


if __name__ == "__main__":
    main()