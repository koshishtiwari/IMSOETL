"""
Transformation Agent - Handles data transformation logic and operations.

This agent is responsible for:
- Generating transformation code/SQL
- Validating transformation logic
- Optimizing transformation pipelines
- Managing transformation templates and patterns
- LLM-enhanced SQL generation and optimization
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..core.base_agent import BaseAgent, AgentType
from ..llm.manager import LLMManager


class TransformationAgent(BaseAgent):
    """Agent responsible for data transformation operations."""
    
    def __init__(self, agent_id: str = "transformation_agent"):
        super().__init__(agent_id, AgentType.TRANSFORMATION)
        self.transformation_templates = {}
        self.optimization_rules = []
        self.llm_manager = None  # Will be initialized later
        self.supported_operations = [
            "filter", "select", "join", "aggregate", "pivot", "unpivot",
            "window", "rank", "sort", "group", "union", "case_when",
            "cast", "format", "clean", "validate", "derive"
        ]
        
    async def initialize(self):
        """Initialize the transformation agent."""
        await self._load_transformation_templates()
        await self._load_optimization_rules()
        self.logger.info("TransformationAgent initialized successfully")
        
    async def _load_transformation_templates(self):
        """Load predefined transformation templates."""
        self.transformation_templates = {
            "data_cleaning": {
                "null_handling": "COALESCE({column}, {default_value})",
                "trim_whitespace": "TRIM({column})",
                "case_standardization": "UPPER({column})",
                "date_formatting": "DATE_FORMAT({column}, '{format}')"
            },
            "aggregations": {
                "sum": "SUM({column})",
                "avg": "AVG({column})",
                "count": "COUNT({column})",
                "count_distinct": "COUNT(DISTINCT {column})",
                "min": "MIN({column})",
                "max": "MAX({column})"
            },
            "window_functions": {
                "row_number": "ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order})",
                "rank": "RANK() OVER (PARTITION BY {partition} ORDER BY {order})",
                "lag": "LAG({column}, {offset}) OVER (PARTITION BY {partition} ORDER BY {order})",
                "lead": "LEAD({column}, {offset}) OVER (PARTITION BY {partition} ORDER BY {order})"
            }
        }
        
    async def _load_optimization_rules(self):
        """Load transformation optimization rules."""
        self.optimization_rules = [
            {
                "name": "filter_pushdown",
                "description": "Push filter conditions as early as possible",
                "priority": 1
            },
            {
                "name": "join_optimization",
                "description": "Optimize join order based on table sizes",
                "priority": 2
            },
            {
                "name": "column_pruning",
                "description": "Remove unused columns early in pipeline",
                "priority": 3
            }
        ]
        
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task assigned to this agent."""
        task_type = task.get("type")
        
        try:
            if task_type == "generate_transformation":
                return await self._handle_generate_transformation(task)
            elif task_type == "validate_transformation":
                return await self._handle_validate_transformation(task)
            elif task_type == "optimize_transformation":
                return await self._handle_optimize_transformation(task)
            elif task_type == "apply_template":
                return await self._handle_apply_template(task)
            else:
                return {
                    "success": False,
                    "error": f"Unknown task type: {task_type}",
                    "agent": self.agent_id,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Error processing task: {e}")
            return {
                "success": False,
                "error": str(e),
                "agent": self.agent_id,
                "timestamp": datetime.now().isoformat()
            }
            
    async def _handle_generate_transformation(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Generate transformation code based on requirements."""
        requirements = task.get("requirements", {})
        source_schema = task.get("source_schema", {})
        target_schema = task.get("target_schema", {})
        
        transformation_spec = await self.generate_transformation_spec(
            requirements, source_schema, target_schema
        )
        
        transformation_code = await self.generate_transformation_code(transformation_spec)
        
        return {
            "success": True,
            "transformation_spec": transformation_spec,
            "transformation_code": transformation_code,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_validate_transformation(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Validate transformation logic."""
        transformation_code = task.get("transformation_code", "")
        source_schema = task.get("source_schema", {})
        target_schema = task.get("target_schema", {})
        
        validation_result = await self.validate_transformation(
            transformation_code, source_schema, target_schema
        )
        
        return {
            "success": True,
            "validation_result": validation_result,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_optimize_transformation(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize transformation pipeline."""
        transformation_spec = task.get("transformation_spec", {})
        
        optimized_spec = await self.optimize_transformation_pipeline(transformation_spec)
        
        return {
            "success": True,
            "original_spec": transformation_spec,
            "optimized_spec": optimized_spec,
            "optimization_applied": optimized_spec.get("optimizations", []),
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_apply_template(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Apply transformation template."""
        template_name = task.get("template_name", "")
        template_params = task.get("template_params", {})
        
        applied_template = await self.apply_transformation_template(
            template_name, template_params
        )
        
        return {
            "success": True,
            "template_name": template_name,
            "applied_template": applied_template,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def generate_transformation_spec(
        self, 
        requirements: Dict[str, Any], 
        source_schema: Dict[str, Any], 
        target_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate transformation specification from requirements."""
        transformation_spec = {
            "operations": [],
            "data_flow": [],
            "dependencies": [],
            "performance_hints": []
        }
        
        # Analyze requirements and generate operations
        operations = requirements.get("operations", [])
        
        for operation in operations:
            op_type = operation.get("type")
            if op_type in self.supported_operations:
                transformation_spec["operations"].append({
                    "type": op_type,
                    "config": operation.get("config", {}),
                    "source_columns": operation.get("source_columns", []),
                    "target_columns": operation.get("target_columns", [])
                })
                
        # Generate data flow
        transformation_spec["data_flow"] = await self._generate_data_flow(
            source_schema, target_schema, transformation_spec["operations"]
        )
        
        # Add performance hints
        transformation_spec["performance_hints"] = await self._generate_performance_hints(
            transformation_spec
        )
        
        return transformation_spec
        
    async def generate_transformation_code(self, transformation_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Generate actual transformation code from specification."""
        code_generators = {
            "sql": self._generate_sql_code,
            "python": self._generate_python_code,
            "spark": self._generate_spark_code
        }
        
        generated_code = {}
        
        for code_type, generator in code_generators.items():
            try:
                code = await generator(transformation_spec)
                generated_code[code_type] = code
            except Exception as e:
                self.logger.warning(f"Failed to generate {code_type} code: {e}")
                
        return generated_code
        
    async def initialize_llm(self, config: Dict[str, Any]) -> bool:
        """Initialize LLM manager for enhanced transformations."""
        try:
            self.llm_manager = LLMManager(config)
            success = await self.llm_manager.initialize()
            if success:
                self.logger.info("LLM manager initialized for transformation agent")
                return True
            else:
                self.logger.warning("LLM manager initialization failed")
                return False
        except Exception as e:
            self.logger.error(f"Failed to initialize LLM manager: {e}")
            return False
            
    async def generate_llm_enhanced_transformation(
        self, 
        source_schema: Dict[str, Any], 
        target_schema: Dict[str, Any],
        requirements: List[str]
    ) -> Dict[str, Any]:
        """Generate transformation using LLM for complex requirements."""
        if not self.llm_manager:
            return await self.generate_transformation_code({})
            
        try:
            # Use LLM to generate SQL transformation
            sql_code = await self.llm_manager.generate_sql_transformation(
                source_schema, target_schema, requirements
            )
            
            # Also get traditional generation for comparison
            traditional_spec = await self.generate_transformation_spec(
                {"requirements": requirements}, source_schema, target_schema
            )
            traditional_code = await self.generate_transformation_code(traditional_spec)
            
            return {
                "llm_generated": {
                    "sql": sql_code,
                    "method": "llm",
                    "confidence": "high"
                },
                "traditional_generated": traditional_code,
                "recommendation": "llm" if len(sql_code.strip()) > 50 else "traditional",
                "metadata": {
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                    "requirements": requirements,
                    "generated_at": datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error(f"LLM-enhanced transformation failed: {e}")
            # Fallback to traditional method
            spec = await self.generate_transformation_spec(
                {"requirements": requirements}, {}, {}
            )
            return await self.generate_transformation_code(spec)
            
    async def optimize_transformation_with_llm(self, sql_code: str, schema_info: Dict[str, Any]) -> str:
        """Use LLM to optimize existing transformation SQL."""
        if not self.llm_manager:
            return sql_code
            
        try:
            prompt = f"""
Optimize the following SQL transformation code for better performance and readability:

Original SQL:
{sql_code}

Schema Information:
{schema_info}

Please provide an optimized version that:
1. Uses appropriate indexes if available
2. Minimizes data movement
3. Uses efficient joins and subqueries
4. Follows SQL best practices

Return only the optimized SQL code without explanations.
"""
            
            optimized_sql = await self.llm_manager.generate(prompt)
            
            # Basic validation - check if it looks like SQL
            if "SELECT" in optimized_sql.upper() or "WITH" in optimized_sql.upper():
                self.logger.info("SQL optimization completed via LLM")
                return optimized_sql.strip()
            else:
                self.logger.warning("LLM optimization didn't return valid SQL, using original")
                return sql_code
                
        except Exception as e:
            self.logger.error(f"LLM SQL optimization failed: {e}")
            return sql_code
        
    async def _generate_sql_code(self, transformation_spec: Dict[str, Any]) -> str:
        """Generate SQL transformation code."""
        operations = transformation_spec.get("operations", [])
        sql_parts = []
        
        sql_parts.append("-- Generated SQL Transformation")
        sql_parts.append("SELECT")
        
        # Generate SELECT clause
        select_columns = []
        for operation in operations:
            if operation["type"] == "select":
                select_columns.extend(operation.get("source_columns", []))
            elif operation["type"] == "aggregate":
                config = operation.get("config", {})
                agg_func = config.get("function", "COUNT")
                source_columns = operation.get("source_columns", ["*"])
                if source_columns:
                    column = source_columns[0]
                    select_columns.append(f"{agg_func}({column}) as {column}_{agg_func.lower()}")
                else:
                    select_columns.append(f"{agg_func}(*) as total_{agg_func.lower()}")
                
        if not select_columns:
            select_columns = ["*"]
            
        sql_parts.append("  " + ",\n  ".join(select_columns))
        sql_parts.append("FROM source_table")
        
        # Generate WHERE clause
        filter_operations = [op for op in operations if op["type"] == "filter"]
        if filter_operations:
            where_conditions = []
            for filter_op in filter_operations:
                config = filter_op.get("config", {})
                condition = config.get("condition", "1=1")
                where_conditions.append(condition)
            sql_parts.append("WHERE " + " AND ".join(where_conditions))
            
        # Generate GROUP BY clause
        group_operations = [op for op in operations if op["type"] == "group"]
        if group_operations:
            group_columns = []
            for group_op in group_operations:
                group_columns.extend(group_op.get("source_columns", []))
            if group_columns:
                sql_parts.append("GROUP BY " + ", ".join(group_columns))
                
        return "\n".join(sql_parts) + ";"
        
    async def _generate_python_code(self, transformation_spec: Dict[str, Any]) -> str:
        """Generate Python transformation code."""
        operations = transformation_spec.get("operations", [])
        
        python_code = [
            "# Generated Python Transformation",
            "import pandas as pd",
            "import numpy as np",
            "",
            "def transform_data(df):",
            "    \"\"\"Apply transformations to the dataframe.\"\"\"",
            "    result_df = df.copy()"
        ]
        
        for operation in operations:
            op_type = operation["type"]
            config = operation.get("config", {})
            
            if op_type == "filter":
                condition = config.get("condition", "True")
                python_code.append(f"    result_df = result_df[{condition}]")
            elif op_type == "select":
                columns = operation.get("source_columns", [])
                if columns:
                    python_code.append(f"    result_df = result_df[{columns}]")
            elif op_type == "aggregate":
                agg_func = config.get("function", "sum")
                group_by = config.get("group_by", [])
                if group_by:
                    python_code.append(f"    result_df = result_df.groupby({group_by}).{agg_func}()")
                    
        python_code.extend([
            "    return result_df",
            "",
            "# Usage: transformed_df = transform_data(source_df)"
        ])
        
        return "\n".join(python_code)
        
    async def _generate_spark_code(self, transformation_spec: Dict[str, Any]) -> str:
        """Generate Spark transformation code."""
        operations = transformation_spec.get("operations", [])
        
        spark_code = [
            "# Generated Spark Transformation",
            "from pyspark.sql import functions as F",
            "",
            "def transform_data(spark_df):",
            "    \"\"\"Apply transformations to the Spark dataframe.\"\"\"",
            "    result_df = spark_df"
        ]
        
        for operation in operations:
            op_type = operation["type"]
            config = operation.get("config", {})
            
            if op_type == "filter":
                condition = config.get("condition", "True")
                spark_code.append(f"    result_df = result_df.filter({condition})")
            elif op_type == "select":
                columns = operation.get("source_columns", [])
                if columns:
                    spark_code.append(f"    result_df = result_df.select({', '.join([f'"{col}"' for col in columns])})")
            elif op_type == "aggregate":
                agg_func = config.get("function", "sum")
                group_by = config.get("group_by", [])
                if group_by:
                    spark_code.append(f"    result_df = result_df.groupBy({', '.join([f'"{col}"' for col in group_by])}).agg(F.{agg_func}('*'))")
                    
        spark_code.extend([
            "    return result_df",
            "",
            "# Usage: transformed_df = transform_data(source_spark_df)"
        ])
        
        return "\n".join(spark_code)
        
    async def validate_transformation(
        self, 
        transformation_code: str, 
        source_schema: Dict[str, Any], 
        target_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate transformation logic."""
        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "suggestions": []
        }
        
        # Basic syntax validation (simplified)
        if not transformation_code.strip():
            validation_result["is_valid"] = False
            validation_result["errors"].append("Transformation code is empty")
            
        # Schema compatibility validation
        source_columns = set(source_schema.get("columns", {}).keys())
        target_columns = set(target_schema.get("columns", {}).keys())
        
        # Check for missing source columns referenced in transformation
        # This is a simplified check - in reality, we'd parse the SQL/code
        for col in source_columns:
            if col in transformation_code:
                validation_result["suggestions"].append(f"Column '{col}' is being used from source")
                
        return validation_result
        
    async def optimize_transformation_pipeline(self, transformation_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize transformation pipeline based on rules."""
        optimized_spec = transformation_spec.copy()
        optimizations_applied = []
        
        operations = optimized_spec.get("operations", [])
        
        # Apply filter pushdown optimization
        filter_ops = [op for op in operations if op["type"] == "filter"]
        non_filter_ops = [op for op in operations if op["type"] != "filter"]
        
        if filter_ops and non_filter_ops:
            # Move filters to the beginning
            optimized_spec["operations"] = filter_ops + non_filter_ops
            optimizations_applied.append("filter_pushdown")
            
        # Apply column pruning
        select_ops = [op for op in operations if op["type"] == "select"]
        if not select_ops:
            # Add a select operation to prune unused columns
            all_referenced_columns = set()
            for op in operations:
                all_referenced_columns.update(op.get("source_columns", []))
                all_referenced_columns.update(op.get("target_columns", []))
                
            if all_referenced_columns:
                select_op = {
                    "type": "select",
                    "source_columns": list(all_referenced_columns),
                    "config": {}
                }
                optimized_spec["operations"].insert(0, select_op)
                optimizations_applied.append("column_pruning")
                
        optimized_spec["optimizations"] = optimizations_applied
        
        return optimized_spec
        
    async def apply_transformation_template(self, template_name: str, template_params: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a transformation template with parameters."""
        template_category = template_params.get("category", "data_cleaning")
        
        if template_category not in self.transformation_templates:
            raise ValueError(f"Unknown template category: {template_category}")
            
        category_templates = self.transformation_templates[template_category]
        
        if template_name not in category_templates:
            raise ValueError(f"Unknown template '{template_name}' in category '{template_category}'")
            
        template = category_templates[template_name]
        
        # Apply template parameters
        applied_template = template
        for param, value in template_params.items():
            if param != "category":
                applied_template = applied_template.replace(f"{{{param}}}", str(value))
                
        return {
            "template": applied_template,
            "category": template_category,
            "parameters_applied": template_params
        }
        
    async def _generate_data_flow(
        self, 
        source_schema: Dict[str, Any], 
        target_schema: Dict[str, Any], 
        operations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Generate data flow specification."""
        data_flow = []
        
        # Start with source
        data_flow.append({
            "step": "source",
            "type": "input",
            "schema": source_schema
        })
        
        # Add transformation steps
        for i, operation in enumerate(operations):
            data_flow.append({
                "step": f"transform_{i+1}",
                "type": "transformation",
                "operation": operation["type"],
                "config": operation.get("config", {})
            })
            
        # End with target
        data_flow.append({
            "step": "target",
            "type": "output",
            "schema": target_schema
        })
        
        return data_flow
        
    async def _generate_performance_hints(self, transformation_spec: Dict[str, Any]) -> List[str]:
        """Generate performance optimization hints."""
        hints = []
        operations = transformation_spec.get("operations", [])
        
        # Check for expensive operations
        if any(op["type"] == "join" for op in operations):
            hints.append("Consider indexing join columns for better performance")
            
        if any(op["type"] == "aggregate" for op in operations):
            hints.append("Aggregations can be memory intensive - consider partitioning")
            
        if len(operations) > 10:
            hints.append("Complex transformation pipeline - consider breaking into stages")
            
        return hints
