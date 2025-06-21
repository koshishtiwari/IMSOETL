"""
Quality Agent - Handles data quality assessment and validation.

This agent is responsible for:
- Data quality profiling and assessment
- Defining and executing data quality rules
- Monitoring data quality metrics
- Generating data quality reports
- Automated data quality validation
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from ..core.base_agent import BaseAgent, AgentType, Message
import statistics
import re

from ..core.base_agent import BaseAgent, AgentType


class DataQualityRule:
    """Represents a data quality rule."""
    
    def __init__(
        self,
        rule_id: str,
        rule_name: str,
        rule_type: str,
        description: str,
        column: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None
    ):
        self.rule_id = rule_id
        self.rule_name = rule_name
        self.rule_type = rule_type
        self.description = description
        self.column = column
        self.parameters = parameters or {}
        self.created_at = datetime.now()
        self.is_active = True
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert rule to dictionary."""
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "rule_type": self.rule_type,
            "description": self.description,
            "column": self.column,
            "parameters": self.parameters,
            "created_at": self.created_at.isoformat(),
            "is_active": self.is_active
        }


class DataQualityResult:
    """Represents the result of a data quality assessment."""
    
    def __init__(
        self,
        rule_id: str,
        passed: bool,
        score: float,
        total_records: int,
        failed_records: int,
        details: Optional[Dict[str, Any]] = None
    ):
        self.rule_id = rule_id
        self.passed = passed
        self.score = score
        self.total_records = total_records
        self.failed_records = failed_records
        self.details = details or {}
        self.assessed_at = datetime.now()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "rule_id": self.rule_id,
            "passed": self.passed,
            "score": self.score,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "success_rate": (self.total_records - self.failed_records) / self.total_records if self.total_records > 0 else 0,
            "details": self.details,
            "assessed_at": self.assessed_at.isoformat()
        }


class QualityAgent(BaseAgent):
    """Agent responsible for data quality assessment and validation."""
    
    def __init__(self, agent_id: str = "quality_agent"):
        super().__init__(agent_id, AgentType.QUALITY)
        self.quality_rules: Dict[str, DataQualityRule] = {}
        self.rule_templates = {}
        self.quality_thresholds = {
            "completeness": 0.95,
            "accuracy": 0.90,
            "consistency": 0.95,
            "validity": 0.90,
            "uniqueness": 0.99
        }
        
        # Register message handlers
        self.register_message_handlers()
        
    def register_message_handlers(self) -> None:
        """Register handlers for different message types."""
        self.register_message_handler("task_assignment", self.handle_task_assignment)
        
    async def handle_task_assignment(self, message: Message) -> None:
        """Handle task assignment messages."""
        task = message.content.get("task", {})
        session_id = message.content.get("session_id")
        
        self.logger.info(f"Received task assignment: {task.get('task_type')} (ID: {task.get('task_id')})")
        
        try:
            # Process the task
            result = await self.process_task(task)
            
            # Send response back to orchestrator
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="task_complete",
                content={
                    "session_id": session_id,
                    "task_id": task.get("task_id"),
                    "result": result
                }
            )
            
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="task_error",
                content={
                    "session_id": session_id,
                    "task_id": task.get("task_id"),
                    "error": str(e)
                }
            )
        
    async def initialize(self):
        """Initialize the quality agent."""
        await self._load_rule_templates()
        await self._load_default_rules()
        self.logger.info("QualityAgent initialized successfully")
        
    async def _load_rule_templates(self):
        """Load predefined quality rule templates."""
        self.rule_templates = {
            "completeness": {
                "description": "Check for null/empty values",
                "parameters": ["threshold", "column"],
                "implementation": self._check_completeness
            },
            "uniqueness": {
                "description": "Check for duplicate values",
                "parameters": ["threshold", "column"],
                "implementation": self._check_uniqueness
            },
            "validity": {
                "description": "Check if values match expected format/pattern",
                "parameters": ["pattern", "column"],
                "implementation": self._check_validity
            },
            "range": {
                "description": "Check if numeric values are within expected range",
                "parameters": ["min_value", "max_value", "column"],
                "implementation": self._check_range
            },
            "consistency": {
                "description": "Check for consistency across related fields",
                "parameters": ["related_columns", "consistency_rule"],
                "implementation": self._check_consistency
            },
            "referential_integrity": {
                "description": "Check foreign key relationships",
                "parameters": ["reference_table", "reference_column", "column"],
                "implementation": self._check_referential_integrity
            }
        }
        
    async def _load_default_rules(self):
        """Load default quality rules."""
        default_rules = [
            DataQualityRule(
                rule_id="default_completeness",
                rule_name="Default Completeness Check",
                rule_type="completeness",
                description="Check that required fields are not null",
                parameters={"threshold": 0.95}
            ),
            DataQualityRule(
                rule_id="default_uniqueness",
                rule_name="Default Uniqueness Check",
                rule_type="uniqueness",
                description="Check for duplicate values in key columns",
                parameters={"threshold": 0.99}
            )
        ]
        
        for rule in default_rules:
            self.quality_rules[rule.rule_id] = rule
            
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task assigned to this agent."""
        task_type = task.get("type")
        
        try:
            if task_type == "assess_quality":
                return await self._handle_assess_quality(task)
            elif task_type == "create_rule":
                return await self._handle_create_rule(task)
            elif task_type == "validate_data":
                return await self._handle_validate_data(task)
            elif task_type == "generate_report":
                return await self._handle_generate_report(task)
            elif task_type == "profile_data":
                return await self._handle_profile_data(task)
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
            
    async def _handle_assess_quality(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Assess data quality for a dataset."""
        dataset = task.get("dataset", {})
        rules = task.get("rules", list(self.quality_rules.keys()))
        
        assessment_results = await self.assess_data_quality(dataset, rules)
        
        return {
            "success": True,
            "dataset_id": dataset.get("id", "unknown"),
            "assessment_results": assessment_results,
            "overall_score": self._calculate_overall_score(assessment_results),
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_create_rule(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new data quality rule."""
        rule_config = task.get("rule_config", {})
        
        rule = await self.create_quality_rule(rule_config)
        
        return {
            "success": True,
            "rule_created": rule.to_dict(),
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_validate_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data against quality rules."""
        data = task.get("data", [])
        schema = task.get("schema", {})
        rule_ids = task.get("rule_ids", [])
        
        validation_results = await self.validate_data(data, schema, rule_ids)
        
        return {
            "success": True,
            "validation_results": validation_results,
            "data_passed": all(result["passed"] for result in validation_results),
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_generate_report(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a data quality report."""
        assessment_results = task.get("assessment_results", [])
        
        report = await self.generate_quality_report(assessment_results)
        
        return {
            "success": True,
            "report": report,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_profile_data(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Profile data to understand its characteristics."""
        data = task.get("data", [])
        schema = task.get("schema", {})
        
        profile = await self.profile_data(data, schema)
        
        return {
            "success": True,
            "data_profile": profile,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def assess_data_quality(
        self, 
        dataset: Dict[str, Any], 
        rule_ids: List[str]
    ) -> List[Dict[str, Any]]:
        """Assess data quality using specified rules."""
        results = []
        
        # Mock data for demonstration
        mock_data = dataset.get("data", self._generate_mock_data(100))
        schema = dataset.get("schema", {})
        
        for rule_id in rule_ids:
            if rule_id in self.quality_rules:
                rule = self.quality_rules[rule_id]
                result = await self._execute_quality_rule(rule, mock_data, schema)
                results.append(result.to_dict())
                
        return results
        
    async def create_quality_rule(self, rule_config: Dict[str, Any]) -> DataQualityRule:
        """Create a new data quality rule."""
        rule = DataQualityRule(
            rule_id=rule_config.get("rule_id", f"rule_{datetime.now().timestamp()}"),
            rule_name=rule_config.get("rule_name", "Custom Rule"),
            rule_type=rule_config.get("rule_type", "custom"),
            description=rule_config.get("description", "Custom quality rule"),
            column=rule_config.get("column"),
            parameters=rule_config.get("parameters", {})
        )
        
        self.quality_rules[rule.rule_id] = rule
        
        return rule
        
    async def validate_data(
        self, 
        data: List[Dict[str, Any]], 
        schema: Dict[str, Any], 
        rule_ids: List[str]
    ) -> List[Dict[str, Any]]:
        """Validate data against quality rules."""
        validation_results = []
        
        for rule_id in rule_ids:
            if rule_id in self.quality_rules:
                rule = self.quality_rules[rule_id]
                result = await self._execute_quality_rule(rule, data, schema)
                validation_results.append(result.to_dict())
                
        return validation_results
        
    async def profile_data(self, data: List[Dict[str, Any]], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Profile data to understand its characteristics."""
        if not data:
            return {"error": "No data provided for profiling"}
            
        profile = {
            "total_records": len(data),
            "columns": {},
            "data_types": {},
            "quality_summary": {}
        }
        
        # Get all columns from first record
        sample_record = data[0]
        columns = list(sample_record.keys())
        
        for column in columns:
            column_data = [record.get(column) for record in data]
            column_profile = await self._profile_column(column, column_data)
            profile["columns"][column] = column_profile
            
        # Generate quality summary
        profile["quality_summary"] = await self._generate_quality_summary(profile)
        
        return profile
        
    async def generate_quality_report(self, assessment_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate a comprehensive data quality report."""
        report = {
            "summary": {
                "total_rules_executed": len(assessment_results),
                "passed_rules": sum(1 for result in assessment_results if result.get("passed", False)),
                "failed_rules": sum(1 for result in assessment_results if not result.get("passed", True)),
                "overall_score": self._calculate_overall_score(assessment_results)
            },
            "detailed_results": assessment_results,
            "recommendations": await self._generate_recommendations(assessment_results),
            "generated_at": datetime.now().isoformat()
        }
        
        return report
        
    async def _execute_quality_rule(
        self, 
        rule: DataQualityRule, 
        data: List[Dict[str, Any]], 
        schema: Dict[str, Any]
    ) -> DataQualityResult:
        """Execute a specific quality rule on data."""
        rule_type = rule.rule_type
        
        if rule_type in self.rule_templates:
            implementation = self.rule_templates[rule_type]["implementation"]
            return await implementation(rule, data, schema)
        else:
            # Custom rule execution
            return await self._execute_custom_rule(rule, data, schema)
            
    async def _check_completeness(
        self, 
        rule: DataQualityRule, 
        data: List[Dict[str, Any]], 
        schema: Dict[str, Any]
    ) -> DataQualityResult:
        """Check data completeness (non-null values)."""
        if not data:
            return DataQualityResult(rule.rule_id, False, 0.0, 0, 0)
            
        column = rule.column
        threshold = rule.parameters.get("threshold", 0.95)
        
        if column:
            # Check specific column
            null_count = sum(1 for record in data if record.get(column) is None or record.get(column) == "")
            total_count = len(data)
        else:
            # Check all columns
            total_fields = 0
            null_fields = 0
            for record in data:
                for key, value in record.items():
                    total_fields += 1
                    if value is None or value == "":
                        null_fields += 1
            null_count = null_fields
            total_count = total_fields
            
        score = (total_count - null_count) / total_count if total_count > 0 else 0
        passed = score >= threshold
        
        return DataQualityResult(
            rule.rule_id, 
            passed, 
            score, 
            total_count, 
            null_count,
            {"completeness_rate": score, "threshold": threshold}
        )
        
    async def _check_uniqueness(
        self, 
        rule: DataQualityRule, 
        data: List[Dict[str, Any]], 
        schema: Dict[str, Any]
    ) -> DataQualityResult:
        """Check data uniqueness (no duplicates)."""
        if not data:
            return DataQualityResult(rule.rule_id, True, 1.0, 0, 0)
            
        column = rule.column
        threshold = rule.parameters.get("threshold", 0.99)
        
        if column:
            values = [record.get(column) for record in data if record.get(column) is not None]
            unique_values = set(values)
            total_count = len(values)
            unique_count = len(unique_values)
            duplicate_count = total_count - unique_count
        else:
            # Check uniqueness of entire records
            record_hashes = set()
            duplicate_count = 0
            for record in data:
                record_hash = hash(tuple(sorted(record.items())))
                if record_hash in record_hashes:
                    duplicate_count += 1
                else:
                    record_hashes.add(record_hash)
            total_count = len(data)
            
        uniqueness_rate = (total_count - duplicate_count) / total_count if total_count > 0 else 1.0
        passed = uniqueness_rate >= threshold
        
        return DataQualityResult(
            rule.rule_id, 
            passed, 
            uniqueness_rate, 
            total_count, 
            duplicate_count,
            {"uniqueness_rate": uniqueness_rate, "threshold": threshold}
        )
        
    async def _check_validity(
        self, 
        rule: DataQualityRule, 
        data: List[Dict[str, Any]], 
        schema: Dict[str, Any]
    ) -> DataQualityResult:
        """Check data validity against patterns."""
        if not data:
            return DataQualityResult(rule.rule_id, True, 1.0, 0, 0)
            
        column = rule.column
        pattern = rule.parameters.get("pattern", ".*")
        
        if not column:
            return DataQualityResult(rule.rule_id, False, 0.0, len(data), len(data))
            
        invalid_count = 0
        total_count = 0
        
        for record in data:
            value = record.get(column)
            if value is not None:
                total_count += 1
                if not re.match(pattern, str(value)):
                    invalid_count += 1
                    
        validity_rate = (total_count - invalid_count) / total_count if total_count > 0 else 1.0
        passed = validity_rate >= rule.parameters.get("threshold", 0.90)
        
        return DataQualityResult(
            rule.rule_id, 
            passed, 
            validity_rate, 
            total_count, 
            invalid_count,
            {"validity_rate": validity_rate, "pattern": pattern}
        )
        
    async def _check_range(
        self, 
        rule: DataQualityRule, 
        data: List[Dict[str, Any]], 
        schema: Dict[str, Any]
    ) -> DataQualityResult:
        """Check if numeric values are within expected range."""
        if not data:
            return DataQualityResult(rule.rule_id, True, 1.0, 0, 0)
            
        column = rule.column
        min_value = rule.parameters.get("min_value")
        max_value = rule.parameters.get("max_value")
        
        if not column or (min_value is None and max_value is None):
            return DataQualityResult(rule.rule_id, False, 0.0, len(data), len(data))
            
        out_of_range_count = 0
        total_count = 0
        
        for record in data:
            value = record.get(column)
            if value is not None and isinstance(value, (int, float)):
                total_count += 1
                if min_value is not None and value < min_value:
                    out_of_range_count += 1
                elif max_value is not None and value > max_value:
                    out_of_range_count += 1
                    
        range_compliance_rate = (total_count - out_of_range_count) / total_count if total_count > 0 else 1.0
        passed = range_compliance_rate >= rule.parameters.get("threshold", 0.95)
        
        return DataQualityResult(
            rule.rule_id, 
            passed, 
            range_compliance_rate, 
            total_count, 
            out_of_range_count,
            {"range_compliance_rate": range_compliance_rate, "min_value": min_value, "max_value": max_value}
        )
        
    async def _check_consistency(
        self, 
        rule: DataQualityRule, 
        data: List[Dict[str, Any]], 
        schema: Dict[str, Any]
    ) -> DataQualityResult:
        """Check consistency across related fields."""
        # Simplified consistency check
        inconsistent_count = 0
        total_count = len(data)
        
        # This would be implemented based on specific consistency rules
        # For now, return a mock result
        consistency_rate = 0.95  # Mock rate
        passed = consistency_rate >= rule.parameters.get("threshold", 0.90)
        
        return DataQualityResult(
            rule.rule_id, 
            passed, 
            consistency_rate, 
            total_count, 
            inconsistent_count,
            {"consistency_rate": consistency_rate}
        )
        
    async def _check_referential_integrity(
        self, 
        rule: DataQualityRule, 
        data: List[Dict[str, Any]], 
        schema: Dict[str, Any]
    ) -> DataQualityResult:
        """Check referential integrity."""
        # Simplified referential integrity check
        violations_count = 0
        total_count = len(data)
        
        # This would check against reference table
        # For now, return a mock result
        integrity_rate = 0.98  # Mock rate
        passed = integrity_rate >= rule.parameters.get("threshold", 0.95)
        
        return DataQualityResult(
            rule.rule_id, 
            passed, 
            integrity_rate, 
            total_count, 
            violations_count,
            {"integrity_rate": integrity_rate}
        )
        
    async def _execute_custom_rule(
        self, 
        rule: DataQualityRule, 
        data: List[Dict[str, Any]], 
        schema: Dict[str, Any]
    ) -> DataQualityResult:
        """Execute a custom quality rule."""
        # For custom rules, return a basic result
        return DataQualityResult(
            rule.rule_id, 
            True, 
            0.9, 
            len(data), 
            0,
            {"note": "Custom rule execution not fully implemented"}
        )
        
    async def _profile_column(self, column_name: str, column_data: List[Any]) -> Dict[str, Any]:
        """Profile a single column."""
        profile = {
            "column_name": column_name,
            "total_values": len(column_data),
            "null_count": sum(1 for value in column_data if value is None),
            "unique_count": len(set(value for value in column_data if value is not None)),
            "data_types": {}
        }
        
        # Count data types
        type_counts = {}
        non_null_values = [value for value in column_data if value is not None]
        
        for value in non_null_values:
            value_type = type(value).__name__
            type_counts[value_type] = type_counts.get(value_type, 0) + 1
            
        profile["data_types"] = type_counts
        
        # For numeric columns, add statistical measures
        numeric_values = [value for value in non_null_values if isinstance(value, (int, float))]
        if numeric_values:
            profile["statistics"] = {
                "min": min(numeric_values),
                "max": max(numeric_values),
                "mean": statistics.mean(numeric_values),
                "median": statistics.median(numeric_values),
                "std_dev": statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0
            }
            
        return profile
        
    async def _generate_quality_summary(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Generate quality summary from profile."""
        total_records = profile.get("total_records", 0)
        columns = profile.get("columns", {})
        
        quality_issues = []
        overall_completeness = []
        
        for column_name, column_profile in columns.items():
            null_count = column_profile.get("null_count", 0)
            completeness = (total_records - null_count) / total_records if total_records > 0 else 0
            overall_completeness.append(completeness)
            
            if completeness < 0.95:
                quality_issues.append(f"Column '{column_name}' has low completeness: {completeness:.2%}")
                
        summary = {
            "overall_completeness": statistics.mean(overall_completeness) if overall_completeness else 0,
            "quality_issues": quality_issues,
            "columns_with_issues": len(quality_issues),
            "total_columns": len(columns)
        }
        
        return summary
        
    async def _generate_recommendations(self, assessment_results: List[Dict[str, Any]]) -> List[str]:
        """Generate recommendations based on assessment results."""
        recommendations = []
        
        for result in assessment_results:
            if not result.get("passed", True):
                rule_id = result.get("rule_id", "unknown")
                score = result.get("score", 0)
                
                if "completeness" in rule_id.lower():
                    recommendations.append(f"Improve data completeness for {rule_id} (current: {score:.2%})")
                elif "uniqueness" in rule_id.lower():
                    recommendations.append(f"Address duplicate values for {rule_id} (current: {score:.2%})")
                elif "validity" in rule_id.lower():
                    recommendations.append(f"Fix data format issues for {rule_id} (current: {score:.2%})")
                else:
                    recommendations.append(f"Address quality issues for {rule_id} (current: {score:.2%})")
                    
        if not recommendations:
            recommendations.append("Data quality looks good! Continue monitoring.")
            
        return recommendations
        
    def _calculate_overall_score(self, assessment_results: List[Dict[str, Any]]) -> float:
        """Calculate overall quality score."""
        if not assessment_results:
            return 0.0
            
        scores = [result.get("score", 0) for result in assessment_results]
        return statistics.mean(scores)
        
    def _generate_mock_data(self, num_records: int) -> List[Dict[str, Any]]:
        """Generate mock data for testing."""
        import random
        
        mock_data = []
        for i in range(num_records):
            record = {
                "id": i + 1,
                "name": f"Record_{i+1}" if random.random() > 0.05 else None,  # 5% null rate
                "value": random.randint(1, 100),
                "category": random.choice(["A", "B", "C"]),
                "created_at": datetime.now().isoformat()
            }
            # Add some duplicates
            if random.random() < 0.02:  # 2% duplicate rate
                record["id"] = random.randint(1, max(1, i))
            mock_data.append(record)
            
        return mock_data
