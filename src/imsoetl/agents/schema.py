"""
Schema Agent - Responsible for schema mapping and analysis.

This agent:
- Analyzes source and target schemas
- Maps fields between different systems
- Identifies data type conversions needed
- Detects schema compatibility issues
- Suggests optimal schema transformations
"""

import asyncio
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from ..core.base_agent import BaseAgent, AgentType, Message


class DataType(str, Enum):
    """Standard data types for schema mapping."""
    INTEGER = "integer"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    FLOAT = "float"
    BOOLEAN = "boolean"
    STRING = "string"
    TEXT = "text"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    JSON = "json"
    BINARY = "binary"
    UNKNOWN = "unknown"


@dataclass
class ColumnSchema:
    """Schema information for a column."""
    name: str
    data_type: DataType
    nullable: bool = True
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    default_value: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_reference: Optional[str] = None
    constraints: Optional[List[str]] = None
    
    def __post_init__(self):
        if self.constraints is None:
            self.constraints = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "nullable": self.nullable,
            "max_length": self.max_length,
            "precision": self.precision,
            "scale": self.scale,
            "default_value": self.default_value,
            "is_primary_key": self.is_primary_key,
            "is_foreign_key": self.is_foreign_key,
            "foreign_key_reference": self.foreign_key_reference,
            "constraints": self.constraints
        }


@dataclass
class TableSchema:
    """Schema information for a table."""
    table_name: str
    source_id: str
    columns: List[ColumnSchema]
    primary_keys: Optional[List[str]] = None
    foreign_keys: Optional[Dict[str, str]] = None
    indexes: Optional[List[Dict[str, Any]]] = None
    constraints: Optional[List[Dict[str, Any]]] = None
    
    def __post_init__(self):
        if self.primary_keys is None:
            self.primary_keys = [col.name for col in self.columns if col.is_primary_key]
        if self.foreign_keys is None:
            self.foreign_keys = {col.name: col.foreign_key_reference 
                               for col in self.columns 
                               if col.is_foreign_key and col.foreign_key_reference}
        if self.indexes is None:
            self.indexes = []
        if self.constraints is None:
            self.constraints = []
    
    def get_column(self, column_name: str) -> Optional[ColumnSchema]:
        """Get a column by name."""
        for col in self.columns:
            if col.name.lower() == column_name.lower():
                return col
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "table_name": self.table_name,
            "source_id": self.source_id,
            "columns": [col.to_dict() for col in self.columns],
            "primary_keys": self.primary_keys,
            "foreign_keys": self.foreign_keys,
            "indexes": self.indexes,
            "constraints": self.constraints
        }


@dataclass
class SchemaMapping:
    """Mapping between source and target schemas."""
    mapping_id: str
    source_schema: TableSchema
    target_schema: TableSchema
    column_mappings: Dict[str, str]  # source_column -> target_column
    transformations: List[Dict[str, Any]]
    compatibility_score: float
    issues: List[str]
    created_at: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "mapping_id": self.mapping_id,
            "source_schema": self.source_schema.to_dict(),
            "target_schema": self.target_schema.to_dict(),
            "column_mappings": self.column_mappings,
            "transformations": self.transformations,
            "compatibility_score": self.compatibility_score,
            "issues": self.issues,
            "created_at": self.created_at.isoformat()
        }


class SchemaAnalyzer:
    """Analyzes and processes schema information."""
    
    TYPE_MAPPINGS = {
        # MySQL to standard types
        "int": DataType.INTEGER,
        "integer": DataType.INTEGER,
        "bigint": DataType.BIGINT,
        "decimal": DataType.DECIMAL,
        "numeric": DataType.DECIMAL,
        "float": DataType.FLOAT,
        "double": DataType.FLOAT,
        "boolean": DataType.BOOLEAN,
        "bool": DataType.BOOLEAN,
        "varchar": DataType.STRING,
        "char": DataType.STRING,
        "text": DataType.TEXT,
        "date": DataType.DATE,
        "datetime": DataType.DATETIME,
        "timestamp": DataType.TIMESTAMP,
        "json": DataType.JSON,
        "blob": DataType.BINARY,
        
        # PostgreSQL types
        "smallint": DataType.INTEGER,
        "serial": DataType.INTEGER,
        "bigserial": DataType.BIGINT,
        "real": DataType.FLOAT,
        "money": DataType.DECIMAL,
        "bytea": DataType.BINARY,
        
        # Snowflake types
        "number": DataType.DECIMAL,
        "variant": DataType.JSON,
        "array": DataType.JSON,
        "object": DataType.JSON,
    }
    
    @classmethod
    def normalize_type(cls, raw_type: str) -> DataType:
        """Normalize a raw database type to standard type."""
        # Clean the type string
        clean_type = raw_type.lower().strip()
        
        # Handle types with parameters like VARCHAR(50)
        if "(" in clean_type:
            clean_type = clean_type.split("(")[0]
        
        return cls.TYPE_MAPPINGS.get(clean_type, DataType.UNKNOWN)
    
    @classmethod
    def parse_column_from_discovery(cls, discovery_column: Dict[str, Any]) -> ColumnSchema:
        """Parse column schema from discovery agent output."""
        return ColumnSchema(
            name=discovery_column.get("name", ""),
            data_type=cls.normalize_type(discovery_column.get("type", "")),
            nullable=discovery_column.get("nullable", True),
            max_length=cls._extract_length(discovery_column.get("type", "")),
            precision=cls._extract_precision(discovery_column.get("type", "")),
            scale=cls._extract_scale(discovery_column.get("type", "")),
            default_value=discovery_column.get("default"),
            is_primary_key=discovery_column.get("primary_key", False),
            is_foreign_key=discovery_column.get("foreign_key") is not None,
            foreign_key_reference=discovery_column.get("foreign_key"),
            constraints=[]
        )
    
    @classmethod
    def _extract_length(cls, type_str: str) -> Optional[int]:
        """Extract length from type string like VARCHAR(50)."""
        if "(" in type_str and ")" in type_str:
            try:
                params = type_str.split("(")[1].split(")")[0]
                if "," not in params:
                    return int(params)
            except (ValueError, IndexError):
                pass
        return None
    
    @classmethod
    def _extract_precision(cls, type_str: str) -> Optional[int]:
        """Extract precision from type string like DECIMAL(10,2)."""
        if "(" in type_str and "," in type_str and ")" in type_str:
            try:
                params = type_str.split("(")[1].split(")")[0]
                precision, _ = params.split(",")
                return int(precision.strip())
            except (ValueError, IndexError):
                pass
        return None
    
    @classmethod
    def _extract_scale(cls, type_str: str) -> Optional[int]:
        """Extract scale from type string like DECIMAL(10,2)."""
        if "(" in type_str and "," in type_str and ")" in type_str:
            try:
                params = type_str.split("(")[1].split(")")[0]
                _, scale = params.split(",")
                return int(scale.strip())
            except (ValueError, IndexError):
                pass
        return None


class SchemaMatcher:
    """Matches schemas between source and target."""
    
    @classmethod
    def create_mapping(
        cls,
        source_schema: TableSchema,
        target_schema: TableSchema,
        mapping_id: str
    ) -> SchemaMapping:
        """Create a schema mapping between source and target."""
        
        # Perform column matching
        column_mappings = cls._match_columns(source_schema, target_schema)
        
        # Identify required transformations
        transformations = cls._identify_transformations(
            source_schema, target_schema, column_mappings
        )
        
        # Calculate compatibility score
        compatibility_score = cls._calculate_compatibility_score(
            source_schema, target_schema, column_mappings
        )
        
        # Identify issues
        issues = cls._identify_issues(
            source_schema, target_schema, column_mappings, transformations
        )
        
        return SchemaMapping(
            mapping_id=mapping_id,
            source_schema=source_schema,
            target_schema=target_schema,
            column_mappings=column_mappings,
            transformations=transformations,
            compatibility_score=compatibility_score,
            issues=issues,
            created_at=datetime.utcnow()
        )
    
    @classmethod
    def _match_columns(
        cls, 
        source_schema: TableSchema, 
        target_schema: TableSchema
    ) -> Dict[str, str]:
        """Match columns between source and target schemas."""
        mappings = {}
        
        # Exact name matches (case-insensitive)
        source_cols = {col.name.lower(): col.name for col in source_schema.columns}
        target_cols = {col.name.lower(): col.name for col in target_schema.columns}
        
        for source_lower, source_name in source_cols.items():
            if source_lower in target_cols:
                mappings[source_name] = target_cols[source_lower]
        
        # Fuzzy matching for remaining columns
        remaining_source = [col for col in source_schema.columns 
                          if col.name not in mappings]
        remaining_target = [col for col in target_schema.columns 
                          if col.name not in mappings.values()]
        
        for source_col in remaining_source:
            best_match = cls._find_best_column_match(source_col, remaining_target)
            if best_match:
                mappings[source_col.name] = best_match.name
                remaining_target.remove(best_match)
        
        return mappings
    
    @classmethod
    def _find_best_column_match(
        cls, 
        source_col: ColumnSchema, 
        target_candidates: List[ColumnSchema]
    ) -> Optional[ColumnSchema]:
        """Find the best matching target column for a source column."""
        best_score = 0
        best_match = None
        
        for target_col in target_candidates:
            score = cls._calculate_column_similarity(source_col, target_col)
            if score > best_score and score > 0.6:  # Minimum threshold
                best_score = score
                best_match = target_col
        
        return best_match
    
    @classmethod
    def _calculate_column_similarity(
        cls, 
        source_col: ColumnSchema, 
        target_col: ColumnSchema
    ) -> float:
        """Calculate similarity score between two columns."""
        score = 0.0
        
        # Name similarity (basic string matching)
        name_similarity = cls._string_similarity(source_col.name, target_col.name)
        score += name_similarity * 0.4
        
        # Type compatibility
        if source_col.data_type == target_col.data_type:
            score += 0.3
        elif cls._are_compatible_types(source_col.data_type, target_col.data_type):
            score += 0.2
        
        # Nullable compatibility
        if source_col.nullable == target_col.nullable:
            score += 0.1
        
        # Key type compatibility
        if source_col.is_primary_key == target_col.is_primary_key:
            score += 0.1
        
        # Length compatibility for string types
        if (source_col.data_type in [DataType.STRING, DataType.TEXT] and
            target_col.data_type in [DataType.STRING, DataType.TEXT]):
            if source_col.max_length and target_col.max_length:
                if target_col.max_length >= source_col.max_length:
                    score += 0.1
        
        return min(score, 1.0)
    
    @classmethod
    def _string_similarity(cls, str1: str, str2: str) -> float:
        """Calculate string similarity (simplified Jaccard similarity)."""
        if not str1 or not str2:
            return 0.0
        
        str1_lower = str1.lower()
        str2_lower = str2.lower()
        
        if str1_lower == str2_lower:
            return 1.0
        
        # Check if one is contained in the other
        if str1_lower in str2_lower or str2_lower in str1_lower:
            return 0.8
        
        # Simple character overlap
        set1 = set(str1_lower)
        set2 = set(str2_lower)
        intersection = set1.intersection(set2)
        union = set1.union(set2)
        
        return len(intersection) / len(union) if union else 0.0
    
    @classmethod
    def _are_compatible_types(cls, type1: DataType, type2: DataType) -> bool:
        """Check if two data types are compatible."""
        # Numeric type compatibility
        numeric_types = {DataType.INTEGER, DataType.BIGINT, DataType.DECIMAL, DataType.FLOAT}
        if type1 in numeric_types and type2 in numeric_types:
            return True
        
        # String type compatibility
        string_types = {DataType.STRING, DataType.TEXT}
        if type1 in string_types and type2 in string_types:
            return True
        
        # Date/time compatibility
        datetime_types = {DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP}
        if type1 in datetime_types and type2 in datetime_types:
            return True
        
        return False
    
    @classmethod
    def _identify_transformations(
        cls,
        source_schema: TableSchema,
        target_schema: TableSchema,
        column_mappings: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """Identify required transformations."""
        transformations = []
        
        for source_col_name, target_col_name in column_mappings.items():
            source_col = source_schema.get_column(source_col_name)
            target_col = target_schema.get_column(target_col_name)
            
            if not source_col or not target_col:
                continue
            
            # Type conversion transformations
            if source_col.data_type != target_col.data_type:
                transformations.append({
                    "type": "type_conversion",
                    "source_column": source_col_name,
                    "target_column": target_col_name,
                    "source_type": source_col.data_type.value,
                    "target_type": target_col.data_type.value,
                    "description": f"Convert {source_col.data_type.value} to {target_col.data_type.value}"
                })
            
            # String length adjustments
            if (source_col.data_type == DataType.STRING and 
                target_col.data_type == DataType.STRING):
                if (source_col.max_length and target_col.max_length and
                    source_col.max_length > target_col.max_length):
                    transformations.append({
                        "type": "string_truncation",
                        "source_column": source_col_name,
                        "target_column": target_col_name,
                        "source_length": source_col.max_length,
                        "target_length": target_col.max_length,
                        "description": f"Truncate string from {source_col.max_length} to {target_col.max_length} characters"
                    })
            
            # Nullable constraints
            if source_col.nullable and not target_col.nullable:
                transformations.append({
                    "type": "null_handling",
                    "source_column": source_col_name,
                    "target_column": target_col_name,
                    "description": f"Handle NULL values in {source_col_name} (target doesn't allow NULLs)"
                })
        
        return transformations
    
    @classmethod
    def _calculate_compatibility_score(
        cls,
        source_schema: TableSchema,
        target_schema: TableSchema,
        column_mappings: Dict[str, str]
    ) -> float:
        """Calculate overall compatibility score (0-1)."""
        if not source_schema.columns:
            return 0.0
        
        total_score = 0.0
        total_columns = len(source_schema.columns)
        
        for source_col in source_schema.columns:
            if source_col.name in column_mappings:
                target_col_name = column_mappings[source_col.name]
                target_col = target_schema.get_column(target_col_name)
                if target_col:
                    col_score = cls._calculate_column_similarity(source_col, target_col)
                    total_score += col_score
            # Unmapped columns get 0 score
        
        return total_score / total_columns
    
    @classmethod
    def _identify_issues(
        cls,
        source_schema: TableSchema,
        target_schema: TableSchema,
        column_mappings: Dict[str, str],
        transformations: List[Dict[str, Any]]
    ) -> List[str]:
        """Identify potential issues with the mapping."""
        issues = []
        
        # Unmapped source columns
        unmapped_source = [col.name for col in source_schema.columns 
                          if col.name not in column_mappings]
        if unmapped_source:
            issues.append(f"Unmapped source columns: {', '.join(unmapped_source)}")
        
        # Unmapped target columns (might need default values)
        mapped_target_cols = set(column_mappings.values())
        unmapped_target = [col.name for col in target_schema.columns 
                          if col.name not in mapped_target_cols and not col.nullable]
        if unmapped_target:
            issues.append(f"Required target columns without mapping: {', '.join(unmapped_target)}")
        
        # Primary key issues
        source_pk_mapped = False
        for col in column_mappings.keys():
            col_schema = source_schema.get_column(col)
            if col_schema and col_schema.is_primary_key:
                source_pk_mapped = True
                break
        if source_schema.primary_keys and not source_pk_mapped:
            issues.append("Source primary key columns are not mapped")
        
        # Data loss warnings
        data_loss_transforms = [t for t in transformations 
                               if t["type"] in ["string_truncation", "type_conversion"]]
        if data_loss_transforms:
            issues.append(f"Potential data loss in {len(data_loss_transforms)} transformations")
        
        return issues


class SchemaAgent(BaseAgent):
    """
    Schema Agent responsible for schema mapping and analysis.
    
    Key capabilities:
    - Parse table schemas from discovery results
    - Map source schemas to target schemas
    - Identify required data transformations
    - Calculate schema compatibility scores
    - Report schema analysis results
    """
    
    def __init__(self, agent_id: str = "schema_main", config: Optional[Dict[str, Any]] = None):
        super().__init__(
            agent_id=agent_id,
            agent_type=AgentType.SCHEMA,
            name="SchemaAgent",
            config=config
        )
        
        # Schema-specific state
        self.parsed_schemas: Dict[str, TableSchema] = {}
        self.schema_mappings: Dict[str, SchemaMapping] = {}
        self.analyzer = SchemaAnalyzer()
        self.matcher = SchemaMatcher()
        
        # Register message handlers
        self.register_message_handlers()
        
        self.logger.info("Schema Agent initialized")
    
    def register_message_handlers(self) -> None:
        """Register handlers for different message types."""
        self.register_message_handler("task_assignment", self.handle_task_assignment)
        self.register_message_handler("parse_schema", self.handle_parse_schema)
        self.register_message_handler("create_mapping", self.handle_create_mapping)
        self.register_message_handler("get_schema", self.handle_get_schema)
    
    async def handle_task_assignment(self, message: Message) -> None:
        """Handle task assignments from the orchestrator."""
        task = message.content.get("task", {})
        session_id = message.content.get("session_id")
        context = message.content.get("context", {})
        
        task_type = task.get("task_type")
        task_id = task.get("task_id")
        
        self.logger.info(f"Received task assignment: {task_type} (ID: {task_id})")
        
        try:
            if task_type == "schema_analysis":
                result = await self._execute_schema_analysis_task(task, context)
            else:
                result = {"error": f"Unknown task type: {task_type}"}
                
            # Send result back to orchestrator
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="task_complete",
                content={
                    "session_id": session_id,
                    "task_id": task_id,
                    "result": result
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error executing task {task_id}: {e}")
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="task_error",
                content={
                    "session_id": session_id,
                    "task_id": task_id,
                    "error": str(e)
                }
            )
    
    async def handle_parse_schema(self, message: Message) -> None:
        """Handle schema parsing requests."""
        table_info = message.content.get("table_info", {})
        
        try:
            schema = await self._parse_table_schema(table_info)
            
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="schema_parsed",
                content={
                    "schema": schema.to_dict()
                }
            )
            
        except Exception as e:
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="schema_parse_error",
                content={
                    "error": str(e),
                    "table_info": table_info
                }
            )
    
    async def handle_create_mapping(self, message: Message) -> None:
        """Handle schema mapping creation requests."""
        source_schema_id = message.content.get("source_schema_id")
        target_schema_id = message.content.get("target_schema_id")
        
        try:
            if (source_schema_id not in self.parsed_schemas or 
                target_schema_id not in self.parsed_schemas):
                raise ValueError("Source or target schema not found")
            
            source_schema = self.parsed_schemas[source_schema_id]
            target_schema = self.parsed_schemas[target_schema_id]
            
            mapping_id = f"mapping_{source_schema_id}_to_{target_schema_id}"
            mapping = self.matcher.create_mapping(
                source_schema, target_schema, mapping_id
            )
            
            self.schema_mappings[mapping_id] = mapping
            
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="mapping_created",
                content={
                    "mapping": mapping.to_dict()
                }
            )
            
        except Exception as e:
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="mapping_error",
                content={
                    "error": str(e),
                    "source_schema_id": source_schema_id,
                    "target_schema_id": target_schema_id
                }
            )
    
    async def handle_get_schema(self, message: Message) -> None:
        """Handle schema retrieval requests."""
        schema_id = message.content.get("schema_id")
        
        if schema_id in self.parsed_schemas:
            schema = self.parsed_schemas[schema_id]
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="schema_response",
                content={
                    "schema": schema.to_dict()
                }
            )
        else:
            await self.send_message(
                receiver_id=message.sender_id,
                message_type="schema_not_found",
                content={
                    "error": f"Schema {schema_id} not found",
                    "schema_id": schema_id
                }
            )
    
    async def _execute_schema_analysis_task(
        self, 
        task: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a schema analysis task."""
        parameters = task.get("parameters", {})
        sources = parameters.get("sources", [])
        
        analysis_results = []
        
        # Look for discovery results in context
        discovery_results = context.get("discovery_results", [])
        
        for source_name in sources:
            try:
                # Find corresponding discovery result
                source_discovery = None
                for discovery_result in discovery_results:
                    if discovery_result.get("source_name") == source_name:
                        source_discovery = discovery_result
                        break
                
                if not source_discovery:
                    # Create mock schema analysis for sources without discovery
                    analysis_results.append({
                        "source_name": source_name,
                        "status": "no_discovery_data",
                        "message": "No discovery data available for schema analysis"
                    })
                    continue
                
                # Parse schemas from analyzed tables
                schemas = []
                analyzed_tables = source_discovery.get("analyzed_tables", [])
                
                for table_info in analyzed_tables:
                    schema = await self._parse_table_schema(table_info)
                    schemas.append(schema.to_dict())
                
                analysis_results.append({
                    "source_name": source_name,
                    "status": "analyzed",
                    "schemas": schemas,
                    "total_schemas": len(schemas)
                })
                
            except Exception as e:
                self.logger.error(f"Error analyzing schema for {source_name}: {e}")
                analysis_results.append({
                    "source_name": source_name,
                    "status": "error",
                    "error": str(e)
                })
        
        return {
            "task_type": "schema_analysis",
            "analyzed_sources": analysis_results,
            "total_sources": len(analysis_results),
            "successful_analyses": len([r for r in analysis_results if r["status"] == "analyzed"])
        }
    
    async def _parse_table_schema(self, table_info: Dict[str, Any]) -> TableSchema:
        """Parse table schema from discovery result."""
        # Simulate parsing delay
        await asyncio.sleep(0.2)
        
        table_name = table_info.get("table_name", "unknown_table")
        source_id = table_info.get("source_id", "unknown_source")
        
        # Parse columns
        columns = []
        raw_columns = table_info.get("columns", [])
        
        for raw_col in raw_columns:
            column = self.analyzer.parse_column_from_discovery(raw_col)
            columns.append(column)
        
        # Create table schema
        schema = TableSchema(
            table_name=table_name,
            source_id=source_id,
            columns=columns
        )
        
        # Store the parsed schema
        schema_id = f"{source_id}_{table_name}"
        self.parsed_schemas[schema_id] = schema
        
        self.logger.info(
            f"Parsed schema for {table_name} from {source_id}: "
            f"{len(columns)} columns, {len(schema.primary_keys or [])} primary keys"
        )
        
        return schema
    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a generic task assigned to this agent."""
        task_type = task.get("type", "unknown")
        
        if task_type == "parse_schemas":
            table_infos = task.get("table_infos", [])
            results = []
            
            for table_info in table_infos:
                try:
                    schema = await self._parse_table_schema(table_info)
                    results.append(schema.to_dict())
                except Exception as e:
                    results.append({"error": str(e), "table_info": table_info})
            
            return {"results": results}
        
        elif task_type == "create_mappings":
            mappings_config = task.get("mappings", [])
            results = []
            
            for mapping_config in mappings_config:
                try:
                    source_id = mapping_config.get("source_schema_id")
                    target_id = mapping_config.get("target_schema_id")
                    
                    if source_id not in self.parsed_schemas or target_id not in self.parsed_schemas:
                        results.append({"error": "Schema not found", "mapping_config": mapping_config})
                        continue
                    
                    source_schema = self.parsed_schemas[source_id]
                    target_schema = self.parsed_schemas[target_id]
                    
                    mapping_id = f"mapping_{source_id}_to_{target_id}"
                    mapping = self.matcher.create_mapping(source_schema, target_schema, mapping_id)
                    
                    self.schema_mappings[mapping_id] = mapping
                    results.append(mapping.to_dict())
                    
                except Exception as e:
                    results.append({"error": str(e), "mapping_config": mapping_config})
            
            return {"results": results}
        
        else:
            return {"error": f"Unknown task type: {task_type}"}
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get a summary of all parsed schemas and mappings."""
        return {
            "total_schemas": len(self.parsed_schemas),
            "total_mappings": len(self.schema_mappings),
            "schemas_by_source": self._get_schemas_by_source(),
            "average_compatibility_score": self._get_average_compatibility(),
            "last_analysis": max(
                [datetime.fromisoformat(schema.to_dict()["created_at"]) 
                 for schema in self.schema_mappings.values()],
                default=None
            )
        }
    
    def _get_schemas_by_source(self) -> Dict[str, int]:
        """Get count of schemas by source."""
        source_counts = {}
        for schema in self.parsed_schemas.values():
            source_id = schema.source_id
            source_counts[source_id] = source_counts.get(source_id, 0) + 1
        return source_counts
    
    def _get_average_compatibility(self) -> float:
        """Get average compatibility score across all mappings."""
        if not self.schema_mappings:
            return 0.0
        
        total_score = sum(mapping.compatibility_score for mapping in self.schema_mappings.values())
        return total_score / len(self.schema_mappings)
