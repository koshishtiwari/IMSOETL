"""
Execution Agent - Handles the execution of data pipelines and transformations.

This agent is responsible for:
- Executing data pipeline tasks using multiple engines
- Managing execution environments (Pandas, DuckDB, Spark)
- Coordinating with external systems
- Handling execution status and monitoring
- Managing parallel and distributed execution
- Providing intelligent engine selection
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from enum import Enum
import json
import subprocess
import tempfile
import os

from ..core.base_agent import BaseAgent, AgentType
from ..engines.manager import ExecutionEngineManager, ExecutionEngineType


class ExecutionStatus(str, Enum):
    """Execution status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class ExecutionEnvironment(str, Enum):
    """Execution environment types."""
    LOCAL = "local"
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    SPARK = "spark"
    CLOUD = "cloud"


class ExecutionTask:
    """Represents an execution task."""
    
    def __init__(
        self,
        task_id: str,
        task_name: str,
        task_type: str,
        command: str,
        environment: ExecutionEnvironment = ExecutionEnvironment.LOCAL,
        dependencies: Optional[List[str]] = None,
        parameters: Optional[Dict[str, Any]] = None
    ):
        self.task_id = task_id
        self.task_name = task_name
        self.task_type = task_type
        self.command = command
        self.environment = environment
        self.dependencies = dependencies or []
        self.parameters = parameters or {}
        self.status = ExecutionStatus.PENDING
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error_message: Optional[str] = None
        self.output: Optional[str] = None
        self.process_id: Optional[int] = None
        self.retry_count = 0
        self.max_retries = 3
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary."""
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "task_type": self.task_type,
            "command": self.command,
            "environment": self.environment.value,
            "dependencies": self.dependencies,
            "parameters": self.parameters,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "output": self.output,
            "process_id": self.process_id,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries
        }
        
    @property
    def duration(self) -> Optional[float]:
        """Get task duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class ExecutionPipeline:
    """Represents a pipeline of execution tasks."""
    
    def __init__(
        self,
        pipeline_id: str,
        pipeline_name: str,
        tasks: List[ExecutionTask],
        execution_mode: str = "sequential"
    ):
        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_name
        self.tasks = {task.task_id: task for task in tasks}
        self.execution_mode = execution_mode  # sequential, parallel, dag
        self.status = ExecutionStatus.PENDING
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.current_task: Optional[str] = None
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert pipeline to dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "tasks": [task.to_dict() for task in self.tasks.values()],
            "execution_mode": self.execution_mode,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "current_task": self.current_task
        }
        
    def get_ready_tasks(self) -> List[ExecutionTask]:
        """Get tasks that are ready to execute (dependencies satisfied)."""
        ready_tasks = []
        
        for task in self.tasks.values():
            if task.status == ExecutionStatus.PENDING:
                # Check if all dependencies are completed
                dependencies_met = all(
                    self.tasks[dep_id].status == ExecutionStatus.COMPLETED
                    for dep_id in task.dependencies
                    if dep_id in self.tasks
                )
                if dependencies_met:
                    ready_tasks.append(task)
                    
        return ready_tasks
        
    @property
    def is_completed(self) -> bool:
        """Check if all tasks in pipeline are completed."""
        return all(
            task.status in [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED]
            for task in self.tasks.values()
        )
        
    @property
    def has_failures(self) -> bool:
        """Check if any tasks have failed."""
        return any(task.status == ExecutionStatus.FAILED for task in self.tasks.values())


class ExecutionAgent(BaseAgent):
    """Agent responsible for executing data pipelines and transformations."""
    
    def __init__(self, agent_id: str = "execution_agent"):
        super().__init__(agent_id, AgentType.EXECUTION)
        self.active_tasks: Dict[str, ExecutionTask] = {}
        self.active_pipelines: Dict[str, ExecutionPipeline] = {}
        self.execution_history: List[Dict[str, Any]] = []
        self.execution_engine_manager: Optional[ExecutionEngineManager] = None
        self.supported_environments = {
            ExecutionEnvironment.LOCAL: self._execute_local,
            ExecutionEnvironment.DOCKER: self._execute_docker,
            ExecutionEnvironment.SPARK: self._execute_spark
        }
        self.max_concurrent_tasks = 5
        
    async def initialize(self):
        """Initialize the execution agent."""
        await self._setup_execution_environments()
        await self._load_execution_templates()
        await self._initialize_execution_engines()
        self.logger.info("ExecutionAgent initialized successfully")
        
    async def _initialize_execution_engines(self):
        """Initialize the execution engine manager."""
        try:
            self.execution_engine_manager = ExecutionEngineManager(self.config)
            await self.execution_engine_manager.initialize()
            self.logger.info("ExecutionEngineManager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize ExecutionEngineManager: {e}")
            self.execution_engine_manager = None
        
    async def _setup_execution_environments(self):
        """Setup and validate execution environments."""
        self.environment_status = {}
        
        # Check local environment
        self.environment_status[ExecutionEnvironment.LOCAL] = True
        
        # Check Docker availability
        try:
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, 
                                  text=True, 
                                  timeout=10)
            self.environment_status[ExecutionEnvironment.DOCKER] = result.returncode == 0
        except Exception:
            self.environment_status[ExecutionEnvironment.DOCKER] = False
            
        # Check Spark availability (simplified check)
        self.environment_status[ExecutionEnvironment.SPARK] = False  # Assume not available
        
        self.logger.info(f"Environment status: {self.environment_status}")
        
    async def _load_execution_templates(self):
        """Load execution templates for common tasks."""
        self.execution_templates = {
            "sql_query": {
                "command_template": "python -c \"import sqlite3; conn = sqlite3.connect('{database}'); cursor = conn.cursor(); cursor.execute('{query}'); print(cursor.fetchall()); conn.close()\"",
                "parameters": ["database", "query"],
                "environment": ExecutionEnvironment.LOCAL
            },
            "python_script": {
                "command_template": "python {script_path}",
                "parameters": ["script_path"],
                "environment": ExecutionEnvironment.LOCAL
            },
            "data_transformation": {
                "command_template": "python -m imsoetl.transformations.{transformation_type} --input {input_file} --output {output_file}",
                "parameters": ["transformation_type", "input_file", "output_file"],
                "environment": ExecutionEnvironment.LOCAL
            },
            "docker_container": {
                "command_template": "docker run --rm -v {volume_mapping} {image} {container_command}",
                "parameters": ["volume_mapping", "image", "container_command"],
                "environment": ExecutionEnvironment.DOCKER
            }
        }
        
    def _extract_table_name_from_sql(self, sql: str) -> Optional[str]:
        """Extract table name from SQL query for parameter mapping.
        
        This method attempts to extract table names from common SQL patterns.
        It's used to map source data files to table names in SQL queries.
        """
        if not sql or not isinstance(sql, str):
            return None
            
        # Convert to lowercase for pattern matching
        sql_lower = sql.lower().strip()
        
        # Common SQL patterns to extract table names
        import re
        
        # Pattern for FROM clause: SELECT ... FROM table_name
        from_pattern = r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        from_match = re.search(from_pattern, sql_lower)
        if from_match:
            return from_match.group(1)
            
        # Pattern for INSERT INTO: INSERT INTO table_name
        insert_pattern = r'\binsert\s+into\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        insert_match = re.search(insert_pattern, sql_lower)
        if insert_match:
            return insert_match.group(1)
            
        # Pattern for UPDATE: UPDATE table_name SET
        update_pattern = r'\bupdate\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+set'
        update_match = re.search(update_pattern, sql_lower)
        if update_match:
            return update_match.group(1)
            
        # Pattern for DELETE FROM: DELETE FROM table_name
        delete_pattern = r'\bdelete\s+from\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        delete_match = re.search(delete_pattern, sql_lower)
        if delete_match:
            return delete_match.group(1)
            
        # Pattern for JOIN clauses: ... JOIN table_name ON
        join_pattern = r'\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+on'
        join_match = re.search(join_pattern, sql_lower)
        if join_match:
            return join_match.group(1)
            
        # If no patterns match, return None
        return None
        
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task assigned to this agent."""
        task_type = task.get("type")
        
        try:
            if task_type == "execute_task":
                return await self._handle_execute_task(task)
            elif task_type == "execute_pipeline":
                return await self._handle_execute_pipeline(task)
            elif task_type == "cancel_execution":
                return await self._handle_cancel_execution(task)
            elif task_type == "get_status":
                return await self._handle_get_status(task)
            elif task_type == "retry_task":
                return await self._handle_retry_task(task)
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
            
    async def _handle_execute_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single task."""
        task_config = task.get("task_config", {})
        
        # Check if this is a direct data task call from orchestrator
        if "type" in task_config and "data" in task_config:
            try:
                result = await self.execute_data_task(task_config)
                return {
                    "success": result.get("success", False),
                    "data": result.get("data"),
                    "execution_time": result.get("execution_time", 0),
                    "rows_processed": result.get("rows_processed", 0),
                    "error": result.get("error"),
                    "agent": self.agent_id,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                return {
                    "success": False,
                    "error": str(e),
                    "agent": self.agent_id,
                    "timestamp": datetime.now().isoformat()
                }
        
        # Handle traditional execution tasks
        task_type = task_config.get("task_type", "custom")
        
        # Handle data processing tasks using execution engines
        if task_type in ["sql_query", "data_transformation", "data_load", "batch_processing"]:
            try:
                result = await self.execute_data_task(task_config)
                return {
                    "success": result.get("success", False),
                    "data_result": result,
                    "agent": self.agent_id,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                return {
                    "success": False,
                    "error": str(e),
                    "agent": self.agent_id,
                    "timestamp": datetime.now().isoformat()
                }
        
        # Handle traditional execution tasks
        execution_task = ExecutionTask(
            task_id=task_config.get("task_id", f"task_{datetime.now().timestamp()}"),
            task_name=task_config.get("task_name", "Unnamed Task"),
            task_type=task_type,
            command=task_config.get("command", ""),
            environment=ExecutionEnvironment(task_config.get("environment", "local")),
            parameters=task_config.get("parameters", {})
        )
        
        result = await self.execute_task(execution_task)
        
        return {
            "success": True,
            "task_result": result,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_execute_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a pipeline of tasks."""
        pipeline_config = task.get("pipeline_config", {})
        
        tasks = []
        for task_config in pipeline_config.get("tasks", []):
            execution_task = ExecutionTask(
                task_id=task_config.get("task_id", f"task_{len(tasks)}"),
                task_name=task_config.get("task_name", f"Task {len(tasks)}"),
                task_type=task_config.get("task_type", "custom"),
                command=task_config.get("command", ""),
                environment=ExecutionEnvironment(task_config.get("environment", "local")),
                dependencies=task_config.get("dependencies", []),
                parameters=task_config.get("parameters", {})
            )
            tasks.append(execution_task)
            
        pipeline = ExecutionPipeline(
            pipeline_id=pipeline_config.get("pipeline_id", f"pipeline_{datetime.now().timestamp()}"),
            pipeline_name=pipeline_config.get("pipeline_name", "Unnamed Pipeline"),
            tasks=tasks,
            execution_mode=pipeline_config.get("execution_mode", "sequential")
        )
        
        result = await self.execute_pipeline(pipeline)
        
        return {
            "success": True,
            "pipeline_result": result,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_cancel_execution(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Cancel a running execution."""
        execution_id = task.get("execution_id", "")
        
        cancelled = await self.cancel_execution(execution_id)
        
        return {
            "success": cancelled,
            "execution_id": execution_id,
            "cancelled": cancelled,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_get_status(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Get execution status."""
        execution_id = task.get("execution_id")
        
        if execution_id:
            status = await self.get_execution_status(execution_id)
        else:
            status = await self.get_all_status()
            
        return {
            "success": True,
            "status": status,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_retry_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Retry a failed task."""
        task_id = task.get("task_id", "")
        
        result = await self.retry_task(task_id)
        
        return {
            "success": result.get("success", False),
            "retry_result": result,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def execute_task(self, task: ExecutionTask) -> Dict[str, Any]:
        """Execute a single task."""
        self.logger.info(f"Starting execution of task: {task.task_id}")
        
        # Add to active tasks
        self.active_tasks[task.task_id] = task
        
        # Update task status
        task.status = ExecutionStatus.RUNNING
        task.started_at = datetime.now()
        
        try:
            # Choose execution method based on environment
            if task.environment in self.supported_environments:
                result = await self.supported_environments[task.environment](task)
            else:
                raise ValueError(f"Unsupported execution environment: {task.environment}")
                
            # Update task status on success
            task.status = ExecutionStatus.COMPLETED
            task.completed_at = datetime.now()
            task.output = result.get("output", "")
            
            result_dict = {
                "success": True,
                "task_id": task.task_id,
                "status": task.status.value,
                "output": task.output,
                "duration": task.duration,
                "completed_at": task.completed_at.isoformat()
            }
            
        except Exception as e:
            # Update task status on failure
            task.status = ExecutionStatus.FAILED
            task.completed_at = datetime.now()
            task.error_message = str(e)
            
            result_dict = {
                "success": False,
                "task_id": task.task_id,
                "status": task.status.value,
                "error": task.error_message,
                "duration": task.duration,
                "completed_at": task.completed_at.isoformat()
            }
            
        finally:
            # Add to execution history
            self.execution_history.append(task.to_dict())
            # Remove from active tasks
            if task.task_id in self.active_tasks:
                del self.active_tasks[task.task_id]
                
        self.logger.info(f"Completed execution of task: {task.task_id} with status: {task.status}")
        return result_dict
        
    async def execute_pipeline(self, pipeline: ExecutionPipeline) -> Dict[str, Any]:
        """Execute a pipeline of tasks."""
        self.logger.info(f"Starting execution of pipeline: {pipeline.pipeline_id}")
        
        # Add to active pipelines
        self.active_pipelines[pipeline.pipeline_id] = pipeline
        
        # Update pipeline status
        pipeline.status = ExecutionStatus.RUNNING
        pipeline.started_at = datetime.now()
        
        try:
            if pipeline.execution_mode == "sequential":
                result = await self._execute_sequential(pipeline)
            elif pipeline.execution_mode == "parallel":
                result = await self._execute_parallel(pipeline)
            elif pipeline.execution_mode == "dag":
                result = await self._execute_dag(pipeline)
            else:
                raise ValueError(f"Unsupported execution mode: {pipeline.execution_mode}")
                
            # Update pipeline status
            pipeline.status = ExecutionStatus.COMPLETED if not pipeline.has_failures else ExecutionStatus.FAILED
            pipeline.completed_at = datetime.now()
            
            result_dict = {
                "success": not pipeline.has_failures,
                "pipeline_id": pipeline.pipeline_id,
                "status": pipeline.status.value,
                "completed_tasks": len([t for t in pipeline.tasks.values() if t.status == ExecutionStatus.COMPLETED]),
                "failed_tasks": len([t for t in pipeline.tasks.values() if t.status == ExecutionStatus.FAILED]),
                "total_tasks": len(pipeline.tasks),
                "execution_details": result
            }
            
        except Exception as e:
            pipeline.status = ExecutionStatus.FAILED
            pipeline.completed_at = datetime.now()
            
            result_dict = {
                "success": False,
                "pipeline_id": pipeline.pipeline_id,
                "status": pipeline.status.value,
                "error": str(e)
            }
            
        finally:
            # Remove from active pipelines
            if pipeline.pipeline_id in self.active_pipelines:
                del self.active_pipelines[pipeline.pipeline_id]
                
        self.logger.info(f"Completed execution of pipeline: {pipeline.pipeline_id} with status: {pipeline.status}")
        return result_dict
        
    async def _execute_local(self, task: ExecutionTask) -> Dict[str, Any]:
        """Execute task in local environment."""
        self.logger.debug(f"Executing task locally: {task.command}")
        
        try:
            # Create a temporary file for script execution if needed
            if task.task_type == "python_script" and "python -c" not in task.command:
                # For complex scripts, create a temporary file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                    f.write(task.command)
                    temp_script = f.name
                
                try:
                    result = subprocess.run(
                        ['python', temp_script],
                        capture_output=True,
                        text=True,
                        timeout=300,  # 5 minute timeout
                        cwd=task.parameters.get('working_directory')
                    )
                finally:
                    os.unlink(temp_script)
            else:
                # Execute command directly
                result = subprocess.run(
                    task.command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=300,
                    cwd=task.parameters.get('working_directory')
                )
                
            task.process_id = result.returncode
            
            if result.returncode == 0:
                return {
                    "success": True,
                    "output": result.stdout,
                    "stderr": result.stderr,
                    "return_code": result.returncode
                }
            else:
                raise RuntimeError(f"Command failed with return code {result.returncode}: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            raise RuntimeError("Command execution timed out")
        except Exception as e:
            raise RuntimeError(f"Local execution failed: {str(e)}")
            
    async def _execute_docker(self, task: ExecutionTask) -> Dict[str, Any]:
        """Execute task in Docker environment."""
        if not self.environment_status.get(ExecutionEnvironment.DOCKER, False):
            raise RuntimeError("Docker environment is not available")
            
        self.logger.debug(f"Executing task in Docker: {task.command}")
        
        try:
            result = subprocess.run(
                task.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout for Docker
            )
            
            if result.returncode == 0:
                return {
                    "success": True,
                    "output": result.stdout,
                    "stderr": result.stderr,
                    "return_code": result.returncode
                }
            else:
                raise RuntimeError(f"Docker command failed with return code {result.returncode}: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            raise RuntimeError("Docker execution timed out")
        except Exception as e:
            raise RuntimeError(f"Docker execution failed: {str(e)}")
            
    async def _execute_spark(self, task: ExecutionTask) -> Dict[str, Any]:
        """Execute task in Spark environment."""
        # Simplified Spark execution - would need proper Spark setup
        self.logger.debug(f"Executing task in Spark: {task.command}")
        
        # For now, fall back to local execution
        return await self._execute_local(task)
        
    async def _execute_sequential(self, pipeline: ExecutionPipeline) -> Dict[str, Any]:
        """Execute pipeline tasks sequentially."""
        results = []
        
        # Sort tasks by dependencies (simplified topological sort)
        ordered_tasks = self._topological_sort(pipeline.tasks)
        
        for task in ordered_tasks:
            pipeline.current_task = task.task_id
            result = await self.execute_task(task)
            results.append(result)
            
            # Stop execution if a task fails
            if not result.get("success", False):
                self.logger.warning(f"Task {task.task_id} failed, stopping pipeline execution")
                break
                
        return {
            "execution_mode": "sequential",
            "task_results": results,
            "completed_tasks": len([r for r in results if r.get("success", False)]),
            "failed_tasks": len([r for r in results if not r.get("success", True)])
        }
        
    async def _execute_parallel(self, pipeline: ExecutionPipeline) -> Dict[str, Any]:
        """Execute pipeline tasks in parallel where possible."""
        results = []
        remaining_tasks = set(pipeline.tasks.keys())
        
        while remaining_tasks and len(self.active_tasks) < self.max_concurrent_tasks:
            # Get tasks that can be executed (dependencies satisfied)
            ready_tasks = []
            for task_id in remaining_tasks:
                task = pipeline.tasks[task_id]
                if all(dep_id not in remaining_tasks for dep_id in task.dependencies):
                    ready_tasks.append(task)
                    
            if not ready_tasks:
                # Wait for some tasks to complete
                await asyncio.sleep(1)
                continue
                
            # Execute ready tasks concurrently
            concurrent_tasks = ready_tasks[:self.max_concurrent_tasks - len(self.active_tasks)]
            
            # Start tasks
            tasks_futures = []
            for task in concurrent_tasks:
                future = asyncio.create_task(self.execute_task(task))
                tasks_futures.append((task.task_id, future))
                remaining_tasks.remove(task.task_id)
                
            # Wait for completion
            for task_id, future in tasks_futures:
                result = await future
                results.append(result)
                
        return {
            "execution_mode": "parallel",
            "task_results": results,
            "completed_tasks": len([r for r in results if r.get("success", False)]),
            "failed_tasks": len([r for r in results if not r.get("success", True)])
        }
        
    async def _execute_dag(self, pipeline: ExecutionPipeline) -> Dict[str, Any]:
        """Execute pipeline as a DAG (Directed Acyclic Graph)."""
        # For now, use the same logic as parallel execution
        return await self._execute_parallel(pipeline)
        
    def _topological_sort(self, tasks: Dict[str, ExecutionTask]) -> List[ExecutionTask]:
        """Perform topological sort on tasks based on dependencies."""
        # Simplified topological sort
        result = []
        visited = set()
        temp_visited = set()
        
        def visit(task_id: str):
            if task_id in temp_visited:
                raise ValueError("Circular dependency detected")
            if task_id in visited:
                return
                
            temp_visited.add(task_id)
            
            task = tasks.get(task_id)
            if task:
                for dep_id in task.dependencies:
                    if dep_id in tasks:
                        visit(dep_id)
                        
            temp_visited.remove(task_id)
            visited.add(task_id)
            if task:
                result.append(task)
                
        for task_id in tasks:
            if task_id not in visited:
                visit(task_id)
                
        return result
        
    async def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running execution."""
        # Check if it's an active task
        if execution_id in self.active_tasks:
            task = self.active_tasks[execution_id]
            task.status = ExecutionStatus.CANCELLED
            task.completed_at = datetime.now()
            
            # Try to kill the process if it exists
            if task.process_id:
                try:
                    os.kill(task.process_id, 9)  # SIGKILL
                except ProcessLookupError:
                    pass  # Process already terminated
                    
            del self.active_tasks[execution_id]
            return True
            
        # Check if it's an active pipeline
        if execution_id in self.active_pipelines:
            pipeline = self.active_pipelines[execution_id]
            pipeline.status = ExecutionStatus.CANCELLED
            pipeline.completed_at = datetime.now()
            
            # Cancel all running tasks in the pipeline
            for task in pipeline.tasks.values():
                if task.status == ExecutionStatus.RUNNING:
                    task.status = ExecutionStatus.CANCELLED
                    task.completed_at = datetime.now()
                    
            del self.active_pipelines[execution_id]
            return True
            
        return False
        
    async def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """Get status of a specific execution."""
        # Check active tasks
        if execution_id in self.active_tasks:
            return self.active_tasks[execution_id].to_dict()
            
        # Check active pipelines
        if execution_id in self.active_pipelines:
            return self.active_pipelines[execution_id].to_dict()
            
        # Check execution history
        for execution in self.execution_history:
            if execution.get("task_id") == execution_id or execution.get("pipeline_id") == execution_id:
                return execution
                
        return {"error": f"Execution {execution_id} not found"}
        
    async def get_all_status(self) -> Dict[str, Any]:
        """Get status of all executions."""
        return {
            "active_tasks": [task.to_dict() for task in self.active_tasks.values()],
            "active_pipelines": [pipeline.to_dict() for pipeline in self.active_pipelines.values()],
            "execution_history": self.execution_history[-50:],  # Last 50 executions
            "environment_status": self.environment_status
        }
        
    async def retry_task(self, task_id: str) -> Dict[str, Any]:
        """Retry a failed task."""
        # Find task in execution history
        task_dict = None
        for execution in self.execution_history:
            if execution.get("task_id") == task_id:
                task_dict = execution
                break
                
        if not task_dict:
            return {"success": False, "error": f"Task {task_id} not found"}
            
        if task_dict.get("status") != ExecutionStatus.FAILED.value:
            return {"success": False, "error": f"Task {task_id} is not in failed state"}
            
        # Recreate task from history
        task = ExecutionTask(
            task_id=f"{task_id}_retry_{datetime.now().timestamp()}",
            task_name=f"Retry: {task_dict.get('task_name', 'Unknown')}",
            task_type=task_dict.get("task_type", "custom"),
            command=task_dict.get("command", ""),
            environment=ExecutionEnvironment(task_dict.get("environment", "local")),
            parameters=task_dict.get("parameters", {})
        )
        
        task.retry_count = task_dict.get("retry_count", 0) + 1
        
        if task.retry_count > task.max_retries:
            return {"success": False, "error": f"Task {task_id} has exceeded maximum retry attempts"}
            
        # Execute the retry
        result = await self.execute_task(task)
        
        return {
            "success": result.get("success", False),
            "retry_task_id": task.task_id,
            "retry_count": task.retry_count,
            "result": result
        }
    
    async def execute_data_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a data processing task using the execution engine manager."""
        if not self.execution_engine_manager:
            raise RuntimeError("ExecutionEngineManager not initialized")
            
        task_type = task_config.get("type", "")
        data_config = task_config.get("data", {})
        
        try:
            if task_type == "sql_query":
                return await self._execute_sql_query(data_config)
            elif task_type == "data_transformation":
                return await self._execute_data_transformation(data_config)
            elif task_type == "data_load":
                return await self._execute_data_load(data_config)
            elif task_type == "batch_processing":
                return await self._execute_batch_processing(data_config)
            else:
                raise ValueError(f"Unsupported data task type: {task_type}")
                
        except Exception as e:
            self.logger.error(f"Data task execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "task_type": task_type
            }
    
    async def _execute_sql_query(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute SQL query using appropriate engine."""
        if not self.execution_engine_manager:
            raise RuntimeError("ExecutionEngineManager not initialized")
            
        query = config.get("query", "")
        params = config.get("parameters", {}).copy()  # Copy to avoid modifying original
        engine_hint = config.get("engine_hint")
        source = config.get("source")
        
        # Convert string engine hint to enum
        if isinstance(engine_hint, str):
            try:
                from ..engines.manager import ExecutionEngineType
                engine_hint = ExecutionEngineType(engine_hint.lower())
            except (ValueError, AttributeError):
                self.logger.warning(f"Invalid engine hint: {engine_hint}, using default")
                engine_hint = None
        
        self.logger.info(f"Executing SQL query: {query[:100]}...")
        
        # Handle source data - load it and add to params for the engine
        if source:
            if isinstance(source, str):
                # If source is a file path, add it to params with a standard name
                # The table name is extracted from the SQL or defaults to 'data'
                table_name = self._extract_table_name_from_sql(query) or "data"
                params[table_name] = source
                self.logger.debug(f"Added source file to params: {table_name} = {source}")
        
        # Execute query using the engine manager
        result = await self.execution_engine_manager.execute_sql(
            sql=query,
            params=params,
            engine_hint=engine_hint
        )
        
        return {
            "success": result.success if result else False,
            "data": result.data if result else None,
            "metadata": result.metadata if result else {},
            "execution_time": result.execution_time if result else 0,
            "rows_processed": result.rows_processed if result else 0,
            "error": result.error if result and not result.success else None
        }
    
    async def _execute_data_transformation(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data transformation using appropriate engine."""
        if not self.execution_engine_manager:
            raise RuntimeError("ExecutionEngineManager not initialized")
            
        source_data = config.get("source_data")
        transformation_spec = config.get("transformation", {})
        engine_hint = config.get("engine_hint")
        
        # Convert string engine hint to enum
        if isinstance(engine_hint, str):
            try:
                from ..engines.manager import ExecutionEngineType
                engine_hint = ExecutionEngineType(engine_hint.lower())
            except (ValueError, AttributeError):
                self.logger.warning(f"Invalid engine hint: {engine_hint}, using default")
                engine_hint = None
        
        self.logger.info(f"Executing data transformation using execution engine manager")
        
        # Execute transformation using the engine manager
        result = await self.execution_engine_manager.execute_transformation(
            source_data=source_data,
            transformation_spec=transformation_spec,
            engine_hint=engine_hint
        )
        
        return {
            "success": result.success if result else False,
            "data": result.data if result else None,
            "metadata": result.metadata if result else {},
            "execution_time": result.execution_time if result else 0,
            "rows_processed": result.rows_processed if result else 0,
            "error": result.error if result and not result.success else None
        }
    
    async def _execute_data_load(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data loading using appropriate engine."""
        if not self.execution_engine_manager:
            raise RuntimeError("ExecutionEngineManager not initialized")
            
        source_path = config.get("source", "")
        format_type = config.get("format", "auto")
        load_options = config.get("options", {})
        
        self.logger.info(f"Executing data load using execution engine manager")
        
        # Execute load using the engine manager
        result = await self.execution_engine_manager.load_data(
            source=source_path,
            format_type=format_type,
            options=load_options
        )
        
        return {
            "success": result.success if result else False,
            "data": result.data if result else None,
            "metadata": result.metadata if result else {},
            "execution_time": result.execution_time if result else 0,
            "rows_processed": result.rows_processed if result else 0,
            "error": result.error if result and not result.success else None
        }
    
    async def _execute_batch_processing(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute batch processing using appropriate engine."""
        if not self.execution_engine_manager:
            raise RuntimeError("ExecutionEngineManager not initialized")
            
        batch_config = config.get("batch", {})
        data_source = config.get("source", {})
        processing_steps = config.get("steps", [])
        
        self.logger.info(f"Executing batch processing using execution engine manager")
        
        # Execute batch processing steps
        results = []
        total_time = 0
        total_rows = 0
        
        for i, step in enumerate(processing_steps):
            step_name = step.get("name", f"step_{i}")
            self.logger.info(f"Executing batch processing step: {step_name}")
            
            # For batch processing, we'll use data transformation
            step_result = await self.execution_engine_manager.execute_transformation(
                source_data=data_source,
                transformation_spec=step,
                engine_hint=batch_config.get("engine_hint")
            )
            
            results.append({
                "step": step_name,
                "success": step_result.success if step_result else False,
                "rows_processed": step_result.rows_processed if step_result else 0,
                "execution_time": step_result.execution_time if step_result else 0,
                "error": step_result.error if step_result and not step_result.success else None
            })
            
            if step_result and step_result.success:
                total_time += step_result.execution_time or 0
                total_rows += step_result.rows_processed or 0
                
                # Update data source for next step if needed
                if step_result.metadata and step_result.metadata.get("output_location"):
                    data_source = {
                        "type": "file",
                        "path": step_result.metadata["output_location"]
                    }
            else:
                # Stop processing on error
                break
        
        return {
            "success": all(r["success"] for r in results),
            "steps_completed": len([r for r in results if r["success"]]),
            "results": results,
            "total_processing_time": total_time,
            "total_rows_processed": total_rows
        }
    
    async def start(self) -> None:
        """Start the execution agent and initialize engines."""
        await super().start()
        await self.initialize()
