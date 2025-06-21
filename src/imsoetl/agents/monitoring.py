"""
Monitoring Agent - Handles monitoring and observability of data pipelines.

This agent is responsible for:
- Monitoring pipeline execution and performance metrics
- Collecting and analyzing system metrics
- Alerting on anomalies and issues
- Generating monitoring reports and dashboards
- Health checks and system status monitoring
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from datetime import datetime, timedelta
from enum import Enum
import statistics
import json
import time

from ..core.base_agent import BaseAgent, AgentType, Message

from ..core.base_agent import BaseAgent, AgentType


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class MetricType(str, Enum):
    """Types of metrics to monitor."""
    PERFORMANCE = "performance"
    QUALITY = "quality"
    RESOURCE = "resource"
    BUSINESS = "business"
    SYSTEM = "system"


class Alert:
    """Represents a monitoring alert."""
    
    def __init__(
        self,
        alert_id: str,
        alert_name: str,
        severity: AlertSeverity,
        message: str,
        metric_name: str,
        threshold: float,
        current_value: float,
        context: Optional[Dict[str, Any]] = None
    ):
        self.alert_id = alert_id
        self.alert_name = alert_name
        self.severity = severity
        self.message = message
        self.metric_name = metric_name
        self.threshold = threshold
        self.current_value = current_value
        self.context = context or {}
        self.created_at = datetime.now()
        self.acknowledged = False
        self.resolved = False
        self.resolved_at: Optional[datetime] = None
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "alert_name": self.alert_name,
            "severity": self.severity.value,
            "message": self.message,
            "metric_name": self.metric_name,
            "threshold": self.threshold,
            "current_value": self.current_value,
            "context": self.context,
            "created_at": self.created_at.isoformat(),
            "acknowledged": self.acknowledged,
            "resolved": self.resolved,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None
        }


class MonitoringRule:
    """Represents a monitoring rule."""
    
    def __init__(
        self,
        rule_id: str,
        rule_name: str,
        metric_name: str,
        condition: str,  # "greater_than", "less_than", "equals", "not_equals", "in_range", "out_of_range"
        threshold: Union[float, Dict[str, float]],
        severity: AlertSeverity,
        description: str = "",
        enabled: bool = True
    ):
        self.rule_id = rule_id
        self.rule_name = rule_name
        self.metric_name = metric_name
        self.condition = condition
        self.threshold = threshold
        self.severity = severity
        self.description = description
        self.enabled = enabled
        self.created_at = datetime.now()
        self.last_triggered: Optional[datetime] = None
        self.trigger_count = 0
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert rule to dictionary."""
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "metric_name": self.metric_name,
            "condition": self.condition,
            "threshold": self.threshold,
            "severity": self.severity.value,
            "description": self.description,
            "enabled": self.enabled,
            "created_at": self.created_at.isoformat(),
            "last_triggered": self.last_triggered.isoformat() if self.last_triggered else None,
            "trigger_count": self.trigger_count
        }
        
    def evaluate(self, current_value: float) -> bool:
        """Evaluate if the rule condition is met."""
        if not self.enabled:
            return False
            
        if self.condition == "greater_than" and isinstance(self.threshold, (int, float)):
            return current_value > self.threshold
        elif self.condition == "less_than" and isinstance(self.threshold, (int, float)):
            return current_value < self.threshold
        elif self.condition == "equals" and isinstance(self.threshold, (int, float)):
            return current_value == self.threshold
        elif self.condition == "not_equals" and isinstance(self.threshold, (int, float)):
            return current_value != self.threshold
        elif self.condition == "in_range":
            if isinstance(self.threshold, dict):
                min_val = self.threshold.get("min", float('-inf'))
                max_val = self.threshold.get("max", float('inf'))
                return min_val <= current_value <= max_val
        elif self.condition == "out_of_range":
            if isinstance(self.threshold, dict):
                min_val = self.threshold.get("min", float('-inf'))
                max_val = self.threshold.get("max", float('inf'))
                return current_value < min_val or current_value > max_val
                
        return False


class MetricCollector:
    """Collects and stores metrics."""
    
    def __init__(self, metric_name: str, metric_type: MetricType):
        self.metric_name = metric_name
        self.metric_type = metric_type
        self.values: List[Tuple[datetime, float]] = []
        self.max_history = 1000  # Keep last 1000 values
        
    def add_value(self, value: float, timestamp: Optional[datetime] = None):
        """Add a metric value."""
        if timestamp is None:
            timestamp = datetime.now()
            
        self.values.append((timestamp, value))
        
        # Keep only recent values
        if len(self.values) > self.max_history:
            self.values = self.values[-self.max_history:]
            
    def get_latest(self) -> Optional[float]:
        """Get the latest metric value."""
        return self.values[-1][1] if self.values else None
        
    def get_average(self, minutes: int = 5) -> Optional[float]:
        """Get average value over the last N minutes."""
        cutoff = datetime.now() - timedelta(minutes=minutes)
        recent_values = [value for timestamp, value in self.values if timestamp >= cutoff]
        return statistics.mean(recent_values) if recent_values else None
        
    def get_percentile(self, percentile: int, minutes: int = 5) -> Optional[float]:
        """Get percentile value over the last N minutes."""
        cutoff = datetime.now() - timedelta(minutes=minutes)
        recent_values = [value for timestamp, value in self.values if timestamp >= cutoff]
        if recent_values:
            sorted_values = sorted(recent_values)
            index = int(len(sorted_values) * percentile / 100)
            return sorted_values[min(index, len(sorted_values) - 1)]
        return None
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert collector to dictionary."""
        return {
            "metric_name": self.metric_name,
            "metric_type": self.metric_type.value,
            "current_value": self.get_latest(),
            "value_count": len(self.values),
            "last_updated": self.values[-1][0].isoformat() if self.values else None
        }


class MonitoringAgent(BaseAgent):
    """Agent responsible for monitoring and observability."""
    
    def __init__(self, agent_id: str = "monitoring_agent"):
        super().__init__(agent_id, AgentType.MONITORING)
        self.metric_collectors: Dict[str, MetricCollector] = {}
        self.monitoring_rules: Dict[str, MonitoringRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.monitoring_interval = 30  # seconds
        self.is_monitoring_active = False
        self.monitoring_task: Optional[asyncio.Task] = None
        
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
        """Initialize the monitoring agent."""
        await self._setup_default_metrics()
        await self._setup_default_rules()
        await self._start_monitoring()
        self.logger.info("MonitoringAgent initialized successfully")
        
    async def _setup_default_metrics(self):
        """Setup default metric collectors."""
        default_metrics = [
            ("pipeline_execution_time", MetricType.PERFORMANCE),
            ("pipeline_success_rate", MetricType.PERFORMANCE),
            ("data_quality_score", MetricType.QUALITY),
            ("cpu_usage", MetricType.RESOURCE),
            ("memory_usage", MetricType.RESOURCE),
            ("disk_usage", MetricType.RESOURCE),
            ("task_failure_rate", MetricType.SYSTEM),
            ("api_response_time", MetricType.SYSTEM),
            ("records_processed", MetricType.BUSINESS),
            ("error_count", MetricType.SYSTEM)
        ]
        
        for metric_name, metric_type in default_metrics:
            self.metric_collectors[metric_name] = MetricCollector(metric_name, metric_type)
            
    async def _setup_default_rules(self):
        """Setup default monitoring rules."""
        default_rules = [
            MonitoringRule(
                rule_id="high_execution_time",
                rule_name="High Pipeline Execution Time",
                metric_name="pipeline_execution_time",
                condition="greater_than",
                threshold=300.0,  # 5 minutes
                severity=AlertSeverity.MEDIUM,
                description="Pipeline execution time is above acceptable threshold"
            ),
            MonitoringRule(
                rule_id="low_success_rate",
                rule_name="Low Pipeline Success Rate",
                metric_name="pipeline_success_rate",
                condition="less_than",
                threshold=0.95,  # 95%
                severity=AlertSeverity.HIGH,
                description="Pipeline success rate has dropped below acceptable level"
            ),
            MonitoringRule(
                rule_id="poor_data_quality",
                rule_name="Poor Data Quality Score",
                metric_name="data_quality_score",
                condition="less_than",
                threshold=0.90,  # 90%
                severity=AlertSeverity.HIGH,
                description="Data quality score is below acceptable threshold"
            ),
            MonitoringRule(
                rule_id="high_cpu_usage",
                rule_name="High CPU Usage",
                metric_name="cpu_usage",
                condition="greater_than",
                threshold=0.80,  # 80%
                severity=AlertSeverity.MEDIUM,
                description="CPU usage is above acceptable threshold"
            ),
            MonitoringRule(
                rule_id="high_memory_usage",
                rule_name="High Memory Usage",
                metric_name="memory_usage",
                condition="greater_than",
                threshold=0.85,  # 85%
                severity=AlertSeverity.MEDIUM,
                description="Memory usage is above acceptable threshold"
            ),
            MonitoringRule(
                rule_id="high_error_count",
                rule_name="High Error Count",
                metric_name="error_count",
                condition="greater_than",
                threshold=10.0,  # 10 errors in monitoring window
                severity=AlertSeverity.HIGH,
                description="Error count is above acceptable threshold"
            )
        ]
        
        for rule in default_rules:
            self.monitoring_rules[rule.rule_id] = rule
            
    async def _start_monitoring(self):
        """Start the monitoring loop."""
        if not self.is_monitoring_active:
            self.is_monitoring_active = True
            self.monitoring_task = asyncio.create_task(self._monitoring_loop())
            
    async def _monitoring_loop(self):
        """Main monitoring loop."""
        self.logger.info("Starting monitoring loop")
        
        while self.is_monitoring_active:
            try:
                # Collect system metrics
                await self._collect_system_metrics()
                
                # Evaluate monitoring rules
                await self._evaluate_monitoring_rules()
                
                # Clean up old data
                await self._cleanup_old_data()
                
                # Sleep until next monitoring cycle
                await asyncio.sleep(self.monitoring_interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.monitoring_interval)
                
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task assigned to this agent."""
        task_type = task.get("type")
        
        try:
            if task_type == "collect_metric":
                return await self._handle_collect_metric(task)
            elif task_type == "create_rule":
                return await self._handle_create_rule(task)
            elif task_type == "get_metrics":
                return await self._handle_get_metrics(task)
            elif task_type == "get_alerts":
                return await self._handle_get_alerts(task)
            elif task_type == "acknowledge_alert":
                return await self._handle_acknowledge_alert(task)
            elif task_type == "generate_report":
                return await self._handle_generate_report(task)
            elif task_type == "health_check":
                return await self._handle_health_check(task)
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
            
    async def _handle_collect_metric(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Handle metric collection request."""
        metric_name = task.get("metric_name", "")
        metric_value = task.get("metric_value", 0.0)
        
        await self.collect_metric(metric_name, metric_value)
        
        return {
            "success": True,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_create_rule(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Handle monitoring rule creation."""
        rule_config = task.get("rule_config", {})
        
        rule = await self.create_monitoring_rule(rule_config)
        
        return {
            "success": True,
            "rule_created": rule.to_dict(),
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_get_metrics(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Handle metrics retrieval request."""
        metric_names = task.get("metric_names", [])
        
        metrics = await self.get_metrics(metric_names)
        
        return {
            "success": True,
            "metrics": metrics,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_get_alerts(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Handle alerts retrieval request."""
        severity_filter = task.get("severity_filter")
        
        alerts = await self.get_alerts(severity_filter)
        
        return {
            "success": True,
            "alerts": alerts,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_acknowledge_alert(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Handle alert acknowledgment."""
        alert_id = task.get("alert_id", "")
        
        acknowledged = await self.acknowledge_alert(alert_id)
        
        return {
            "success": acknowledged,
            "alert_id": alert_id,
            "acknowledged": acknowledged,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_generate_report(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Handle monitoring report generation."""
        report_type = task.get("report_type", "summary")
        time_range = task.get("time_range", {"hours": 24})
        
        report = await self.generate_monitoring_report(report_type, time_range)
        
        return {
            "success": True,
            "report": report,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def _handle_health_check(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Handle health check request."""
        components = task.get("components", [])
        
        health_status = await self.perform_health_check(components)
        
        return {
            "success": True,
            "health_status": health_status,
            "agent": self.agent_id,
            "timestamp": datetime.now().isoformat()
        }
        
    async def collect_metric(self, metric_name: str, value: float, timestamp: Optional[datetime] = None):
        """Collect a metric value."""
        if metric_name not in self.metric_collectors:
            # Auto-create collector for new metrics
            self.metric_collectors[metric_name] = MetricCollector(metric_name, MetricType.SYSTEM)
            
        collector = self.metric_collectors[metric_name]
        collector.add_value(value, timestamp)
        
        self.logger.debug(f"Collected metric: {metric_name} = {value}")
        
    async def create_monitoring_rule(self, rule_config: Dict[str, Any]) -> MonitoringRule:
        """Create a new monitoring rule."""
        rule = MonitoringRule(
            rule_id=rule_config.get("rule_id", f"rule_{datetime.now().timestamp()}"),
            rule_name=rule_config.get("rule_name", "Custom Rule"),
            metric_name=rule_config.get("metric_name", ""),
            condition=rule_config.get("condition", "greater_than"),
            threshold=rule_config.get("threshold", 0.0),
            severity=AlertSeverity(rule_config.get("severity", "medium")),
            description=rule_config.get("description", ""),
            enabled=rule_config.get("enabled", True)
        )
        
        self.monitoring_rules[rule.rule_id] = rule
        
        return rule
        
    async def get_metrics(self, metric_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get current metric values."""
        if metric_names is None:
            metric_names = list(self.metric_collectors.keys())
            
        metrics = {}
        for metric_name in metric_names:
            if metric_name in self.metric_collectors:
                collector = self.metric_collectors[metric_name]
                metrics[metric_name] = {
                    "current_value": collector.get_latest(),
                    "average_5min": collector.get_average(5),
                    "p95_5min": collector.get_percentile(95, 5),
                    "metric_type": collector.metric_type.value
                }
                
        return metrics
        
    async def get_alerts(self, severity_filter: Optional[str] = None) -> Dict[str, Any]:
        """Get current alerts."""
        active_alerts = []
        for alert in self.active_alerts.values():
            if severity_filter is None or alert.severity.value == severity_filter:
                active_alerts.append(alert.to_dict())
                
        recent_alerts = []
        cutoff = datetime.now() - timedelta(hours=24)
        for alert in self.alert_history:
            if alert.created_at >= cutoff:
                if severity_filter is None or alert.severity.value == severity_filter:
                    recent_alerts.append(alert.to_dict())
                    
        return {
            "active_alerts": active_alerts,
            "recent_alerts": recent_alerts,
            "total_active": len(active_alerts),
            "total_recent": len(recent_alerts)
        }
        
    async def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.acknowledged = True
            self.logger.info(f"Alert {alert_id} acknowledged")
            return True
        return False
        
    async def generate_monitoring_report(self, report_type: str, time_range: Dict[str, int]) -> Dict[str, Any]:
        """Generate a monitoring report."""
        cutoff = datetime.now() - timedelta(**time_range)
        
        report = {
            "report_type": report_type,
            "time_range": time_range,
            "generated_at": datetime.now().isoformat(),
            "summary": {},
            "metrics_summary": {},
            "alerts_summary": {},
            "recommendations": []
        }
        
        # Metrics summary
        for metric_name, collector in self.metric_collectors.items():
            recent_values = [value for timestamp, value in collector.values if timestamp >= cutoff]
            if recent_values:
                report["metrics_summary"][metric_name] = {
                    "count": len(recent_values),
                    "average": statistics.mean(recent_values),
                    "min": min(recent_values),
                    "max": max(recent_values),
                    "std_dev": statistics.stdev(recent_values) if len(recent_values) > 1 else 0
                }
                
        # Alerts summary
        recent_alerts = [alert for alert in self.alert_history if alert.created_at >= cutoff]
        alert_by_severity = {}
        for alert in recent_alerts:
            severity = alert.severity.value
            alert_by_severity[severity] = alert_by_severity.get(severity, 0) + 1
            
        report["alerts_summary"] = {
            "total_alerts": len(recent_alerts),
            "by_severity": alert_by_severity,
            "resolved_alerts": len([a for a in recent_alerts if a.resolved]),
            "acknowledged_alerts": len([a for a in recent_alerts if a.acknowledged])
        }
        
        # Overall summary
        report["summary"] = {
            "total_metrics": len(self.metric_collectors),
            "active_monitoring_rules": len([r for r in self.monitoring_rules.values() if r.enabled]),
            "total_alerts": len(recent_alerts),
            "critical_alerts": alert_by_severity.get("critical", 0),
            "monitoring_health": "healthy" if alert_by_severity.get("critical", 0) == 0 else "degraded"
        }
        
        # Generate recommendations
        report["recommendations"] = await self._generate_monitoring_recommendations(report)
        
        return report
        
    async def perform_health_check(self, components: Optional[List[str]] = None) -> Dict[str, Any]:
        """Perform health check on system components."""
        health_status = {
            "overall_health": "healthy",
            "components": {},
            "checked_at": datetime.now().isoformat()
        }
        
        # Default components to check
        if components is None:
            components = ["monitoring", "metrics", "alerts", "rules"]
            
        for component in components:
            if component == "monitoring":
                health_status["components"]["monitoring"] = {
                    "status": "healthy" if self.is_monitoring_active else "unhealthy",
                    "details": f"Monitoring loop active: {self.is_monitoring_active}"
                }
            elif component == "metrics":
                active_metrics = len([c for c in self.metric_collectors.values() if c.get_latest() is not None])
                health_status["components"]["metrics"] = {
                    "status": "healthy" if active_metrics > 0 else "warning",
                    "details": f"Active metrics: {active_metrics}/{len(self.metric_collectors)}"
                }
            elif component == "alerts":
                critical_alerts = len([a for a in self.active_alerts.values() if a.severity == AlertSeverity.CRITICAL])
                health_status["components"]["alerts"] = {
                    "status": "healthy" if critical_alerts == 0 else "critical",
                    "details": f"Critical alerts: {critical_alerts}, Total active: {len(self.active_alerts)}"
                }
            elif component == "rules":
                enabled_rules = len([r for r in self.monitoring_rules.values() if r.enabled])
                health_status["components"]["rules"] = {
                    "status": "healthy" if enabled_rules > 0 else "warning",
                    "details": f"Enabled rules: {enabled_rules}/{len(self.monitoring_rules)}"
                }
                
        # Determine overall health based on components
        component_statuses = [comp["status"] for comp in health_status["components"].values()]
        if "critical" in component_statuses:
            health_status["overall_health"] = "critical"
        elif "unhealthy" in component_statuses:
            health_status["overall_health"] = "unhealthy"
        elif "warning" in component_statuses:
            health_status["overall_health"] = "warning"
            
        return health_status
        
    async def _collect_system_metrics(self):
        """Collect system metrics."""
        try:
            # Mock system metrics collection
            # In a real implementation, these would collect actual system metrics
            
            import random
            
            # CPU usage (mock)
            cpu_usage = random.uniform(0.1, 0.9)
            await self.collect_metric("cpu_usage", cpu_usage)
            
            # Memory usage (mock)
            memory_usage = random.uniform(0.2, 0.8)
            await self.collect_metric("memory_usage", memory_usage)
            
            # Error count (mock)
            error_count = random.randint(0, 5)
            await self.collect_metric("error_count", error_count)
            
            # API response time (mock)
            api_response_time = random.uniform(50, 500)  # milliseconds
            await self.collect_metric("api_response_time", api_response_time)
            
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {e}")
            
    async def _evaluate_monitoring_rules(self):
        """Evaluate monitoring rules and generate alerts."""
        for rule in self.monitoring_rules.values():
            if not rule.enabled:
                continue
                
            metric_name = rule.metric_name
            if metric_name not in self.metric_collectors:
                continue
                
            collector = self.metric_collectors[metric_name]
            current_value = collector.get_latest()
            
            if current_value is not None and rule.evaluate(current_value):
                # Rule triggered, create alert
                alert_id = f"{rule.rule_id}_{datetime.now().timestamp()}"
                
                # Check if we already have an active alert for this rule
                existing_alert = None
                for alert in self.active_alerts.values():
                    if alert.metric_name == metric_name and not alert.resolved:
                        existing_alert = alert
                        break
                        
                if not existing_alert:
                    alert = Alert(
                        alert_id=alert_id,
                        alert_name=rule.rule_name,
                        severity=rule.severity,
                        message=f"{rule.description}. Current value: {current_value}, Threshold: {rule.threshold}",
                        metric_name=metric_name,
                        threshold=float(rule.threshold) if isinstance(rule.threshold, (int, float)) else 0.0,
                        current_value=current_value,
                        context={"rule_id": rule.rule_id}
                    )
                    
                    self.active_alerts[alert_id] = alert
                    self.alert_history.append(alert)
                    
                    rule.last_triggered = datetime.now()
                    rule.trigger_count += 1
                    
                    self.logger.warning(f"Alert triggered: {alert.alert_name} - {alert.message}")
                    
    async def _cleanup_old_data(self):
        """Clean up old data to prevent memory issues."""
        # Clean up resolved alerts from active alerts
        resolved_alerts = []
        for alert_id, alert in list(self.active_alerts.items()):
            # Auto-resolve alerts older than 1 hour if metric is back to normal
            if alert.created_at < datetime.now() - timedelta(hours=1):
                metric_name = alert.metric_name
                if metric_name in self.metric_collectors:
                    collector = self.metric_collectors[metric_name]
                    current_value = collector.get_latest()
                    
                    # Find the corresponding rule
                    rule = None
                    for r in self.monitoring_rules.values():
                        if r.metric_name == metric_name:
                            rule = r
                            break
                            
                    if rule and current_value is not None and not rule.evaluate(current_value):
                        alert.resolved = True
                        alert.resolved_at = datetime.now()
                        resolved_alerts.append(alert_id)
                        
        for alert_id in resolved_alerts:
            del self.active_alerts[alert_id]
            
        # Keep only last 500 alerts in history
        if len(self.alert_history) > 500:
            self.alert_history = self.alert_history[-500:]
            
    async def _generate_monitoring_recommendations(self, report: Dict[str, Any]) -> List[str]:
        """Generate monitoring recommendations based on report data."""
        recommendations = []
        
        alerts_summary = report.get("alerts_summary", {})
        metrics_summary = report.get("metrics_summary", {})
        
        # Check for high alert volume
        total_alerts = alerts_summary.get("total_alerts", 0)
        if total_alerts > 50:
            recommendations.append("High alert volume detected. Consider reviewing alert thresholds to reduce noise.")
            
        # Check for unacknowledged critical alerts
        critical_alerts = alerts_summary.get("by_severity", {}).get("critical", 0)
        if critical_alerts > 0:
            recommendations.append(f"You have {critical_alerts} critical alerts that need immediate attention.")
            
        # Check for metrics with high variability
        for metric_name, stats in metrics_summary.items():
            if stats.get("std_dev", 0) > stats.get("average", 0):
                recommendations.append(f"Metric '{metric_name}' shows high variability. Consider investigating the cause.")
                
        # Check for resource usage patterns
        if "cpu_usage" in metrics_summary:
            cpu_avg = metrics_summary["cpu_usage"].get("average", 0)
            if cpu_avg > 0.8:
                recommendations.append("High average CPU usage detected. Consider scaling resources or optimizing processes.")
                
        if "memory_usage" in metrics_summary:
            memory_avg = metrics_summary["memory_usage"].get("average", 0)
            if memory_avg > 0.8:
                recommendations.append("High average memory usage detected. Consider increasing memory allocation or optimizing memory usage.")
                
        if not recommendations:
            recommendations.append("System monitoring looks healthy. Continue regular monitoring.")
            
        return recommendations
        
    async def stop_monitoring(self):
        """Stop the monitoring loop."""
        self.is_monitoring_active = False
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Monitoring stopped")
