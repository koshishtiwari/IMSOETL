IMSOETL: I'm so ETL is an agentic data engineering platform that uses Generative AI agents to autonomously discover, design, execute, and maintain data pipelines through natural language interfaces and intelligent orchestration.

Core Architecture

Agent Hierarchy
Orchestrator Agent (Master)
├── Discovery Agent
├── Schema Agent  
├── Transformation Agent
├── Quality Agent
├── Execution Agent
└── Monitoring Agent

Workflow Logic
Phase 1: Intent Processing

User provides natural language requirement
Orchestrator Agent parses intent and creates execution plan
Breaks down complex requests into atomic tasks
Assigns tasks to specialized agents

Phase 2: Discovery & Analysis

Discovery Agent identifies all relevant data sources
Schema Agent maps source/target schemas and relationships
Quality Agent assesses data quality and identifies issues
Agents communicate findings back to Orchestrator

Phase 3: Pipeline Design

Transformation Agent designs transformation logic
Execution Agent creates optimal execution plan
Quality Agent defines validation rules
Orchestrator reviews and approves pipeline design

Phase 4: Execution & Monitoring

Execution Agent runs the pipeline
Monitoring Agent tracks performance and errors
Quality Agent validates output data
All agents report status to Orchestrator

Key Components
1. Agent Communication Layer (A2A)

Standardized message protocol between agents
Event-driven architecture for real-time coordination
Shared context and state management

2. MCP Integration Layer

Unified connectors for databases, APIs, file systems
Protocol abstraction for different data sources
Authentication and security management

3. Natural Language Interface

Intent recognition and parsing
Context understanding for complex requirements
Feedback loop for clarification

4. Pipeline Engine

Code generation for actual ETL processes
Support for multiple execution environments (Spark, Airflow, etc.)
Version control and rollback capabilities

Sample Workflow
User Input: "Move last 30 days of customer orders from MySQL to Snowflake, clean the phone numbers, and create a daily summary table"
Agent Orchestration:

Orchestrator: Breaks into subtasks - data extraction, phone cleaning, aggregation, loading
Discovery: Connects to MySQL, identifies orders table, analyzes schema
Schema: Maps MySQL orders schema to Snowflake target schema
Quality: Identifies phone number formats and cleaning rules needed
Transformation: Creates SQL for phone cleaning and daily aggregation
Execution: Generates Airflow DAG with appropriate tasks
Monitoring: Sets up alerts and performance tracking

Resources
Gen AI models
Gemini using API
gemma3:4b using Ollama