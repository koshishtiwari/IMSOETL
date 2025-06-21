# IMSOETL: I'm so ETL 🤖

**Agentic Data Engineering Platform with AI-Powered ETL Pipelines**

IMSOETL is an intelligent data engineering platform that uses AI agents to autonomously discover, design, execute, and maintain data pipelines through natural language interfaces and intelligent orchestration.

## 🚀 New: LLM Integration

IMSOETL now features **local-first AI** with fallback to cloud providers:

### 🏠 Local AI (Primary)
- **Ollama** with **Gemma3:4b** model for completely local inference
- No API costs, full privacy, always available
- Multimodal capabilities for advanced data understanding

### ☁️ Cloud AI (Fallback)  
- **Google Gemini API** for complex scenarios
- Automatic failover when local AI is unavailable
- Enterprise-grade performance and reliability

### 🧠 AI-Enhanced Features
- **Natural Language Intent Parsing**: Convert plain English to structured ETL plans
- **SQL Generation**: Automatically generate optimized SQL from requirements
- **Data Quality Suggestions**: AI-powered recommendations for data validation
- **Schema Mapping**: Intelligent field mapping between source and target
- **Code Optimization**: AI-enhanced SQL and transformation optimization

## 🏗️ Core Architecture

### Agent Hierarchy
Orchestrator Agent (Master)
├── Discovery Agent
├── Schema Agent  
├── Transformation Agent
├── Quality Agent
├── Execution Agent
└── Monitoring Agent

### Workflow Logic
#### Phase 1: Intent Processing

- User provides natural language requirement
- Orchestrator Agent parses intent and creates execution plan
- Breaks down complex requests into atomic tasks
- Assigns tasks to specialized agents

#### Phase 2: Discovery & Analysis

- Discovery Agent identifies all relevant data sources
- Schema Agent maps source/target schemas and relationships
- Quality Agent assesses data quality and identifies issues
- Agents communicate findings back to Orchestrator

#### Phase 3: Pipeline Design

- Transformation Agent designs transformation logic
- Execution Agent creates optimal execution plan
- Quality Agent defines validation rules
- Orchestrator reviews and approves pipeline design   

#### Phase 4: Execution & Monitoring

- Execution Agent runs the pipeline
- Monitoring Agent tracks performance and errors
- Quality Agent validates output data
- All agents report status to Orchestrator

## 🛠️ Key Comp onents
1. **Agent Communication Layer (A2A)**

- Standardized message protocol between agents
- Event-driven architecture for real-time coordination
- Shared context and state management

2. **MCP Integration Layer**

- Unified connectors for databases, APIs, file systems
- Protocol abstraction for different data sources
- Authentication and security management

3. **Natural Language Interface**

- Intent recognition and parsing
- Context understanding for complex requirements
- Feedback loop for clarification

4. **Pipeline Engine**

- Code generation for actual ETL processes
- Support for multiple execution environments (Spark, Airflow, etc.)
- Version control and rollback capabilities

## 🎯 Quick Start

### Prerequisites
- Python 3.11+
- Optional: Ollama installed locally for local AI
- Optional: Gemini API key for cloud AI fallback

### Installation
```bash
# Clone the repository
git clone https://github.com/yourusername/IMSOETL.git
cd IMSOETL

# Install dependencies
pip install -e .

# Optional: Install and start Ollama for local AI
# Visit https://ollama.ai for installation instructions
ollama pull gemma3:4b
```

### Environment Setup
```bash
# Optional: Set Gemini API key for cloud fallback
export GEMINI_API_KEY="your-gemini-api-key"
```

### Basic Usage

#### 1. Interactive Mode
```bash
# Start interactive ETL session
imsoetl interactive

# Example interactions:
> "Extract customer data from MySQL and load into PostgreSQL"
> "Clean phone numbers and validate email addresses"
> "Monitor the pipeline and send alerts on failures"
```

#### 2. LLM-Powered Features
```bash
# Test LLM integration
python llm_demo.py

# Test natural language intent parsing
imsoetl llm-intent "Copy sales data from last month and create a summary report"

# Generate SQL from natural language
imsoetl llm "Show me customers who placed orders in the last 30 days"
```

#### 3. Agent Commands
```bash
# Discover data sources
imsoetl discover --source mysql://localhost/mydb

# Validate data quality
imsoetl validate --table customers --checks email,phone

# Run comprehensive demo
imsoetl demo
```

## 📚 Sample Workflow

**User Input:** "Move last 30 days of customer orders from MySQL to PostgreSQL, clean the phone numbers, and create a daily summary table"

**AI-Enhanced Agent Orchestration:**

1. **Orchestrator + LLM**: 
   - Parses natural language intent using local Gemma3:4b
   - Breaks into subtasks: extraction, cleaning, aggregation, loading
   - Creates intelligent execution plan

2. **Discovery Agent**: 
   - Connects to MySQL, identifies orders table
   - Analyzes schema and data patterns

3. **Schema Agent**: 
   - Maps MySQL orders schema to PostgreSQL target
   - Uses AI to suggest optimal field mappings

4. **Quality Agent**: 
   - Identifies phone number formats needing cleaning
   - AI suggests data quality rules and validations

5. **Transformation Agent**: 
   - LLM generates optimized SQL for phone cleaning
   - Creates aggregation logic for daily summaries

6. **Execution Agent**: 
   - Generates execution pipeline with error handling
   - Optimizes for performance and reliability

7. **Monitoring Agent**: 
   - Sets up real-time alerts and performance tracking
   - AI-powered anomaly detection

## 🔧 Configuration

### LLM Configuration (config/default.yaml)
```yaml
llm:
  ollama:
    enabled: true
    base_url: "http://localhost:11434"
    model: "gemma3:4b"
    timeout: 30
    
  gemini:
    enabled: true
    model: "gemini-1.5-flash"
    api_key: "${GEMINI_API_KEY}"
```

## 🧪 Examples

### Natural Language to ETL Pipeline
```python
from imsoetl.llm.manager import LLMManager
from imsoetl.core.orchestrator import OrchestratorAgent

# Initialize with local AI
llm_manager = LLMManager(config)
await llm_manager.initialize()

# Parse complex requirements
intent = await llm_manager.parse_intent(
    "Migrate customer data, clean emails, validate phone numbers, "
    "and create a daily report with quality metrics"
)

# Result: Structured ETL plan with source/target mapping,
# transformation rules, and quality checks
```

### AI-Generated SQL Transformations
```python
from imsoetl.agents.transformation import TransformationAgent

transform_agent = TransformationAgent()
await transform_agent.initialize_llm(config)

# Generate complex transformation SQL
result = await transform_agent.generate_llm_enhanced_transformation(
    source_schema={"table": "raw_customers", ...},
    target_schema={"table": "clean_customers", ...},
    requirements=["Clean phone numbers", "Validate emails", "Handle nulls"]
)

# Result: Optimized SQL with error handling and data validation
```