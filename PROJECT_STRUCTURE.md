# SQL Bottleneck Analyzer - Project Structure

## Project Overview
SQL Bottleneck Analyzer is a comprehensive SQL Server performance analysis tool with enhanced disk analysis, intelligent recommendations, and AI-powered insights.

## Directory Structure

```
Sql_bottleneck/
├── .env                    # Environment configuration (not in git)
├── .env.example           # Environment template
├── .gitignore             # Git ignore rules
├── main.py                # Main application entry point
├── requirements.txt       # Python dependencies
├── README.md              # Main project documentation
├── PROJECT_README.md      # Detailed project information
├── ENHANCEMENT_SUMMARY.md # Recent enhancements documentation
├── AI_INTEGRATION_GUIDE.md # AI integration documentation
├── SQL_AUTH_SETUP.md      # SQL Server authentication setup
├── PROJECT_STRUCTURE.md   # This file
├── LICENSE                # Project license
│
├── src/                   # Source code
│   ├── core/              # Core system components
│   │   ├── __init__.py
│   │   ├── config_manager.py        # Configuration management
│   │   ├── file_cleanup_manager.py  # File cleanup operations
│   │   ├── performance_analyzer.py  # Main analysis coordinator
│   │   ├── sql_connection.py        # SQL Server connection manager
│   │   └── sql_version_manager.py   # SQL Server version handling
│   │
│   ├── analyzers/         # Analysis modules
│   │   ├── __init__.py
│   │   ├── ai_analyzer.py           # AI-powered analysis
│   │   ├── disk_analyzer.py         # Enhanced disk performance analysis
│   │   ├── index_analyzer.py        # Index analysis
│   │   ├── advanced_index_analyzer.py # Advanced index analysis
│   │   ├── missing_index_analyzer.py # Missing index detection
│   │   ├── plan_cache_analyzer.py   # Plan cache analysis
│   │   ├── server_config_analyzer.py # Server configuration analysis
│   │   ├── server_database_analyzer.py # Server and database info
│   │   ├── tempdb_analyzer.py       # TempDB analysis
│   │   ├── wait_statistics_analyzer.py # Wait statistics analysis
│   │   └── intelligent_recommendations.py # Intelligent correlation engine
│   │
│   ├── services/          # Service layer
│   │   ├── __init__.py
│   │   └── ai_service.py            # Azure OpenAI integration
│   │
│   ├── perfmon/           # Performance Monitor integration
│   │   ├── __init__.py
│   │   ├── template_manager.py      # PerfMon template management
│   │   └── performance_analyzer.py  # PerfMon data analysis
│   │
│   └── reporting/         # Report generation
│       ├── __init__.py
│       ├── pdf_generator.py         # PDF report generation
│       └── data_formatter.py        # Data formatting utilities
│
├── perfmon/               # Performance Monitor templates
│   └── templates/
│       └── sql_performance_template.xml # Enhanced PerfMon template (29 counters)
│
├── sql_scripts/           # SQL Server scripts
│   ├── database_analysis.sql
│   ├── disk_performance.sql
│   ├── index_analysis.sql
│   ├── missing_indexes.sql
│   ├── plan_cache_analysis.sql
│   ├── server_config.sql
│   ├── tempdb_analysis.sql
│   └── wait_statistics.sql
│
├── logs/                  # Application logs (auto-cleaned)
├── reports/               # Generated reports (auto-cleaned)
├── tests/                 # Test files
└── .venv/                 # Python virtual environment (not in git)
```

## Key Components

### Core System (`src/core/`)
- **performance_analyzer.py**: Main orchestrator with intelligent correlation integration
- **config_manager.py**: Environment and configuration management
- **sql_connection.py**: Robust SQL Server connection handling
- **file_cleanup_manager.py**: Automatic cleanup of old files

### Enhanced Analyzers (`src/analyzers/`)
- **disk_analyzer.py**: Comprehensive disk analysis with formatting and tempdb validation
- **intelligent_recommendations.py**: Multi-metric correlation analysis engine
- **ai_analyzer.py**: Azure OpenAI integration for intelligent insights
- **Advanced analysis modules**: Index, wait statistics, plan cache, and configuration analysis

### Performance Monitoring (`src/perfmon/`)
- **template_manager.py**: PerfMon data collection management
- **sql_performance_template.xml**: 29 comprehensive performance counters

### AI Integration (`src/services/`)
- **ai_service.py**: Azure OpenAI client with custom HTTP handling

### Reporting (`src/reporting/`)
- **pdf_generator.py**: Professional PDF report generation
- **data_formatter.py**: Data processing and formatting utilities

## Recent Enhancements (October 2025)

### Enhanced Disk Analysis
- Block size and allocation unit detection via PowerShell
- TempDB placement validation against best practices
- Drive configuration and shared storage analysis
- Automated recommendations for disk optimization

### Expanded Performance Counters
- Increased from 10 to 29 comprehensive performance counters
- Advanced disk I/O latency metrics (Read/Write latencies)
- SQL Server specific counters (Page Life Expectancy, Log Flushes)
- Memory and network performance monitoring

### Intelligent Correlation Engine
- Memory-disk correlation analysis
- CPU-IO bottleneck identification
- TempDB contention detection
- Index performance correlation
- Transaction log performance analysis
- Priority-based recommendation system

### Performance Thresholds
- Page Life Expectancy < 300 seconds (memory pressure)
- Disk latency > 20ms (critical threshold)
- Disk queue length > 2 per disk (I/O bottleneck)
- CPU utilization > 80% sustained (performance warning)

## Configuration

### Environment Variables (.env)
```
# Azure OpenAI Configuration
AZURE_OPENAI_ENDPOINT=your_endpoint
AZURE_OPENAI_API_KEY=your_api_key
AZURE_OPENAI_DEPLOYMENT_NAME=your_deployment

# SQL Server Configuration
DEFAULT_SQL_SERVER=localhost
SQL_CONNECTION_TIMEOUT=30

# Application Settings
NIGHT_MODE_DELAY=2
DEBUG_MODE=false
```

### Key Features
- **Multi-platform support**: Windows, Linux compatibility considerations
- **Automatic cleanup**: Configurable retention periods for logs and reports
- **AI-powered insights**: Optional Azure OpenAI integration
- **Comprehensive logging**: Detailed operation tracking
- **Professional reporting**: PDF generation with charts and recommendations

## Usage Examples

### Basic Analysis
```bash
python main.py -s localhost --perfmon-duration 5
```

### Enhanced Analysis with AI
```bash
python main.py -s localhost --perfmon-duration 30 --ai-analysis
```

### Scheduled Analysis
```bash
python main.py -s localhost --schedule --perfmon-duration 60
```

## Dependencies
- **pyodbc**: SQL Server connectivity
- **psutil**: System performance monitoring
- **reportlab**: PDF report generation
- **openai**: Azure OpenAI integration
- **python-dotenv**: Environment configuration
- **Standard libraries**: logging, json, datetime, subprocess

## Testing and Validation
- Import integrity testing
- Syntax validation for all components
- PowerShell integration testing
- Correlation algorithm validation
- Error handling verification
- Full end-to-end analysis testing

## Maintenance
- Automatic log rotation (configurable retention)
- Report cleanup (configurable retention)
- Performance counter template updates
- Dependency updates via requirements.txt

---

**Last Updated**: October 28, 2025
**Version**: Enhanced with Intelligent Correlations
**Status**: Production Ready