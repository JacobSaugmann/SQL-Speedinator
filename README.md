# ⚡ SQL Speedinator

<div align="center">

**Lightning Fast SQL Server Performance Analysis with AI-Powered Insights**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-2012+-red.svg)](https://www.microsoft.com/sql-server)

*Making your SQL Server fly with enterprise-grade performance analysis!* 🚀

</div>

## 🌟 Enterprise Features

### 🎯 **Core Analysis Engine**
- **⚡ Lightning Fast Analysis**: Comprehensive SQL Server performance analysis
- **🤖 AI-Powered Insights**: Azure OpenAI integration for intelligent bottleneck identification
- **📊 Performance Monitor Integration**: Windows Performance Counters with enterprise-level monitoring
- **📈 Real-time Data Collection**: Automated PerfMon data collection and analysis
- **📋 Responsive PDF Reports**: Ultra-compact reports with automatic table adaptation

### 🏢 **NEW: Enterprise Production Features**

#### 🧠 **Smart Collection Management**
- **Intelligent Detection**: Automatically finds and reuses existing PerfMon collections
- **Smart Matching**: 80% counter similarity algorithm prevents collection duplication
- **Auto Cleanup**: Managed collections are automatically cleaned up on completion
- **Resource Optimization**: Eliminates unnecessary performance impact

#### 🗣️ **AI Dialog System**
- **Multi-turn Conversations**: Sophisticated dialog context with memory across sessions
- **Token Management**: Configurable limits (5000 tokens default) with usage tracking
- **Confidence Scoring**: Prioritizes local logic over AI recommendations when confidence is low
- **Context Preservation**: Maintains conversation history for better problem-solving

#### 🛡️ **Server Performance Protection**
- **Real-time Monitoring**: Continuous CPU, memory, and connection monitoring during analysis
- **Configurable Thresholds**: Customizable protection limits (80% CPU, 85% memory default)
- **Automatic Abort**: Immediately stops analysis if server performance is compromised
- **Background Protection**: Non-blocking monitoring thread with violation detection

#### 📊 **Advanced Status Tracking**
- **Real-time Progress**: Live CMD updates with beautiful progress bars and status indicators
- **Phase Management**: Tracks all analysis phases from initialization to completion
- **Weighted Progress**: Accurate overall progress calculation based on phase complexity
- **Professional Output**: Headers, summaries, and emoji indicators for clear feedback

### 🔧 **Analysis Capabilities**
- **🔍 Index Optimization**: Fragmentation analysis with custom maintenance scripts
- **🔍 Missing Index Detection**: Intelligent identification of missing indexes
- **⚙️ Configuration Review**: Best practice checks and recommendations
- **💾 TempDB Analysis**: Performance and configuration optimization
- **📈 Plan Cache Evaluation**: Query performance analysis
- **🕒 Night Mode**: Scheduled runs with minimal production impact
- **🔐 Secure Authentication**: Windows Authentication as default with SQL fallback

## 🚀 Quick Start

### Prerequisites
- Python 3.7+
- SQL Server 2012+ (any edition)
- Windows Authentication or SQL Server Authentication
- **Windows OS** (for Performance Monitor integration)
- Administrative privileges (for PerfMon data collection)

### Installation

```bash
git clone https://github.com/JacobSaugmann/SQL-Speedinator.git
cd SQL-Speedinator
pip install -r requirements.txt
```

### Enterprise Configuration

Copy `.env.example` to `.env` and configure enterprise settings:

```env
# AI Dialog System
AI_MAX_TOKENS_PER_SESSION=5000
AI_ENABLE_DIALOG_MODE=true
AI_MIN_CONFIDENCE_THRESHOLD=0.7

# Server Performance Protection
PROTECTION_ENABLED=true
PROTECTION_MAX_CPU_PERCENT=80.0
PROTECTION_MAX_MEMORY_PERCENT=85.0
PROTECTION_MAX_CONNECTIONS=500
PROTECTION_MAX_BLOCKING_SESSIONS=10
PROTECTION_CHECK_INTERVAL_SECONDS=5

# Smart PerfMon Collection Management
PERFMON_ENABLE_SMART_REUSE=true
PERFMON_COLLECTION_PREFIX=SQL_SPEEDINATOR
PERFMON_AUTO_CLEANUP=true
PERFMON_MATCH_THRESHOLD=0.8

# Azure OpenAI (Optional)
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AI_ANALYSIS_ENABLED=true

# Authentication
USE_WINDOWS_AUTH=true
```

### Usage Examples

```bash
# Basic enterprise analysis with all protection features
python main.py -s your-server-name

# Complete enterprise analysis with AI dialog and performance protection
python main.py -s your-server-name --ai-analysis

# Production-safe analysis with PerfMon data collection (duration in MINUTES)
python main.py -s your-server-name --perfmon-duration 240

# Short performance monitoring (30 minutes) with AI analysis
python main.py -s your-server-name --perfmon-duration 30 --ai-analysis

# Quick analysis with 5 minutes of performance data collection
python main.py -s your-server-name --perfmon-duration 5

# Analyze existing Performance Monitor data with enterprise features
python main.py -s your-server-name --perfmon-file "C:\PerfLogs\sql_perf.blg"

# Full enterprise suite: AI + 2-hour PerfMon + Protection + Status tracking
python main.py -s your-server-name --perfmon-duration 120 --ai-analysis

# Night mode with enterprise protection
python main.py -s your-server-name --night-mode
```

### ⏱️ Performance Monitor Duration

**Important**: The `--perfmon-duration` parameter is specified in **MINUTES**:
- `--perfmon-duration 5` = 5 minutes of data collection
- `--perfmon-duration 30` = 30 minutes of data collection  
- `--perfmon-duration 120` = 2 hours of data collection
- `--perfmon-duration 240` = 4 hours of data collection

Recommended durations:
- **Quick Test**: 5-10 minutes
- **Standard Analysis**: 30-60 minutes
- **Comprehensive Analysis**: 120-240 minutes
- **Production Monitoring**: 240+ minutes

## 📊 Enterprise Performance Monitor Integration

SQL Speedinator includes enterprise-level Performance Monitor integration:

- **🖥️ System Metrics**: CPU, Memory, Disk I/O monitoring with protection thresholds
- **🗃️ SQL Server Counters**: 100+ specialized performance counters with smart collection reuse
- **📈 Real-time Collection**: Automated data collection with configurable duration and protection
- **🤖 AI Bottleneck Analysis**: Cross-component correlation with multi-turn dialog support
- **📋 Comprehensive Reports**: Detailed performance metrics with status tracking

## 🏗️ Enterprise Architecture

```
sql-speedinator/
├── src/
│   ├── analyzers/              # Performance analyzers
│   ├── core/                   # Core enterprise functionality
│   │   ├── analysis_status_tracker.py    # Real-time status tracking
│   │   └── server_performance_protector.py # Production protection
│   ├── perfmon/               # Performance Monitor integration
│   │   └── template_manager.py # Smart collection management
│   ├── services/              # AI and external services
│   │   └── ai_dialog_system.py # Multi-turn AI conversations
│   └── reports/               # Enterprise report generation
├── perfmon/
│   ├── templates/             # PerfMon XML templates
│   └── data/                  # Collected performance data
├── tests/                     # Enterprise test suite
├── templates/                 # Report templates
└── sql_scripts/              # SQL query scripts
```

## 📊 Sample Enterprise Output

SQL Speedinator generates comprehensive enterprise-grade PDF reports with:

- **🔍 Executive Summary**: AI-powered bottleneck identification with dialog insights
- **🛡️ Protection Summary**: Server protection status and threshold monitoring
- **📊 Performance Monitor Analysis**: System and SQL Server performance metrics
- **💾 Disk Performance**: I/O metrics and performance trends with real-time protection
- **🔧 Index Analysis**: Fragmentation, missing, and unused indexes
- **⚙️ Configuration Review**: Best practice recommendations
- **📈 Query Performance**: Plan cache analysis
- **🤖 AI Recommendations**: Multi-turn dialog insights and prioritized action plans
- **📋 Status Timeline**: Complete analysis timeline with phase tracking

## 🧪 Enterprise Testing

```bash
# Test enterprise features
python -c "import src.core.analysis_status_tracker; print('Status tracker OK')"
python -c "import src.core.server_performance_protector; print('Protection OK')"
python -c "import src.services.ai_dialog_system; print('AI dialog OK')"
python -c "import src.perfmon.template_manager; print('Smart collections OK')"

# Test main application
python main.py --help
```

## 📚 Enterprise Documentation

- **[Setup Guide](SQL_AUTH_SETUP.md)** - SQL Server connection setup
- **[AI Integration Guide](AI_INTEGRATION_GUIDE.md)** - Azure OpenAI setup with enterprise features
- **[Project Details](PROJECT_README.md)** - Complete enterprise project overview
- **[Test Documentation](tests/README.md)** - Enterprise testing information

## 🚀 Enterprise Deployment

SQL Speedinator is now production-ready with:

✅ **Server Protection**: Real-time monitoring prevents performance impact  
✅ **Resource Management**: Smart collection reuse eliminates duplication  
✅ **Professional Feedback**: Beautiful status tracking with progress indicators  
✅ **AI Intelligence**: Multi-turn conversations with confidence scoring  
✅ **Enterprise Configuration**: Comprehensive settings for all protection features  

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by SQL Server community best practices
- Built with ❤️ for enterprise database administrators and developers

---

<div align="center">

**⚡ SQL Speedinator Enterprise Edition** - *Production-ready SQL Server performance analysis with enterprise-grade protection!*

[Report Bug](https://github.com/JacobSaugmann/SQL-Speedinator/issues) · [Request Feature](https://github.com/JacobSaugmann/SQL-Speedinator/issues)

</div>