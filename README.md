# ⚡ SQL Speedinator

<div align="center">

**Lightning Fast SQL Server Performance Analysis with AI-Powered Insights**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-2012+-red.svg)](https://www.microsoft.com/sql-server)

*Making your SQL Server fly!* 🚀

</div>

## 🌟 Features

- **⚡ Lightning Fast Analysis**: Comprehensive SQL Server performance analysis
- **🤖 AI-Powered Insights**: Azure OpenAI integration for intelligent bottleneck identification
- **📊 Responsive PDF Reports**: Ultra-compact reports with automatic table adaptation
- **🔧 Index Optimization**: Fragmentation analysis with custom maintenance scripts
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

### Installation

```bash
git clone https://github.com/JacobSaugmann/SQL-Speedinator.git
cd SQL-Speedinator
pip install -r requirements.txt
```

### Configuration

1. Copy `.env.example` to `.env`
2. Configure your settings:

```env
# Windows Authentication (Recommended)
USE_WINDOWS_AUTH=true

# Azure OpenAI (Optional)
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AI_ANALYSIS_ENABLED=true
```

### Usage

```bash
# Basic analysis
python main.py -s your-server-name

# With AI analysis
python main.py -s your-server-name --ai-analysis

# Night mode (minimal impact)
python main.py -s your-server-name --night-mode
```

## 📊 Sample Output

SQL Speedinator generates comprehensive PDF reports with:

- **🔍 Executive Summary**: AI-powered bottleneck identification
- **💾 Disk Performance**: I/O metrics and performance trends  
- **🔧 Index Analysis**: Fragmentation, missing, and unused indexes
- **⚙️ Configuration Review**: Best practice recommendations
- **📈 Query Performance**: Plan cache analysis
- **🤖 AI Recommendations**: Prioritized action plans

## 🏗️ Project Structure

```
sql-speedinator/
├── src/
│   ├── analyzers/          # Performance analyzers
│   ├── core/              # Core functionality  
│   ├── reports/           # Report generation
│   └── services/          # External services (AI)
├── tests/                 # Test suite
├── templates/             # Report templates
└── sql_scripts/          # SQL query scripts
```

## 🧪 Testing

```bash
# Test connection
python tests/test_connection.py

# Test AI integration  
python tests/test_ai_integration.py

# Test responsive tables
python tests/test_responsive_tables.py
```

## 📚 Documentation

- **[Setup Guide](SQL_AUTH_SETUP.md)** - SQL Server connection setup
- **[AI Integration Guide](AI_INTEGRATION_GUIDE.md)** - Azure OpenAI setup
- **[Project Details](PROJECT_README.md)** - Complete project overview
- **[Test Documentation](tests/README.md)** - Testing information

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by SQL Server community best practices
- Built with ❤️ for database administrators and developers

---

<div align="center">

**⚡ SQL Speedinator** - *Lightning fast SQL Server performance analysis!*

[Report Bug](https://github.com/JacobSaugmann/SQL-Speedinator/issues) · [Request Feature](https://github.com/JacobSaugmann/SQL-Speedinator/issues)

</div>

**SQL Speedinator** gør din SQL Server hurtigere ved at identificere og løse performance bottlenecks med AI-powered analyse!

## Features
- **⚡ Hurtig Performance Analyse**: Disk performance, I/O metrics, wait statistics
- **🤖 AI Integration**: Azure OpenAI powered bottleneck identifikation og anbefalinger
- **📊 Responsive PDF Rapporter**: Ultra-kompakte rapporter med automatisk tabel tilpasning
- **🔧 Index Optimering**: Fragmentering analyse og custom maintenance scripts
- **🔍 Missing Index Analyse**: Intelligent identifikation af manglende indexes
- **⚙️ Server Konfiguration**: Best practice kontrol og anbefalinger
- **💾 TempDB Analyse**: Performance og konfiguration optimering
- **📈 Plan Cache Evaluation**: Query performance analyse
- **🕒 Scheduling Support**: Natlig scheduled kørsel uden production impact
- **🔐 Sikker Authentication**: Windows Authentication som standard

## Installation
```bash
pip install -r requirements.txt
```

## Brug
```bash
# Quick start
python main.py -s SERVER_NAVN

# Med specifikke indstillinger
python main.py -s SERVER_NAVN [-n] [--output OUTPUT_DIR]
```

Parametre:
- `-s, --server`: SQL Server navn (påkrævet)
- `-n, --night-mode`: Kør i natlig mode med minimal belastning
- `--output`: Output directory for rapporter (standard: ./reports)

## Konfiguration
Kopier `env.example` til `.env` og konfigurer:

### Windows Authentication (Anbefalet)
```env
USE_WINDOWS_AUTH=true           # Standard og sikker
```

### Azure OpenAI Integration
```env
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AI_ANALYSIS_ENABLED=true
```

Se [SQL_AUTH_SETUP.md](SQL_AUTH_SETUP.md) for detaljeret setup guide.

## Rapport Output
Genererer en omfattende ultra-kompakt PDF rapport med:
- **🔍 Executive Summary**: AI-powered bottleneck identifikation
- **💾 Disk Performance Analysis**: I/O metrics og performance trends
- **🔧 Index Analysis**: Fragmentering, missing og unused indexes med custom maintenance scripts
- **⚙️ Server Configuration Review**: Best practice anbefalinger
- **💾 TempDB Analysis**: Performance og konfiguration optimering
- **📈 Plan Cache Review**: Query performance analyse
- **🤖 AI Anbefalinger**: Prioriterede handlingsplaner og solutions
- **📊 Responsive Design**: Tabeller tilpasser sig automatisk til side bredde

## 🧪 Testing
```bash
# Test forbindelse
python tests/test_connection.py

# Test alle funktioner
python tests/test_ai_integration.py
python tests/test_responsive_tables.py
```

## 📚 Dokumentation
- **[Komplet guide](PROJECT_README.md)** - Detaljeret funktionsoversigt
- **[Authentication setup](SQL_AUTH_SETUP.md)** - SQL Server forbindelse guide
- **[Test dokumentation](tests/README.md)** - Test funktioner

---

**SQL Speedinator** - *Making your SQL Server fly!* ⚡🚀