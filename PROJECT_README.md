# ⚡ SQL Speedinator

Et omfattende værktøj til analyse af SQL Server performance med AI-integration og ultra-kompakte PDF rapporter.

## 🚀 Funktioner

- **Performance Analyse**: Komplet analyse af SQL Server performance metrics
- **AI Integration**: Azure OpenAI integration for intelligent bottleneck analyse
- **Responsive PDF Rapporter**: Ultra-kompakte rapporter med Schultz design
- **Automatisk Index Vedligeholdelse**: Tilpassede index maintenance scripts
- **Responsive Tabeller**: Tabeller tilpasser sig automatisk til side bredde med tekst wrapping

## 📁 Projekt Struktur

```
sql_speedinator/
├── src/                          # Kildekode
│   ├── analyzers/               # Performance analysers
│   ├── core/                    # Kerne funktionalitet
│   ├── reports/                 # Rapport generering
│   └── services/                # Eksterne services (AI)
├── tests/                       # Test filer
│   ├── test_connection.py      # SQL forbindelse test
│   ├── test_ai_integration.py  # AI integration test
│   ├── test_enhancements.py    # PDF enhancement test
│   ├── test_responsive_tables.py # Responsive tabel test
│   ├── test_simple_pdf.py      # Simpel PDF test
│   └── README.md              # Test dokumentation
├── reports/                     # Genererede rapporter
├── logs/                        # Log filer
├── .env                        # Konfiguration
├── env.example                 # Konfiguration eksempel
└── main.py                     # Hovedprogram
```

## ⚙️ Konfiguration

Kopier `env.example` til `.env` og udfyld konfigurationen:

### SQL Server Connection
```env
# Windows Authentication (anbefalet - standard)
USE_WINDOWS_AUTH=true            # true = Windows Auth (default), false = SQL Auth
SQL_USERNAME=                    # Kun nødvendig hvis USE_WINDOWS_AUTH=false
SQL_PASSWORD=                    # Kun nødvendig hvis USE_WINDOWS_AUTH=false
SQL_TRUSTED_CONNECTION=yes       # Lad være 'yes'

# For SQL Server Authentication (kun hvis nødvendig)
USE_WINDOWS_AUTH=false           # Skift til false for SQL Auth
SQL_USERNAME=myuser              # Udfyld SQL brugernavn
SQL_PASSWORD=mypassword          # Udfyld SQL adgangskode
```

### Azure OpenAI Integration
```env
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4
AI_ANALYSIS_ENABLED=true
```

### Rapport Indstillinger
```env
ULTRA_COMPACT_MODE=true          # Ultra-kompakt design
SCHULTZ_DESIGN=true              # Schultz corporate design
PDF_COMPRESSION=true             # PDF komprimering
```

## 🔧 Installation

1. Installer dependencies:
```bash
pip install -r requirements.txt
```

2. Konfigurer .env fil med dine indstillinger

3. Kør analyse:
```bash
python main.py -s YOUR_SERVER_NAME
```

## 🧪 Testing

Kør tests fra rod mappen:

```bash
# Test SQL Server forbindelse
python tests/test_connection.py

# Test AI integration  
python tests/test_ai_integration.py

# Test PDF enhancements
python tests/test_enhancements.py

# Test responsive tabeller
python tests/test_responsive_tables.py

# Test PDF generering
python tests/test_simple_pdf.py
```

### Test Kategorier

- **Forbindelse**: `test_connection.py` - SQL Server forbindelse og basic queries
- **AI Integration**: `test_ai_integration.py` - Azure OpenAI funktionalitet  
- **PDF Enhancements**: `test_enhancements.py` - Ultra-compact design og Schultz styling
- **Responsive Design**: `test_responsive_tables.py` - Tabel responsivitet og tekst wrapping
- **End-to-End**: `test_simple_pdf.py` - Komplet PDF rapport generering

## 📊 Rapport Funktioner

### Responsive Tabeller
- Tabeller tilpasser sig automatisk til side bredde (6.0 tommer)
- Automatisk tekst wrapping i celler
- Optimerede kolonne vægte for bedre læsbarhed

### AI Analyse
- Intelligent identifikation af performance bottlenecks
- Anbefalinger baseret på best practices
- 47-58% token optimering for cost efficiency

### Design
- Ultra-kompakt layout for minimal fil størrelse
- Schultz corporate farvepalette
- Modern underline styling
- Responsive design principper

## 🔍 Performance Analyse

Værktøjet analyserer:
- **Wait Statistics**: Identifikation af wait types og bottlenecks
- **I/O Performance**: Disk performance metrics
- **Index Analyse**: Fragmentering og rebuild anbefalinger  
- **Missing Indexes**: Manglende index identifikation
- **Server Konfiguration**: Konfiguration anbefalinger
- **TempDB Analyse**: TempDB performance og konfiguration
- **Plan Cache**: Query plan analyse

## 🤖 AI Integration

Azure OpenAI integration giver:
- Intelligent performance bottleneck identifikation
- Kontekstuelle anbefalinger
- Prioriterede handlingsplaner
- Token-optimeret data transmission

## 📈 Responsive Design

Alle tabeller implementerer:
- Automatisk kolonne bredde beregning
- Tekst wrapping til flere linjer
- Konsistent side bredde (6.0 tommer)
- Ultra-kompakt spacing

## 📝 Logs

Se `logs/sql_speedinator.log` for detaljeret logging af alle operationer.

## 🔒 Sikkerhed og Authentication

### Windows Authentication (Anbefalet)
- **Standard**: Værktøjet bruger Windows Authentication som standard
- **Sikkerhed**: Bruger dine nuværende Windows credentials
- **Ingen adgangskoder**: Ingen adgangskoder gemmes i konfiguration
- **Konfiguration**: `USE_WINDOWS_AUTH=true` (standard)

### SQL Server Authentication
- **Kun hvis nødvendig**: Kun brug hvis Windows Auth ikke er muligt
- **Konfiguration**: Sæt `USE_WINDOWS_AUTH=false` og udfyld SQL_USERNAME/SQL_PASSWORD
- **Sikkerhed**: Adgangskoder gemmes i .env fil (hold denne sikker)

### Best Practices
- Brug altid Windows Authentication når muligt
- Hold .env filen ude af version control
- Brug stærke adgangskoder for SQL Authentication
- Overvej service accounts for scheduled tasks

## 🔒 Sikkerhed

- Understøtter Windows Authentication som standard (anbefalet)
- SQL Server Authentication kun når nødvendigt
- Sikker konfiguration gennem .env filer
- Azure OpenAI integration med API nøgler