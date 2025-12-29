---
description: Development and code quality guidelines for SQL Speedinator project
applyTo: 'src/**'
---

# SQL Speedinator - Python Best Practice Guide

**Formål:** Høj kodekvalitet, vedligeholdelse, skalerbarhed  
**Status:** Active - følges ved alle contributions

⚠️ **VIGTIG:** Denne fil skal selv overholde sin egen regel: **Max 350 linjer**.  
Når nye regler tilføjes → fjern mindre kritiske. Ingen "bare add" uden cleanup!

---

## 1. METODESTØRRELSE & KOMPLEKSITET

**Target:** Max 30-40 linjer pr. metode, 1 ansvar pr. funktion

✅ **Gør det:**
- Split lange metoder i mindre helper-funktioner
- Hver metode laver ÉN ting
- Bruge private helper-metoder (_method_name) for detaljer

❌ **Gør det IKKE:**
- Methods over 40 linjer
- 300+ linjer funktioner (main.py `run_analysis` = 260 linjer ❌)
- Nested loops uden grund (O(n²) eller værre)

---

## 1b. OUTPUT & LOGGING - KRITISK REGEL ⚠️

**ALDRIG print() til logging - ALTID self.logger:**

```python
self.logger.info("Starting analysis...")      # ✅ Normal info
self.logger.warning("High fragmentation")     # ✅ Advarsler
self.logger.error(f"Failed: {e}")             # ✅ Fejl med kontekst
self.logger.debug("Development details")      # ✅ Dev info
```

✅ **Brug logger fordi:**
- Kontrolleres via config (on/off per niveau)
- Timestamps + module names automatisk
- Production-ready

❌ **ALDRIG:**
- print("...") til logging - Ingen kontrol, uprofessionelt
- **EMOJI I LOGGER, PRINT ELLER OUTPUT** - Windows console (cp1252) kan ikke håndtere Unicode emoji
- Emoji i logger.info/warning/error - Giver UnicodeEncodeError på Windows
- Emoji i print() statements - Giver encoding fejl i console
- print() til fejlmeddelelser - Ingen kontekst

**Windows Console Encoding - ABSOLUT FORBUD MOD EMOJI:**
```python
# ❌ DÅRLIGT - Crasher på Windows console
self.logger.info("✅ Analysis complete")
self.logger.warning("⚠️ High CPU")
self.logger.error("❌ Failed")
print("🚀 Starting...")
print("✅ Done!")

# ✅ GODT - ASCII only ALTID
self.logger.info("Analysis complete")
self.logger.warning("High CPU detected")
self.logger.error("Operation failed")
print("Starting...")
print("Done!")
```

**Regel:** INGEN emoji nogen steder i koden - hverken logger, print, output eller kommentarer.

### ❌ DONT - Hvad skal du UNDGÅ

```python
# ❌ DÅRLIGT - 300+ linjer, 10+ ansvarsområder (se main.py::run_analysis)
def run_analysis(server_name, output_path, config, night_mode=False, 
                ai_analysis=False, perfmon_file=None, perfmon_duration=240):
    # ... PerfMon setup (80 linjer)
    # ... Connection logic (30 linjer)
    # ... Analysis execution (50 linjer)
    # ... Report generation (40 linjer)
    # ... Cleanup (60 linjer)
    # = 260+ linjer i ÉN metode!

# ❌ DÅRLIGT - Constructor med 100+ linjer styling (se pdf_report_generator.py)
def __init__(self, config=None):
    # Setup 50+ paragraph styles...
    self.styles.add(ParagraphStyle(...))  # x50
    # = 200+ linjer i constructor!

# ❌ DÅRLIGT - Method med complexity O(n²)
def analyze_all_combinations(self, items: List):
    for item1 in items:           # Loop 1
        for item2 in items:       # Loop 2
            for item3 in items:   # Loop 3 - OMG!
                self.process(item1, item2, item3)  # Cubic complexity!
```

---

## 2. KLASSE-DESIGN & SINGLE RESPONSIBILITY

**Mål:** <200 linjer per klasse, 5-8 fokuserede metoder

✅ **Godt klassdesign:**
```python
class WaitStatsAnalyzer(BaseAnalyzer):
    """Analyzes SQL Server wait statistics"""
    
    def analyze(self) -> Dict[str, Any]:
        wait_data = self._get_wait_stats()
        high_waits = self._identify_high_waits(wait_data)
        return {'wait_types': wait_data, 'high_waits': high_waits}
```

❌ **MONSTER KLASSE (1752 linjer):**
- 50+ paragraph styles
- 30+ sektion-generator metoder
- 20+ tabel-formatter metoder
- **RESULTAT:** Umulig at teste, vedligeholde, scale

**Løsning:** Split i 4 klasser:
- PDFStyleManager (styling)
- SectionBuilder (sektion-generering)
- TextFormatter (tekst-operations)
- PDFReportGenerator (orchestration)

---

## 3. INHERITANCE & ABSTRAKTION

✅ **Altid BaseAnalyzer abstract klasse:**
```python
from abc import ABC, abstractmethod

class BaseAnalyzer(ABC):
    """Abstract base for all analyzers"""
    
    def __init__(self, connection: SQLServerConnection, config: ConfigManager):
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def analyze(self) -> Dict[str, Any]:
        """Run analysis - implementeres af subklasser"""
        pass
```

❌ **IKKE:** Samme struktur uden base klasse - IDE kan ikke hjælpe, ingen kontrakts-dokumentation

---

## 4. KONFIGURATION & KONSTANTER

✅ **Centraliseret - alle konstanter i ConfigManager:**
```python
@property
def disk_latency_threshold_ms(self) -> int:
    return self._get_int('DISK_LATENCY_THRESHOLD', 20)

# Brug fra klasse
threshold = self.config.disk_latency_threshold_ms
```

❌ **IKKE:** Hardkodede værdier spredt omkring i kode - `frag_threshold = 30`

---

## 5. ERROR HANDLING

✅ **DO - Specific exceptions:**
```python
class DatabaseConnectionError(Exception):
    """Raised when SQL connection fails"""
    pass

try:
    cursor.execute(query)
except pyodbc.DatabaseError as e:
    raise DatabaseConnectionError(f"Database error: {e}")
except Exception as e:
    self.logger.error(f"Unexpected error: {e}", exc_info=True)
    raise

# ✅ Graceful degradation - return partial results
results = {}
for analyzer_name, analyzer in self.analyzers.items():
    try:
        results[analyzer_name] = analyzer.analyze()
    except Exception as e:
        self.logger.error(f"{analyzer_name} failed: {e}")
        results[analyzer_name] = {'error': str(e)}
```

❌ **IKKE:**
- `except:` eller `except Exception:` uden specifik type
- Fail fast uden fallback - hele systemet dør hvis én analyzer fejler
- Ingen kontekst i fejlbesked

---

## 6. TYPE HINTS & DOKUMENTATION

✅ **Altid type hints og docstrings:**
```python
def analyze_indexes(self, database: str, limit: int = 100) -> Dict[str, Any]:
    """Analyze index fragmentation and unused indexes.
    
    Args:
        database: Database name to analyze
        limit: Maximum number of indexes to return
        
    Returns:
        Dict with 'fragmented' and 'unused' keys
        
    Raises:
        DatabaseConnectionError: If connection fails
    """
    pass
```

❌ **IKKE:**
- Manglende type hints: `def analyze(self, data):`
- Manglende docstrings på public metoder
- Docstrings uden Args/Returns/Raises

---

## 7. NAMING CONVENTIONS

✅ **DO - Clear naming:**
```python
# ✅ Navn fortæller hvad det gør
def calculate_index_fragmentation_percentage(self) -> float:
    pass

def identify_slow_disk_performance(self) -> List[str]:
    pass

# ✅ Beskrivende variable navne
high_fragmentation_indexes = []
slow_disk_drives = []
wait_recommendations = []

# ✅ Boolean navne starter med is_/has_
is_database_online = True
has_slow_disk = False
should_rebuild_index = True
```

❌ **IKKE:**
- Uklare navne: `process()`, `get_data()`, `fix_things()`
- Single-letter variables uden loop: `x = get_data()`, `y = process(x)`
- Abbreviationer uden kontekst: `frag_idx`, `db_wait`

---

## 8. PERFORMANCE & DRY

✅ **DO - Efficient code:**
- Limit result sets i SQL, ikke i Python
- Brug generators for store data sæt
- Cache repeated queries
- Undgå nested loops (O(n²) eller værre)

❌ **IKKE:**
- Load all data så filter i Python
- Query i loops (1000 queries = 1000 roundtrips)
- Duplicate queries eller logic

---

## 9. CODE REVIEW CHECKLIST

Før du committer:

```
□ Metoder under 40 linjer?          □ Docstrings på alle public metoder?
□ Klasser under 300 linjer?         □ Ingen hardkodede values?
□ Type hints overalt?               □ Error handling specifikt?
□ Return types specifikt?           □ ALDRIG print() - brug logger
□ Tests kørt i korrekt .venv?       □ Midlertidige filer ryddet op?
```

---

## 9b. TEST & DEPLOYMENT - KRITISK ⚠️

**ALTID brug korrekt virtual environment:**

```powershell
# ✅ KORREKT - Aktivér .venv først
cd "C:\Users\jsa\Scripts\Python Projects\Sql_bottleneck"
.\.venv\Scripts\Activate.ps1
python main.py -s localhost --perfmon-duration 120

# ✅ KORREKT - Pytest i venv
.\.venv\Scripts\Activate.ps1
python -m pytest tests/ -v

# ❌ FORKERT - Global Python environment
python main.py  # Bruger system Python, ikke project dependencies!
```

**Cleanup efter tests:**

```python
# ✅ ALTID ryd op efter tests
import os
import shutil
from pathlib import Path

def cleanup_test_artifacts():
    """Fjern midlertidige test filer og dokumenter"""
    temp_patterns = [
        'tests/fixtures/temp_*.pdf',
        'tests/output/*.csv',
        'tests/coverage_html/',
        '**/__pycache__',
        '**/*.pyc'
    ]
    for pattern in temp_patterns:
        for path in Path('.').glob(pattern):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)
```

❌ **ALDRIG efterlad:**
- Test output filer (PDFs, CSVs) i repo
- __pycache__ directories
- .pyc compiled files
- Midlertidige log filer fra tests

---

## 10. GIT COMMIT FORMAT

```
<type>(<scope>): <subject>

Examples:
  feat(analyzers): Add new MemoryAnalyzer class
  fix(connection): Handle timeout gracefully
  refactor(pdf): Split PDFReportGenerator
  test(index): Add fragmentation detection test

Types: feat, fix, refactor, test, docs, perf, chore
```

---

## ⚠️ FILE MAINTENANCE RULE

**Denne copilot-instructions fil SKAL selv overholde Regel #2 (klasse-design):**

- **Max 350 linjer** (aktuelt: ~285 linjer ✅)
- **1 ansvar:** Define Python coding standards only
- **Når nye regler tilføjes:** Fjern mindre kritiske eller fold ind i eksisterende sektion
- **ALDRIG bare add:** Skal blive kompakt, fokuseret, actionable

**Hvis filen vokser > 350 linjer:**
1. Fjern redundante eksempler
2. Fold sekvenser sammen
3. Flytte details til selve koden som kommentarer
4. Ikke tilføje nye sekvenser uden at fjerne andre

**Formål:** Ensure copilot kan læse hele filen i kontekst uden token-overhead.
