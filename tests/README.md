# Test Files

Denne mappe indeholder test-filer til SQL Speedinator.

## Test Filer

### `test_connection.py`
- Tester SQL Server forbindelse og grundlæggende funktionalitet
- Verificerer konfiguration loading
- Tester database forbindelse og basic queries
- Validerer connection pooling og error handling

### `test_ai_integration.py`
- Tester AI integration med Azure OpenAI
- Verificerer AI analyse funktionalitet med mock data
- Tester token optimering og response parsing
- Validerer AI bottleneck identifikation og anbefalinger

### `test_enhancements.py`
- Tester PDF rapport generator enhancements
- Verificerer ultra-compact design implementation
- Tester Schultz design elementer
- Validerer responsive tabel funktionalitet

### `test_responsive_tables.py`
- Tester responsive tabel bredde beregninger
- Verificerer at alle tabeller passer inden for side bredden (6.0 tommer)
- Tester tekst wrapping funktionalitet
- Validerer at optimerede kolonne vægte fungerer korrekt

### `test_simple_pdf.py`
- Simpel test til generering af PDF rapport med responsive tabeller
- Bruger test data til at verificere PDF generering
- Kontrollerer fil størrelse og ultra-compact design
- Tester AI integration output

## Kørsel af Tests

```bash
# Fra rod mappen
cd "C:\Users\jsa\Scripts\Python Projects\Sql_speedinator"

# Test SQL Server forbindelse
python tests/test_connection.py

# Test AI integration
python tests/test_ai_integration.py

# Test PDF enhancements
python tests/test_enhancements.py

# Test responsive tabeller
python tests/test_responsive_tables.py

# Test simpel PDF generering
python tests/test_simple_pdf.py
```

## Test Kategorier

### Forbindelse Tests
- `test_connection.py` - Grundlæggende SQL Server forbindelse

### Funktionalitets Tests  
- `test_ai_integration.py` - AI analyse funktionalitet
- `test_enhancements.py` - PDF rapport enhancements

### Design Tests
- `test_responsive_tables.py` - Responsive tabel implementation
- `test_simple_pdf.py` - Komplet PDF generering

## Test Resultater

Alle tests verificerer:
- ✅ SQL Server forbindelse fungerer korrekt
- ✅ AI integration virker som forventet
- ✅ PDF enhancements implementeret korrekt
- ✅ Tabeller passer perfekt inden for side bredde
- ✅ Automatisk tekst wrapping fungerer korrekt
- ✅ Ultra-compact design bevares

## Bemærkninger

- Tests bruger test data og mock data hvor relevant
- PDF output gemmes i tests mappen
- AI tests kræver korrekt Azure OpenAI konfiguration
- Alle tests understøtter den nye responsive tabel implementation