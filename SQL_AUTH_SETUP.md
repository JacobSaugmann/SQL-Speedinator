# SQL Speedinator - Authentication Setup Guide

## 🔐 Windows Authentication (Anbefalet - Standard)

Windows Authentication er den anbefalede og sikre måde at forbinde til SQL Server på.

### Konfiguration
```env
# I .env filen
USE_WINDOWS_AUTH=true             # Standard - brug Windows Authentication
SQL_USERNAME=                     # Lad være tom
SQL_PASSWORD=                     # Lad være tom
SQL_TRUSTED_CONNECTION=yes        # Lad være 'yes'
```

### Fordele
- ✅ Ingen adgangskoder i konfigurationsfiler
- ✅ Bruger dine Windows credentials automatisk
- ✅ Integreret med Windows sikkerhed
- ✅ Standard og anbefalet af Microsoft

### Krav
- Du skal have adgang til SQL Server med din Windows bruger
- SQL Server skal være konfigureret til at acceptere Windows Authentication

---

## 🔑 SQL Server Authentication (Kun hvis nødvendigt)

Brug kun SQL Server Authentication hvis Windows Authentication ikke er muligt.

### Konfiguration
```env
# I .env filen
USE_WINDOWS_AUTH=false            # Skift til SQL Authentication
SQL_USERNAME=din_sql_bruger       # SQL Server brugernavn
SQL_PASSWORD=din_sql_adgangskode   # SQL Server adgangskode
SQL_TRUSTED_CONNECTION=yes        # Lad være 'yes'
```

### Sikkerhedsovervejelser
- ⚠️ Adgangskoder gemmes i .env fil
- ⚠️ Hold .env filen sikker og ude af version control
- ⚠️ Brug stærke adgangskoder
- ⚠️ Overvej at bruge environment variables i produktion

### Eksempel SQL Server bruger oprettelse
```sql
-- På SQL Server (kør som administrator)
CREATE LOGIN [sql_performance_user] WITH PASSWORD='StrongPassword123!';
CREATE USER [sql_performance_user] FOR LOGIN [sql_performance_user];

-- Giv nødvendige permissions
ALTER SERVER ROLE [db_datareader] ADD MEMBER [sql_performance_user];
GRANT VIEW SERVER STATE TO [sql_performance_user];
GRANT VIEW ANY DEFINITION TO [sql_performance_user];
```

---

## 🧪 Test din konfiguration

Kør connection test for at verificere din opsætning:

```bash
cd "C:\Users\jsa\Scripts\Python Projects\Sql_speedinator"
python tests/test_connection.py
```

### Forventet output (Windows Auth)
```
✓ Configuration loaded successfully
  - SQL Driver: ODBC Driver 17 for SQL Server
  - Windows Authentication: True
  - Trusted Connection: True
```

### Forventet output (SQL Auth)
```
✓ Configuration loaded successfully
  - SQL Driver: ODBC Driver 17 for SQL Server
  - Windows Authentication: False
  - SQL Username: din_sql_bruger
```

---

## 🚀 Kør analyse

Efter konfiguration kan du køre performance analyse:

```bash
# Med Windows Authentication (anbefalet)
python main.py -s LOCALHOST

# Med SQL Authentication
python main.py -s LOCALHOST

# Med remote server
python main.py -s SERVER01\SQLINSTANCE01
```

---

## 🔧 Fejlfinding

### Windows Authentication fejler
- Kontroller at din Windows bruger har adgang til SQL Server
- Verificer at SQL Server accepterer Windows Authentication
- Sørg for at SQL Server service kører

### SQL Authentication fejler
- Kontroller brugernavn og adgangskode
- Verificer at SQL Server Authentication er aktiveret
- Kontroller at brugeren har de nødvendige permissions

### ODBC Driver fejl
- Installer "ODBC Driver 17 for SQL Server" fra Microsoft
- Opdater driver til nyeste version

---

## 📋 Permissions påkrævet

Performance analysen kræver følgende SQL Server permissions:

- `VIEW SERVER STATE` - For DMV adgang
- `VIEW ANY DEFINITION` - For metadata adgang  
- `db_datareader` - For at læse system databaser
- Adgang til system databases (master, msdb, etc.)

Disse permissions er automatisk inkluderet for Windows Administrators.