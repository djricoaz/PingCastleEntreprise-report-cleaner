# PingCastleEntreprise-report-cleaner
# PingCastle Enterprise Report Cleaner

A maintenance tool for **PingCastle Enterprise** SQL databases running for years with frequent scans (e.g., weekly).  
It helps **reduce database size** while keeping a long-term trace of security posture.

## Problem / Use case

Many customers run PingCastle scans weekly for 5–6+ years. This creates a large number of reports in the database.

**Goal:**
- Keep **all reports** for the most recent *N days* (e.g., last 365 days).
- For reports **older than N days**: keep **only one report per month per domain**
  - Kept report = the **latest** report of that month (based on **Generation** = “Last report”).
  - Other reports in the same month/domain become candidates for removal.
- Optionally **archive** the reports before deletion.

This keeps meaningful historical monthly snapshots while significantly reducing storage.

---

## How it works (high level)

1. Connects to the PingCastle **SQL Server** database.
2. Reads the `Reports` table (auto-detect schema) and joins domain name from `Domains`.
3. Builds a retention plan based on:
   - **Generation date** (aka “Last report”)
   - Your chosen threshold (1 year / 6 months / custom days)
4. Produces CSV plan outputs:
   - All reports
   - Reports kept (recent)
   - Reports kept (monthly for old data)
   - Reports to delete (extras)
5. Dry-run by default (no changes).
6. If confirmed:
   - Optionally archives selected report rows (JSONL or CSV, optional ZIP)
   - Deletes dependent rows (child tables with FK to `Reports`)
   - Deletes the report rows
   - Updates `Domains.FirstReportID / LastReportID / NumberOfReport` to remain consistent

---

## Repository layout

# PingCastle Enterprise Report Cleaner

A maintenance tool for **PingCastle Enterprise** SQL databases running for years with frequent scans (e.g., weekly).  
It helps **reduce database size** while keeping a long-term trace of security posture.

## Problem / Use case

Many customers run PingCastle scans weekly for 5–6+ years. This creates a large number of reports in the database.

**Goal:**
- Keep **all reports** for the most recent *N days* (e.g., last 365 days).
- For reports **older than N days**: keep **only one report per month per domain**
  - Kept report = the **latest** report of that month (based on **Generation** = “Last report”).
  - Other reports in the same month/domain become candidates for removal.
- Optionally **archive** the reports before deletion.

This keeps meaningful historical monthly snapshots while significantly reducing storage.

---

## How it works (high level)

1. Connects to the PingCastle **SQL Server** database.
2. Reads the `Reports` table (auto-detect schema) and joins domain name from `Domains`.
3. Builds a retention plan based on:
   - **Generation date** (aka “Last report”)
   - Your chosen threshold (1 year / 6 months / custom days)
4. Produces CSV plan outputs:
   - All reports
   - Reports kept (recent)
   - Reports kept (monthly for old data)
   - Reports to delete (extras)
5. Dry-run by default (no changes).
6. If confirmed:
   - Optionally archives selected report rows (JSONL or CSV, optional ZIP)
   - Deletes dependent rows (child tables with FK to `Reports`)
   - Deletes the report rows
   - Updates `Domains.FirstReportID / LastReportID / NumberOfReport` to remain consistent

---

## Repository layout

# PingCastle Enterprise Report Cleaner

A maintenance tool for **PingCastle Enterprise** SQL databases running for years with frequent scans (e.g., weekly).  
It helps **reduce database size** while keeping a long-term trace of security posture.

## Problem / Use case

Many customers run PingCastle scans weekly for 5–6+ years. This creates a large number of reports in the database.

**Goal:**
- Keep **all reports** for the most recent *N days* (e.g., last 365 days).
- For reports **older than N days**: keep **only one report per month per domain**
  - Kept report = the **latest** report of that month (based on **Generation** = “Last report”).
  - Other reports in the same month/domain become candidates for removal.
- Optionally **archive** the reports before deletion.

This keeps meaningful historical monthly snapshots while significantly reducing storage.

---

## How it works (high level)

1. Connects to the PingCastle **SQL Server** database.
2. Reads the `Reports` table (auto-detect schema) and joins domain name from `Domains`.
3. Builds a retention plan based on:
   - **Generation date** (aka “Last report”)
   - Your chosen threshold (1 year / 6 months / custom days)
4. Produces CSV plan outputs:
   - All reports
   - Reports kept (recent)
   - Reports kept (monthly for old data)
   - Reports to delete (extras)
5. Dry-run by default (no changes).
6. If confirmed:
   - Optionally archives selected report rows (JSONL or CSV, optional ZIP)
   - Deletes dependent rows (child tables with FK to `Reports`)
   - Deletes the report rows
   - Updates `Domains.FirstReportID / LastReportID / NumberOfReport` to remain consistent

---

## Repository layout

.
├── src
│ ├── pingcastle_maintenance.py
│ ├── PingCastle-Maintenance.ps1
│ ├── generate_fakeport_synthetic_pingcastle.py (optional)
│ └── template.xml (optional)
├── examples
│ └── ... (optional test datasets)
└── README.md
---

## Requirements

### System
- Windows (recommended, since most PingCastle deployments run on Windows)
- Access to the SQL Server hosting PingCastle Enterprise DB

### Software
- Python 3.10+ recommended (or use the PowerShell wrapper that bootstraps a `.venv`)
- Microsoft SQL Server ODBC driver
  - **ODBC Driver 17 for SQL Server** or **ODBC Driver 18 for SQL Server**

### Permissions
- Read access to PingCastle DB for dry-run / plan export
- Read + delete permissions for cleanup mode:
  - Delete from `Reports`
  - Delete from dependent tables referencing `Reports` (FK)
  - Update in `Domains`

---

## Quick start 

### 1) Run via PowerShell wrapper

```powershell
cd .\src
powershell -ExecutionPolicy Bypass -File .\PingCastle-Maintenance.ps1

