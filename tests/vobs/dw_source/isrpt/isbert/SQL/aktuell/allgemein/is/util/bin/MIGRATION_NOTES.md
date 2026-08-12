# Migration Notes: Shared Utility Binaries

**Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`  
**Version:** 3.0.9 (Parameter) / 1.1.3 (SQL*Plus) / 1.0.0 (MsgErr)  
**Target Platform:** Google Cloud Platform (GCP) — Cloud Composer (Apache Airflow) / Python 3.x with transitional Oracle DB connectivity (`oracledb`) and long-term BigQuery/Cloud Logging alignment.

---

## 1. Summary

This migration covers the transition of four core KornShell (KSH) utility libraries into native, high-performance Python 3.x modules. These libraries provide the foundational orchestration, logging, parameter validation, and date arithmetic services for the entire Information Services (IS) data warehouse ETL pipeline.

The following legacy shell scripts have been fully migrated:
*   `f_alis_msgerr.ksh` $\rightarrow$ `f_alis_msgerr.py` (Job registration, status tracking, and error telemetry)
*   `h_alis_date.ksh` $\rightarrow$ `h_alis_date.py` (Date arithmetic, validation, and range calculations)
*   `h_alis_parameter.ksh` $\rightarrow$ `h_alis_parameter.py` (Parameter parsing, normalization, and business domain mapping)
*   `h_alis_sqlplus.ksh` $\rightarrow$ `h_alis_sqlplus.py` (Safe SQL*Plus execution wrapper)

---

## 2. Generated Artifacts

| Legacy File | Generated Python File | Role / Functional Description |
| :--- | :--- | :--- |
| `f_alis_msgerr.ksh` | `f_alis_msgerr.py` | Manages job execution states (OK, ABORTED), registers execution instances via Oracle sequences, logs application errors, and appends timing metrics to the `BERT_MELDUNG` database package. |
| `h_alis_date.ksh` | `h_alis_date.py` | Performs calendar calculations, leap-year checks, date additions, and period range determinations. Replaces legacy database round-trips with native Python math. |
| `h_alis_parameter.ksh` | `h_alis_parameter.py` | Normalizes verbose metric names (Kennzahlen) and source systems into standard 3-letter codes. Enforces system-metric compatibility matrices and tracks global error states. |
| `h_alis_sqlplus.ksh` | `h_alis_sqlplus.py` | Verifies SQL script readability and parameter completeness on the filesystem before invoking `sqlplus` in a non-interactive subshell. |

---

## 3. Key Design Decisions

### Native Python Date Math over Database Round-Trips
In the legacy KSH implementation, basic date checks and previous-month calculations were routed through `sqlplus` queries against the Oracle `dual` table. In `h_alis_date.py`, these have been completely replaced with native Python `datetime` and `calendar` operations. This eliminates database connection latency, reduces CPU consumption on the database server, and ensures the date utility can run completely offline.

### Stateful Environment Emulation for Parameters
Legacy calling scripts rely heavily on sourcing these libraries to mutate the parent shell's environment variables (using `eval` and `set -A`). To preserve this behavior without breaking legacy orchestration patterns during the phased migration, `h_alis_parameter.py` utilizes `os.environ` and dynamic dictionary updates to track global error states (`ErrNr`, `ErrArg`) and return normalized values.

### Verbatim Preservation of German Print Literals
In compliance with operational monitoring requirements, all terminal outputs, assertion failures, and error messages have been preserved **character-for-character in German** (e.g., `"Argh!, keine EintragsNummer bei Aufruf von..."`, `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"`). This ensures that legacy log-scraping tools, regex-based alert monitors, and UC4 post-processing instructions continue to function without modification.

### Transitional Oracle DB Support
While the long-term target architecture is Google Cloud BigQuery, the logging (`f_alis_msgerr.py`) and execution (`h_alis_sqlplus.py`) modules retain transitional Oracle database support via the modern `oracledb` thin client. This supports a phased migration strategy where orchestration is moved to Python/Airflow first, while the underlying data warehouse remains on Oracle prior to the final BigQuery cutover.

---

## 4. Manual Steps Before Go-Live

### 1. Environment Variables & Secrets
The following environment variables must be configured in the execution environment (e.g., Airflow Worker environment, local OS profile, or Airflow Variables):
*   `DW_ORAUSER`: The Oracle database connection string. Format: `username/password@hostname:port/service_name` or a valid TNS alias.
*   `DW_DIR_PROT`: The absolute path to the directory where execution protocol logs are written.
*   `DW_DIR_ROOT`: The root directory of the project codebase (used by `h_alis_sqlplus.py` to locate auxiliary SQL wrappers).

### 2. IAM & Permissions
*   The service account executing the Python scripts must have **Read** permissions on all SQL script directories.
*   The service account must have **Write** permissions on the directory specified by `DW_DIR_PROT` to generate log files.
*   Network firewall rules (e.g., GCP VPC egress rules) must allow TCP traffic on port 1521 (or the configured Oracle port) to the target database.

### 3. Database Schema Verification
Ensure that the following database objects exist in the Oracle schema associated with `DW_ORAUSER`:
*   The sequence `bert_sequence` (used to generate unique execution tracking numbers).
*   The package `BERT_MELDUNG` with the following procedures:
    *   `SetzeStatusOk(eintrags_nr)`
    *   `SetzeStatusAbbruch(eintrags_nr)`
    *   `Erzeuge_Eintrag(eintrags_nr, job_kennung, programm_name, log_datei)`
    *   `Fehler(typ, eintrags_nr, fehler_nr, zusatz1, zusatz2)`
    *   `SetzeZusatzInfos(eintrags_nr, stichtag, info_text)`

### 4. Airflow Deployment
These files are **shared utility libraries** and do not contain DAG definitions. They must be deployed to the Airflow `plugins/` or a shared `libs/` directory on the Cloud Composer environment so they can be imported by migrated DAGs:
```python
from is.util.bin.h_alis_date import AddiereDatum, LetzterTagDesMonats
from is.util.bin.h_alis_parameter import konvertiereKennzahl, pruefeZeitParameter
```

---

## 5. Known Gaps & Unresolved References

### Unsupplied External SQL Files
The legacy date and logging libraries referenced several external SQL scripts that were not provided in the migration source bundle:
*   `d_alis_vormonat.sql` (Replaced with native Python Gregorian calendar math in `DWDate_Vormonat`).
*   `d_alis_datum_zeitraum.sql` (Replaced with native Python `relativedelta` math in `DWDate_Gib_Zeitraum`).
*   `d_alis_spaufruf_p1.sql` through `d_alis_spaufruf_p5.sql` (Replaced with direct PL/SQL anonymous block executions via `oracledb`).

*Risk:* If these unsupplied SQL scripts contained custom non-Gregorian business calendars or holiday-specific logic, the native Python simulations may diverge. **Manual verification against the legacy database's calendar rules is required.**

### Unmigrated Downstream Consumers
A total of 12 downstream ETL jobs (e.g., `DW.BERT_RECHNUNGSDATEN`, `DW.BERT_ABLAUFSTEUERUNG`) still source the legacy `.ksh` files. These downstream jobs must be migrated to Python to utilize the new `.py` libraries. Until then, both the legacy `.ksh` and modern `.py` files must be maintained in parallel.

### Redesign (B4) Items for BigQuery Cutover
The logging (`f_alis_msgerr.py`) and execution (`h_alis_sqlplus.py`) modules are currently coupled to Oracle-specific technologies (`oracledb`, `sqlplus`, PL/SQL packages). When the database is migrated to BigQuery:
1.  `BERT_MELDUNG` calls must be redesigned to write to a BigQuery audit table (e.g., using `google-cloud-bigquery` client library).
2.  `h_alis_sqlplus.py` must be retired or refactored into a BigQuery execution wrapper that runs SQL queries via the BigQuery API instead of launching a `sqlplus` subprocess.

---

## 6. Validation

To validate the correctness of the migrated Python modules, execute the following test suite from the command line.

### 1. Parameter Normalization Validation
Verify that verbose metrics are correctly mapped to 3-letter codes and that errors are caught:
```bash
# Test successful conversion
export ErrNr=0; export ErrArg=""
python3 h_alis_parameter.py konvertiereKennzahl bestand
# Expected output: ErrNr: 0, ErrArg: "", and os.environ['bestand'] is mutated to 'bst'

# Test invalid combination check
python3 h_alis_parameter.py pruefeSystemKennzahl carmen twe
# Expected output: ErrNr: 195, ErrArg: "Ungueltige Kombination carmen twe"
```

### 2. Date Arithmetic Validation
Verify that leap years and date additions are calculated correctly without database access:
```bash
# Test leap year check (1999 is not a leap year, Feb has 28 days)
python3 h_alis_date.py TageimMonat 1999 02
# Expected output: 28

# Test leap year check (2000 is a leap year, Feb has 29 days)
python3 h_alis_date.py TageimMonat 2000 02
# Expected output: 29

# Test date addition across month boundary
python3 h_alis_date.py AddiereDatum 19991230 5
# Expected output: 20000104
```

### 3. Database Logging Validation (Integration Test)
Verify connection handling and sequence generation (requires active Oracle DB connection):
```bash
export DW_ORAUSER="user/password@host:port/service"
python3 f_alis_msgerr.py ermittle-nr my_var
# Expected output: A unique integer sequence number (e.g., 123456)
```

### 4. SQL*Plus Wrapper Validation
Verify that file checks and execution wrappers work as expected:
```bash
# Test missing file validation
python3 h_alis_sqlplus.py 999999 /nonexistent/path/script.sql
# Expected exit code: 201 (File not readable)
```

---

## 7. Rollback Procedure

In the event of an operational failure or validation discrepancy post-deployment, execute the following rollback steps:

1.  **Revert Calling Scripts:** Revert any migrated calling scripts from importing the Python modules back to sourcing the legacy shell libraries:
    ```bash
    # Revert this:
    # python3 my_job.py
    
    # Back to this:
    # . f_alis_msgerr.ksh
    # . h_alis_date.ksh
    # . h_alis_parameter.ksh
    # . h_alis_sqlplus.ksh
    ```
2.  **Keep Legacy Files Intact:** Do not delete or modify the original `.ksh` files in the legacy bin directory during the transition phase. They must remain in place to allow instantaneous rollback.
3.  **Database State:** No database rollback is required for the utility code itself, as the underlying Oracle packages (`BERT_MELDUNG`) and sequence (`bert_sequence`) are not modified by the Python migration.