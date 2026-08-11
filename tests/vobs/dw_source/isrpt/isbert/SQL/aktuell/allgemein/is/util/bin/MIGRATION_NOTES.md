# Migration Notes: Shared Utility Binaries

**Target Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`  
**Source Platform:** KornShell (KSH) / Oracle SQL*Plus  
**Target Platform:** Python 3 / Google Cloud Platform (GCP) / Cloud Composer (Apache Airflow)

---

## 1. Summary

This migration covers the transition of the core shared utility libraries from a legacy Unix/Oracle environment to a modern, cloud-native Python 3 architecture. These utilities provide foundational services—such as centralized error handling, date arithmetic, parameter validation, and database execution wrappers—for the entire Information Services (IS) data warehouse ecosystem.

The legacy KornShell scripts have been refactored into modular, high-performance Python modules. While they maintain strict backward compatibility with legacy command-line interfaces (CLI) and environment-variable-driven states, they eliminate unnecessary database round-trips by performing calculations natively in Python.

---

## 2. Generated Artifacts

The migration has produced four Python modules corresponding to the legacy KSH files:

| Legacy File | Migrated Python File | Role / Responsibility |
| :--- | :--- | :--- |
| `f_alis_msgerr.ksh` | `f_alis_msgerr.py` | **Centralized Error & Run Tracker:** Interfaces with the `BERT_MELDUNG` Oracle package to register job runs, log application errors, record execution metrics, and handle aborted states. |
| `h_alis_date.ksh` | `h_alis_date.py` | **Date Arithmetic Library:** Performs leap-year-safe date calculations, format validations, and reporting-window offset computations natively in Python. Includes a CLI wrapper for shell compatibility. |
| `h_alis_parameter.ksh` | `h_alis_parameter.py` | **Parameter Parser & Validator:** Standardizes and validates KPIs (Kennzahlen), source systems, master data names, and timeframes. Manages global error states (`ErrNr`, `ErrArg`). |
| `h_alis_sqlplus.ksh` | `h_alis_sqlplus.py` | **SQL\*Plus Execution Wrapper:** Safely executes external SQL scripts with parameter validation, file readability checks, and error code propagation. |

---

## 3. Key Design Decisions

### Python-Native Date Math over Database Queries
In the legacy shell scripts, simple operations like validating a date format or checking if one date was less than another required launching a heavy `sqlplus` session to query the Oracle `DUAL` table. 
* **Decision:** All date validations, leap-year checks, and day/month additions in `h_alis_date.py` have been rewritten using Python's standard `datetime`, `calendar`, and `dateutil` libraries.
* **Trade-off:** This completely eliminates database network latency and connection overhead, resulting in a significant performance increase during parameter parsing phases.

### State Preservation via Environment Variables
The legacy parameter utility (`h_alis_parameter.ksh`) relies on mutating global shell variables (`ErrNr`, `ErrArg`) and using `eval` to pass values back to caller scripts.
* **Decision:** The migrated `h_alis_parameter.py` synchronizes its internal state with `os.environ` dynamically.
* **Trade-off:** While direct environment mutation is generally discouraged in pure Python, this approach was chosen to guarantee that legacy-style calling scripts (which may still be running in a hybrid migration phase) can source or call this utility without breaking their state-tracking logic.

### Transitional Subprocess Wrapper for SQL*Plus
The `h_alis_sqlplus.py` module retains the ability to launch `sqlplus` via Python's `subprocess` module.
* **Decision:** Keep the `sqlplus` command-line execution interface intact rather than immediately converting all SQL scripts to native Python database client executions.
* **Trade-off:** This allows downstream ETL jobs to be migrated incrementally without requiring a simultaneous rewrite of hundreds of legacy `.sql` files. It is flagged as a candidate for future B4 Redesign (see Section 5).

---

## 4. Manual Steps Before Go-Live

Before deploying these utilities to production, the following infrastructure and configuration steps must be completed:

### 1. Database Schema & Objects
Ensure that the target tracking database (Oracle) contains the required logging schema and packages:
* The `BERT_MELDUNG` PL/SQL package must be compiled and accessible.
* The sequence generator `SEQ_BERT_MELDUNG_ID` must be active.
* The execution tracking tables must be granted read/write permissions for the migration database user.

### 2. IAM & Secret Management
The database connection string (`DW_ORAUSER`) contains sensitive credentials and must not be hardcoded.
* **Action:** Store the connection string in **GCP Secret Manager** (e.g., `projects/PROJECT_ID/secrets/dw-oracle-user`).
* **Permissions:** Grant the Cloud Composer service account the Secret Manager Secret Accessor (`roles/secretmanager.secretAccessor`) role.

### 3. Environment Variables
Configure the following environment variables in the Cloud Composer (Airflow) environment:
* `DW_ORAUSER`: Sourced dynamically from Secret Manager at runtime.
* `DW_DIR_ROOT`: Points to the root directory of the migrated system scripts (typically `/home/airflow/gcs/dags/`).
* `DW_DIR_PROT`: Points to the GCS bucket or local mount directory where execution logs are deposited.

### 4. Scheduling & Deployment
These files are shared utility libraries and **must not** be scheduled as standalone Airflow DAGs.
* **Action:** Deploy these files to the `/dags/dependencies/` or `/plugins/` directory of your Cloud Composer GCS bucket so they are available in the Python search path (`sys.path`) for all executing DAG tasks.

---

## 5. Known Gaps & Unresolved References

### B4 Redesign: SQL*Plus Retirement
* **Gap:** `h_alis_sqlplus.py` still relies on the local availability of the `sqlplus` binary within the Airflow worker containers.
* **Redesign Plan:** This is a temporary transitional state. Once the downstream SQL scripts are migrated to BigQuery, this module should be retired. Downstream DAGs must be refactored to use native Airflow BigQuery operators (`BigQueryInsertJobOperator`) or the Google Cloud BigQuery Python client, eliminating the need for SQL*Plus entirely.

### Unmigrated Downstream Callers
There are 12 downstream jobs that source or execute these utilities which are **not yet migrated**:
* `DW.BERT_ABLAUFSTEUERUNG`
* `DW.BERT_AUSD_BP_TA_MSISDN`
* `DW.BERT_AUSD_BP_TA_P_BASISPROD`
* `DW.BERT_AUSD_V_TA_PERIOD`
* `DW.BERT_AUSD_V_TA_P_VERTRAG`
* `DW.BERT_AUSD_V_TA_VERTRAG_TMP`
* `DW.BERT_DROP_TEMP_TABLE`
* `DW.BERT_P_ADRESSEN`
* `DW.BERT_P_AUSTAUSCH`
* `DW.BERT_P_GESCHAEFTSP`
* `DW.BERT_P_RECH_EMPF`
* `DW.BERT_RECHNUNGSDATEN`

*Impact:* End-to-end integration testing of these utilities cannot be fully finalized until these downstream consumers are migrated to Python/Airflow.

### Global Trap Interception
* **Gap:** Legacy KSH scripts captured uncaught errors globally using shell-level traps (`trap 'DWMSG_Fehlerbehandlung' ERR`). Python does not have a direct equivalent for shell traps.
* **Mitigation:** Migrated Python tasks must wrap their execution blocks in standard `try...except` blocks, or utilize Airflow's `on_failure_callback` to explicitly call `fehlerbehandlung` in the event of a task failure.

---

## 6. Validation

To validate the correctness of the migrated Python utilities, execute the following test suites:

### Unit Testing
Run the pytest suite designed for the date and parameter utilities:
```bash
pytest test_h_alis_date.py
pytest test_h_alis_parameter.py
```

### CLI Integration Testing
Verify that the CLI wrappers produce the exact outputs expected by legacy shell scripts.

#### 1. Date Addition Validation
```bash
python3 h_alis_date.py AddiereDatum 20231024 5
# Expected Output: 20231029
```

#### 2. Date Range Calculation Validation
```bash
python3 h_alis_date.py DWDate_Gib_Zeitraum -1 M YYYYMMDD START_VAR END_VAR
# Expected Output:
# START_VAR=20230901
# END_VAR=20230930
```

#### 3. Error Output Verification
Verify that invalid inputs trigger the exact German error messages and exit codes:
```bash
python3 h_alis_date.py DWDate_Gib_Zeitraum -1 X YYYYMMDD START_VAR END_VAR
# Expected Standard Error Output:
# !! Interner Fehler bei der Rueckgabe von Datumswerten
#    Funktion: DWDate_Gib_Zeitraum
#    1 Zeile erwartet, 0 Zeile(n) bekommen
# Expected Exit Code: 1
```

---

## 7. Rollback Procedure

In the event of an issue during deployment or integration testing, follow these rollback steps:

1. **Remove Python Dependencies:** Delete the migrated `.py` files from the Cloud Composer GCS bucket `/dags/dependencies/` directory to prevent downstream tasks from importing them.
2. **Restore Legacy Sourcing:** If running in a hybrid environment, re-enable the sourcing of the legacy `.ksh` files in the wrapper scripts.
3. **Database State Reset:** If a job failed during validation and left an entry in the `BERT_MELDUNG` table in an inconsistent state, manually reset the status using SQL*Plus:
   ```sql
   -- To reset a stuck run status to Aborted
   EXEC BERT_MELDUNG.SetzeStatusAbbruch(&eintrags_nr);
   COMMIT;
   ```