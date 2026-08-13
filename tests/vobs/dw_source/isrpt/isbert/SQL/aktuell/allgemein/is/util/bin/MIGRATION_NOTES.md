# Migration Notes: Shared Utility Binaries

**Migration Target Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`  
**Target Platform:** Google Cloud Platform (GCP) / Cloud Composer (Apache Airflow) / BigQuery

---

## 1. Summary

This migration covers the transition of four core KornShell (`.ksh`) utility libraries into production-grade Python 3 modules. These utilities form the operational backbone of the "Information Services" (IS) data warehouse pipeline, standardizing error handling, date arithmetic, parameter parsing, and database execution.

### Migrated Components
*   **Error Management & Logging:** `f_alis_msgerr.ksh` $\rightarrow$ `f_alis_msgerr.py`
*   **Date Arithmetic & Validation:** `h_alis_date.ksh` $\rightarrow$ `h_alis_date.py`
*   **Parameter Parsing & Validation:** `h_alis_parameter.ksh` $\rightarrow$ `h_alis_parameter.py`
*   **SQL\*Plus Execution Wrapper:** `h_alis_sqlplus.ksh` $\rightarrow$ `h_alis_sqlplus.py`

### Target Architecture
The migrated Python modules are designed to run within **Cloud Composer (Apache Airflow)** worker nodes. They replace legacy shell-level sourcing (`. h_alis_parameter.ksh`) with native Python imports, while maintaining command-line interfaces (CLI) via `argparse` to support hybrid execution phases.

---

## 2. Generated Artifacts

The following table lists each generated Python module and its specific operational role:

| Generated File Path | Language | Legacy Source File | Operational Role |
| :--- | :--- | :--- | :--- |
| `allgemein/is/util/bin/f_alis_msgerr.py` | Python 3 | `f_alis_msgerr.ksh` | Manages execution tracking, job registration, and error state logging. Interfaces with the `BERT_MELDUNG` database package. |
| `allgemein/is/util/bin/h_alis_date.py` | Python 3 | `h_alis_date.ksh` | Performs in-memory date calculations, leap year checks, and relative period generation. Eliminates legacy database round-trips for date math. |
| `allgemein/is/util/bin/h_alis_parameter.py` | Python 3 | `h_alis_parameter.ksh` | Standardizes and validates system names, metrics, domains, and reporting intervals. Manages global error states (`ErrNr`, `ErrArg`). |
| `allgemein/is/util/bin/h_alis_sqlplus.py` | Python 3 | `h_alis_sqlplus.ksh` | Validates SQL script readability and executes SQL*Plus processes safely, preventing interactive hangs via stdin redirection. |

---

## 3. Key Design Decisions

### Python 3 vs. Bash/KSH
*   **Decision:** Convert all utilities to native Python 3 modules.
*   **Reasoning:** Python provides superior error handling, native date arithmetic, and seamless integration with GCP client libraries (BigQuery, Cloud Logging). It eliminates fragile shell-level side effects like dynamic variable assignment via `eval`, temporary file parsing in `/tmp`, and subprocess overhead for basic math.

### In-Memory Date Math
*   **Decision:** Replace Oracle-dependent date calculations (e.g., `SELECT TO_DATE(...) FROM dual`) with Python's standard `datetime` and `calendar` libraries.
*   **Reasoning:** Reduces database connection overhead, network latency, and licensing costs. Date validation and arithmetic are now executed entirely in-memory on the Cloud Composer worker.

### Preservation of Legacy Interfaces (Hybrid Compatibility)
*   **Decision:** Implement `argparse` CLI entry points and fallback to `os.environ` for configuration.
*   **Reasoning:** Because downstream ETL jobs are migrated in phases, the Python modules must support both direct Python imports (`import h_alis_parameter`) and shell-level execution via subprocesses during the transition period.

### Trade-offs & Bridging Strategy
*   **Subprocess SQL\*Plus Calls:** The migrated scripts retain `sqlplus` subprocess execution and `oracledb` connections. While a pure BigQuery-native logging system is the ultimate target, preserving the Oracle PL/SQL interface (`BERT_MELDUNG`) ensures that migrated utilities can immediately interoperate with the existing database backend without breaking upstream/downstream dependencies.

---

## 4. Manual Steps Before Go-Live

To deploy these utility modules successfully, the following infrastructure and configuration steps must be completed:

### 1. Schema & Dataset Creation
If migrating the logging backend from Oracle to BigQuery:
*   Create a centralized logging dataset (e.g., `${GCP_PROJECT}.metadata_logging`).
*   Create the target logging table equivalent to the legacy `BERT_MELDUNG` schema:
    ```sql
    CREATE TABLE `metadata_logging.bert_meldung` (
        eintrags_nr INT64 NOT NULL,
        job_kennung STRING,
        programmname STRING,
        log_datei STRING,
        status STRING,
        stichtag DATE,
        zusatzinfos STRING,
        insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    ```

### 2. IAM & Permissions
The Cloud Composer Service Account must be granted the following IAM roles:
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the logging dataset.
*   **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.
*   **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`) if database credentials are stored in Secret Manager.

### 3. Connection Strings & Secrets
*   Configure the Oracle database connection string in Cloud Composer as an Airflow Connection (`oracle_default`) or set the `DW_ORAUSER` environment variable.
*   Format: `username/password@hostname:port/service_name`

### 4. Environment Variables
Ensure the following environment variables are set in the Cloud Composer environment:
*   `GCP_PROJECT`: Target GCP Project ID.
*   `BQ_DATASET`: Target BigQuery logging dataset.
*   `DW_DIR_ROOT`: Root directory of the SQL/ETL repository.
*   `DW_DIR_PROT`: Directory path where execution logs are written.

### 5. Deployment & Scheduling
*   These files are **shared utility libraries** and must not be scheduled as standalone DAGs.
*   Deploy the `.py` files to the Cloud Composer DAGs folder under a shared utilities directory (e.g., `/home/airflow/gcs/dags/utils/bin/`).
*   Ensure the directory is added to the Python path (`sys.path`) or packaged as a Python wheel and installed on the Composer cluster.

---

## 5. Known Gaps & Unresolved References

### 1. Unmigrated Downstream Jobs
The following 12 downstream jobs source these utilities and are not yet migrated. They must be updated to import the new Python modules once their migration begins:
*   `DW.BERT_ABLAUFSTEUERUNG`
*   `DW.BERT_AUSD_BP_TA_MSISDN`
*   `DW.BERT_AUSD_BP_TA_P_BASISPROD`
*   `DW.BERT_AUSD_V_TA_PERIOD`
*   `DW.BERT_AUSD_V_TA_P_VERTRAG`
*   `DW.BERT_AUSD_V_TA_VERTRAG_TMP`
*   `DW.BERT_DROP_TEMP_TABLE`
*   `DW.BERT_P_ADRESSEN`
*   `DW.BERT_P_AUSTAUSCH`
*   `DW.BERT_P_GESCHAEFTSP`
*   `DW.BERT_P_RECH_EMPF`
*   `DW.BERT_RECHNUNGSDATEN`

### 2. Unsupplied SQL Wrapper Scripts
The internal logic of the following SQL wrapper scripts (referenced by `f_alis_msgerr.py` and `h_alis_sqlplus.py`) was not supplied in the source bundle:
*   `d_alis_spaufruf_p1.sql`
*   `d_al_is_ermittlenr.sql`
*   `d_alis_spaufruf_p3.sql`
*   `d_alis_spaufruf_p4.sql`
*   `d_alis_spaufruf_p5.sql`
*   `d_alis_vormonat.sql`
*   `d_alis_datum_zeitraum.sql`

### 3. Redesign (B4) Items
*   **PL/SQL Package Migration:** The Oracle package `BERT_MELDUNG` must be rewritten as a Python module or BigQuery stored procedures.
*   **Subprocess Elimination:** Replace all remaining `subprocess.run(["sqlplus", ...])` calls with native Python DB-API calls (`oracledb` for Oracle, or `google.cloud.bigquery` for BigQuery) to eliminate OS-level shell dependencies.

---

## 6. Validation

To validate the correctness of the migrated Python modules, execute the following test cases on a Cloud Composer worker or local development environment:

### Test Case 1: Date Arithmetic (`h_alis_date.py`)
*   **Command:**
    ```bash
    python3 h_alis_date.py AddiereDatum 20231025 5
    ```
*   **Expected Output:**
    ```text
    20231030
    ```
*   **Command (Leap Year Check):**
    ```bash
    python3 h_alis_date.py TageimMonat 2024 2
    ```
*   **Expected Output:**
    ```text
    29
    ```

### Test Case 2: Parameter Validation (`h_alis_parameter.py`)
*   **Command:**
    ```bash
    export MY_SYS="SAP"
    python3 h_alis_parameter.py --action konvertiereSystem --arg1 MY_SYS
    echo $MY_SYS
    ```
*   **Expected Output:**
    ```text
    sap
    ```
*   **Command (Invalid Combination Check):**
    ```bash
    python3 h_alis_parameter.py --action pruefeSystemKennzahl --arg1 sap --arg2 zug
    ```
*   **Expected Output (Stderr):**
    ```text
    ERROR: ErrNr=195, ErrArg=Ungueltige Kombination sap zug
    ```

### Test Case 3: SQL Execution Validation (`h_alis_sqlplus.py`)
*   **Command (Missing File Check):**
    ```bash
    python3 h_alis_sqlplus.py 99999 /nonexistent/path/script.sql
    ```
*   **Expected Output (Stderr):**
    ```text
    ERROR LOG: Entry=99999, Severity=E, Code=201, Msg=/nonexistent/path/script.sql
    ```
*   **Expected Exit Code:** `201`

---

## 7. Rollback Procedure

If critical issues are discovered in production, follow these steps to roll back to the legacy KornShell execution environment:

1.  **Revert Environment Variables:**
    Restore the legacy environment variables pointing to the KSH scripts:
    ```bash
    export PATH="/vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin:$PATH"
    ```
2.  **Restore Shell Sourcing:**
    In any partially migrated wrapper scripts, revert Python imports back to shell sourcing:
    ```bash
    # Revert this:
    # python3 -m is.util.bin.h_alis_parameter ...
    
    # Back to this:
    . h_alis_parameter.ksh
    ```
3.  **Database State Verification:**
    Verify that no orphaned records exist in the `BERT_MELDUNG` table due to failed Python status updates:
    ```sql
    SELECT * FROM BERT_MELDUNG WHERE STATUS = 'abgebrochen' AND INSERT_TIMESTAMP > CURRENT_DATE;
    ```
4.  **Log Inspection:**
    Check the Cloud Composer Airflow task logs and the legacy log directory (`$DW_DIR_PROT`) to identify the root cause of the failure.