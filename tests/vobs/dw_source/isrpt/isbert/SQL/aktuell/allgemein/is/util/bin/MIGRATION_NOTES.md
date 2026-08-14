# Migration Notes: Shared Utility Binaries

**Migration Target Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`  
**Target Platform:** Google Cloud Platform (GCP) / Cloud Composer (Apache Airflow) / BigQuery & Oracle Hybrid

---

## 1. Summary

This migration covers the transition of four core KornShell (`.ksh`) utility libraries to Python 3. These libraries form the operational foundation of the Information Services (IS) BERT reporting platform, standardizing error handling, date arithmetic, parameter validation, and database script execution.

The legacy shell scripts have been refactored into modular, importable Python scripts that support both direct programmatic import (within Airflow DAGs and Python Operators) and command-line execution (to maintain compatibility with legacy wrappers).

### Migrated Components

| Legacy Source File | Migrated Python File | Primary Functionality |
| :--- | :--- | :--- |
| `f_alis_msgerr.ksh` | `f_alis_msgerr.py` | Error logging, status tracking, and PL/SQL package `BERT_MELDUNG` integration. |
| `h_alis_date.ksh` | `h_alis_date.py` | Leap-year-aware date arithmetic, period generation, and Oracle-to-Python format translation. |
| `h_alis_parameter.ksh` | `h_alis_parameter.py` | Parameter parsing, business code normalization, and system/metric compatibility validation. |
| `h_alis_sqlplus.ksh` | `h_alis_sqlplus.py` | Safe execution wrapper for external SQL*Plus scripts with pre-flight checks. |

---

## 2. Generated Artifacts

### `f_alis_msgerr.py`
* **Role:** Replaces `f_alis_msgerr.ksh`. It manages execution tracking and logs errors to the Oracle database using the `oracledb` library. It wraps calls to the `BERT_MELDUNG` PL/SQL package (e.g., `SetzeStatusOk`, `SetzeStatusAbbruch`, `Fehler`, `Erzeuge_Eintrag`).
* **Execution Modes:**
  * **Module Import:** `import f_alis_msgerr`
  * **CLI Mode:** `python3 f_alis_msgerr.py <Function_Name> [args...]`

### `h_alis_date.py`
* **Role:** Replaces `h_alis_date.ksh`. It provides native Python date calculations, eliminating database roundtrips (e.g., `SELECT ... FROM DUAL`) for simple date math. It translates Oracle-style date format strings (e.g., `YYYYMMDD`) to Python `strftime` equivalents.
* **Execution Modes:**
  * **Module Import:** `import h_alis_date`
  * **CLI Mode:** `python3 h_alis_date.py <Function_Name> [args...]`

### `h_alis_parameter.py`
* **Role:** Replaces `h_alis_parameter.ksh`. It parses and normalizes ETL parameters, mapping verbose German business terms to standardized 3-letter shortcodes (e.g., `"zugang"` $\rightarrow$ `"zug"`). It also enforces strict validation rules for system/metric combinations.
* **Execution Modes:**
  * **Module Import:** `import h_alis_parameter`
  * **CLI Mode:** `python3 h_alis_parameter.py [options]`

### `h_alis_sqlplus.py`
* **Role:** Replaces `h_alis_sqlplus.ksh`. It acts as a safety wrapper for executing legacy SQL*Plus scripts. It verifies that the target SQL file exists and is readable before spawning a `sqlplus` subprocess, preventing silent failures.
* **Execution Modes:**
  * **Module Import:** `from h_alis_sqlplus import starte_sql_skript`
  * **CLI Mode:** `python3 h_alis_sqlplus.py <eintrags_nr> <script_path> [script_args...]`

---

## 3. Key Design Decisions

### Python-Native Date Arithmetic
* **Decision:** Replaced database-driven date calculations (e.g., `select to_date(...) from dual`) with native Python `datetime` and `calendar` modules in `h_alis_date.py`.
* **Trade-off:** This significantly reduces database connection overhead and network latency. However, it requires that the system time on Cloud Composer worker nodes is synchronized with the database server timezone (CET/CEST).

### Dual-Mode Execution (Import vs. CLI)
* **Decision:** All migrated scripts implement both a clean programmatic API (functions/classes) and a command-line interface (via `argparse` or `sys.argv`).
* **Reason:** This allows newly migrated Airflow DAGs to import the utilities natively, while allowing unmigrated shell scripts to call them as subprocesses during the transition phase.

### Oracle Client Integration (`oracledb`)
* **Decision:** Used the modern `oracledb` library in Thin mode where possible to connect to the Oracle database for logging and status updates.
* **Trade-off:** Thin mode eliminates the need to install the heavy Oracle Instant Client on Cloud Composer workers for simple metadata logging. However, legacy SQL execution via `h_alis_sqlplus.py` still spawns the `sqlplus` CLI, which *does* require the Instant Client.

### Literal Preservation Rule
* **Decision:** All German console outputs, warning messages, and error strings have been preserved character-for-character (e.g., `"Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben"`).
* **Reason:** This ensures that legacy log parsers, regex-based monitoring tools, and downstream automation patterns continue to function without modification.

---

## 4. Manual Steps Before Go-Live

### 1. Environment Variables Configuration
Ensure the following environment variables are configured in the Cloud Composer environment:
* `DW_ORAUSER`: Oracle database connection string (e.g., `username/password@hostname:port/service_name`).
* `DW_DIR_ROOT`: Root directory of the migrated scripts (e.g., `/home/airflow/gcs/dags`).
* `DW_DIR_PROT`: Target directory where log files are written (e.g., `/home/airflow/gcs/logs`).

### 2. Secret Management
Do not store plain-text credentials in `DW_ORAUSER` within the Airflow environment variables. Instead:
1. Store the database credentials in **Google Cloud Secret Manager**.
2. Configure an Airflow Connection (e.g., `oracle_default`) pointing to the database.
3. Resolve `DW_ORAUSER` dynamically at runtime within the DAGs using the Secret Manager or Airflow Connection.

### 3. Deployment to Cloud Composer
Copy the migrated Python files to the Cloud Composer GCS bucket under the designated shared utility path:
```bash
gsutil cp f_alis_msgerr.py gs://<composer-bucket>/dags/allgemein/is/util/bin/
gsutil cp h_alis_date.py gs://<composer-bucket>/dags/allgemein/is/util/bin/
gsutil cp h_alis_parameter.py gs://<composer-bucket>/dags/allgemein/is/util/bin/
gsutil cp h_alis_sqlplus.py gs://<composer-bucket>/dags/allgemein/is/util/bin/
```

### 4. Oracle Instant Client Installation
Because `h_alis_sqlplus.py` invokes the `sqlplus` executable, you must ensure that the Oracle Instant Client and `sqlplus` utility are installed on the Cloud Composer worker GKE nodes. This can be achieved via a custom Composer environment build or by using a KubernetesPodOperator with a pre-configured container image.

---

## 5. Known Gaps & Unresolved References

### Downstream Sourcing Gaps
There are 14 downstream operational jobs that depend on these utility libraries. None of these jobs have been migrated yet:
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
* `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`
* `DW.DWH_VVTN_IAR_BGF_GUTSCHR`

*Impact:* End-to-end integration testing cannot be completed until these downstream consumer jobs are migrated to Python/Airflow.

### Subprocess Dependencies in `h_alis_parameter.py`
* **Gap:** `h_alis_parameter.py` still invokes `DWDate_Datum_Check`, `DWDate_Datum_LE`, and `DWDate_Gib_Zeitraum` via subprocesses.
* **Redesign Item (B4):** These should be refactored to import and call the native Python functions in `h_alis_date.py` directly, eliminating the overhead of spawning shell subprocesses.

---

## 6. Validation

To validate the migrated scripts, execute the following test cases in the target environment.

### Test Case 1: Date Arithmetic (`h_alis_date.py`)
Verify that leap years and date additions are calculated correctly.
```bash
# Test leap year calculation (should return 0 for True)
python3 h_alis_date.py LetzterTagDesMonats 20240229
echo $? # Expected output: 0

# Test non-leap year calculation (should return 1 for False)
python3 h_alis_date.py LetzterTagDesMonats 20230229
echo $? # Expected output: 1

# Test date addition
python3 h_alis_date.py AddiereDatum 20231231 5
# Expected output: 20240105
```

### Test Case 2: Parameter Validation (`h_alis_parameter.py`)
Verify that business codes are mapped and validated correctly.
```bash
# Test metric normalization
python3 h_alis_parameter.py --test-kennzahl zugang
# Expected output: Result: zug, ErrNr: 0, ErrArg: 

# Test invalid system/metric combination
python3 h_alis_parameter.py --test-zeitparam 20230101 20221231 None
# Expected output: ErrNr: 195, ErrArg: Anfangsdatum ist nicht kleiner gleich Endedatum
```

### Test Case 3: Database Logging (`f_alis_msgerr.py`)
Verify connection and procedure execution against the Oracle database.
```bash
# Set temporary environment variables for testing
export DW_ORAUSER="test_user/test_pass@test_host:1521/test_service"
export DW_DIR_PROT="/tmp"

# Generate a log filename
python3 f_alis_msgerr.py DWMSG_Logdateiname MY_VAR JOB_TEST 99999
# Expected output: /tmp/JOB_TEST_<timestamp>_99999.log
```

---

## 7. Rollback Procedure

If critical issues are encountered during the deployment or execution of the migrated Python utilities, follow these steps to roll back to the legacy KornShell implementation:

1. **Revert Environment Variables:**
   Update the Cloud Composer environment variables or local shell profiles to point back to the legacy `.ksh` files:
   ```bash
   export PATH="/vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin:$PATH"
   ```

2. **Restore Shell Sourcing:**
   If any migrated wrapper scripts were modified to call Python, revert them to source the original shell libraries:
   ```bash
   # Revert to legacy sourcing
   . h_alis_date.ksh
   . f_alis_msgerr.ksh
   ```

3. **Verify Legacy Execution:**
   Run a quick sanity check using the legacy shell utilities to ensure the database connection and date math function correctly:
   ```bash
   ./h_alis_date.ksh LetzterTagDesMonats 20240229
   echo $? # Expected output: 0
   ```