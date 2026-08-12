# Migration Notes

**Migration Target:** Python 3.x (Cloud Composer / Apache Airflow)  
**Source Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`  
**Target Platform:** Google Cloud Platform (GCP) / Cloud Composer interfacing with Oracle DB (via `oracledb`) and preparing for BigQuery integration.

---

## 1. Summary

The legacy KornShell (KSH) utility libraries located in `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin` have been migrated to native Python 3.x modules. These libraries form the core operational framework for the "Information Services" (IS) data warehouse, managing:
*   Centralized execution tracking, logging, and error handling.
*   In-memory and database-driven date arithmetic.
*   Job parameter parsing, validation, and domain normalization.
*   Robust execution wrappers for database script execution.

The migration preserves the exact business logic, validation rules, and German-language diagnostic outputs of the legacy system while modernizing the execution model to run efficiently within a containerized Cloud Composer (Airflow) environment.

---

## 2. Generated Artifacts

The following Python modules have been generated to replace the legacy KornShell scripts:

| Legacy Shell Script | Generated Python Module | Role / Description |
| :--- | :--- | :--- |
| `f_alis_msgerr.ksh` | `f_alis_msgerr.py` | **Execution Tracking & Logging:** Manages job registration, status updates (OK/Aborted), error reporting, and timing checkpoints by executing PL/SQL procedures within the `BERT_MELDUNG` package. Supports both CLI execution and direct Python imports. |
| `h_alis_date.ksh` | `h_alis_date.py` | **Date Arithmetic Utility:** Performs calendar math, leap-year checks, month-length lookups, and date range generation. Replaces legacy database-dependent date checks with high-performance, in-memory Python `datetime` and `calendar` operations. |
| `h_alis_parameter.ksh` | `h_alis_parameter.py` | **Parameter Parser & Validator:** Normalizes verbose German key figures (e.g., "bestand" to "bst") and source systems, enforces domain compatibility matrices, and validates date-range parameters. |
| `h_alis_sqlplus.ksh` | `h_alis_sqlplus.py` | **SQL\*Plus Execution Wrapper:** Validates parameter completeness and file readability on the filesystem before invoking the `sqlplus` CLI to execute database scripts. |

---

## 3. Key Design Decisions

### In-Memory Date Math Optimization
In the legacy `h_alis_date.ksh` library, simple date validations and arithmetic operations (such as checking if a date is the last day of a month or adding days) were routed to the Oracle database via SQL\*Plus queries against the `DUAL` table. 
*   **Decision:** These operations have been completely refactored in `h_alis_date.py` to use Python's native `datetime` and `calendar` libraries.
*   **Trade-off:** This eliminates significant database connection overhead and network round-trips, resulting in faster task execution and reduced database load.

### Elimination of SQL Wrapper Scripts
The legacy logging framework relied on external SQL wrapper scripts (e.g., `d_alis_spaufruf_p1.sql`, `d_alis_spaufruf_p4.sql`) to execute procedures in the `BERT_MELDUNG` package.
*   **Decision:** The migrated `f_alis_msgerr.py` executes these PL/SQL blocks directly using native bind variables via the `oracledb` library.
*   **Trade-off:** This removes filesystem dependencies on external `.sql` files, simplifies deployment, and consolidates the execution logic within the Python codebase.

### State Preservation for Parameter Validation
The legacy parameter validation library (`h_alis_parameter.ksh`) used global shell variables (`ErrNr` and `ErrArg`) to track validation failures and bypass subsequent checks if an error was already set.
*   **Decision:** This state-propagation pattern was preserved in `h_alis_parameter.py` using global module-level variables.
*   **Trade-off:** While global state is generally discouraged in modern Python design, preserving this pattern ensures 100% backward compatibility with the execution flow of downstream scripts that rely on these exact error codes.

### Dual-Use Execution Model (CLI & Module)
All migrated scripts are designed to support two execution modes:
1.  **As an Importable Module:** Downstream Python tasks and Airflow DAGs can import functions directly (e.g., `from h_alis_date import addiere_datum`).
2.  **As a Command-Line Interface (CLI):** Scripts can be executed directly from the shell or Airflow `BashOperator` using `argparse` entry points, outputting shell-compatible variable assignments (using `eval` patterns) to support hybrid migration phases.

---

## 4. Manual Steps Before Go-Live

Before deploying the migrated modules to production, the following manual setup steps must be completed:

### 1. Database Schema Verification
Ensure that the `BERT_MELDUNG` PL/SQL package and its associated tables and sequences exist and are fully compiled in the target Oracle database. The logging framework relies on the following database objects:
*   `BERT_MELDUNG.Erzeuge_Eintrag`
*   `BERT_MELDUNG.SetzeStatusOk`
*   `BERT_MELDUNG.SetzeStatusAbbruch`
*   `BERT_MELDUNG.Fehler`
*   `BERT_MELDUNG.ErmittleNr`
*   `BERT_MELDUNG.SetzeZusatzInfos`

### 2. IAM & Permissions
*   **Cloud Composer Service Account:** Ensure the service account running the Cloud Composer environment has read access to the Google Secret Manager secret containing the database credentials.
*   **Log Directory Access:** The environment variable `DW_DIR_PROT` must point to a directory where the Airflow worker has write permissions (or be mapped to a Google Cloud Storage bucket mount).

### 3. Connection Strings & Secrets
Do not hardcode database credentials. Configure the `DW_ORAUSER` environment variable in Cloud Composer to retrieve credentials securely from Secret Manager.
*   **Format:** `username/password@hostname:port/service_name`

### 4. Environment Variables
The following environment variables must be configured in the Cloud Composer environment:
*   `DW_ORAUSER`: Oracle database connection string.
*   `DW_DIR_ROOT`: Root directory of the migrated codebase.
*   `DW_DIR_PROT`: Target directory for protocol and execution logs.

---

## 5. Known Gaps & Unresolved References

### Missing SQL Wrapper Scripts (B4 Redesign Items)
The original SQL wrapper scripts referenced in the legacy codebase were not provided in the source payload:
*   `d_alis_spaufruf_p1.sql`
*   `d_alis_spaufruf_p4.sql`
*   `d_al_is_ermittlenr.sql`
*   `d_alis_vormonat.sql`
*   `d_alis_datum_zeitraum.sql`

**Mitigation:** The Python modules have implemented native equivalents for these scripts (e.g., executing PL/SQL blocks directly or performing date math in-memory). These assumptions **must be verified** against the actual database schema during integration testing.

### Downstream Integration
There are 12 downstream consumer jobs that depend on these utility libraries:
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

These downstream jobs are not yet migrated. Their orchestration wiring and import structures must be updated to reference the new `.py` modules as they are migrated.

### External Command Dependency
`h_alis_sqlplus.py` still relies on a local installation of the `sqlplus` CLI. If the target database platform is migrated to BigQuery in a later phase, these database-side routines must be completely refactored into native BigQuery SQL operators, and the `sqlplus` dependency must be retired.

---

## 6. Validation

To validate the migrated Python modules, execute the following test suites in the target environment:

### 1. Parameter Validation Self-Tests
Run the built-in self-tests in `h_alis_parameter.py` to verify key-figure mapping and date chronology validation:
```bash
python3 h_alis_parameter.py --test
```
*   **Passing Criteria:** The script outputs `Executing Library Validation Tests...` and exits with code `0` without raising exceptions.

### 2. Date Utility Functional Tests
Verify that the date utility functions return correct values and exit codes:
```bash
# Test leap year and month length logic
python3 h_alis_date.py LetzterTagDesMonats 20240229
echo $? # Should output 0 (True)

python3 h_alis_date.py LetzterTagDesMonats 20230229
echo $? # Should output 1 (False)

# Test date addition
python3 h_alis_date.py AddiereDatum 20231025 10
# Should output: 20231104
```

### 3. Logging Integration Tests (Mocked or Connected)
Verify that `f_alis_msgerr.py` can connect to the database and execute logging procedures:
```bash
# Export temporary test variables
export DW_ORAUSER="test_user/test_pass@test_host:1521/test_service"
export DW_DIR_ROOT="/tmp"
export DW_DIR_PROT="/tmp"

# Run log entry creation (will fail if DB is unreachable, verifying connection logic)
python3 f_alis_msgerr.py ErzeugeEintrag 999999 TEST_JOB TEST_PROG /tmp/test.log
```

---

## 7. Rollback Procedure

If critical issues are encountered during deployment or go-live, perform the following steps to roll back to the legacy KornShell execution model:

1.  **Revert Environment Variables:**
    Ensure that any orchestration tasks (UC4 or Airflow) point their execution paths back to the legacy `.ksh` files instead of the new `.py` files.
2.  **Restore Sourcing Commands:**
    In any partially migrated scripts, restore the legacy sourcing syntax:
    ```bash
    . f_alis_msgerr.ksh
    . h_alis_date.ksh
    . h_alis_parameter.ksh
    . h_alis_sqlplus.ksh
    ```
3.  **Verify Legacy Paths:**
    Confirm that the legacy KornShell files are still present and executable on the worker nodes:
    ```bash
    chmod +x vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/*.ksh
    ```
4.  **Database State Cleanup:**
    If the migration failed mid-run, manually clean up any orphaned execution tracking records in the `BERT_MELDUNG` table by setting their status to aborted using the legacy SQL wrappers.