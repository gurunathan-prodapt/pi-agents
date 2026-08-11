# MIGRATION_NOTES.md

**Job/Module:** Shared Files — `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin`  
**Source Technology:** KornShell (KSH) & Oracle SQL\*Plus / PL/SQL  
**Target Technology:** Python 3, Google Cloud Platform (GCP) Cloud Composer (Apache Airflow), and Google Cloud BigQuery (with transitional Oracle DB support)

---

## 1. Summary

This migration covers the transition of four core KornShell (KSH) utility libraries from an on-premises Oracle Data Warehouse environment to a GCP-native Python 3 architecture. These utilities provide centralized error management, date arithmetic, parameter validation, and database client execution wrappers.

*   **Source Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/`
*   **Target Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/`
*   **Target Platform:** GCP Cloud Composer (Airflow) for orchestration, BigQuery for target data warehousing, and transitional Oracle DB connectivity via the `python-oracledb` driver.

---

## 2. Generated Artifacts

The migration has generated four Python modules corresponding to the legacy KSH scripts:

| Legacy KSH File | Migrated Python File | Role & Responsibility |
| :--- | :--- | :--- |
| `f_alis_msgerr.ksh` | `f_alis_msgerr.py` | **Centralized Logging & State Ledger:** Manages execution registration, status updates (`OK`, `Abbruch`), error reporting, and timing metrics using direct Oracle PL/SQL calls (`BERT_MELDUNG` package). |
| `h_alis_date.ksh` | `h_alis_date.py` | **Date Arithmetic & Validation:** Performs calendar calculations, leap-year checks, month-end validation, and sliding window calculations. Uses native Python `datetime` with a BigQuery SQL fallback for complex Oracle formats. |
| `h_alis_parameter.ksh` | `h_alis_parameter.py` | **Parameter Normalization & Routing:** Standardizes long business terms into short codes, validates system-metric compatibility, and maps metrics to functional domains (`tn`, `us`, `gd`) and schedules (`t`, `m`). |
| `h_alis_sqlplus.ksh` | `h_alis_sqlplus.py` | **SQL Execution Wrapper:** Safely executes external SQL scripts via `sqlplus` subprocesses, ensuring file readability and non-interactive execution (`</dev/null`). |

---

## 3. Key Design Decisions

### Python as the Target Language
The legacy scripts define a complex library of utility functions containing validation logic, database client sessions, file operations, error handling wrappers, and dynamic variable evaluations. These operations cannot be modeled in pure SQL, making Python the optimal target language.

### Native Python vs. Database Roundtrips
*   **Legacy Approach:** The legacy date utility (`h_alis_date.ksh`) frequently initiated SQL\*Plus sessions to perform basic date validations and arithmetic.
*   **Migrated Approach:** All standard date arithmetic (`LetzterTagDesMonats`, `TageimMonat`, `AddiereDatum`) has been rewritten using native Python `datetime` and `calendar` libraries. This eliminates database roundtrips, reducing execution latency and network overhead.
*   **Fallback Mechanism:** For highly complex Oracle-specific date formats, `h_alis_date.py` leverages the `google.cloud.bigquery` client SDK to evaluate the format safely in the cloud.

### State Management & Thread Safety
*   **Legacy Approach:** The KSH scripts relied heavily on dynamic variable assignment via `eval` (e.g., `eval "$VarName=$Value"`) to modify the caller's environment.
*   **Migrated Approach:** Python functions return values directly or manipulate a structured context dictionary (`context_dict`). This ensures thread safety, prevents environment variable pollution, and aligns with modern programming practices.

### Transitional Oracle Support
To support a phased migration where the database is not yet fully migrated to BigQuery, `f_alis_msgerr.py` utilizes the `oracledb` driver to interact directly with the legacy `BERT_MELDUNG` PL/SQL package.

---

## 4. Manual Steps Before Go-Live

### 1. Schema & Dataset Creation
*   **BigQuery Audit Dataset:** Ensure the BigQuery dataset defined by the `BQ_DATASET` environment variable exists and contains the necessary audit/logging tables if logging is redirected to BigQuery.
*   **Transitional Oracle DB:** Ensure the transitional Oracle database contains the `BERT_MELDUNG` package and the `BERT_SEQ` sequence.

### 2. IAM & Permissions
*   **GCP Service Account:** The Cloud Composer service account must have the following roles:
    *   `roles/bigquery.jobUser` (to run validation queries)
    *   `roles/bigquery.dataViewer` / `roles/bigquery.dataEditor` on the `BQ_DATASET`
    *   `roles/secretmanager.secretAccessor` (if database credentials are stored in Secret Manager)

### 3. Connection Strings & Secrets
*   **`DW_ORAUSER` Configuration:** Store the Oracle connection string securely in Google Cloud Secret Manager or as an Airflow Connection. At runtime, this must be exposed to the Python environment as the `DW_ORAUSER` environment variable (e.g., `user/password@hostname:port/service_name`).

### 4. Deployment & Scheduling
*   **Shared Library Path:** These files are utility libraries and **must not** be scheduled as standalone Airflow DAGs.
*   **Deployment Location:** Deploy these files to the Airflow `dags/` or `plugins/` directory, or package them as a private library and install them in the Cloud Composer environment's `PYTHONPATH` so they can be imported by other DAGs:
    ```python
    from is.util.bin.h_alis_date import AddiereDatum
    ```

---

## 5. Known Gaps & Unresolved References

### 1. Unmigrated Downstream Dependencies
The following 12 downstream jobs depend on these utility libraries and are currently **not yet migrated**. End-to-end integration and execution state reporting cannot be finalized until these jobs are migrated to GCP:
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

### 2. Missing External SQL Scripts
The following external SQL scripts referenced by the date and error utilities were not supplied in the source code and must be verified during downstream migration:
*   `d_alis_vormonat.sql` (referenced by `DWDate_Vormonat`)
*   `d_alis_datum_zeitraum.sql` (referenced by `DWDate_Gib_Zeitraum`)
*   `d_al_is_ermittlenr.sql` (referenced by `DWMSG_ErmittleNr`)

### 3. B4 Redesign Items (Logging Architecture)
*   **Issue:** The central logging mechanism (`f_alis_msgerr.py`) currently performs row-by-row transactional updates to an Oracle database via `oracledb`. In a serverless cloud environment (BigQuery), transactional row-by-row logging is an anti-pattern that can lead to concurrency bottlenecks.
*   **Recommendation:** Redesign the logging architecture to write structured logs to **Google Cloud Logging** or **Cloud Pub/Sub**, or buffer log entries and perform batch inserts into BigQuery.

---

## 6. Validation

To validate the migrated Python modules, execute them from the command line in an environment with the appropriate environment variables set.

### 1. Validate Date Arithmetic (`h_alis_date.py`)
Run the following commands to verify native Python date calculations:
```bash
# Test date addition (Expected output: 20231031)
python3 h_alis_date.py AddiereDatum 20231026 5

# Test month-end check (Expected output: 0 for True)
python3 h_alis_date.py LetzterTagDesMonats 20231031
echo $?

# Test date comparison (Expected output: 0 for True)
python3 h_alis_date.py DWDate_Datum_LE 20231001 20231026
echo $?
```

### 2. Validate Parameter Normalization (`h_alis_parameter.py`)
Verify that parameter mappings and system-metric compatibility checks function correctly:
```bash
# Set environment variables for testing
export TEST_VAR="gutschrift"

# Run normalization (Expected: TEST_VAR is updated to "gut" in os.environ)
python3 -c "import os, h_alis_parameter; h_alis_parameter.konvertiereKennzahl('TEST_VAR'); print(os.environ['TEST_VAR'])"
```

### 3. Success Criteria
*   All CLI test commands return an exit code of `0`.
*   Date calculations match the legacy Oracle output exactly (including leap years).
*   German console outputs and error messages are preserved exactly as-is (e.g., `"Datum {datum1} ist groesser als {datum2}"`).

---

## 7. Rollback Procedure

If issues are encountered during the deployment of these utility libraries, follow these steps to roll back to the legacy environment:

1.  **Revert Environment Variables:** Point the environment variables (`DW_DIR_ROOT`, `DW_DIR_PROT`) back to the legacy on-premises NFS mount paths.
2.  **Restore Sourcing Links:** Revert any migrated wrapper scripts that import the Python modules to source the legacy `.ksh` files instead:
    ```bash
    # Revert this:
    # python3 f_alis_msgerr.py DWMSG_SetzeStatusOK 12345
    
    # Back to this:
    . f_alis_msgerr.ksh
    DWMSG_SetzeStatusOK 12345
    ```
3.  **Verify Oracle Logging State:** Check the Oracle database `BERT_MELDUNG` table to ensure no orphaned or corrupted log entries exist from the failed Python execution. If necessary, manually update the status of active runs using SQL\*Plus:
    ```sql
    EXEC BERT_MELDUNG.SetzeStatusAbbruch(:eintrags_nr);
    COMMIT;
    ```