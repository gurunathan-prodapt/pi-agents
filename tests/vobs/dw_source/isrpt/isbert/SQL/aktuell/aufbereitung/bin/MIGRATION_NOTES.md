# Migration Notes: Shared Files — `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin`

## 1. Summary
This migration transfers the legacy Korn Shell (KSH) utility script `gestern.ksh` to a native Python 3 script (`gestern.py`). 

* **Source Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`
* **Target Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.py`
* **Target Platform:** Google Cloud Platform (GCP) / Cloud Composer (Apache Airflow) environment.
* **Purpose:** Dynamically calculates and outputs date strings representing "today" and "yesterday" in both `YYYYMMDD` and `YYYYMM` formats. These values are consumed by downstream orchestration pipelines for partitioning, file naming, and query filtering.

---

## 2. Generated Artifacts
The migration produces a single, self-contained Python executable:

| File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.py` | Python 3 | Replaces the legacy shell script. Uses Python's native `datetime` library to safely compute and format dates, outputting them to standard output (`stdout`) in the exact format expected by legacy downstream consumers. |

---

## 3. Key Design Decisions

### Python vs. Bash/SQL
The legacy script contained manual date arithmetic (e.g., modulo math for leap years, manual month-length lookups, and string padding). While this could be implemented via SQL or shell utilities, converting it to a native Python script was chosen for the following reasons:
* **Robustness:** Python's standard `datetime` and `timedelta` modules natively handle all calendar anomalies (leap years, varying month lengths, and year-end transitions) without custom, error-prone logic.
* **Maintainability:** Eliminates the legacy dependency on external system commands like `date` and `expr`.
* **Integration:** Python scripts integrate seamlessly with Cloud Composer (Airflow) tasks, either as a `BashOperator` execution target or as an imported utility module.

### Output Format Preservation
The legacy script header claimed to return dates in `YYMMDD` format, but the actual KSH implementation used `%Y` (a 4-digit year), resulting in `YYYYMMDD`. The Python script preserves the actual implemented `YYYYMMDD` and `YYYYMM` formats to prevent breaking downstream consumers.

### Literal Output Enforcement
To comply with legacy error-handling patterns, the Python script explicitly catches exceptions and outputs the exact German error string `"Fehler !!!!"` to standard error (`stderr`) before exiting with code `1`.

---

## 4. Manual Steps Before Go-Live

### Schema & Dataset Creation
* **None:** This is a stateless utility script. No database schemas, tables, or datasets are modified or created.

### IAM & Permissions
* Ensure that the service account executing the Airflow DAGs/tasks has read and execution permissions for the target directory and file:
  ```bash
  chmod +x vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.py
  ```

### Connection Strings & Secrets
* **None:** The script runs entirely locally and does not connect to databases, APIs, or external cloud resources.

### Scheduling & Orchestration
* This script is **not** independently scheduled. It is a shared utility invoked dynamically by parent workflows.
* When migrating parent workflows to Airflow, integrate this script using one of the following patterns:
  * **Option A (Subprocess):** Execute via `BashOperator`:
    ```python
    get_dates = BashOperator(
        task_id='get_dates',
        bash_command='python3 /path/to/bin/gestern.py',
        do_xcom_push=True
    )
    ```
  * **Option B (Direct Import):** Refactor the parent Airflow DAG to import the logic directly as a Python helper function, bypassing subprocess execution entirely.

---

## 5. Known Gaps & Unresolved References

### Unmigrated Downstream Dependencies
The following downstream consumer jobs are not yet migrated to the target platform:
* `DW.BERT_ABLAUFSTEUERUNG`
* `DW.BERT_AUSD_BP_TA_MSISDN`
* `DW.BERT_AUSD_BP_TA_P_BASISPROD`
* `DW.BERT_DROP_TEMP_TABLE`
* `DW.BERT_P_ADRESSEN`
* `DW.BERT_P_AUSTAUSCH`
* `DW.BERT_P_GESCHAEFTSP`
* `DW.BERT_P_RECH_EMPF`
* `DW.BERT_RECHNUNGSDATEN`

> [!WARNING]
> End-to-end integration testing and parameter verification cannot be finalized until these downstream consumer jobs are migrated and configured to capture the output of `gestern.py`.

### Redesign (B4) Opportunities
While this script has been migrated as a direct functional equivalent, future iterations of the Airflow DAGs should leverage Airflow's native execution date macros (e.g., `{{ ds }}`, `{{ yesterday_ds_nodash }}`) instead of calling an external script. This would simplify the architecture and eliminate the need to maintain this utility file.

---

## 6. Validation

### Manual Execution Test
Run the script directly from a terminal in the target environment:
```bash
python3 vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.py
```

### Expected Output Format
The script must output exactly four space-separated values in the format:
`[Today_YYYYMMDD] [Yesterday_YYYYMMDD] [TodayMonth_YYYYMM] [YesterdayMonth_YYYYMM]`

*Example (if run on October 27, 2023):*
```text
20231027 20231026 202310 202310
```

### Boundary Testing Scenarios
To verify calendar math robustness, temporarily mock the system date (e.g., using Python's `freezegun` library in a test suite) to validate the following transitions:
1. **Standard Month Transition:** Run on `2023-11-01` $\rightarrow$ Yesterday must resolve to `20231031`.
2. **Leap Year Transition:** Run on `2024-03-01` $\rightarrow$ Yesterday must resolve to `20240229`.
3. **Non-Leap Year Transition:** Run on `2023-03-01` $\rightarrow$ Yesterday must resolve to `20230228`.
4. **Year-End Transition:** Run on `2024-01-01` $\rightarrow$ Yesterday must resolve to `20231231`.

---

## 7. Rollback Procedure
Because this utility is stateless, rolling back does not require database restoration or state recovery:
1. Revert the calling orchestration task (e.g., Airflow DAG or legacy wrapper script) to point back to the legacy `gestern.ksh` path or legacy environment.
2. If necessary, restore the previous version of the orchestration DAG file from version control.