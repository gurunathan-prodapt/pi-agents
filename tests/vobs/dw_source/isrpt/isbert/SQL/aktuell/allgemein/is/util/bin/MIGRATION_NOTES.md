# MIGRATION_NOTES.md

**Component:** Shared Utility Libraries  
**Source Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/`  
**Target Platform:** Google Cloud Platform (GCP) — Cloud Composer (Apache Airflow), BigQuery, Google Cloud Storage (GCS), and Cloud Logging.

---

## 1. Summary

This migration covers the transition of four core KornShell (`.ksh`) utility libraries to native Python 3 modules. These libraries provide the foundational infrastructure for session orchestration, audit telemetry, date arithmetic, parameter validation, and database execution wrappers across the entire **Information Services (IS) / BERT** data warehouse pipeline.

### Migrated Components
1. **`f_alis_msgerr.ksh`** $\rightarrow$ `f_alis_msgerr.py` (Session Log, Telemetry, and Error Management)
2. **`h_alis_date.ksh`** $\rightarrow$ `h_alis_date.py` (Date Arithmetic and Calendar Utilities)
3. **`h_alis_parameter.ksh`** $\rightarrow$ `h_alis_parameter.py` (Parameter Parsing, Normalization, and Validation)
4. **`h_alis_sqlplus.ksh`** $\rightarrow$ `h_alis_sqlplus.py` (SQL Execution Wrapper)

### Target Architecture
The legacy Oracle-centric shell environment has been modernized to run natively on **Google Cloud Platform (GCP)**. 
* **Orchestration:** Apache Airflow (Cloud Composer) executes these utilities as native Python tasks or imports them as shared modules.
* **Audit Database:** Oracle PL/SQL package `BERT_MELDUNG` and its underlying tables are replaced by a centralized **Google BigQuery** audit dataset.
* **Storage:** Local log directories (`DW_DIR_PROT`) are migrated to **Google Cloud Storage (GCS)** buckets.
* **Execution:** Database interactions are transitioned from Oracle `sqlplus` CLI subprocesses to parameterized BigQuery SQL execution via the `google-cloud-bigquery` client library.

---

## 2. Generated Artifacts

The following table lists each generated Python file, its role, and its operational context:

| Generated File Path | Role | Operational Context / Description |
| :--- | :--- | :--- |
| `allgemein/is/util/bin/f_alis_msgerr.py` | Session Log & Telemetry | Manages pipeline execution states (`LAUFEND`, `OK`, `ABBRUCH`). Writes operational telemetry, error codes, and timing annotations directly to the BigQuery audit table `bert_meldung`. |
| `allgemein/is/util/bin/h_alis_date.py` | Date Arithmetic Library | Performs calendar calculations, leap-year checks, date additions, and relative date-range generation (`DWDate_Gib_Zeitraum`) in-memory. |
| `allgemein/is/util/bin/h_alis_parameter.py` | Parameter Normalization | Parses and validates incoming runtime parameters. Maps long-form business terms to standardized 3-character KPI codes and validates system-KPI compatibility. |
| `allgemein/is/util/bin/h_alis_sqlplus.py` | SQL Execution Wrapper | Performs pre-flight checks (file existence, readability) and executes SQL scripts. Serves as a bridge for running legacy SQL scripts or BigQuery DDL/DML. |

---

## 3. Key Design Decisions

### 3.1. Shift from Database-Driven to In-Memory Date Logic
* **Decision:** Replaced all Oracle `DUAL` queries inside `h_alis_date.ksh` with native Python `datetime` and `calendar` operations.
* **Reasoning:** The legacy shell script executed a SQL\*Plus session to query `DUAL` for basic date validations and comparisons. Moving this logic in-memory eliminates database connection overhead, reduces network latency, and lowers BigQuery query costs.

### 3.2. BigQuery Audit Table instead of Oracle PL/SQL Package
* **Decision:** Migrated the state-tracking logic of the Oracle package `BERT_MELDUNG` to parameterized BigQuery DML statements (`INSERT`, `UPDATE`) targeting a structured table named `bert_meldung`.
* **Reasoning:** BigQuery does not support procedural PL/SQL packages in the same manner as Oracle. Parameterized SQL queries executed via the official Google Cloud SDK provide a secure, serverless, and highly scalable audit trail.

### 3.3. High-Resolution Nanosecond Run IDs
* **Decision:** Replaced the Oracle sequence generator (`BERT_MELDUNG.GetNextSequenceVal`) in `dwmsg_ermittle_nr()` with a high-resolution nanosecond timestamp string (`time.time_ns()`).
* **Reasoning:** BigQuery does not feature lightweight, low-latency sequence generators. Generating unique IDs natively in Python prevents database serialization bottlenecks.

### 3.4. Dual-Mode Interface (CLI & Importable Module)
* **Decision:** Equipped all migrated Python scripts with both an `argparse` Command Line Interface (CLI) and standard Python function definitions.
* **Reasoning:** This preserves backward compatibility. During the hybrid migration phase, legacy KornShell scripts can still invoke these utilities via subprocess CLI calls, while fully migrated Airflow DAGs can import them as native Python modules.

### 3.5. Strict Preservation of Legacy Error Messages
* **Decision:** Retained all literal German error strings (e.g., `"Datum {datum1} ist groesser als {datum2}"`) and specific error codes (e.g., `194`, `195`, `196`, `201`).
* **Reasoning:** Downstream monitoring tools (such as Robomon or legacy log parsers) rely on exact string matching to trigger alerts. Altering these messages would break operational monitoring.

---

## 4. Manual Steps Before Go-Live

### 4.1. BigQuery Audit Table Creation
Before executing any jobs, the centralized audit table must be created in BigQuery. Execute the following DDL in your target GCP project:

```sql
CREATE TABLE `YOUR_GCP_PROJECT.YOUR_AUDIT_DATASET.bert_meldung` (
  eintrags_nr STRING NOT NULL OPTIONS(description="Unique run execution ID"),
  job_kennung STRING OPTIONS(description="Unique identifier of the executing job"),
  programmname STRING OPTIONS(description="Name of the executing script/module"),
  log_datei STRING OPTIONS(description="GCS path or local path to the execution log"),
  start_zeit TIMESTAMP OPTIONS(description="Job execution start timestamp"),
  end_zeit TIMESTAMP OPTIONS(description="Job execution completion/failure timestamp"),
  status STRING OPTIONS(description="Execution state: LAUFEND, OK, ABBRUCH"),
  fehler_typ STRING OPTIONS(description="Alert severity: F (Fatal), E (Error), W (Warning)"),
  fehler_nr INT64 OPTIONS(description="Standardized error lookup code"),
  zusatz1 STRING OPTIONS(description="Optional custom error context 1"),
  zusatz2 STRING OPTIONS(description="Optional custom error context 2"),
  stichtag DATE OPTIONS(description="Business data processing date"),
  zusatzinfos STRING OPTIONS(description="Timing and performance annotations")
)
PARTITION BY DATE(start_zeit)
CLUSTER BY job_kennung, status;
```

### 4.2. IAM Permissions & Service Account Configuration
The Service Account assigned to the Cloud Composer environment (or GKE execution nodes) must be granted the following IAM roles:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the audit dataset.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the GCP project level.
* **Storage Object Admin** (`roles/storage.objectAdmin`) on the GCS bucket designated for logs.

### 4.3. Environment Variables
The following environment variables must be configured in the Cloud Composer Airflow environment:

| Variable Name | Expected Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `your-gcp-project-id` | The target Google Cloud Project ID. |
| `BQ_DATASET` | `your_audit_dataset` | The BigQuery dataset containing `bert_meldung`. |
| `GCS_LOGS_BUCKET` | `gs://your-logs-bucket-name` | The GCS bucket path where execution logs are written. |

### 4.4. Library Deployment
To make these utility modules importable by Airflow DAGs:
1. Copy `f_alis_msgerr.py`, `h_alis_date.py`, `h_alis_parameter.py`, and `h_alis_sqlplus.py` to the `/dags/` or `/plugins/` directory of your Cloud Composer environment.
2. Alternatively, package them as a private Python library and list them in your Composer `requirements.txt`.

---

## 5. Known Gaps & Unresolved References

### 5.1. Truncated Code in `h_alis_parameter.py` (Critical B4 Redesign Item)
* **Gap:** The generated code for `h_alis_parameter.py` was truncated during the conversion process inside the `pruefeSystemKennzahl` function.
* **Impact:** The functions `gibBereich`, `gibIntervall`, `pruefeZeitraum`, `pruefeZahlPositiv`, `pruefeZeitParameter`, and `konvertiereZeitspanne` are missing from the generated file.
* **Action Required:** A developer must manually complete the mapping arrays and date-range logic in `h_alis_parameter.py` using the provided Python pseudocode as a reference before deploying this module.

### 5.2. Missing Legacy SQL Scripts
* **Gap:** The legacy utilities referenced several external Oracle SQL scripts (`d_alis_vormonat.sql`, `d_alis_datum_zeitraum.sql`, `d_al_is_ermittlenr.sql`, `d_alis_spaufruf_p1.sql`, etc.) which were not provided.
* **Impact:** While their core behaviors (such as subtracting a month or calculating date boundaries) have been successfully modeled using standard Gregorian calendar rules, any custom business calendars, fiscal overrides, or holiday logic embedded in those original SQL files must be manually verified.

### 5.3. External Command Dependency in `h_alis_sqlplus.py`
* **Gap:** `h_alis_sqlplus.py` contains a call to the external binary `DWMSG_MeldeFehler`.
* **Impact:** If this binary is not installed on the Airflow worker nodes, execution will fail.
* **Action Required:** Refactor `h_alis_sqlplus.py` to import `dwmsg_melde_fehler` directly from `f_alis_msgerr.py` as a native Python function call, removing the external process dependency.

### 5.4. Unmigrated Downstream Jobs
The following 12 downstream jobs are currently **not yet migrated**:
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

**Coexistence Strategy:** If these jobs must run in a hybrid state (KSH calling Python), ensure that a Python 3 environment is available on the legacy execution runners, and invoke the utilities using their CLI entry points.

---

## 6. Validation

To validate the correctness of the migrated modules, execute the following test suites.

### 6.1. Unit Testing (Python `pytest`)
Create a test script `test_utilities.py` to verify date arithmetic and parameter validation:

```python
import pytest
from h_alis_date import letzter_tag_des_monats, tage_im_monat, addiere_datum

def test_letzter_tag_des_monats():
    assert letzter_tag_des_monats("20240229") is True  # Leap year
    assert letzter_tag_des_monats("20230228") is True  # Non-leap year
    assert letzter_tag_des_monats("20230227") is False

def test_tage_im_monat():
    assert tage_im_monat(2024, 2) == 29
    assert tage_im_monat(2023, 2) == 28

def test_addiere_datum():
    assert addiere_datum("20231231", 1) == "20240101"
    assert addiere_datum("20240228", 1) == "20240229"
```

Run the tests using:
```bash
pytest test_utilities.py
```

### 6.2. Integration Testing (CLI & BigQuery)
Verify that the telemetry module successfully writes to BigQuery.

1. **Initialize an entry:**
   ```bash
   python3 f_alis_msgerr.py DWMSG_ErzeugeEintrag "TEST_RUN_001" "JOB_VAL_01" "test_script.py" "gs://my-logs/test.log"
   ```
2. **Verify in BigQuery:**
   ```sql
   SELECT * FROM `YOUR_GCP_PROJECT.YOUR_AUDIT_DATASET.bert_meldung` WHERE eintrags_nr = 'TEST_RUN_001';
   -- Expected: Status = 'LAUFEND'
   ```
3. **Append timing info:**
   ```bash
   python3 f_alis_msgerr.py DWMSG_AppendTimingInfos "TEST_RUN_001" "Step 1 Completed" "YYYY-MM-DD HH24:MI:SS"
   ```
4. **Set status to OK:**
   ```bash
   python3 f_alis_msgerr.py DWMSG_SetzeStatusOK "TEST_RUN_001"
   ```
5. **Verify final state in BigQuery:**
   ```sql
   SELECT status, zusatzinfos FROM `YOUR_GCP_PROJECT.YOUR_AUDIT_DATASET.bert_meldung` WHERE eintrags_nr = 'TEST_RUN_001';
   -- Expected: Status = 'OK', zusatzinfos contains 'Step 1 Completed [Timestamp]'
   ```

### 6.3. Definition of "Passing"
* **Exit Codes:** All CLI commands must return exit code `0` on success.
* **Output Format:** Functions designed for shell evaluation (e.g., `DWDate_Vormonat`) must print clean, eval-compatible strings (e.g., `MY_VAR='20231001'`) to `stdout`.
* **Database State:** BigQuery rows must accurately reflect updates without throwing schema mismatch or permission errors.

---

## 7. Rollback Procedure

In the event of an operational failure or data discrepancy during cutover, follow these steps to revert to the legacy environment:

1. **Revert Orchestration Sourcing:**
   Modify the calling scripts or Airflow DAGs to point back to the legacy KornShell files:
   ```bash
   # Revert this:
   # python3 f_alis_msgerr.py ...
   
   # Back to this:
   . /vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
   ```
2. **Keep Oracle Package Active:**
   Do not compile or disable the Oracle `BERT_MELDUNG` package or drop its underlying tables during the co-existence phase. They must remain active to receive telemetry from rolled-back jobs.
3. **Data Reconciliation:**
   If jobs executed successfully on GCP but wrote telemetry to BigQuery, run a manual export of the `bert_meldung` BigQuery table and load those records back into the Oracle `BERT_MELDUNG` table to maintain historical audit continuity.