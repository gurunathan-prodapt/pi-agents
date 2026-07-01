# MIGRATION DESIGN DOCUMENT: `k_ausd_v_ta_apn_ve.ksh`

This document details the migration plan and specifications for converting the legacy KornShell wrapper job `k_ausd_v_ta_apn_ve.ksh` into an equivalent cloud-native architecture on Google Cloud Platform (GCP) with Google BigQuery and Apache Airflow (Cloud Composer).

---

## 1. LINEAGE & CONTEXT

### Lineage Summary
* **Upstream Producer:** This script is designed to be called through a framing or coordinating shell script (`r_ausd_vertrag.ksh`) or scheduler, as indicated by the message: *"Bitte ueber Rahmenscript aufrufen"* (Please call via frame script).
* **Core Script Executed:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_apn_ve.sql`
* **Downstream Target Table:** `ta_apn_ve` (referenced via `v_TabName='ta_apn_ve'`).
* **Lineage Edges (Automated Metadata):** None directly found in automated parser results, but derived from shell variables and comments.

### External System Replacements
* **Oracle SQL*Plus Client (`h_alis_sqlplus.ksh`):** Replaced by the `BigQueryInsertJobOperator` or `BigQueryExecuteQueryOperator` in Airflow, or direct procedural execution inside Google Cloud BigQuery.
* **Unix Shared Filesystem Paths (`$HOME/.dw_init`, `$DW_DIR_UTL`):** Environment profiles are migrated to Cloud Composer environment variables. The temporary record storage (`bert_k_ausd_v_ta_apn_ve_$$.tmp`) is replaced by BigQuery's procedural variables (e.g., `DECLARE v_records INT64;` capturing `@@row_count`) or transactional log tables.
* **Control / Operational Logging (`job_table`):** Replaced with native BigQuery monitoring/audit logging or a centralized database logging table (`dw_isbert.job_table`).

---

## 2. TARGET FILE PLAN

The legacy KornShell script and its associated execution flow are divided into two primary GCP components:

| Target Relative Path | Target Language | Source File / Logic Origin | Purpose |
| :--- | :--- | :--- | :--- |
| `dags/dag_k_ausd_v_ta_apn_ve.py` | Python / Airflow | `k_ausd_v_ta_apn_ve.ksh` (getopts, orchestration, error flow) | Orchestrator DAG to validate inputs, execute BigQuery procedural logic, and log the job execution. |
| `stored_procedures/sp_k_ausd_v_ta_apn_ve.sql` | GoogleSQL | `k_ausd_v_ta_apn_ve.ksh` (core SQL wrapper, job update, parameter checks) | Stored procedure encapsulating parameter validation, the core business query execution, active job deactivation, and row count tracking. |

---

## 3. VERBATIM MCP TOOL MIGRATION DESIGN & PSEUDOCODE

Below is the exact output from the specialized migration tool analyzing the source file logic:

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh ===
Document: Shell Script Analysis

1. Purpose
- Control script for `r_ausd_vertrag.ksh`.
- Main responsibilities:
  - Ignore active jobs.
  - Invoke an SQL script.
  - Register the job in a job table.
  - Deactivate old active jobs.
- Acts as a wrapper/orchestrator around SQL execution and parameter validation.

2. Input Parameters
- `-j` / `p_JobKennung`
  - Job identifier.
- `-f` / `p_EintragsNr`
  - Entry number / record identifier.
- `-h`
  - Help flag; prints message and exits.

3. Outputs
- Console messages:
  - Error messages via `DWMSG_MeldeFehler`
  - `"Bitte ueber Rahmenscript aufrufen"`
  - `" ---------- ENDE Datenverarbeitung ----------"`
- Temporary file output:
  - `"$DW_DIR_UTL/bert_k_ausd_v_ta_apn_ve_$$.tmp"`
  - Used to store number of processed/provided records.
- Final variable:
  - `v_records` loaded from temp file content.

4. Dependencies
- Environment file:
  - `$HOME/.dw_init`
- Sourced utility scripts:
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
- External SQL script:
  - `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_apn_ve.sql`
- Shell built-ins / commands:
  - `getopts`
  - `print`
  - `echo`
  - `cat`
  - `eval`
  - `set +e`, `set -eu`
- Custom functions/macros:
  - `pruefeParameterGesetzt`
  - `DWMSG_MeldeFehler`
  - `starteSQLSkript`

5. Logic Flow
- Load environment and helper scripts.
- Parse command-line parameters using `getopts`.
- Validate required parameters:
  - Job identifier
  - Entry number
- If validation fails:
  - Emit error message
  - Exit with error code
- Enable strict shell behavior (`set -eu`).
- Load SQL execution helper.
- Define SQL script path.
- Define temp file path for record count.
- Execute SQL script via `starteSQLSkript` with job and entry parameters.
- Print completion message.
- Read record count from temp file into `v_records`.

6. External File / Tool Usage
- Reads:
  - `$HOME/.dw_init`
  - sourced `.ksh` utility files
  - SQL script file
  - temp file containing record count
- Writes:
  - temp file created by SQL execution helper
- Likely relies on:
  - Oracle SQL*Plus or similar DB client through `h_alis_sqlplus.ksh`

7. BigQuery Replicability
- Replicable in BigQuery SQL and Stored Procedures:
  - Parameter handling can be mapped to stored procedure parameters.
  - Validation can be implemented with `ASSERT` or procedural `IF`.
  - SQL script execution can be translated into BigQuery DML/DDL inside a stored procedure.
  - Job logging / job table insertion can be implemented as table writes.
  - Record count can be captured using `@@row_count` or `COUNT(*)` logic.
- Not directly replicable:
  - Shell sourcing of environment files.
  - `getopts` parsing.
  - Temp file creation and `cat`/`eval` pattern.
  - External SQL*Plus wrapper behavior.

8. Functionality Gaps and Alternatives
- Environment initialization:
  - Replace with procedure parameters, dataset/project configuration, or deployment-time variables.
- Temp file record count:
  - Replace with `DECLARE v_records INT64;` and assign from `@@row_count` or query result.
- SQL script invocation:
  - Replace with inline BigQuery SQL statements or dynamic SQL via `EXECUTE IMMEDIATE`.
- Error messaging:
  - Replace with `ASSERT`, `RAISE USING MESSAGE`, or logging table inserts.
- Job control / active job handling:
  - Replace with job status table and transactional updates.
- Unsupported shell helper functions:
  - If they contain complex logic, isolate into Python or external orchestration only if unavoidable.

Break Down of the Script

1. Data Extraction and Ingestion
- No direct file-based extraction.
- SQL execution is delegated to external SQL script `d_ausd_v_ta_apn_ve.sql`.
- Input parameters are passed into the SQL execution wrapper.

2. Transformations and Aggregations
- No transformations in this wrapper script itself.
- Transformation logic is assumed to reside in the referenced SQL script.

3. Error Handling and Logging
- Parameter parsing errors:
  - Missing argument (`ErrNr=193`)
  - Unknown parameter (`ErrNr=192`)
- Validation errors:
  - Missing required parameters
- Error reporting:
  - `DWMSG_MeldeFehler`
  - Console echo
- Exit codes:
  - Uses shell exit status to signal failure.

4. Output or Result Storage
- SQL execution result count stored in temp file.
- Temp file content loaded into `v_records`.

Mapping Bash Constructs to BigQuery SQL

1. Environment Variables
- `$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`
  - Map to stored procedure parameters, constants, or deployment configuration tables.

2. Loops
- `while getopts ...`
  - Replace with stored procedure parameters; no loop needed.
- No other loops present.

3. Conditionals
- `case` on parameter flags
  - Replace with procedural `IF` blocks in BigQuery scripting.
- Validation `if [ ! $ErrNr -eq 0 ]`
  - Replace with `IF ErrNr != 0 THEN ... END IF;`

4. Functions and Commands
- `getopts`
  - Replace with procedure parameters.
- `print`, `echo`
  - Replace with `SELECT` statements for debugging or logging table inserts.
- `cat`
  - Replace with direct variable assignment or query result capture.
- `eval`
  - Not needed in BigQuery.
- `set -eu`
  - Replace with explicit `ASSERT` / `IF` checks.

5. File Manipulation
- Temp file creation and reading
  - Replace with BigQuery variables and/or audit tables.
- SQL script file
  - Replace with inline SQL or dynamic SQL stored in a procedure.

Assumptions and Additional Notes
- The referenced SQL script `d_ausd_v_ta_apn_ve.sql` contains the actual business logic; this wrapper only orchestrates execution.
- The job table and active-job handling are assumed to be implemented in the SQL script or helper routines.
- BigQuery implementation should preserve:
  - parameter validation
  - job identification
  - execution logging
  - record count capture
- If the SQL script contains Oracle-specific syntax, it must be rewritten to BigQuery SQL.
- If helper scripts contain complex logic not visible here, they may require separate migration.

Configuration Files Required for BigQuery Execution
- Stored procedure definition file
- Optional deployment configuration:
  - project id
  - dataset name
  - job log table name
  - target table name
- Optional orchestration config:
  - Cloud Composer / Workflows / Cloud Scheduler configuration if external scheduling is needed
- Optional parameter mapping config:
  - JSON/YAML for runtime values if not passed directly to the procedure

Pseudocode: BQ SQL Pseudocode

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_apn_ve';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
      (job_kennung, eintrags_nr, err_nr, err_arg, created_at)
    VALUES
      (p_JobKennung, p_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());

    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS message;
    LEAVE;
  END IF;

  -- Job registration / active job handling
  INSERT INTO `project.dataset.job_table`
    (job_kennung, eintrags_nr, tab_name, status, created_at)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'ACTIVE', CURRENT_TIMESTAMP());

  -- Core SQL logic translated from d_ausd_v_ta_apn_ve.sql
  -- Example placeholder for actual business logic:
  -- INSERT INTO target_table (...)
  -- SELECT ...
  -- FROM source_table
  -- WHERE ...
  -- AND active_job_flag = FALSE;

  -- Capture affected row count
  SET v_records = @@row_count;

  -- Deactivate old active jobs
  UPDATE `project.dataset.job_table`
  SET status = 'INACTIVE',
      updated_at = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr <> p_EintragsNr
    AND status = 'ACTIVE';

  -- Persist record count
  INSERT INTO `project.dataset.job_run_summary`
    (job_kennung, eintrags_nr, tab_name, records_processed, created_at)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
END;
```

Python Pseudocode (if applicable)

```python
def validate_parameters(p_JobKennung, p_EintragsNr):
    ErrNr = 0
    ErrArg = ""

    if not p_JobKennung:
        ErrNr = 193
        ErrArg = "Jobkennung"

    if ErrNr == 0 and not p_EintragsNr:
        ErrNr = 193
        ErrArg = "EintragsNr"

    return ErrNr, ErrArg
```

---

## 4. CROSS-FILE DEPENDENCIES & SCHEMAS

The execution context of `k_ausd_v_ta_apn_ve.ksh` expects a stateful relational schema to manage long-running data-warehouse batch runs. 

### Operational Table Schemas

#### 1. `job_table`
Tracks execution status of the pipeline steps for synchronization.
```sql
CREATE TABLE IF NOT EXISTS `dw_isbert.job_table` (
  job_kennung STRING NOT NULL,
  eintrags_nr STRING NOT NULL,
  tab_name STRING,
  status STRING, -- 'ACTIVE', 'INACTIVE', 'FAILED', 'COMPLETED'
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

#### 2. `job_run_summary`
Tracks the execution metadata and number of records loaded into target structures.
```sql
CREATE TABLE IF NOT EXISTS `dw_isbert.job_run_summary` (
  job_kennung STRING,
  eintrags_nr STRING,
  tab_name STRING,
  records_processed INT64,
  created_at TIMESTAMP
);
```

#### 3. `job_error_log`
Used for recording parameter mismatch or structural execution errors.
```sql
CREATE TABLE IF NOT EXISTS `dw_isbert.job_error_log` (
  job_kennung STRING,
  eintrags_nr STRING,
  err_nr INT64,
  err_arg STRING,
  created_at TIMESTAMP
);
```

---

## 5. ENVIRONMENT-SPECIFIC CONFIGURATION

To ensure flexibility across different release environments, the target Airflow DAG and BigQuery commands must resolve parameters using the following environment profile:

| Property | Development Value | Production Value | Description |
| :--- | :--- | :--- | :--- |
| **GCP Project ID** | `gcp-proj-dw-dev` | `gcp-proj-dw-prod` | Target GCP project hosting BigQuery resources |
| **BigQuery Dataset** | `dw_isbert_dev` | `dw_isbert_prod` | Namespace containing tables & stored procedures |
| **Airflow Connection ID**| `bigquery_default` | `bigquery_default` | Orchestrator credential profile name |
| **Error Notify Target** | `dev-alerts@company.com` | `prod-alerts@company.com` | Pub/Sub topic or email address for failed runs |

---

## 6. RISKS & MANUAL MIGRATION STEPS

1. **Unresolved SQL Dependency (`d_ausd_v_ta_apn_ve.sql`):**
   * *Description:* This control wrapper relies entirely on the SQL statement defined in `d_ausd_v_ta_apn_ve.sql` to populate `ta_apn_ve`.
   * *Mitigation:* The conversion engineer must locate `d_ausd_v_ta_apn_ve.sql`, convert its Oracle/Teradata queries into GoogleSQL, and embed that logic into the core placeholder segment of `sp_k_ausd_v_ta_apn_ve.sql`.
2. **Active Job Locking Logic Verification:**
   * *Description:* The legacy system disables older jobs if they are registered as active. Concurrent script executions might trigger race conditions.
   * *Mitigation:* In BigQuery, multi-statement transactions (`BEGIN TRANSACTION ... COMMIT TRANSACTION`) must wrap the job registration and the inactivation logic to guarantee ACID isolation.
3. **Record Count Accuracy (`@@row_count`):**
   * *Description:* The legacy wrapper dumps record count via a temporary text file output.
   * *Mitigation:* The converted BigQuery stored procedure should directly assign `@@row_count` immediately after the converted operational DML statement is executed to avoid capturing secondary statement row counts.