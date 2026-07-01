# MIGRATION DESIGN DOCUMENT: k_ausd_bp_ta_cntrct_dist.ksh

This document outlines the migration design for converting the legacy KornShell (KSH) script `k_ausd_bp_ta_cntrct_dist.ksh` to BigQuery.

---

## 1. VERBATIM MCP TOOL OUTPUT

Below is the complete output from the `shellscript_to_bqsql_design` tool, providing the structured analysis and conversion pseudo-code:

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh ===
Document: Shell Script Analysis

1. Purpose
- Control script for the batch job `r_ausd_bp_ta_cntrct_dist.ksh`.
- Orchestrates environment setup, parameter validation, date validation, SQL execution, and job/result bookkeeping.
- Executes a SQL script to produce output records and stores the record count in a temporary file.

2. Input Parameters
- `-j` Jobkennung
  - Job identifier.
- `-f` EintragsNr
  - Entry number / run identifier.
- `-s` Stichtag
  - Key business date in `DDMMYYYY`.
- `-l` wiederanlaufWert
  - Restart/recovery value; defaults to `0` if not provided.
- `-h`
  - Prints usage hint and exits.

3. Outputs
- Executes a SQL script:
  - `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_cntrct_dist.sql`
- Writes record count to:
  - `$DW_DIR_UTL/bert_k_ausd_bp_ta_cntrct_dist.tmp`
- Emits status messages to stdout/stderr.
- Potentially creates a job entry in a job table via commented-out job management calls.

4. Dependencies
- Environment file:
  - `$HOME/.dw_init`
- Shell utility scripts:
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
- Date helper script:
  - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
- SQL script:
  - `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_cntrct_dist.sql`
- Temporary file path:
  - `$DW_DIR_UTL/bert_k_ausd_bp_ta_cntrct_dist.tmp`
- External commands / shell built-ins:
  - `getopts`, `print`, `set`, `eval`, `cat`
- Commented-out utilities:
  - `FOSJobDeaktivate`
  - `FOSJobErzeugeEintrag`
  - `sed`, `sort`, `join`

5. Logic Flow
- Source environment and utility scripts.
- Parse command-line parameters using `getopts`.
- Set target table name `PoolBasisprodukt`.
- Validate required parameters:
  - Jobkennung
  - Stichtag
  - EintragsNr
- If validation fails:
  - Log error
  - Print error message
  - Exit with error code
- Validate date format of `p_Stichtag` as `DDMMYYYY`.
- Source SQL execution helper.
- Define SQL script path and temp file path.
- Initialize restart value to `0` if empty.
- Derive today/yesterday values from helper script `gestern.ksh`.
- Execute SQL script via `starteSQLSkript` with parameters:
  - entry number
  - SQL script path
  - entry number
  - job id
  - business date
  - temp file
  - root directory
  - today
  - yesterday
- Print end-of-processing message.
- Read record count from temp file.
- Commented-out job entry creation indicates intended post-processing bookkeeping.

6. External File / API / Tool Usage
- Reads:
  - environment initialization file
  - helper scripts
  - SQL script
  - temp file containing record count
- Writes:
  - temp file
  - SQL output files indirectly via SQL script/helper
- Uses shell-based job/error/date utilities.
- No direct API calls.
- No direct BigQuery API usage in original script.

7. Replicability in BigQuery
- Core logic is replicable in BigQuery SQL and stored procedures:
  - Parameter handling via stored procedure parameters.
  - Validation via procedural `IF` and `ASSERT`-style checks.
  - Date validation via `SAFE.PARSE_DATE` / format checks.
  - SQL execution via BigQuery scripting and `EXECUTE IMMEDIATE` if dynamic SQL is needed.
  - Record count storage via table insert/update instead of temp file.
- The shell-specific environment sourcing and helper scripts are not directly portable and must be replaced by:
  - procedure parameters
  - BigQuery scripting variables
  - logging tables
  - orchestration workflow outside BigQuery if needed

8. Functionality Gaps and Alternatives
- `DWMSG_MeldeFehler`
  - Replace with insert into error log table or `SELECT`/`ASSERT` failure.
- `pruefeParameterGesetzt`
  - Replace with `IF param IS NULL THEN SIGNAL/ASSERT`.
- `DWDate_Datum_Check`
  - Replace with `SAFE.PARSE_DATE('%d%m%Y', ...)` and null check.
- `starteSQLSkript`
  - Replace with BigQuery stored procedure or dynamic SQL execution.
- `gestern.ksh`
  - Replace with `CURRENT_DATE()` and `DATE_SUB`.
- Temp file record count
  - Replace with `DECLARE v_records INT64; SET v_records = (SELECT COUNT(*) ...)` and persist to table.
- Commented-out FOS job management
  - Replace with orchestration metadata tables or external workflow tool.
- File-based post-processing (`sed`, `sort`, `join`)
  - Replace with SQL transformations, staging tables, and `MERGE`/`JOIN` queries.
- If the SQL script contains unsupported procedural/file logic, isolate that logic in Python or orchestration and keep data transformations in BigQuery.

Assumptions and Additional Notes
- The referenced SQL script `d_ausd_bp_ta_cntrct_dist.sql` is assumed to contain the actual business transformation logic; this shell script is only the wrapper.
- The temp file is assumed to contain a single integer record count.
- The commented-out file processing section is not active and therefore not part of runtime behavior, but its intent is captured as a migration alternative.
- BigQuery implementation should use a stored procedure as the primary entry point.
- Any job-control semantics should be modeled with metadata tables if required.
- No renaming of business concepts is introduced in the migration pseudocode.

Breakdown of the Script

A. Data Extraction and Ingestion
- Loads environment and helper scripts.
- Derives date values.
- Executes the SQL script that performs the actual extraction/transformation.
- Reads record count from temp file.

B. Transformations and Aggregations
- No active shell-level transformations.
- Commented-out section indicates intended file cleanup, sorting, and joining of output datasets.
- These operations should be implemented in SQL joins, deduplication, and staging-table transformations.

C. Error Handling and Logging
- Parameter validation.
- Date validation.
- Error reporting via `DWMSG_MeldeFehler`.
- Exit on invalid input.
- `set -e` / `set +e` toggles shell error behavior.

D. Output or Result Storage
- SQL output generated by the SQL script.
- Record count stored in temp file.
- Potential job table entry creation is commented out.

BQ SQL Pseudocode

```sql
-- BigQuery Stored Procedure pseudocode
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_cntrct_dist`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_today DATE DEFAULT CURRENT_DATE();
  DECLARE v_yesterday DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err STRING DEFAULT NULL;

  -- Default restart value
  IF p_wiederanlaufWert IS NULL THEN
    SET p_wiederanlaufWert = 0;
  END IF;

  -- Required parameter checks
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err = 'Jobkennung fehlt';
    INSERT INTO `project.dataset.error_log`(job_name, error_message, created_at)
    VALUES (v_TabName, v_err, CURRENT_TIMESTAMP());
    ASSERT FALSE AS v_err;
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET v_err = 'EintragsNr fehlt';
    INSERT INTO `project.dataset.error_log`(job_name, error_message, created_at)
    VALUES (v_TabName, v_err, CURRENT_TIMESTAMP());
    ASSERT FALSE AS v_err;
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SET v_err = 'Stichtag fehlt';
    INSERT INTO `project.dataset.error_log`(job_name, error_message, created_at)
    VALUES (v_TabName, v_err, CURRENT_TIMESTAMP());
    ASSERT FALSE AS v_err;
  END IF;

  -- Date validation equivalent to DDMMYYYY
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    SET v_err = CONCAT('Ungueltiges Datum: ', p_Stichtag);
    INSERT INTO `project.dataset.error_log`(job_name, error_message, created_at)
    VALUES (v_TabName, v_err, CURRENT_TIMESTAMP());
    ASSERT FALSE AS v_err;
  END IF;

  -- Main SQL execution placeholder
  -- Equivalent to starteSQLSkript(...)
  EXECUTE IMMEDIATE """
    INSERT INTO `project.dataset.target_table`
    SELECT
      *
    FROM `project.dataset.source_table`
    WHERE business_date = @stichtag_date
  """
  USING v_stichtag_date AS stichtag_date;

  -- Record count equivalent to temp file content
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.target_table`
    WHERE business_date = v_stichtag_date
  );

  -- Persist job/result metadata instead of temp file
  INSERT INTO `project.dataset.job_run_log`(
    job_name,
    entry_nr,
    stichtag,
    records_written,
    restart_value,
    created_at
  )
  VALUES (
    v_TabName,
    p_EintragsNr,
    v_stichtag_date,
    v_records,
    p_wiederanlaufWert,
    CURRENT_TIMESTAMP()
  );

  -- Optional: emulate end message
  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS status_message;
END;
```

Low-Level Pseudocode

```text
PROCEDURE r_ausd_bp_ta_cntrct_dist(p_JobKennung, p_EintragsNr, p_Stichtag, p_wiederanlaufWert)

  SET v_TabName = 'PoolBasisprodukt'
  SET v_records = 0
  SET v_today = CURRENT_DATE()
  SET v_yesterday = CURRENT_DATE() - 1 day

  IF p_wiederanlaufWert IS NULL
    SET p_wiederanlaufWert = 0
  END IF

  IF p_JobKennung missing
    log error
    stop with failure
  END IF

  IF p_EintragsNr missing
    log error
    stop with failure
  END IF

  IF p_Stichtag missing
    log error
    stop with failure
  END IF

  TRY parse p_Stichtag as DDMMYYYY
  IF parse fails
    log error
    stop with failure
  END IF

  EXECUTE main SQL logic corresponding to d_ausd_bp_ta_cntrct_dist.sql

  SET v_records = count of produced rows

  STORE job metadata and record count in BigQuery table

  OUTPUT end-of-processing status

END PROCEDURE
```

Python Pseudocode (if applicable)

```python
def validate_ddmmyyyy(p_Stichtag: str) -> bool:
    from datetime import datetime
    try:
        datetime.strptime(p_Stichtag, "%d%m%Y")
        return True
    except ValueError:
        return False
```

Configuration Files Required for BigQuery Execution
- BigQuery stored procedure deployment script
- Dataset/table DDL for:
  - target table
  - error log table
  - job run log table
- Optional orchestration config:
  - Cloud Composer DAG / Workflows / Dataform / dbt job config
- Optional parameter file:
  - JSON/YAML for runtime values:
    - `p_JobKennung`
    - `p_EintragsNr`
    - `p_Stichtag`
    - `p_wiederanlaufWert`
- Optional SQL file for the migrated logic:
  - `r_ausd_bp_ta_cntrct_dist.sql`

---

## 2. SYSTEM MIGRATION CONTEXT & ENVIRONMENT

The following details supplement the raw code migration with architectural, environment-specific, and lineage context.

### 2.1 Lineage & Execution Sequence
- **Upstream Trigger**: The wrapper script printout `Bitte ueber Rahmenscript aufrufen` indicates this script is not executed standalone; it is triggered by an orchestration framework or shell-based scheduler frame script (e.g., `r_ausd_bp_ta_cntrct_dist.ksh`).
- **Target Extraction/Transformation**: This wrapper parses metadata and executes the underlying SQL transformation file: `d_ausd_bp_ta_cntrct_dist.sql`.
- **Downstream Tracking**: Historically, record counts were output to `$DW_DIR_UTL/bert_k_ausd_bp_ta_cntrct_dist.tmp` and registered into the FOS metadata framework via `FOSJobErzeugeEintrag`. On BigQuery, this sequence is mapped to a metadata logging table write or Airflow XCom record update.

### 2.2 External System Replacements
- **Legacy Shell Infrastructure**: Environment profiles (`.dw_init`), message logs (`f_alis_msgerr.ksh`), and date checkers (`h_alis_date.ksh`) are replaced by native Google Cloud Composer (Airflow) variables, custom Airflow Python operators, or embedded BigQuery procedural code (`SAFE.PARSE_DATE`).
- **Date Derivation**: The helper `gestern.ksh` is replaced by native BigQuery functions (`CURRENT_DATE()` and `DATE_SUB(...)`) or Airflow dynamic templated variables (`{{ ds }}`).
- **FOS Job Monitoring**: Comments regarding `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` will map directly to Airflow tasks updating a dedicated metadata schema (e.g. `audit_log.job_runs`) or built-in Airflow database states.

### 2.3 Cross-File Dependencies
- **`d_ausd_bp_ta_cntrct_dist.sql`**: Contains the core business transformations which need to be translated into BQ-compatible SQL syntax and executed within (or directly preceding) this wrapper sequence.
- **`gestern.ksh`**: An utility providing current/previous run dates.

### 2.4 Target File Plan

| Target File | Language/Format | Purpose | Source File |
| :--- | :--- | :--- | :--- |
| `dags/k_ausd_bp_ta_cntrct_dist.py` | Python / Airflow DAG | Orchestrates variables, executes BQ Stored Procedure, and parses completion status. | `k_ausd_bp_ta_cntrct_dist.ksh` |
| `ddl/sp_k_ausd_bp_ta_cntrct_dist.sql` | SQL (BigQuery Stored Proc) | Performs parameter validations, triggers the underlying migrated SQL code, and logs run metadata. | `k_ausd_bp_ta_cntrct_dist.ksh` |
| `ddl/d_ausd_bp_ta_cntrct_dist.sql` | SQL (BigQuery) | Ported version of the original transformation query to run against target datasets. | `d_ausd_bp_ta_cntrct_dist.sql` |

### 2.5 Environment-Specific Configs
- **Target GCP Project ID**: `gcp-project-id` (to be injected dynamically during deployment).
- **Target BigQuery Dataset**: `isbert_schema` (historically `ISBERT_SCHEMA`).
- **Target Logging Dataset**: `audit_log` (replaces FOS table tracking).

### 2.6 Risks & Manual Migration Steps
- **Nested SQL Script Translation**: The actual implementation relies heavily on `d_ausd_bp_ta_cntrct_dist.sql` which is not packaged in this wrapper code. That SQL file must be separately migrated to BQSQL dialect.
- **Orchestration Bindings**: The variables `$p_datum_heute` and `$p_datum_gestern` are passed as arguments to the original SQL file via `starteSQLSkript`. In BigQuery, these must be defined as variables or parameters in the stored procedure and utilized in the actual transformation query.
- **Legacy Commented Logic**: The commented file manipulations (`sed`, `sort`, `join`) represent older pipeline structures. It must be verified with business analysts if these operations (e.g. joining multiple `.dat` / `.csv` files) are still required at staging level, in which case they must be migrated to BigQuery staging tables and joins.