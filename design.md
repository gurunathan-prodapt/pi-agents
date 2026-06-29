# MIGRATION DESIGN DOCUMENT

This document details the migration plan for the KornShell control script `k_ausd_bp_ta_bpr_apn.ksh` to BigQuery. It contains the verbatim output from the CodeMaverick migration tool followed by the operational context, execution plan, environmental replacements, and risks.

---

## VERBATIM MCP TOOL OUTPUT

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh ===
Document: Shell Script Analysis

1. Purpose
- Control script for `r_ausd_bp_ta_bpr_apn.ksh`.
- Orchestrates environment setup, parameter validation, date validation, SQL execution, and post-processing of record count.
- Intended to run a database extraction/processing SQL script and capture the number of produced records.

2. Input Parameters
- `-j` `p_JobKennung`: Job identifier.
- `-f` `p_EintragsNr`: Entry number.
- `-s` `p_Stichtag`: Key date in `DDMMYYYY` format.
- `-l` `p_wiederanlaufWert`: Restart/recovery value; defaults to `0` if not provided.
- `-h`: Help message and exit.

3. Outputs
- Console messages:
  - “Bitte ueber Rahmenscript aufrufen”
  - “Pruefe Datum”
  - “Datum OK”
  - “ ---------- ENDE Datenverarbeitung ----------”
  - Error messages via `DWMSG_MeldeFehler` and `echo`
- Temporary file containing record count:
  - `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp`
- SQL-driven output files are implied by the called SQL script and commented post-processing section.

4. Dependencies
- Sourced shell libraries:
  - `$HOME/.dw_init`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
- External helper scripts:
  - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
- SQL script:
  - `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_apn.sql`
- External functions/macros:
  - `pruefeParameterGesetzt`
  - `DWMSG_MeldeFehler`
  - `DWDate_Datum_Check`
  - `starteSQLSkript`
- File system variables:
  - `HOME`, `BERT_DIR_ROOT`, `DW_DIR_UTL`

5. Logic Flow
- Load environment and utility scripts.
- Parse command-line options using `getopts`.
- Validate required parameters:
  - Job identifier
  - Key date
  - Entry number
- If validation fails:
  - Set error code and argument
  - Emit error message
  - Exit with error code
- Validate date format using `DWDate_Datum_Check`.
- Load SQL execution helper.
- Define SQL script path and temp file path.
- Initialize restart value to `0` if empty.
- Compute today/yesterday via `gestern.ksh`.
- Execute SQL script via `starteSQLSkript`, passing job metadata, date, temp file, root directory, and date context.
- Print completion message.
- Read record count from temp file.
- Job-table insertion is commented out.

6. External File Dependencies / APIs / Tools
- Shell utilities:
  - `getopts`
  - `cat`
  - `eval`
  - `print`
- Commented file-processing tools:
  - `sed`
  - `sort`
  - `join`
- SQL execution wrapper likely around Oracle SQL*Plus or similar.
- No direct API calls.

7. BigQuery Replicability
- Core logic is replicable in BigQuery using:
  - Stored procedures for orchestration
  - Procedure parameters for shell variables
  - `ASSERT`/`IF`/`BEGIN...EXCEPTION` for validation and error handling
  - `EXECUTE IMMEDIATE` for dynamic SQL execution if needed
  - Tables instead of temp files for record counts and job metadata
- Date validation can be replicated with `SAFE.PARSE_DATE` or regex checks.
- The actual SQL script content is not present, so only the wrapper/control logic can be fully mapped here.

8. Functionality Gaps and Alternatives
- Shell environment sourcing has no direct BigQuery equivalent:
  - Replace with procedure parameters, session variables, or configuration tables.
- `getopts` parsing is external to BigQuery:
  - Replace with stored procedure parameters.
- `DWDate_Datum_Check`:
  - Replace with SQL validation logic or a Python UDF if strict format enforcement is needed.
- `starteSQLSkript`:
  - Replace with BigQuery stored procedure logic or orchestration workflow.
- File-based temp record count:
  - Replace with a staging/control table.
- Commented `sed/sort/join` file processing:
  - Replace with SQL transformations using `REGEXP_REPLACE`, `QUALIFY`, `ROW_NUMBER`, `FULL OUTER JOIN`, and `STRING_AGG`.
- Job deactivation/creation:
  - Replace with control tables and status updates.

9. Script Segmentation
- Data extraction and ingestion
  - SQL script execution via `starteSQLSkript`
  - Record count capture from temp file
- Transformations and aggregations
  - None in active shell logic
  - Commented file cleanup/sort/join pipeline exists as legacy post-processing
- Error handling and logging
  - Parameter validation
  - Date validation
  - Error code propagation
  - Console/error messages
- Output or result storage
  - Temp file for record count
  - Potential job table entry creation (commented)

Assumptions and Additional Notes
- The actual SQL business logic resides in `d_ausd_bp_ta_bpr_apn.sql` and is not included; only wrapper behavior is translated.
- The commented file-processing block is treated as legacy/non-executed logic but mapped conceptually to BigQuery equivalents.
- `p_wiederanlaufWert` is initialized but not used in the visible active logic.
- `v_TabName='PoolBasisprodukt'` is used only for commented job management.
- BigQuery implementation should use a control table for job metadata and record counts instead of filesystem temp files.
- If the SQL script depends on Oracle-specific syntax, it must be rewritten for BigQuery SQL.

Pseudocode: BQ SQL Pseudocode

```sql
-- BigQuery Stored Procedure: control wrapper for d_ausd_bp_ta_bpr_apn logic

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_apn`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_stichtag_date DATE;
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_restart_value STRING DEFAULT '0';

  -- Default restart value
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET v_restart_value = '0';
  ELSE
    SET v_restart_value = p_wiederanlaufWert;
  END IF;

  -- Required parameter checks
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_err_msg = 'Jobkennung fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET v_err_msg = 'Stichtag fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET v_err_msg = 'EintragsNr fehlt';
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- Date validation: DDMMYYYY
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    SET v_err_msg = CONCAT('Ungültiges Datum: ', p_Stichtag);
    RAISE USING MESSAGE = v_err_msg;
  END IF;

  -- Equivalent of gestern.ksh
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Execute migrated SQL logic
  -- Option A: inline translated SQL from d_ausd_bp_ta_bpr_apn.sql
  -- Option B: call another stored procedure containing the migrated SQL
  CALL `project.dataset.d_ausd_bp_ta_bpr_apn`(
    p_EintragsNr,
    p_JobKennung,
    p_Stichtag,
    v_restart_value,
    v_datum_heute,
    v_datum_gestern
  );

  -- Capture record count from result table instead of temp file
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.output_table`
    WHERE stichtag = v_stichtag_date
  );

  -- Persist job/control record instead of FOSJobErzeugeEintrag
  INSERT INTO `project.dataset.job_control_table` (
    tab_name,
    status_code,
    process_type,
    stichtag_from,
    stichtag_to,
    job_type,
    active_flag,
    record_count,
    description,
    job_kennung,
    eintrags_nr,
    created_at
  )
  VALUES (
    v_TabName,
    'A',
    'I',
    v_stichtag_date,
    v_stichtag_date,
    'J',
    'N',
    v_records,
    'Initialbefuellung',
    p_JobKennung,
    p_EintragsNr,
    CURRENT_TIMESTAMP()
  );

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
END;
```

Low-Level Pseudocode

```text
PROCEDURE r_ausd_bp_ta_bpr_apn(p_JobKennung, p_EintragsNr, p_Stichtag, p_wiederanlaufWert)

  SET v_TabName = 'PoolBasisprodukt'
  SET v_records = 0

  IF p_wiederanlaufWert is null or empty
    SET v_restart_value = '0'
  ELSE
    SET v_restart_value = p_wiederanlaufWert
  END IF

  IF p_JobKennung missing
    THROW error 'Jobkennung fehlt'
  END IF

  IF p_Stichtag missing
    THROW error 'Stichtag fehlt'
  END IF

  IF p_EintragsNr missing
    THROW error 'EintragsNr fehlt'
  END IF

  PARSE p_Stichtag as DDMMYYYY into v_stichtag_date
  IF parse failed
    THROW error 'Ungültiges Datum'
  END IF

  SET v_datum_heute = current date
  SET v_datum_gestern = current date - 1 day

  EXECUTE migrated SQL logic using:
    - p_EintragsNr
    - p_JobKennung
    - p_Stichtag
    - v_restart_value
    - v_datum_heute
    - v_datum_gestern

  SET v_records = count rows from produced result table for current stichtag

  INSERT job control row with:
    - table name
    - status
    - process type
    - date range
    - job type
    - active flag
    - record count
    - description
    - job metadata

  OUTPUT completion message

END PROCEDURE
```

Python Pseudocode (if applicable)

```python
def validate_ddmmyyyy(p_stichtag: str) -> bool:
    from datetime import datetime
    try:
        datetime.strptime(p_stichtag, "%d%m%Y")
        return True
    except ValueError:
        return False
```

Configuration Files Required for BigQuery Execution
- BigQuery dataset/table configuration for:
  - target output table
  - job control table
  - staging/result table if needed
- Optional orchestration config:
  - Cloud Composer DAG
  - Cloud Workflows definition
  - Cloud Scheduler trigger
- Optional parameter/config table:
  - environment-specific root paths replaced by dataset/project references
- Optional migration mapping file:
  - source shell parameter names to BigQuery procedure parameters

---

## CONTEXT THE MCP COULD NOT SEE

### 1. Lineage
The control script `k_ausd_bp_ta_bpr_apn.ksh` has the following dependencies and lineage edges:
* **Upstream/Input Triggering**: This control script is called from an overall frame script (noted in console statements as *"Bitte ueber Rahmenscript aufrufen"*).
* **Direct Execution Link**: It triggers the SQL script `d_ausd_bp_ta_bpr_apn.sql` (located in `${BERT_DIR_ROOT}/aufbereitung/sql/`) passing parameters extracted from arguments.
* **Downstream Consumers**: The output metrics are written to a metadata/control file (`bert_k_ausd_bp_ta_bpr_apn.tmp`), which is historically logged to a control table (FOS Job management, e.g., `PoolBasisprodukt`). The target output table populated by the inner SQL logic serves as an upstream input for downstream reporting or data lake staging.

### 2. External System Replacements
* **Oracle SQL*Plus Execution wrapper (`h_alis_sqlplus.ksh`)**: Replaced by direct BigQuery procedural execution. Instead of triggering a shell wrapper that logs into Oracle via SQL*Plus, Cloud Composer (Airflow) or Cloud Workflows can trigger the BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_bpr_apn` directly.
* **Temporary Files (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp`)**: These filesystem paths for capturing record counts are migrated to a dedicated BigQuery control table structure (`project.dataset.job_control_table`). This eliminates disk writes and local state variables.
* **Date Computation (`gestern.ksh`)**: The Shell calculation of today's and yesterday's date is replaced with standard SQL date functions: `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.

### 3. Cross-File Dependencies
* **Shared Tables**: 
  * `PoolBasisprodukt` – Key processed table.
  * `job_control_table` – Migrated control registry which tracks executed jobs, replacement for `FOSJobErzeugeEintrag`.
* **Execution Call Chains**:
  * Legacy: `Rahmenscript` $\rightarrow$ `k_ausd_bp_ta_bpr_apn.ksh` $\rightarrow$ `d_ausd_bp_ta_bpr_apn.sql`
  * Target BQ: `Airflow DAG` $\rightarrow$ `r_ausd_bp_ta_bpr_apn` (Stored Procedure Wrapper) $\rightarrow$ `d_ausd_bp_ta_bpr_apn` (Inner business logic SP)

### 4. Target File Plan
Below is the list of target files to be generated during the build phase:

| Source File Path | Target File Relative Path | Target Language | Description / Role |
| :--- | :--- | :--- | :--- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` | `gcp_target/stored_procedures/r_ausd_bp_ta_bpr_apn.sql` | BigQuery SQL (Stored Procedure) | Wrapper procedure which validates parameters, computes dates, and calls downstream extraction. |
| (External Dependency) | `gcp_target/stored_procedures/d_ausd_bp_ta_bpr_apn.sql` | BigQuery SQL (Stored Procedure) | Business logic migration of original extraction SQL script. |
| (Orchestration Config) | `gcp_target/dags/dag_r_ausd_bp_ta_bpr_apn.py` | Python (Airflow DAG) | Airflow workflow scheduling and invocation framework. |

### 5. Environment-Specific Values
The Build Agent must populate the following configuration properties when compiling target code:
* **`${GCP_PROJECT_ID}`**: Target Google Cloud Project ID (e.g., `prod-data-platform`).
* **`${GCP_DATASET}`**: BigQuery Target Dataset Name (e.g., `isbert_schema`).
* **Connection / Region**: Target execution region (e.g., `europe-west3`).
* **Scheduling Rules**: Intended execution frequency (typically daily, triggered following upstream data readiness of pool source tables).

### 6. Risks and Manual Steps
* **Oracle SQL Syntax**: The actual business queries inside `d_ausd_bp_ta_bpr_apn.sql` are written for Oracle and may use Oracle-specific features (e.g., specific joins, string formatting, analytical functions). They must be carefully reviewed and refactored during the build phase of the inner SP.
* **Unresolved Sourced Helpers**: Common logic such as error messaging (`f_alis_msgerr.ksh`) or parameter checks (`h_alis_parameter.ksh`) are replaced by standard declarative BigQuery procedural handling (using `ASSERT`, standard conditional statements, and system error routing). Manual verification must be performed to ensure all required logging features are modeled correctly in the GCP audit logging ecosystem.