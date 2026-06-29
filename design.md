# MIGRATION DESIGN DOCUMENT

**Migration Metadata**
* **Seed Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh`
* **Seed Type:** KSH (KornShell)
* **Source Root:** `/home/gurunathan_t/test_lineage_data`
* **Target Platform:** BigQuery

---

## 1. SYSTEM CONTEXT & LINEAGE

The source KornShell script `k_ausd_bp_ta_bpr_apn.ksh` is a wrapper/control script that manages parameters, executes date checks, sets environment variables, and launches the Oracle SQL*Plus script `d_ausd_bp_ta_bpr_apn.sql`.

### Upstream & Downstream Lineage
* **Upstream Producer:** Orchestration or scheduling tools (e.g., UC4, Cron, or parent wrapper scripts) pass mandatory parameters such as Job Identifier (`-j`), Entry Number (`-f`), Key Date (`-s`), and optionally a Restart Value (`-l`).
* **Execution/Call Chain:** 
  1. `k_ausd_bp_ta_bpr_apn.ksh` parses and validates inputs.
  2. Resolves environment configurations via utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.).
  3. Computes dates (`gestern.ksh`).
  4. **Executes SQL (Downstream):** Launches `d_ausd_bp_ta_bpr_apn.sql` (Oracle SQL*Plus execution wrapper `h_alis_sqlplus.ksh`).
  5. **Post-Processing (Commented legacy section):** Historical logic indicates extracting, sorting, cleaning, and joining file-based data (`cibasis_data24.dat`, `cibasis_data96.dat`, etc.) into `cibasisprodukt.csv`.
* **Downstream Target:** The table `PoolBasisprodukt` is the central conceptual target that this wrapper populates or updates.

### Cross-File Dependencies
The script relies on several shared utility scripts (`BERT_DIR_ROOT/allgemein/is/util/bin/*`):
* `f_alis_msgerr.ksh` (Error messaging)
* `h_alis_date.ksh` (Date utilities and validations)
* `h_alis_parameter.ksh` (Parameter parsing)
* `h_alis_sqlplus.ksh` (SQL*Plus execution environment wrapper)
* `gestern.ksh` (Date calculator)

---

## 2. EXTERNAL SYSTEM REPLACEMENTS

* **Oracle RDBMS Database:** Migrated to Google Cloud Platform **BigQuery** tables and datasets.
* **Oracle SQL\*Plus Wrapper (`h_alis_sqlplus.ksh`):** Replaced by BigQuery Stored Procedures, utilizing Google Standard SQL, `DECLARE`, and scripting blocks.
* **Local POSIX File System / Temp Files:** Replaced by **BigQuery Temporary Tables** (e.g., `CREATE TEMP TABLE`) or persistent staging tables if they need to be preserved across jobs.
* **Shell Error Framework (`f_alis_msgerr.ksh`):** Translated into BigQuery structured validation `IF ... THEN ERROR` or `ASSERT` statements, throwing explicit runtime exceptions where parameters are invalid.
* **Legacy Shell Pre-processing (`sed`/`sort`/`join`):** Migrated to standard SQL operations (`REGEXP_REPLACE`, `ORDER BY`, `FULL OUTER JOIN`) within BigQuery, eliminating local OS file system requirements.

---

## 3. TARGET FILE PLAN

To recreate the wrapper's orchestration and parameters in the modern Cloud-native environment, we will produce two target components:

| Target File Path | Target Language | Source Component | Purpose |
| :--- | :--- | :--- | :--- |
| `bigquery/stored_procedures/sp_k_ausd_bp_ta_bpr_apn.sql` | Google SQL (Stored Procedure) | `k_ausd_bp_ta_bpr_apn.ksh` (and implicitly `d_ausd_bp_ta_bpr_apn.sql` integration) | Contains the validation logic, orchestration flow, date checks, and the main execution block of the process. |
| `orchestration/dags/dag_k_ausd_bp_ta_bpr_apn.py` | Python (Airflow / Cloud Composer DAG) | Shell parameters & scheduling context | Orchestrates execution, schedules the stored procedure, and manages execution parameters. |

---

## 4. ENVIRONMENT-SPECIFIC CONFIGURATION

When deploying to Google Cloud, the Build Agent or Deployment pipelines must parameterize the following values:
* **GCP Project ID:** Configured via Airflow Connection or BigQuery Stored Procedure execution environment.
* **Target Dataset:** E.g., `project_id.isbert_dataset` (where table `PoolBasisprodukt` resides).
* **Scheduling:** Airflow DAGs will schedule this job based on business requirements, passing the logical execution date formatted as `DDMMYYYY` as the `p_Stichtag` parameter.

---

## 5. RISKS AND MANUAL STEPS

1. **SQL Logic Integration:** The exact logic contained within the core business transformation file `d_ausd_bp_ta_bpr_apn.sql` is not present inside the wrapper. The target BigQuery stored procedure must incorporate the compiled output of that SQL script once migrated.
2. **Commented Post-Processing Logic Verification:** In the source shell script, several sections of `sed`, `sort`, and `join` are commented out. During the migration phase, verify if these steps are legacy and deprecated, or if they need to be re-activated in BigQuery (a SQL-equivalent formulation is provided in the MCP output below).
3. **Date Formats:** Oracle and Shell used `DDMMYYYY` formats. BigQuery's native `DATE` objects use `YYYY-MM-DD`. While the Stored Procedure accepts `p_Stichtag` as string to maintain backward compatibility, it parses it using `PARSE_DATE('%d%m%Y', p_Stichtag)`. Ensure downstream systems adapt to BigQuery native date data types.

---

## 6. VERBATIM MCP TOOL OUTPUT

The section below contains the complete analysis and translation generated by the CodeMaverick Migration Tool.

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh ===
Document: Shell Script Analysis

1. Purpose
- Control script for `r_ausd_bp_ta_bpr_apn.ksh`.
- Orchestrates environment setup, parameter validation, date validation, SQL execution, and post-processing of record count.
- Intended to run a database extraction/load process for table/job context `PoolBasisprodukt`.

2. Input Parameters
- `-j` `p_JobKennung`: Job identifier.
- `-f` `p_EintragsNr`: Entry number.
- `-s` `p_Stichtag`: Key date / as-of date in `DDMMYYYY`.
- `-l` `p_wiederanlaufWert`: Restart/recovery value, optional.
- `-h`: Help message and exit.

3. Outputs
- Console messages:
  - “Bitte ueber Rahmenscript aufrufen”
  - “Pruefe Datum”
  - “Datum OK”
  - “---------- ENDE Datenverarbeitung ----------”
  - Error messages via `DWMSG_MeldeFehler`
- Temporary file containing record count:
  - `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp`
- SQL execution side effects:
  - Executes external SQL script `d_ausd_bp_ta_bpr_apn.sql`
  - Produces downstream output files and/or database changes
- Final record count read from temp file into `v_records`

4. Dependencies
- Sourced shell libraries:
  - `$HOME/.dw_init`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
- External helper script:
  - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
- External SQL script:
  - `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bpr_apn.sql`
- External functions/macros used:
  - `pruefeParameterGesetzt`
  - `DWMSG_MeldeFehler`
  - `DWDate_Datum_Check`
  - `starteSQLSkript`
- Shell built-ins / commands:
  - `getopts`
  - `set`
  - `print`
  - `cat`
  - `eval`

5. Logic Flow
- Load environment and utility scripts.
- Parse command-line options.
- Set table name `PoolBasisprodukt`.
- Validate required parameters:
  - Job identifier
  - Key date
  - Entry number
- If validation fails:
  - Emit error message
  - Exit with error code
- Validate date format `DDMMYYYY`.
- Load SQL execution helper.
- Define SQL script path and temp file path.
- Initialize restart value to `0` if empty.
- Derive current date and previous date from `gestern.ksh`.
- Execute SQL script with parameters.
- Print end-of-processing message.
- Read record count from temp file.
- Job-table entry creation is present but commented out.

6. External File Dependencies / APIs / Tools
- File-based parameter and environment initialization.
- SQL script execution through Oracle SQL*Plus wrapper.
- Temporary file for record count.
- Commented-out file post-processing logic:
  - `sed`, `sort`, `join`
  - Output files:
    - `cibasis_data24.dat`
    - `cibasis_data96.dat`
    - `cibasis_fax.dat`
    - `cibasis_24_96.tmp`
    - `cibasisprodukt.csv`

7. Replicability in BigQuery
- Fully replicable:
  - Parameter validation
  - Date validation
  - Conditional branching
  - Record counting
  - SQL-driven transformations
- Partially replicable:
  - External SQL script execution must be converted into BigQuery SQL script or stored procedure.
  - File-based temp output must be replaced with BigQuery tables, temp tables, or procedure variables.
- Not directly replicable:
  - Shell sourcing of environment files
  - `gestern.ksh` date derivation
  - `eval` and file-based `cat` usage
- Recommended BigQuery equivalents:
  - Stored procedure parameters
  - `ASSERT` / `IF` / `BEGIN...EXCEPTION`
  - `DECLARE` variables
  - `CURRENT_DATE()` and `DATE_SUB`
  - Temporary tables
  - `CREATE TEMP TABLE`
  - `INSERT INTO`
  - `SELECT COUNT(*) INTO` via scripting variables

8. Functionality Gaps and Alternatives
- Shell environment initialization:
  - Replace with procedure parameters and deployment-time configuration tables.
- `gestern.ksh`:
  - Replace with `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
- SQL*Plus wrapper:
  - Replace with BigQuery scripting or scheduled query orchestration.
- File-based record count:
  - Replace with `DECLARE v_records INT64; SET v_records = (SELECT COUNT(*) ...)`.
- Commented file merge/sort/join logic:
  - Replace with SQL joins, deduplication, and `STRING_AGG`/`ARRAY_AGG` if needed.
- Error framework:
  - Replace with `ASSERT`, `RAISE USING MESSAGE`, or `BEGIN...EXCEPTION`.
- If any downstream file export is required:
  - Use BigQuery extract jobs to Cloud Storage.
- If complex parsing or OS-level file manipulation is required:
  - Use Python in Cloud Functions / Dataflow / Cloud Run, then load results into BigQuery.

Assumptions and Additional Notes
- The active logic in the script is primarily orchestration; the core business transformation resides in the external SQL script `d_ausd_bp_ta_bpr_apn.sql`.
- The commented-out post-processing section is treated as non-executed legacy logic, but its intended behavior is mapped to SQL equivalents.
- The temp file record count is assumed to represent the number of processed rows from the SQL execution.
- The BigQuery implementation assumes the SQL script logic can be embedded into a stored procedure or a sequence of DML/DDL statements.
- Any job-control table insertion is not active in the source script and is therefore represented only as optional downstream logic.

Breakdown of the Script

A. Data Extraction and Ingestion
- Loads environment and helper scripts.
- Reads runtime parameters.
- Derives dates.
- Executes external SQL script.

B. Transformations and Aggregations
- No active transformations in the shell script itself.
- Commented legacy logic indicates:
  - whitespace removal
  - sorting
  - deduplication
  - joining multiple output files

C. Error Handling and Logging
- Parameter presence checks.
- Date format validation.
- Error code propagation.
- Console logging.

D. Output or Result Storage
- Temporary record count file.
- Potential job table entry creation.
- Potential CSV/file outputs in commented section.

BigQuery SQL Pseudocode

```sql
-- BigQuery Stored Procedure Pseudocode
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
  DECLARE v_restart_value STRING;
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_err_nr INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_nr = 1;
    SET v_err_msg = 'Jobkennung fehlt';
  END IF;

  IF v_err_nr = 0 AND (p_Stichtag IS NULL OR TRIM(p_Stichtag) = '') THEN
    SET v_err_nr = 1;
    SET v_err_msg = 'Stichtag fehlt';
  END IF;

  IF v_err_nr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET v_err_nr = 1;
    SET v_err_msg = 'EintragsNr fehlt';
  END IF;

  IF v_err_nr != 0 THEN
    RAISE USING MESSAGE = CONCAT('FEHLER: ', CAST(v_err_nr AS STRING), ' ', v_err_msg);
  END IF;

  -- Date validation: DDMMYYYY
  SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);

  -- Restart value initialization
  SET v_restart_value = IFNULL(p_wiederanlaufWert, '0');

  -- Date derivation equivalent to gestern.ksh
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Core SQL logic replacement for d_ausd_bp_ta_bpr_apn.sql
  -- Replace with actual transformation logic from the external SQL script
  CREATE TEMP TABLE tmp_result AS
  SELECT
    *
  FROM `project.dataset.source_table`
  WHERE some_date_column = v_stichtag_date;

  -- Record count equivalent to temp file output
  SET v_records = (
    SELECT COUNT(*)
    FROM tmp_result
  );

  -- Optional: persist results
  -- INSERT INTO `project.dataset.target_table`
  -- SELECT * FROM tmp_result;

  -- Optional: job tracking equivalent to commented FOSJobErzeugeEintrag
  -- INSERT INTO `project.dataset.job_table`
  -- (tab_name, status, type, stichtag, ... , records, comment)
  -- VALUES
  -- (v_TabName, 'A', 'I', v_stichtag_date, v_stichtag_date, 'J', 'N', v_records, 'Initialbefuellung');

  SELECT
    v_TabName AS tab_name,
    p_JobKennung AS job_kennung,
    p_EintragsNr AS eintrags_nr,
    p_Stichtag AS stichtag,
    v_restart_value AS wiederanlauf_wert,
    v_datum_heute AS datum_heute,
    v_datum_gestern AS datum_gestern,
    v_records AS records;
END;
```

BQ-Compliant Pseudocode for Commented File Merge Logic

```sql
-- Equivalent of sed/sort/join pipeline using SQL tables
CREATE OR REPLACE PROCEDURE `project.dataset.merge_cibasis_outputs`()
BEGIN
  CREATE TEMP TABLE data24_clean AS
  SELECT DISTINCT
    REGEXP_REPLACE(line, r' ', '') AS line
  FROM `project.dataset.cibasis_data24_source`;

  CREATE TEMP TABLE data96_clean AS
  SELECT DISTINCT
    REGEXP_REPLACE(line, r' ', '') AS line
  FROM `project.dataset.cibasis_data96_source`;

  CREATE TEMP TABLE fax_clean AS
  SELECT DISTINCT
    REGEXP_REPLACE(line, r' ', '') AS line
  FROM `project.dataset.cibasis_fax_source`;

  CREATE TEMP TABLE data24_96 AS
  SELECT
    COALESCE(d24.key, d96.key) AS key,
    d24.value1,
    d96.value2
  FROM (
    SELECT SPLIT(line, ';')[OFFSET(0)] AS key,
           SPLIT(line, ';')[OFFSET(1)] AS value1
    FROM data24_clean
  ) d24
  FULL OUTER JOIN (
    SELECT SPLIT(line, ';')[OFFSET(0)] AS key,
           SPLIT(line, ';')[OFFSET(1)] AS value2
    FROM data96_clean
  ) d96
  USING (key);

  CREATE TEMP TABLE cibasisprodukt AS
  SELECT
    COALESCE(d96.key, fax.key) AS key,
    d24.value1,
    d96.value2,
    fax.value3
  FROM (
    SELECT SPLIT(line, ';')[OFFSET(0)] AS key,
           SPLIT(line, ';')[OFFSET(1)] AS value1
    FROM data24_clean
  ) d24
  FULL OUTER JOIN (
    SELECT SPLIT(line, ';')[OFFSET(0)] AS key,
           SPLIT(line, ';')[OFFSET(1)] AS value2
    FROM data96_clean
  ) d96
  USING (key)
  FULL OUTER JOIN (
    SELECT SPLIT(line, ';')[OFFSET(0)] AS key,
           SPLIT(line, ';')[OFFSET(1)] AS value3
    FROM fax_clean
  ) fax
  USING (key);

  SELECT * FROM cibasisprodukt;
END;
```

Python Pseudocode (if applicable)

```python
# Only for unsupported external orchestration or file-based preprocessing
def parse_and_validate_parameters(p_JobKennung, p_EintragsNr, p_Stichtag, p_wiederanlaufWert):
    if not p_JobKennung:
        raise ValueError("Jobkennung fehlt")
    if not p_EintragsNr:
        raise ValueError("EintragsNr fehlt")
    if not p_Stichtag:
        raise ValueError("Stichtag fehlt")
    return {
        "p_JobKennung": p_JobKennung,
        "p_EintragsNr": p_EintragsNr,
        "p_Stichtag": p_Stichtag,
        "p_wiederanlaufWert": p_wiederanlaufWert or "0"
    }

def derive_dates():
    from datetime import date, timedelta
    today = date.today()
    yesterday = today - timedelta(days=1)
    return today, yesterday

def validate_ddmmyyyy(stichtag):
    from datetime import datetime
    return datetime.strptime(stichtag, "%d%m%Y").date()
```

Configuration Files Required for BigQuery Execution
- BigQuery stored procedure deployment script
- Dataset/table DDL for:
  - source tables
  - target tables
  - optional job tracking table
  - optional staging/temp tables
- Optional parameter/configuration table for runtime values
- Optional orchestration config:
  - Cloud Composer DAG
  - Cloud Scheduler job
  - Dataform workflow
- Optional service account / IAM configuration for execution permissions