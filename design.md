# MIGRATION DESIGN DOCUMENT: `k_aurd_rechstan.ksh` to BigQuery & Airflow

## 1. EXECUTIVE SUMMARY
This document outlines the migration design for the KornShell script `k_aurd_rechstan.ksh` from a legacy Oracle SQL*Plus execution environment to Google BigQuery and Google Cloud Composer (Apache Airflow). 

The legacy script operates as an orchestration wrapper that:
1. Loads environment configurations and helper scripts.
2. Parses command-line inputs (Jobkennung, EintragsNr, Stichtag, wiederanlaufWert).
3. Validates date formats and parameters.
4. Executes the core database transformation SQL script `d_aurd_rechstan.sql` via SQL*Plus.
5. Captures execution metrics (record counts) and updates job execution log tables.

In the target BigQuery environment, this shell script is migrated into a combination of a **Cloud Composer (Airflow) DAG** for high-level orchestration/scheduling, and a **BigQuery Stored Procedure** for parameter/date validation and job control management.

---

## 2. VERBATIM MCP TOOL OUTPUT

Below is the complete, unmodified output from the migration design generation tool. Do not modify or paraphrase any pseudo code or logic structures presented here.

```markdown
=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh ===
Document: Shell Script Analysis

1. Purpose
- Control script for `r_ausd_vertrag.ksh` / `d_aurd_rechstan.sql`.
- Orchestrates parameter validation, date validation, SQL execution, and job-table bookkeeping.
- Ignores active jobs during processing.
- Intended to deactivate old active jobs and create a job-table entry after SQL execution.

2. Input Parameters
- `-j` Jobkennung
  - Job identifier.
- `-f` EintragsNr
  - Entry number / execution identifier.
- `-s` Stichtag
  - Key business date in `DDMMYYYY` format.
- `-l` wiederanlaufWert
  - Restart / resume value; defaults to `0` if omitted.
- `-h`
  - Help; prints message and exits.

3. Outputs
- Console messages:
  - “Bitte ueber Rahmenscript aufrufen”
  - “Pruefe Datum”
  - “Datum OK”
  - “ ---------- ENDE Datenverarbeitung ----------”
  - Error messages via `DWMSG_MeldeFehler`
- Temporary file containing record count:
  - `$DW_DIR_UTL/bert_k_aurd_rechstan_$$.tmp`
- Job-table entry creation is intended but commented out in the script.

4. Dependencies
- Environment initialization:
  - `$HOME/.dw_init`
- Sourced utility scripts:
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
- External script:
  - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
- SQL script:
  - `${BERT_DIR_ROOT}/aufbereitung/sql/d_aurd_rechstan.sql`
- Shell utilities:
  - `getopts`
  - `set`
  - `cat`
  - `eval`
  - `print`
- Likely database execution wrapper:
  - `starteSQLSkript`
- Commented-out job management routines:
  - `FOSJobDeaktivate`
  - `FOSJobErzeugeEintrag`

5. Logic Flow
- Load environment and helper libraries.
- Parse command-line options.
- Set target table name `RKopfStan`.
- Validate required parameters:
  - Jobkennung
  - Stichtag
  - EintragsNr
- If validation fails:
  - Emit error message
  - Exit with error code
- Validate date format `DDMMYYYY`.
- Load SQL execution helper.
- Define SQL script path.
- Define temp file for record count.
- Initialize restart value to `0` if missing.
- Optionally deactivate active jobs:
  - Present but commented out.
- Derive yesterday and today values from `gestern.ksh`.
- Execute SQL script with parameters.
- Print end-of-processing message.
- Read record count from temp file.
- Optionally create job-table entry:
  - Present but commented out.

6. External File / API / Tool Usage
- Reads:
  - Environment file
  - Utility scripts
  - SQL script
  - Date helper script output
  - Temp file with record count
- Writes:
  - Temp file
  - Console output
- Database interaction:
  - Through `starteSQLSkript`
- No direct API calls.

7. BigQuery Replicability
- Core logic is replicable in BigQuery SQL and stored procedures:
  - Parameter validation via procedure parameters and `ASSERT` / `IF`.
  - Date validation via `SAFE.PARSE_DATE` or regex checks.
  - SQL execution logic via BigQuery scripting and dynamic SQL.
  - Record counting via `SELECT COUNT(*)`.
  - Job bookkeeping via BigQuery tables.
- Shell-specific behavior not directly portable:
  - Sourcing environment files
  - External helper scripts
  - Temp file usage
  - `eval`-based variable assignment
- These can be replaced with:
  - Procedure parameters
  - BigQuery scripting variables
  - Temporary tables
  - Logging tables
  - Optional Cloud Functions / Python for unsupported orchestration

8. Functionality Gaps and Alternatives
- Environment sourcing:
  - Replace with procedure parameters or deployment-time configuration tables.
- `gestern.ksh` date derivation:
  - Replace with BigQuery date functions:
    - `CURRENT_DATE()`
    - `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`
- `DWDate_Datum_Check`:
  - Replace with `SAFE.PARSE_DATE('%d%m%Y', ...)` and validation logic.
- `starteSQLSkript`:
  - Replace with BigQuery stored procedure or dynamic SQL execution.
- Temp file record count:
  - Replace with `DECLARE v_records INT64; SELECT COUNT(*) INTO v_records ...`
- Commented job-table routines:
  - Implement as BigQuery DML against a job control table.
- If `d_aurd_rechstan.sql` contains unsupported procedural logic:
  - Move only that portion to Python or Cloud Workflows, then feed results back into BigQuery tables.

Break Down the Script

1. Data Extraction and Ingestion
- Reads runtime parameters.
- Loads yesterday/today values.
- Executes external SQL script.

2. Transformations and Aggregations
- Date validation.
- Restart value defaulting.
- Record count retrieval from temp file.

3. Error Handling and Logging
- Missing parameter detection.
- Unknown parameter detection.
- Date format validation.
- Error reporting through `DWMSG_MeldeFehler`.
- Console logging.

4. Output or Result Storage
- Temporary record-count file.
- Intended job-table entry.
- SQL script output in database.

Assumptions and Additional Notes
- The actual business transformation resides in `d_aurd_rechstan.sql`, which is not included here.
- The script itself is primarily an orchestration wrapper.
- The commented-out job management calls indicate intended but inactive functionality.
- BigQuery implementation assumes the SQL logic from `d_aurd_rechstan.sql` can be expressed in BigQuery SQL or split into supported subqueries/procedures.
- If the SQL script depends on Oracle-specific syntax, it must be rewritten for BigQuery.

BigQuery SQL Pseudocode

```sql
-- BigQuery Stored Procedure: control wrapper for d_aurd_rechstan logic

CREATE OR REPLACE PROCEDURE `project.dataset.r_aurd_rechstan_control`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'RKopfStan';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_wiederanlaufWert STRING DEFAULT '0';
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err STRING DEFAULT '';
  DECLARE v_errnr INT64 DEFAULT 0;

  -- Required parameter checks
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_errnr = 1;
    SET v_err = 'Jobkennung fehlt';
  END IF;

  IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
    SET v_errnr = 1;
    SET v_err = 'EintragsNr fehlt';
  END IF;

  IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
    SET v_errnr = 1;
    SET v_err = 'Stichtag fehlt';
  END IF;

  IF v_errnr <> 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (job_name, entry_nr, stichtag, error_code, error_message, created_at)
    VALUES
    ('r_aurd_rechstan', p_EintragsNr, p_Stichtag, v_errnr, v_err, CURRENT_TIMESTAMP());

    SELECT FORMAT('FEHLER: 0 E %d %s', v_errnr, v_err) AS message;
    LEAVE;
  END IF;

  -- Date validation equivalent to DDMMYYYY
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    INSERT INTO `project.dataset.job_error_log`
    (job_name, entry_nr, stichtag, error_code, error_message, created_at)
    VALUES
    ('r_aurd_rechstan', p_EintragsNr, p_Stichtag, 193, 'Ungueltiges Datum', CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = 'Ungueltiges Datum im Format DDMMYYYY';
  END IF;

  -- Default restart value
  IF p_wiederanlaufWert IS NULL OR TRIM(p_wiederanlaufWert) = '' THEN
    SET v_wiederanlaufWert = '0';
  ELSE
    SET v_wiederanlaufWert = p_wiederanlaufWert;
  END IF;

  -- Optional: deactivate active jobs
  -- UPDATE `project.dataset.job_table`
  -- SET active_flag = 'N', updated_at = CURRENT_TIMESTAMP()
  -- WHERE table_name = v_TabName
  --   AND active_flag = 'Y';

  -- Execute migrated SQL logic from d_aurd_rechstan.sql
  -- Replace this block with the actual BigQuery translation of the SQL script.
  EXECUTE IMMEDIATE """
    -- Example placeholder for core processing
    INSERT INTO `project.dataset.target_table`
    SELECT *
    FROM `project.dataset.source_table`
    WHERE business_date = @stichtag_date
  """
  USING v_stichtag_date AS stichtag_date;

  -- Record count equivalent to temp file output
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.target_table`
    WHERE business_date = v_stichtag_date
  );

  -- Intended job-table entry creation
  INSERT INTO `project.dataset.job_table`
  (
    table_name,
    status_code,
    active_flag,
    stichtag_from,
    stichtag_to,
    job_type,
    restart_flag,
    record_count,
    description,
    job_kennung,
    eintrags_nr,
    created_at
  )
  VALUES
  (
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

Python Pseudocode (if applicable)

```python
# Only for unsupported orchestration or external SQL translation support.
# Keep logic isolated and minimal.

def parse_ddmmyyyy(date_str):
    from datetime import datetime
    return datetime.strptime(date_str, "%d%m%Y").date()

def default_restart_value(value):
    return "0" if value is None or str(value).strip() == "" else value

def validate_required(job_kennung, eintrags_nr, stichtag):
    if not job_kennung or not str(job_kennung).strip():
        raise ValueError("Jobkennung fehlt")
    if not eintrags_nr or not str(eintrags_nr).strip():
        raise ValueError("EintragsNr fehlt")
    if not stichtag or not str(stichtag).strip():
        raise ValueError("Stichtag fehlt")

def get_yesterday_today():
    from datetime import date, timedelta
    today = date.today()
    yesterday = today - timedelta(days=1)
    return today, yesterday

def run_control(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert):
    validate_required(job_kennung, eintrags_nr, stichtag)
    stichtag_date = parse_ddmmyyyy(stichtag)
    wiederanlauf_wert = default_restart_value(wiederanlauf_wert)
    today, yesterday = get_yesterday_today()

    # Call BigQuery stored procedure or execute translated SQL externally.
    # No additional logic or renaming.
    return {
        "job_kennung": job_kennung,
        "eintrags_nr": eentrags_nr,
        "stichtag_date": stichtag_date,
        "wiederanlauf_wert": wiederanlauf_wert,
        "datum_heute": today,
        "datum_gestern": yesterday,
    }
```

Configuration Files Required for BigQuery Execution
- BigQuery dataset/table definitions:
  - `job_table`
  - `job_error_log`
  - target tables used by the migrated SQL
- Deployment configuration:
  - Project ID
  - Dataset name
  - Region
- Optional parameter/config table:
  - For environment values previously sourced from `.dw_init`
- Optional orchestration config:
  - Cloud Composer / Workflows / Scheduler job definition
- Optional external SQL translation artifact:
  - BigQuery-compatible rewrite of `d_aurd_rechstan.sql`
```

---

## 3. SUPPLEMENTAL CONTEXT (ANALYSIS DETAILS)

This section contains critical systems engineering details that the automated MCP parser could not detect from the source file context alone.

### A. Lineage and Call Chain
*   **Upstream Orchestration:** Schedulers (such as UC4 or a parent framework shell script like `r_aurd_rechstan.ksh`) invoke `k_aurd_rechstan.ksh` with system arguments.
*   **Active Execution Link:** 
    *   `k_aurd_rechstan.ksh` **--[EXECUTES_SQL]-->** `d_aurd_rechstan.sql` (Located in: `${BERT_DIR_ROOT}/aufbereitung/sql/d_aurd_rechstan.sql`).
*   **Downstream Database Target:** Updates the `RKopfStan` table (and other targets defined internally in `d_aurd_rechstan.sql`).
*   **Target State Orchestration:** The legacy system is replaced by a Cloud Composer (Apache Airflow) DAG that schedules and executes a call to the BigQuery Stored Procedure, passing standard runtime parameters.

### B. External System Replacements
*   **Unix KornShell (`getopts` and date checks):** Migrated to **Apache Airflow DAG parameters** (`dag_run.conf` or tasks arguments) and integrated validation checks within the **BigQuery Stored Procedure**.
*   **SQL*Plus Wrapper Utility (`starteSQLSkript`):** Migrated to the `BigQueryInsertJobOperator` executing a call to the control Stored Procedure.
*   **Local Temporary Text Files:** Capturing record counts to a temporary text file (`bert_k_aurd_rechstan_$$`.tmp) is eliminated. This is replaced by a BigQuery scripting variable `DECLARE v_records INT64` and returned via output table DML directly.
*   **Legacy Logging (`DWMSG_MeldeFehler`):** Handled via standard DML logging into a BigQuery `job_error_log` table.

### C. Cross-File Dependencies
The legacy shell script sources several utility files that must be decommissioned and standard library equivalents mapped:
*   `$HOME/.dw_init` $\rightarrow$ Replaced by Composer Airflow variables / GCP environment-level settings.
*   `f_alis_msgerr.ksh` $\rightarrow$ Sourced for logging. Replaced by table-based DML error writing or native Python logging in Airflow.
*   `h_alis_date.ksh` $\rightarrow$ Sourced for date format validation. Replaced by native BigQuery syntax `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)`.
*   `h_alis_parameter.ksh` $\rightarrow$ Sourced for validation. Replaced by `IF` validation checks in the Stored Procedure block.
*   `h_alis_sqlplus.ksh` $\rightarrow$ Sourced for database connectivity wrapper logic. Replaced by BigQuery's native execution engine.
*   `gestern.ksh` $\rightarrow$ Used to calculate yesterday's and today's dates. Replaced by BigQuery expressions `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` and `CURRENT_DATE()`.

### D. Target File Plan
The migration strategy involves converting the controller shell script into two deployment-ready files.

| Source File Path | Target File Path | Language | Purpose |
| :--- | :--- | :--- | :--- |
| `vobs/dw_source/isrpt/.../k_aurd_rechstan.ksh` | `dags/k_aurd_rechstan_dag.py` | Python (Apache Airflow) | Orchestrates execution, sets DAG parameters, handles retries, and invokes the BQ stored procedure. |
| `vobs/dw_source/isrpt/.../k_aurd_rechstan.ksh` | `gcp/bigquery/stored_procedures/r_aurd_rechstan_control.sql` | GoogleSQL (BigQuery) | Encapsulates parameter checks, date checks, executes the migrated business logic (originally in `d_aurd_rechstan.sql`), and updates job state metadata. |

---

## 4. ENVIRONMENT-SPECIFIC VALUES & CONFIGURATIONS

The Build Agent must use the following configuration mappings during deployment:
*   **Project ID:** Replace `project` in standard templates with the target deployment Google Cloud project ID (e.g., `gcp-isbert-prod`).
*   **Dataset:** Replace `dataset` with the corresponding environment dataset schema (e.g., `isbert_aufbereitung`).
*   **Airflow Connection:** Use connection ID `google_cloud_default` or a custom connection ID defined in the Composer environment.
*   **Database Job Tables:** 
    *   Metadata tracking table: `isbert_aufbereitung.job_table`
    *   Job error logging table: `isbert_aufbereitung.job_error_log`

---

## 5. RISKS & MANUAL STEPS

1.  **Core SQL Translation Gap (B4 Redesign Item):**
    *   The database transformations are contained within the separate file `d_aurd_rechstan.sql` (invoked via `starteSQLSkript` in the source). This SQL file **must be migrated to GoogleSQL** and either embedded directly in the `EXECUTE IMMEDIATE` block of the stored procedure or executed as a separate task in the Airflow DAG.
2.  **Bind Variable Replacements:**
    *   Any positional SQL*Plus substitution variables (e.g., `&1`, `&2`) inside `d_aurd_rechstan.sql` must be mapped to explicit BigQuery execution variables (e.g., `@stichtag_date`, `@datum_gestern`) when transitioning.
3.  **Job Control Migration:**
    *   The legacy script contains commented-out job control actions (`FOSJobDeaktivate` and `FOSJobErzeugeEintrag`). Verify if these logging mechanics need to be active. If so, create the underlying `job_table` and `job_error_log` schemas before executing.