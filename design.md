# MIGRATION DESIGN DOCUMENT: k_ausd_bp_ta_bcp_iccid.ksh Migration to BigQuery

## 1. Executive Summary & Migration Context
The shell script `k_ausd_bp_ta_bcp_iccid.ksh` is a control/orchestration wrapper written in KornShell (Ksh) that validates input parameters, manages operational dates, and executes a corresponding SQL*Plus script (`d_ausd_bp_ta_bcp_iccid.sql`). Historically executed in an Oracle environment on-premises, this job is being migrated to Google Cloud Platform (GCP).

The primary migration path is to translate the shell orchestration, validation logic, and execution steps into a **BigQuery Stored Procedure** (using BigQuery scripting) or an equivalent orchestration task (such as Google Cloud Composer/Airflow DAG). The core data transformations defined in the downstream SQL will reside directly in BigQuery DML.

---

## 2. LINEAGE, CROSS-FILE DEPENDENCIES & ENVIRONMENT SPECIFICS

### 2.1 Technical Lineage & Call Chain
- **Upstream Trigger**: Typically called by a scheduling tool (such as UC4/Automic) passing execution parameters:
  - `-j` (Jobkennung / Job ID)
  - `-f` (EintragsNr / Log ID)
  - `-s` (Stichtag / Business Date in DDMMYYYY format)
  - `-l` (WiederanlaufWert / Restart Value)
- **Downstream Call**: Executes SQL file `d_ausd_bp_ta_bcp_iccid.sql` via `h_alis_sqlplus.ksh`.
- **Target Entities**:
  - Main operational logic targets: `PoolBasisprodukt` table (represented in legacy as `v_TabName`).
  - Commented file-based exports write to/read from:
    - `${BERT_DIR_ROOT}/aufbereitung/sql/cibasis_data24.dat`
    - `${BERT_DIR_ROOT}/aufbereitung/sql/cibasis_data96.dat`
    - `${BERT_DIR_ROOT}/aufbereitung/sql/cibasis_fax.dat`
    - Combined into output file: `cibasisprodukt.csv`.
  - Operational Logging: Writes logging metrics to a temporary state file: `$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_iccid.tmp`.

### 2.2 Sourced Script Dependencies (Legacy)
The legacy shell script sources several utility frameworks that are retired during migration:
- `. $HOME/.dw_init` (Replaced by Cloud Composer / BigQuery environment-specific global connection/project variables)
- `f_alis_msgerr.ksh` (Replaced by native BigQuery `RAISE USING MESSAGE` and logging to a central Cloud Logging/Audit structure)
- `h_alis_date.ksh` (Replaced by native BigQuery `SAFE.PARSE_DATE` and date functions)
- `h_alis_parameter.ksh` (Replaced by BigQuery Stored Procedure input variable assertions)
- `h_alis_sqlplus.ksh` (Replaced by native execution of migrated dynamic/static SQL commands)
- `gestern.ksh` (Replaced by `CURRENT_DATE()` and `DATE_SUB(..., INTERVAL 1 DAY)`)

### 2.3 External System Replacements & Target Platform Setup
- **On-Premise Filesystem / Local Staging**: The legacy script references path variables like `${BERT_DIR_ROOT}` and temporary folders like `$DW_DIR_UTL`. All target operations must execute in-memory within BigQuery or staging areas in Google Cloud Storage (GCS).
- **Control Tables**: Legacy commands like `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` (currently commented out but present) will map to a metadata tracking table inside BigQuery (e.g. `isbert_metadata.job_tracking`).

---

## 3. VERBATIM SOURCE ANALYSIS & DESIGN GENERATION (MCP OUTPUT)

Below is the complete, unmodified analysis and design generation provided by the CodeMaverick Migration Processor:

=== START OF VERBATIM MCP RESULT ===

### Document: Shell Script Analysis

#### Purpose
- Control script for a downstream data preparation job related to `k_ausd_bp_ta_bcp_iccid.ksh`.
- Performs environment initialization, parameter parsing/validation, date validation, SQL script execution, and post-processing of record counts.
- Intended to orchestrate a database-driven extraction/load workflow and optionally job tracking.

#### Inputs
- Command-line parameters:
  - `-j` Jobkennung
  - `-f` EintragsNr
  - `-s` Stichtag
  - `-l` wiederanlaufWert
  - `-h` help
- Sourced environment/config:
  - `$HOME/.dw_init`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
  - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
  - SQL file: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_bcp_iccid.sql`

#### Outputs
- Console messages for progress and errors.
- Temporary file containing record count:
  - `$DW_DIR_UTL/bert_k_ausd_bp_ta_bcp_iccid.tmp`
- SQL execution side effects from the referenced SQL script.
- Potential job-table entry creation is present but commented out.

#### Dependencies
- Shell environment variables:
  - `HOME`
  - `BERT_DIR_ROOT`
  - `DW_DIR_UTL`
- External shell utilities/functions:
  - `pruefeParameterGesetzt`
  - `DWMSG_MeldeFehler`
  - `DWDate_Datum_Check`
  - `starteSQLSkript`
- External date helper:
  - `gestern.ksh` returning today/yesterday values
- SQL script file:
  - `d_ausd_bp_ta_bcp_iccid.sql`

#### Logic Flow
1. Source environment and helper scripts.
2. Parse command-line options with `getopts`.
3. Validate required parameters:
   - Jobkennung
   - Stichtag
   - EintragsNr
4. If validation fails:
   - Emit error via message framework
   - Print error text
   - Exit with error code
5. Validate date format of `p_Stichtag` as `DDMMYYYY`.
6. Source SQL execution helper.
7. Set SQL script path and temp file path.
8. Default `p_wiederanlaufWert` to `0` if empty.
9. Obtain today/yesterday values from helper script.
10. Execute SQL script via `starteSQLSkript`, passing job and date parameters plus temp file path.
11. Print end-of-processing message.
12. Read record count from temp file.
13. Job-table entry creation is commented out.

#### External File Dependencies / APIs / Tools
- Shell sourcing of environment and utility scripts.
- SQL execution wrapper around Oracle-style SQL*Plus behavior.
- Temporary file read/write.
- No direct API calls.
- Commented-out file post-processing commands:
  - `sed`
  - `sort`
  - `join`

#### Replicability in BigQuery SQL / Stored Procedures
- Parameter parsing maps cleanly to stored procedure parameters.
- Required parameter validation maps to procedural `IF` checks.
- Date validation can be implemented with `SAFE.PARSE_DATE` or regex checks.
- SQL script execution can be migrated into BigQuery scripting using `EXECUTE IMMEDIATE`, `INSERT`, `CREATE TABLE AS SELECT`, or stored procedures.
- Record count can be captured using `@@row_count` where applicable, or via `COUNT(*)` into a variable.
- Job tracking can be implemented as inserts into a metadata table.
- File-based temp output should be replaced by BigQuery tables or variables.

#### Functionality Gaps / Alternatives
- Shell-specific sourcing and helper functions are not directly portable:
  - Replace with BigQuery stored procedures and script variables.
- `starteSQLSkript` wrapper behavior is unknown:
  - Replace with explicit BigQuery SQL statements or dynamic SQL.
- `gestern.ksh` date derivation:
  - Replace with `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` and `CURRENT_DATE()`.
- Commented file operations (`sed`, `sort`, `join`) should be migrated to SQL joins, deduplication, and string functions.
- Any job-control framework integration should be handled by a separate orchestration layer or metadata tables.

#### Breakdown of Script

##### 1. Data Extraction and Ingestion
- Loads environment and helper scripts.
- Determines execution dates.
- Runs the SQL script that likely performs the actual extraction/load.
- Reads resulting record count from temp file.

##### 2. Transformations and Aggregations
- No active transformations in the shell script itself.
- Commented-out post-processing indicates:
  - whitespace removal
  - deduplication
  - joins across output files
- These are candidates for SQL-based transformations.

##### 3. Error Handling and Logging
- Parameter validation with error codes:
  - `193` missing argument
  - `192` unknown parameter
- Date validation with explicit progress messages.
- Error reporting via `DWMSG_MeldeFehler`.
- Exit on validation failure.

##### 4. Output or Result Storage
- Temporary record-count file.
- Potential job table entry creation, currently commented out.
- SQL script likely writes to database tables or exports.

#### Assumptions and Additional Notes
- The referenced SQL file contains the core business logic and is not included here; only orchestration logic is visible.
- The BigQuery migration assumes the SQL script logic can be rewritten as BigQuery DML/DDL.
- Any file-based export workflow should be replaced by BigQuery tables, views, or Cloud Storage exports if needed.
- If the SQL script depends on Oracle-specific syntax, it must be translated separately.
- The commented-out file processing section is treated as inactive but documented for completeness.

#### BigQuery SQL Pseudocode

```sql
-- BigQuery Script / Stored Procedure Pseudocode

CREATE OR REPLACE PROCEDURE dataset.k_ausd_bp_ta_bcp_iccid(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err STRING DEFAULT '';
  DECLARE v_errnr INT64 DEFAULT 0;

  -- Required parameter checks
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_errnr = 1;
    SET v_err = 'Jobkennung fehlt';
  END IF;

  IF v_errnr = 0 AND (p_Stichtag IS NULL OR p_Stichtag = '') THEN
    SET v_errnr = 1;
    SET v_err = 'Stichtag fehlt';
  END IF;

  IF v_errnr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET v_errnr = 1;
    SET v_err = 'EintragsNr fehlt';
  END IF;

  IF v_errnr <> 0 THEN
    INSERT INTO dataset.job_error_log(job_name, error_code, error_message, created_at)
    VALUES (v_TabName, v_errnr, v_err, CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = CONCAT('FEHLER: ', CAST(v_errnr AS STRING), ' ', v_err);
  END IF;

  -- Date validation for DDMMYYYY
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    INSERT INTO dataset.job_error_log(job_name, error_code, error_message, created_at)
    VALUES (v_TabName, 193, CONCAT('Ungueltiges Datum: ', p_Stichtag), CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = CONCAT('FEHLER: 193 Ungueltiges Datum: ', p_Stichtag);
  END IF;

  -- Default restart value
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET p_wiederanlaufWert = '0';
  END IF;

  -- Core SQL logic migrated from external SQL file
  -- Equivalent of: starteSQLSkript ...
  -- Replace with actual BigQuery DML/DDL from d_ausd_bp_ta_bcp_iccid.sql
  EXECUTE IMMEDIATE """
    -- Example placeholder for migrated logic
    -- CREATE OR REPLACE TABLE dataset.target_table AS
    -- SELECT ...
    -- FROM dataset.source_table
    -- WHERE business_date = @stichtag_date
  """ USING v_stichtag_date AS stichtag_date;

  -- Capture record count
  SET v_records = @@row_count;

  -- Optional job tracking
  INSERT INTO dataset.job_tracking(
    job_name,
    status,
    entry_type,
    business_date,
    run_date,
    run_flag,
    restart_flag,
    record_count,
    description,
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
    CURRENT_TIMESTAMP()
  );
END;
```

#### Python Pseudocode (if applicable)

```python
# Only for unsupported orchestration or external file-based behavior
# Keep isolated; core data logic should remain in BigQuery SQL.

def validate_ddmmyyyy(date_str: str) -> bool:
    import re
    from datetime import datetime
    if not re.match(r"^\d{8}$", date_str or ""):
        return False
    try:
        datetime.strptime(date_str, "%d%m%Y")
        return True
    except ValueError:
        return False

def derive_today_yesterday():
    from datetime import date, timedelta
    today = date.today()
    yesterday = today - timedelta(days=1)
    return today, yesterday
```

#### Configuration Files Required for BigQuery Execution
- BigQuery stored procedure SQL file
- Dataset/table DDL definitions for:
  - target business tables
  - job tracking table
  - error log table
- Optional orchestration config:
  - Cloud Composer DAG, Dataform workflow, or scheduled query config
- Optional parameter/config file:
  - environment-specific dataset names
  - project IDs
  - job metadata mappings

=== END OF VERBATIM MCP RESULT ===

---

## 4. TARGET FILE PLAN

The migration team must generate the following components as part of the operational target deployment.

| Target Relative Path | Target Language | Description | Sourced From / Maps to |
| :--- | :--- | :--- | :--- |
| `gcp_migration/bigquery/procedures/k_ausd_bp_ta_bcp_iccid.sql` | GoogleSQL (DDL/DML) | BigQuery Stored Procedure containing validation logic, metadata execution logging, and orchestration structure. | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh` |
| `gcp_migration/bigquery/ddl/job_tracking_tables.sql` | GoogleSQL (DDL) | Creation statements for `job_tracking` and `job_error_log` tables to replace legacy on-premises FOS tables and log files. | Implicit logging and FOS framework parameters |
| `gcp_migration/airflow/dags/dag_k_ausd_bp_ta_bcp_iccid.py` | Python (Airflow DAG) | Cloud Composer Orchestration script that triggers parameter parsing, establishes operational date params, and executes the BQ SP. | Replacement for Cron/UC4 triggering of the KornShell wrapper |

---

## 5. ENVIRONMENT-SPECIFIC VALUES & RUNTIME MAPPINGS

During implementation, parameters from config files and the orchestration layer must resolve to the following Google Cloud resource names:

- **Target Project**: `gcp-project-id` (e.g. `prj-dw-isbert-prod`)
- **Target Metadata Dataset**: `isbert_metadata` (Replacements for `dataset.job_tracking` and `dataset.job_error_log` used in pseudocode)
- **Target Core Schema Dataset**: `isbert_schema` (Target location of transformed tables like `PoolBasisprodukt`)
- **Execution Date Parameterization**:
  - `p_Stichtag` must map to `{{ ds_nodash }}` in Airflow (re-formatting `YYYYMMDD` string parameter internally to the required `DDMMYYYY` layout, or transitioning the stored procedure signature directly to BigQuery standard `DATE` format).

---

## 6. RISKS, ASSUMPTIONS, AND MANUAL REDESIGN STEPS

1. **Undocumented SQL Logic (`d_ausd_bp_ta_bcp_iccid.sql`)**: 
   - *Risk*: This script only orchestrates the SQL. The target dataset and transformations executed within `d_ausd_bp_ta_bcp_iccid.sql` represent the true workload.
   - *Mitigation*: The `d_ausd_bp_ta_bcp_iccid.sql` script must be compiled, translated to BigQuery SQL, and its content embedded inside the `EXECUTE IMMEDIATE` block or called as a separate modular transaction.
2. **Commented Post-Processing Steps (`sed`/`sort`/`join`)**:
   - *Analysis*: The legacy developer disabled file parsing steps (whitespace deletion, sorting, and merging CSVs). If downstream modules still expect `cibasisprodukt.csv` output on Cloud Storage, these files should be exported directly from BigQuery target tables using standard BigQuery-to-GCS export statements (`EXPORT DATA OPTIONS...`), entirely bypassing UNIX command line utilities.
3. **Date Formats**:
   - Standardizing input parameter strings to BigQuery native `DATE` objects as early as possible in the orchestration DAG is recommended over performing regex or substring evaluations on `DDMMYYYY` formatted strings.