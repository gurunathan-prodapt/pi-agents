# MIGRATION DESIGN DOCUMENT
**Source File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`  
**Target Platform:** Google Cloud BigQuery  
**Conversion Type:** Shell Helper Script to Google Cloud Native Migration Design  

---

## 1. EXECUTIVE SUMMARY & TARGET PLATFORM OVERVIEW
The source component `h_alis_sqlplus.ksh` is a helper utility script written in KornShell (`KSH`). Its primary purpose is to define a reusable function, `starteSQLSkript`, which safely executes Oracle SQL*Plus scripts. It performs input parameter validation, verifies that the target SQL script file is readable on the filesystem, prints execution details for logs, disables shell-level exit-on-error (`set +e`), invokes SQL*Plus using the `${DW_ORAUSER}` connection, captures the execution exit status, restores shell-level exit-on-error (`set -e`), and propagates the return code to the caller.

In the target **Google Cloud BigQuery** environment, direct shell script file executions and Oracle SQL*Plus calls are obsolete. Depending on the enterprise orchestrator chosen for the wider migration (e.g., Google Cloud Composer / Apache Airflow), this utility must be replaced by a modern, cloud-native equivalent. 

This design document outlines two distinct target patterns:
1. **Option A (Airflow / Python Orchestration Helper):** A Python module `h_alis_sqlplus.py` designed to be imported by Apache Airflow DAGs or Python-based runner tasks. This approach preserves the external orchestrator-centric model of execution, replacing filesystem checks with Google Cloud Storage (`GCS`) checks and SQL*Plus execution with `BigQueryInsertJobOperator` or Python BigQuery Client calls.
2. **Option B (BigQuery Native Stored Procedure & Scripting Registry):** A native BigQuery Stored Procedure that mimics the parameters, verification, logging, and error propagation using a script registry table and dynamic SQL (`EXECUTE IMMEDIATE`).

---

## 2. VERBATIM MCP DESIGN GENERATION (TRANSFORMATION LOGIC)

The following content is the verbatim output of the CodeMaverick Migration design tool, outlining the core functional analysis, transformations, and pseudocode templates:

```markdown
=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh ===
Document: Shell Script Analysis

1. Purpose
- The script defines a reusable shell function `starteSQLSkript` for launching an Oracle SQL*Plus script.
- It validates required inputs, checks that the target SQL script file is readable, logs the invocation details, and then executes `sqlplus` with the provided Oracle user and script parameters.
- It returns the SQL*Plus exit code to the caller.

2. Input Parameters
- `p_Eintragsnr`:
  - Error entry number used for error reporting.
- `p_Skript`:
  - Path/name of the SQL*Plus script to execute.
- Additional positional parameters:
  - Passed through to the SQL*Plus script as script arguments.

3. Outputs
- Console/log messages:
  - Announces SQL*Plus invocation.
  - Prints script name and parameters.
- Error reporting:
  - Calls `DWMSG_MeldeFehler` for missing arguments or unreadable script file.
- Return code:
  - Returns `196` if required arguments are missing.
  - Returns `201` if the script file is not readable.
  - Otherwise returns the exit code from `sqlplus`.

4. Dependencies
- Shell environment variables:
  - `DW_ORAUSER`
  - `Modul_Name`
  - `Modul_Version`
- External function:
  - `DWMSG_MeldeFehler`
- External command:
  - `sqlplus`
- File system dependency:
  - Readability of `$p_Skript`
- Shell behavior:
  - `set +e` / `set -e` to temporarily disable/restore immediate exit on error.

5. Logic Flow
- Capture first two arguments.
- Shift away the first two arguments so remaining parameters can be forwarded.
- Validate required inputs:
  - If entry number or script name is missing, report error and return `196`.
- Validate file readability:
  - If script file is not readable, report error and return `201`.
- Log invocation details.
- Temporarily disable shell exit-on-error.
- Execute SQL*Plus:
  - `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
- Capture SQL*Plus exit code.
- Restore shell exit-on-error.
- Return captured exit code.

6. External File Dependencies, APIs, or Tools
- SQL script file referenced by `p_Skript`.
- SQL*Plus executable.
- Error logging routine `DWMSG_MeldeFehler`.

7. BigQuery Replicability Assessment
- Core control-flow logic is replicable in BigQuery scripting:
  - Parameter validation
  - Conditional branching
  - Logging via `SELECT`
  - Return-code handling via `ASSERT`, `RAISE`, or status tables
- Direct SQL*Plus execution is not replicable in BigQuery SQL because BigQuery cannot invoke SQL*Plus or read local filesystem paths.
- File readability checks are not directly available in BigQuery SQL.
- Passing arbitrary script parameters is replicable via stored procedure parameters or arrays.
- The actual execution of an external SQL script must be replaced by:
  - Stored procedure calls
  - Dynamic SQL (`EXECUTE IMMEDIATE`)
  - Orchestration outside BigQuery if the script is external.

8. Functionality Gaps and Alternatives
- Gap: Local file existence/readability check
  - Alternative: Store scripts in BigQuery tables, Cloud Storage, or deploy as stored procedures.
- Gap: SQL*Plus invocation
  - Alternative: Convert SQL*Plus script logic into BigQuery stored procedures or dynamic SQL.
- Gap: Shell-level error handling (`set -e`)
  - Alternative: Use BigQuery scripting `BEGIN...EXCEPTION...END` blocks and explicit status handling.
- Gap: External logging function `DWMSG_MeldeFehler`
  - Alternative: Implement logging into a BigQuery audit table or use `SELECT` statements for script output.
- Gap: Arbitrary positional parameter forwarding
  - Alternative: Use procedure parameters, arrays, or JSON payloads.

Break Down the Script

1. Data Extraction and Ingestion
- No data extraction from databases occurs in this shell function itself.
- The function ingests:
  - Entry number
  - SQL script path
  - Additional script parameters

2. Transformations and Aggregations
- No data transformation or aggregation logic.
- Only argument validation and command assembly.

3. Error Handling and Logging
- Missing parameter check:
  - Emits error `196`.
- Unreadable script file check:
  - Emits error `201`.
- Logs SQL*Plus invocation details to stdout.
- Captures and returns SQL*Plus exit status.

4. Output or Result Storage
- Output is the SQL*Plus execution result on stdout/stderr.
- No persistent storage is written by this function.
- Return code is the primary status output.

Assumptions and Additional Notes
- `DWMSG_MeldeFehler` is assumed to log and/or persist errors externally.
- `Modul_Name` and `Modul_Version` are assumed to be defined in the calling environment.
- The SQL*Plus script is assumed to be logically equivalent to a BigQuery stored procedure or dynamic SQL block.
- Since BigQuery cannot execute shell commands or access local files, the script must be restructured around BigQuery-native execution units.
- If the original SQL*Plus script contains Oracle-specific SQL, additional migration work is required before BigQuery execution.

Configuration Files Required for BigQuery Execution
- BigQuery stored procedure definition file or deployment script
- Optional parameter/configuration table for:
  - module name
  - module version
  - error codes
  - script registry
- Optional audit/logging table
- Optional Cloud Storage manifest if external SQL assets are referenced
- Optional orchestration config:
  - Cloud Composer / Workflows / Dataform / dbt job configuration

BQ SQL Pseudocode

```sql
-- BigQuery Script / Stored Procedure equivalent for starteSQLSkript

CREATE OR REPLACE PROCEDURE `project.dataset.starteSQLSkript`(
  p_Eintragsnr INT64,
  p_Skript STRING,
  p_Parameter ARRAY<STRING>
)
BEGIN
  DECLARE errcode INT64 DEFAULT 0;
  DECLARE v_script_exists BOOL DEFAULT FALSE;
  DECLARE v_sql STRING DEFAULT '';
  DECLARE v_params STRING DEFAULT '';
  DECLARE v_msg STRING DEFAULT '';

  -- Validate required parameters
  IF p_Eintragsnr IS NULL OR p_Skript IS NULL OR p_Skript = '' THEN
    SET v_msg = CONCAT('Fehler ', CAST(p_Eintragsnr AS STRING), ': ',
                       'alis_sqlplus V1.1.3 starteSQLSkript');
    SELECT v_msg AS error_message;
    -- Equivalent to return 196
    SET errcode = 196;
    SELECT errcode AS return_code;
    RETURN;
  END IF;

  -- Validate script availability
  -- BigQuery cannot check local file readability; replace with registry/table lookup
  SET v_script_exists = EXISTS (
    SELECT 1
    FROM `project.dataset.sql_script_registry`
    WHERE script_name = p_Skript
      AND is_active = TRUE
  );

  IF NOT v_script_exists THEN
    SELECT CONCAT('Fehler ', CAST(p_Eintragsnr AS STRING), ': ', p_Skript) AS error_message;
    SET errcode = 201;
    SELECT errcode AS return_code;
    RETURN;
  END IF;

  -- Log invocation details
  SET v_params = (
    SELECT STRING_AGG(param, ' ')
    FROM UNNEST(p_Parameter) AS param
  );

  SELECT 'Rufe SQL*PLUS auf mit folgenden Einstellungen' AS info_message;
  SELECT CONCAT('Sql*Plus-Skript : ', p_Skript) AS info_message;
  SELECT CONCAT('Skript-Parameter: ', IFNULL(v_params, '')) AS info_message;

  -- Replace SQL*Plus execution with BigQuery-native execution
  -- Option A: dispatch to stored procedure registry
  -- Option B: execute dynamic SQL stored in a table
  SET v_sql = (
    SELECT script_sql
    FROM `project.dataset.sql_script_registry`
    WHERE script_name = p_Skript
      AND is_active = TRUE
    LIMIT 1
  );

  BEGIN
    EXECUTE IMMEDIATE v_sql;
    SET errcode = 0;
  EXCEPTION WHEN ERROR THEN
    SET errcode = 1;
    SELECT CONCAT('Execution failed for script: ', p_Skript) AS error_message;
  END;

  SELECT errcode AS return_code;
END;
```

BQ SQL Pseudocode - Alternative with Script Registry and Parameter Substitution

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.starteSQLSkript`(
  p_Eintragsnr INT64,
  p_Skript STRING,
  p_Parameter ARRAY<STRING>
)
BEGIN
  DECLARE errcode INT64 DEFAULT 0;
  DECLARE v_sql STRING;
  DECLARE v_script_exists BOOL DEFAULT FALSE;
  DECLARE v_param1 STRING DEFAULT NULL;
  DECLARE v_param2 STRING DEFAULT NULL;
  DECLARE v_param3 STRING DEFAULT NULL;

  IF p_Eintragsnr IS NULL OR p_Skript IS NULL OR p_Skript = '' THEN
    SELECT 196 AS return_code;
    RETURN;
  END IF;

  SET v_script_exists = EXISTS (
    SELECT 1
    FROM `project.dataset.sql_script_registry`
    WHERE script_name = p_Skript
  );

  IF NOT v_script_exists THEN
    SELECT 201 AS return_code;
    RETURN;
  END IF;

  SET v_param1 = IF(ARRAY_LENGTH(p_Parameter) >= 1, p_Parameter[OFFSET(0)], NULL);
  SET v_param2 = IF(ARRAY_LENGTH(p_Parameter) >= 2, p_Parameter[OFFSET(1)], NULL);
  SET v_param3 = IF(ARRAY_LENGTH(p_Parameter) >= 3, p_Parameter[OFFSET(2)], NULL);

  SET v_sql = (
    SELECT REPLACE(
             REPLACE(
               REPLACE(script_sql, '${1}', IFNULL(v_param1, '')),
               '${2}', IFNULL(v_param2, '')
             ),
             '${3}', IFNULL(v_param3, '')
           )
    FROM `project.dataset.sql_script_registry`
    WHERE script_name = p_Skript
    LIMIT 1
  );

  BEGIN
    EXECUTE IMMEDIATE v_sql;
    SET errcode = 0;
  EXCEPTION WHEN ERROR THEN
    SET errcode = 1;
  END;

  SELECT errcode AS return_code;
END;
```

Python Pseudocode (if applicable)

```python
# Only required if external orchestration is needed for unsupported shell behavior.
# This Python layer replaces:
# - local file readability checks
# - SQL script lookup from filesystem
# - dispatch to BigQuery stored procedures / dynamic SQL execution

def starteSQLSkript(p_Eintragsnr, p_Skript, p_Parameter):
    if p_Eintragsnr is None or not p_Skript:
        log_error(p_Eintragsnr, 196, "alis_sqlplus V1.1.3 starteSQLSkript")
        return 196

    if not script_registry_contains(p_Skript):
        log_error(p_Eintragsnr, 201, p_Skript)
        return 201

    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_Skript}")
    print(f"Skript-Parameter: {' '.join(p_Parameter)}")

    try:
        execute_bigquery_script(p_Skript, p_Parameter)
        return 0
    except Exception:
        return 1
```
```

---

## 3. MIGRATION CONTEXT & ENVIRONMENT ARCHITECTURE

### 3.1 Lineage and Dependencies
- **Upstream Pipelines / Job Triggers:** Since this is a core utility shell script (`h_alis_sqlplus.ksh`), it is called by dozens of parent shell scripts (historically running as UC4 tasks or crontabs) to invoke SQL calculations.
- **Downstream Consumers:** The scripts loaded via `$p_Skript` (historically `.sql` scripts executing via SQL*Plus containing Oracle dialect calculations, DDLs, and DMLs).
- **Execution Order:** 
  1. Parent script references `h_alis_sqlplus.ksh`.
  2. Parent script calls `starteSQLSkript` passing an error entry ID, a path to a `.sql` script, and dynamic command-line arguments.
  3. The utility validates the existence of the script locally, and executes it.

### 3.2 External System Replacements
* **Oracle SQL*Plus (`sqlplus`) $\rightarrow$ Google BigQuery Engine:** All Oracle-specific syntax inside downstream `.sql` files must be compiled into BigQuery standard SQL (BQSQL).
* **Local Filesystem Disk Storage $\rightarrow$ Google Cloud Storage (GCS) or BigQuery Catalog:** 
  - Instead of deploying raw physical `.sql` files to an application server disk, scripts will be stored as BigQuery Stored Procedures inside a dedicated dataset (e.g., `dw_utility_ds`) or placed in a secure GCS bucket (`gs://<environment>-sql-scripts/`).
* **Error Logging (`DWMSG_MeldeFehler`) $\rightarrow$ BigQuery Logging Table / Cloud Logging:**
  - The custom shell function `DWMSG_MeldeFehler` will be replaced either by:
    - Writing logs to a BigQuery audit log table (`project.dw_audit.error_logs`).
    - Standard Python/Airflow logger streams pointing to Google Cloud Operations Suite (formerly Stackdriver).

### 3.3 Cross-File Dependencies & Call Chains
* **Shared Environment Variables:**
  - `${DW_ORAUSER}` represents the database connection schema. In BigQuery, this maps directly to GCP project-level and dataset-level credentials/service accounts configured via IAM.
  - `ModulName` and `ModulVersion` must be captured in the execution metadata.

---

## 4. TARGET FILE PLAN & CONFIGURATION

Depending on the migration path chosen by the engineering team (Airflow orchestration vs. BigQuery native scheduling), the build agent must generate one or both of the following target files:

### Target File 1: Python Orchestration Utility
- **Target Path:** `dags/utils/h_alis_sqlplus.py`
- **Language:** Python 3.x (with `google-cloud-bigquery` client libraries)
- **Source Component:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`
- **Purpose:** Replicates the validation, log output, error checking, and SQL execution within a modern Python-based orchestrator (e.g., Cloud Composer).

### Target File 2: BigQuery Native Helper Stored Procedure
- **Target Path:** `ddl/procedures/starteSQLSkript.sql`
- **Language:** GoogleSQL (BigQuery Stored Procedure)
- **Source Component:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`
- **Purpose:** Provides database-internal procedural simulation of script-based validation and execution tracking, utilizing a script metadata registry table.

---

## 5. ENVIRONMENT SPECIFIC CONFIGURATIONS
For successful deployment across different environments (Development, Test, Production), the build system must substitute the following variables:

| Legacy Environment Element | GCP Target Config Variable | Dev/UAT Mapping | Production Mapping |
|:---|:---|:---|:---|
| `DW_ORAUSER` | `GCP_PROJECT_ID` | `gcp-is-dw-dev` | `gcp-is-dw-prod` |
| Filesystem Directory Paths | `GCS_SQL_BUCKET` | `gs://gcp-is-dw-dev-sql-scripts` | `gs://gcp-is-dw-prod-sql-scripts` |
| Standard Dataset Directory | `BQ_UTILITY_DATASET` | `dw_utility_dev` | `dw_utility_prod` |
| DB Error Logging | `BQ_LOGGING_DATASET` | `dw_audit_dev` | `dw_audit_prod` |

---

## 6. SYSTEM RISK ASSESSMENT & MITIGATION STRATEGY

### Risk 1: Positional Argument Forwarding (`$*` / `$p_Parameter`)
* **Problem:** SQL*Plus scripts accept positional syntax parameters (e.g. `&1`, `&2`) replaced dynamically at execution time. BigQuery stored procedures and queries require named parameters or specific query parameters.
* **Mitigation:**
  - **Option A (Python approach):** The Python helper reads the template SQL file from GCS, performs safe regex template substitution (e.g., swapping `{1}` with positional argument 1), and sends the clean, compile-ready standard SQL string to BigQuery.
  - **Option B (BigQuery approach):** Create standard procedure calls where each migrated SQL script gets its own strongly typed stored procedure with defined parameters, instead of attempting to run dynamic text templates directly.

### Risk 2: Hard-Coded Validation Error Codes (`196`, `201`)
* **Problem:** Traditional orchestrators relied on custom exit numbers to alter job stream flows.
* **Mitigation:** In Python / Apache Airflow, raise specific Python exceptions (`ValueError`, `FileNotFoundError`) that bubble up cleanly to the Airflow execution task and trigger appropriate alert callbacks. For the Stored Procedure option, use SQL `DECLARE EXCEPTION` or conditional `RAISE` statements.

### Risk 3: Script Registry Table Setup
* **Problem:** If Option B (stored procedure using dynamic SQL) is utilized, it relies on a `sql_script_registry` table.
* **Mitigation:** The build steps must include the deployment of this metadata table. The structural schema is detailed below:

```sql
CREATE OR REPLACE TABLE `project.dataset.sql_script_registry` (
  script_name STRING OPTIONS(description="The logical name of the script equivalent"),
  script_sql STRING OPTIONS(description="The complete BQSQL code or stored procedure invoke command"),
  is_active BOOL OPTIONS(description="Active execution flag"),
  last_modified TIMESTAMP
);
```