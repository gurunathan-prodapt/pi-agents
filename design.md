# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh

## 1. Purpose & Scope
This job, `k_ausd_bp_ta_rn_vertrag.ksh`, is a control script (Kontrollscript) designed to orchestrate parameter validation, date validation, SQL execution, and job/result bookkeeping for a process related to `PoolBasisprodukt`. It acts as an integration point, invoking an underlying SQL script (`d_ausd_bp_ta_rn_vertrag.sql`) to perform the core data transformation. The script reads command-line parameters, validates them, and then executes the SQL script, capturing the number of processed records in a temporary file. This particular job was assembled from 1 component and its stage distribution is noted as medium.

## 2. Source Inventory
The job consists of a single KornShell script.
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh`
  - **Technology:** KornShell script
  - **Complexity Tier:** Unknown (no data in `file_complexity` table)
  - **Migration Automation Bucket:** `semi_auto`

## 3. Target Architecture
The target architecture in BigQuery will involve:
- **BigQuery Stored Procedure:** The core logic of the KornShell script, including parameter parsing, validation, date checks, and the execution of the underlying SQL logic, will be migrated into a BigQuery stored procedure, for example, `project.dataset.r_ausd_bp_ta_rn_vertrag`.
- **BigQuery SQL Statements:** The logic currently residing in `d_ausd_bp_ta_rn_vertrag.sql` will be converted into native BigQuery DML/DDL statements or potentially nested BigQuery stored procedures, called from the main orchestration stored procedure.
- **Configuration Tables:** Environment variables and static configurations (`$HOME/.dw_init`, `${BERT_DIR_ROOT}`) will be replaced with BigQuery configuration tables or passed as stored procedure parameters.
- **Audit/Error Logging Tables:** The existing error handling (`DWMSG_MeldeFehler`) and job bookkeeping will be replaced with inserts into dedicated BigQuery audit and error log tables (e.g., `project.dataset.job_error_log`, `project.dataset.job_audit_log`).
- **Temporary Tables/Variables:** The use of temporary files (e.g., `$DW_DIR_UTL/bert_k_ausd_bp_ta_rn.tmp`) for capturing record counts will be replaced by BigQuery scalar variables or temporary tables.

## 4. Data Flow & Lineage
The lineage analysis indicates the following:
- **Invocation:** The script `k_ausd_bp_ta_rn_vertrag.ksh` is invoked by another script, `r_ausd_bp_ta_rn_vertrag.ksh`.
- **Execution:** `k_ausd_bp_ta_rn_vertrag.ksh` `EXECUTES_SQL` `SQL_SCRIPT:D_AUSD_BP_TA_RN_VERTRAG.SQL`. This means the shell script's primary function is to prepare and then trigger an Oracle SQL script.
- **Data Source/Target:** The data flow will largely depend on the `d_ausd_bp_ta_rn_vertrag.sql` script's operations. The KornShell script itself reads parameters, performs some internal date calculations, and captures the record count from the SQL script's execution.

**Migrated Data Flow:**
1. An external orchestrator (e.g., Airflow, Cloud Composer) invokes the BigQuery stored procedure `project.dataset.r_ausd_bp_ta_rn_vertrag` with necessary parameters.
2. The stored procedure performs parameter validation and date checks.
3. The stored procedure then executes the transformed BigQuery SQL logic (from `d_ausd_bp_ta_rn_vertrag.sql`).
4. Record counts or other metrics are captured within BigQuery variables and logged to audit tables.
5. Error conditions are logged to an error table and potentially raise BigQuery errors.

## 5. Transformation Logic
The KornShell script (`k_ausd_bp_ta_rn_vertrag.ksh`) acts as an orchestration layer.
- **Parameter Handling:** It uses `getopts` to parse input parameters (`-j`, `-f`, `-s`, `-l`). These will be directly translated to input parameters of the BigQuery stored procedure.
- **Environment Setup:** It sources several utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) for initialization and helper functions. These will be replaced by BigQuery's built-in functions, configuration tables, or equivalent BigQuery scripting logic.
- **Validation:** It includes `pruefeParameterGesetzt` for parameter validation and `DWDate_Datum_Check` for date format validation. These checks will be replicated using BigQuery's conditional logic (`IF` statements) and `SAFE.PARSE_DATE` function.
- **Date Derivation:** It calls an external script `gestern.ksh` to determine `p_datum_heute` and `p_datum_gestern`. This will be replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB()` functions.
- **SQL Execution:** The central part is the `starteSQLSkript` function which executes `d_ausd_bp_ta_rn_vertrag.sql`. The content of `d_ausd_bp_ta_rn_vertrag.sql` will be directly translated to BigQuery SQL and executed within the stored procedure.
- **Record Count Capture:** It reads the record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn.tmp`). In BigQuery, this will be handled by capturing the row count from the executed SQL or explicitly selecting `COUNT(*)` into a variable.
- **Commented-out Post-processing:** The script contains commented-out sections for `sed`, `sort`, and `join` operations on temporary `.dat` files. These suggest potential historical file-based post-processing. If this logic ever becomes active and relevant, it would be migrated using BigQuery's string manipulation, sorting, and `JOIN` capabilities.
- **Job Management:** Commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls indicate an integration with a job management system. These would be replaced by inserts into BigQuery audit tables or by features of the orchestration layer (e.g., Airflow operators).

## 6. External Dependencies
- **Oracle Database (implicit):** The `d_ausd_bp_ta_rn_vertrag.sql` script is an SQL script, highly likely targeting an Oracle database, given the context of a "legacy source repo" and "SQLPLUS" helper script. This Oracle database will be the primary source for the data migration to BigQuery.
- **KornShell Utilities:** The script relies on standard KornShell commands (`getopts`, `print`, `set`, `eval`, `cat`) and specific utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`). These will need to be re-implemented using BigQuery scripting features, standard SQL functions, or replaced by orchestration layer functionalities.
- **File System:** The script interacts with the local file system for temporary files (`tmpFile`). This will be replaced by BigQuery's in-memory variables or temporary tables.
- **Job Management System (commented-out):** References to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` suggest an integration with a job management system, although currently commented out. If these functionalities are required in the new environment, they would be handled by the chosen orchestration platform (e.g., Airflow) or by custom audit logging in BigQuery.

There are no `external_systems` or `unresolved_targets` explicitly listed in the `lineage_assembled_jobs` for this specific job, beyond the implicit Oracle database inferred from the SQL script execution.

## 7. Unresolved / Risks
- **Underlying SQL Logic (`d_ausd_bp_ta_rn_vertrag.sql`):** The exact transformation logic within the referenced SQL script is not available in this analysis. This is a critical gap. The migration effort for this job will heavily depend on the complexity of that SQL. A separate analysis of `d_ausd_bp_ta_rn_vertrag.sql` is required to fully define the transformation.
- **Commented-Out Logic:** The commented-out `sed`, `sort`, `join` commands and FOS job management calls need clarification. Are these truly obsolete, or could they become active in specific scenarios? If they are needed, they represent additional migration effort.
- **Shell-Specific Environment:** Sourcing shell-specific environment files (`. $HOME/.dw_init`) and `eval` commands are not directly replicable in BigQuery. These aspects will require careful analysis to determine if they contain critical configurations or logic that needs to be translated into BigQuery parameters or configuration tables.
- **Utility Script Equivalence:** The functionality of custom utility scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, and `gestern.ksh` must be fully understood and replicated accurately in BigQuery SQL or the orchestration layer.

## 8. Build Plan
The migration will primarily involve converting the KornShell script into a BigQuery stored procedure and its invoked SQL script into BigQuery SQL.

1.  **Analyze `d_ausd_bp_ta_rn_vertrag.sql`:** Obtain and analyze the content of `d_ausd_bp_ta_rn_vertrag.sql` to understand its data sources, transformation logic, and target tables.
2.  **Define BigQuery Target Schema:** Create the necessary BigQuery datasets and tables (including target tables from `d_ausd_bp_ta_rn_vertrag.sql` and audit/error log tables).
3.  **Create BigQuery Stored Procedure (Orchestration):**
    *   **Language:** BigQuery SQL (for stored procedures).
    *   **File:** `bq_sp_r_ausd_bp_ta_rn_vertrag.sql` (or similar).
    *   **Content:**
        *   Define procedure parameters corresponding to the shell script's `getopts` arguments.
        *   Implement parameter validation using BigQuery `IF` conditions and `SIGNAL` for errors, logging to `job_error_log`.
        *   Implement date validation using `SAFE.PARSE_DATE`.
        *   Replace `gestern.ksh` calls with `CURRENT_DATE()` and `DATE_SUB()`.
        *   Integrate the migrated BigQuery SQL logic from `d_ausd_bp_ta_rn_vertrag.sql`.
        *   Capture and log record counts to `job_audit_log`.
4.  **Create BigQuery SQL (Transformation Logic):**
    *   **Language:** BigQuery SQL.
    *   **File:** `bq_d_ausd_bp_ta_rn_vertrag.sql` (or integrated directly into the stored procedure).
    *   **Content:** Translate the existing Oracle SQL from `d_ausd_bp_ta_rn_vertrag.sql` into equivalent BigQuery SQL. This includes converting data types, functions, and syntax.
5.  **Implement Configuration Management:** If any critical environment variables from `.dw_init` or `BERT_DIR_ROOT` are used, migrate them to BigQuery configuration tables or make them stored procedure parameters.
6.  **Develop Orchestration:** If `r_ausd_bp_ta_rn_vertrag.ksh` is part of a larger workflow, create an Airflow DAG or similar orchestrator to call the new BigQuery stored procedure, passing the required parameters.
7.  **Testing:** Develop comprehensive unit and integration tests for the BigQuery stored procedure and SQL logic.