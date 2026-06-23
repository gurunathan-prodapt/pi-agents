# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh

## 1. Purpose & Scope

This migration design document outlines the plan for migrating the KornShell script `k_ausd_v_ta_action_assoc.ksh` to Google Cloud Platform, targeting BigQuery for data processing and Python for orchestration.

The original KornShell script serves as a control script. Its primary responsibilities include:
*   Parsing command-line parameters (job identifier `p_JobKennung` and entry number `p_EintragsNr`).
*   Performing parameter validation.
*   Loading environment variables and utility shell scripts for error handling, date functions, parameter parsing, and SQL execution.
*   Orchestrating the execution of an external SQL script, `d_ausd_v_ta_action_assoc.sql`, which is responsible for the core data manipulation related to the `ta_action_assoc` table.
*   Logging progress and errors.
*   Reading the count of processed records from a temporary file generated during SQL execution.

The scope of this migration is to re-implement this control flow and its associated data processing in a BigQuery-native environment, specifically using Python for orchestration and BigQuery SQL for data transformations.

## 2. Source Inventory

The job `5af228f1` consists of a single primary component:

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`
    *   **Technology**: KornShell
    *   **Complexity Tier**: Medium
    *   **Automation Bucket**: Semi-Auto (B2)
    *   **Summary**: A control script managing job parameters, error logging, and orchestrating the execution of an associated SQL script to manage `ta_action_assoc` data.

**Key Dependencies Identified in the Source Script**:
*   **External SQL Script**: `d_ausd_v_ta_action_assoc.sql` (located at `${BERT_DIR_ROOT}/aufbereitung/sql/`) - This script contains the core data manipulation logic for `ta_action_assoc`.
*   **Sourced Utility Scripts**:
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter validation utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL execution utilities (likely defines `starteSQLSkript`).

## 3. Target Architecture

The migrated job will leverage Google Cloud Platform services:

*   **Orchestration**: A Python script (e.g., deployed via Cloud Composer/Airflow or Cloud Functions/Workflows) will replace the KornShell wrapper. This Python script will handle parameter parsing, validation, error handling, and the execution of BigQuery SQL.
*   **Data Processing**: The SQL logic from `d_ausd_v_ta_action_assoc.sql` will be converted to BigQuery SQL (BQSQL). This BQSQL will be embedded within the Python orchestration script or called as a separate BQSQL file.
*   **Data Storage**: All processed data will reside in BigQuery datasets and tables. The `ta_action_assoc` table, which is the primary target of the original SQL script, will be migrated to a corresponding BigQuery table.
*   **Logging & Monitoring**: Standard GCP logging (Cloud Logging) and monitoring (Cloud Monitoring) will be utilized for observability.

## 4. Data Flow & Lineage

The original job's data flow, as derived from the KornShell script, is as follows:

1.  **Initialization**: The `k_ausd_v_ta_action_assoc.ksh` script starts, loading environment variables and utility functions.
2.  **Parameter Acquisition**: Command-line arguments (`-j`, `-f`) are parsed to obtain `p_JobKennung` and `p_EintragsNr`.
3.  **Validation**: Input parameters are validated using sourced utility functions (e.g., `pruefeParameterGesetzt`). If validation fails, an error is reported, and the script exits.
4.  **SQL Execution**: The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) is invoked, executing `d_ausd_v_ta_action_assoc.sql`. This SQL script is responsible for reading from and writing to relevant database tables, specifically managing the `ta_action_assoc` table. This execution likely involves writing intermediate results or record counts to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_action_assoc_$$.tmp`).
5.  **Record Count Retrieval**: The script reads the `v_records` count from the temporary file.
6.  **Completion**: The script logs completion and exits.

In the target BigQuery environment, this flow will translate to:

1.  A Python orchestration script will be triggered.
2.  Parameters for job identifier and entry number will be passed to the Python script.
3.  Python functions will handle parameter validation and error handling, replacing the shell utility scripts.
4.  The Python script will execute the converted BQSQL (from `d_ausd_v_ta_action_assoc.sql`) using the BigQuery client library.
5.  Instead of a temporary file, the Python script will capture the number of processed records directly from the BigQuery job results or by issuing a `SELECT COUNT(*)` on the target table if required.
6.  Logging will be handled by Python's standard logging integrated with Cloud Logging.

## 5. Transformation Logic

The KornShell script itself contains minimal direct transformation logic, acting primarily as an orchestrator. The transformation logic resides within the `d_ausd_v_ta_action_assoc.sql` file.

**Shell Script to Python Conversion**:
*   **Environment Sourcing**: The `. $HOME/.dw_init` and other `.` commands will be replaced by Python environment variable lookups (e.g., `os.getenv`) and custom Python modules that replicate the functionality of the sourced shell scripts (e.g., `f_alis_msgerr.ksh` for error handling).
*   **Parameter Parsing**: `getopts` will be replaced by Python's `argparse` module or by directly consuming runtime parameters provided by the orchestration platform.
*   **Parameter Validation**: The `pruefeParameterGesetzt` calls will be re-implemented as Python functions.
*   **SQL Execution**: The `starteSQLSkript` function call will be replaced by a Python function that uses the BigQuery client library to execute the `d_ausd_v_ta_action_assoc.sql` content (converted to BQSQL). The parameters passed to `starteSQLSkript` (`$p_EintragsNr`, `$Name_SQLskript`, `$p_JobKennung`) will be passed as query parameters to the BigQuery job.
*   **Temporary File Handling**: The `tmpFile` mechanism for recording processed record counts will be replaced by obtaining this information directly from BigQuery job metadata or by performing a count query in BigQuery.
*   **Error Handling**: The `DWMSG_MeldeFehler` calls will be replaced by Python exception handling and logging.

**SQL Script to BQSQL Conversion**:
*   The contents of `d_ausd_v_ta_action_assoc.sql` must be translated into equivalent BigQuery SQL. This may involve syntax changes, data type mapping, and potential re-architecting of logic to leverage BigQuery's capabilities (e.g., partitioning, clustering, or specific functions).

## 6. External Dependencies

The original script has several dependencies:

*   **Sourced Shell Scripts**:
    *   `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`.
    *   **Migration Plan**: These will be analyzed, and their essential functionalities will be re-implemented as Python helper functions or modules. Generic utilities might be integrated into a common Python library.
*   **SQL Script**:
    *   `d_ausd_v_ta_action_assoc.sql`.
    *   **Migration Plan**: The content of this file needs to be converted to BQSQL. The resulting BQSQL will be executed by the Python orchestration script.
*   **Oracle Database / SQLPlus (Implied by `h_alis_sqlplus.ksh`)**:
    *   The original script likely interacts with an Oracle database via `sqlplus` as orchestrated by `h_alis_sqlplus.ksh`.
    *   **Migration Plan**: All database interactions will be redirected to BigQuery. This means the `d_ausd_v_ta_action_assoc.sql` (once converted to BQSQL) will query/update BigQuery tables. The `h_alis_sqlplus.ksh` functionality will be replaced by BigQuery client library calls in Python.
*   **Temporary File System**:
    *   `$DW_DIR_UTL/bert_k_ausd_v_ta_action_assoc_$$.tmp`
    *   **Migration Plan**: This will be replaced by direct BigQuery API calls to retrieve job statistics or by performing a follow-up count query on the target table. Temporary file usage for inter-process communication should be avoided in the BigQuery paradigm.

## 7. Unresolved / Risks

**Unresolved Items**:

1.  **Content of `d_ausd_v_ta_action_assoc.sql`**: The most significant unresolved item is the actual SQL code within `d_ausd_v_ta_action_assoc.sql`. Without this, the core data transformation logic cannot be fully designed or migrated to BQSQL.
2.  **Detailed Logic of Sourced Utilities**: While the purpose of utility scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` is known, their precise internal logic (especially `pruefeParameterGesetzt`, `DWMSG_MeldeFehler`, and `starteSQLSkript`) is required for accurate re-implementation in Python. This includes understanding how `starteSQLSkript` handles the temporary file `tmpFile`.

**Risks**:

1.  **Complexity of `d_ausd_v_ta_action_assoc.sql`**: If the SQL script is complex (e.g., uses proprietary Oracle SQL features, complex procedural logic, or involves intricate temporary table management), its conversion to BQSQL might be challenging and require significant refactoring.
2.  **Implicit Dependencies**: The `lineage_edges` query did not return any direct dependencies, which could mean there are implicit dependencies or interactions not captured by the lineage system. A thorough manual review of the SQL script and utility scripts will be crucial to uncover these.
3.  **Performance Tuning**: The translated BQSQL may require performance tuning to optimize costs and execution times in BigQuery, especially if the original SQL was highly optimized for Oracle.

## 8. Build Plan

The build plan will proceed once the contents of `d_ausd_v_ta_action_assoc.sql` and the detailed logic of the utility scripts are available.

1.  **Translate `d_ausd_v_ta_action_assoc.sql` to BQSQL**:
    *   **Artifact**: `d_ausd_v_ta_action_assoc.bqsql` (BigQuery SQL file).
    *   **Language**: BigQuery SQL.
2.  **Develop Python Orchestration Script**:
    *   **Artifact**: `k_ausd_v_ta_action_assoc.py` (Python file).
    *   **Language**: Python.
    *   **Functionality**:
        *   Argument parsing for `p_JobKennung` and `p_EintragsNr`.
        *   Re-implementation of parameter validation logic.
        *   Integration of error handling and logging (using `logging` module and Cloud Logging).
        *   Execution of `d_ausd_v_ta_action_assoc.bqsql` using the BigQuery client library, passing parameters as query job configurations.
        *   Retrieval of processed record counts directly from BigQuery job statistics or a follow-up query.
3.  **Develop Python Utility Modules (if needed)**:
    *   **Artifact**: e.g., `error_handling.py`, `parameter_utils.py` (Python files).
    *   **Language**: Python.
    *   **Functionality**: Modularized re-implementation of common functions from the original shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
4.  **Deployment Configuration**:
    *   **Artifact**: Deployment configuration files (e.g., Cloud Composer DAG, Cloud Workflow definition, Cloud Function deployment script).
    *   **Language**: YAML/JSON/Python.
    *   **Functionality**: Define how `k_ausd_v_ta_action_assoc.py` will be scheduled and executed in the GCP environment, including environment variables and service account permissions.