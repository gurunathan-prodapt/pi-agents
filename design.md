# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh

## 1. Purpose & Scope

This KornShell script, `k_ausd_v_ta_inv_acc.ksh`, serves as a control and orchestration script. Its primary purpose is to manage the execution of a related SQL script, `d_ausd_v_ta_inv_acc.sql`, which performs data processing. The script ensures that necessary environment variables are loaded, parameters are validated, and common utility functions for error handling and SQL execution are available. It reads runtime parameters, executes the SQL script, ignores already active jobs, and retrieves the number of processed records from a temporary file. The overall job purpose, as assembled, is described as "Job assembled from 1 component(s); stage dist: medium=1".

The scope of this migration involves re-platforming this KornShell orchestration logic and its invoked SQL processing to a Google Cloud Platform (GCP) environment, specifically targeting Cloud Composer (Airflow) for orchestration and BigQuery for data processing.

## 2. Source Inventory

The migration job consists of a single primary source file:

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh`
    *   **Technology**: Shell Script (KornShell)
    *   **Complexity Tier**: Medium
    *   **Automation Bucket**: Semi-Auto
    *   **Summary**: A control script responsible for parameter parsing, sourcing utility scripts for error handling, date checks, and SQL*Plus execution, before calling a specific SQL script (`d_ausd_v_ta_inv_acc.sql`) to perform data processing, and managing job status and error reporting.

## 3. Target Architecture

The target architecture for this job on GCP will involve:

*   **Orchestration**: Google Cloud Composer (Apache Airflow). The KornShell script's orchestration logic will be re-implemented as an Airflow DAG written in Python. This DAG will manage the sequence of tasks, parameter passing, and error handling.
*   **Data Processing**: Google BigQuery. The SQL script, `d_ausd_v_ta_inv_acc.sql`, which is currently executed by the KornShell script, will be migrated to BigQuery SQL. This may involve creating BigQuery tables, views, or stored procedures.
*   **Utility Functions**: Helper functions and environment variable loading (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will need to be re-implemented natively in Python within the Airflow DAG or as separate Python modules.
*   **Temporary Data**: Any temporary files used to pass record counts (`$tmpFile`) will be replaced by Airflow XComs or direct BigQuery query results.

## 4. Data Flow & Lineage

The original job flow is as follows:

1.  **Environment Initialization**: The script sources `$HOME/.dw_init` to set up environment variables.
2.  **Utility Sourcing**: It then sources several utility KornShell scripts:
    *   `f_alis_msgerr.ksh`: For error reporting via `DWMSG_MeldeFehler`.
    *   `h_alis_date.ksh`: (Sourced but not directly used in this script).
    *   `h_alis_parameter.ksh`: Provides `pruefeParameterGesetzt` for parameter validation.
    *   `h_alis_sqlplus.ksh`: Provides `starteSQLSkript` for SQL execution.
3.  **Parameter Parsing & Validation**: Command-line arguments `-j` (Jobkennung) and `-f` (EintragsNr) are parsed using `getopts`. These parameters are then validated using `pruefeParameterGesetzt`.
4.  **SQL Script Execution**: The `starteSQLSkript` function is called to execute `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_inv_acc.sql`. The `v_TabName` variable is set to 'ta_inv_acc', implying the SQL script interacts with a table of this name.
5.  **Record Count Retrieval**: After SQL execution, the script attempts to read a record count from a temporary file `$DW_DIR_UTL/bert_k_ausd_v_ta_inv_acc_$$.tmp` into the `v_records` variable.
6.  **Error Reporting**: In case of parameter validation failures, `DWMSG_MeldeFehler` is invoked, and the script exits with an error code.

**Inferred Data Flow / Lineage:**
*   **Inputs**: Command-line parameters (`p_JobKennung`, `p_EintragsNr`), environment variables (e.g., `BERT_DIR_ROOT`, `DW_DIR_UTL`), and data from the `ta_inv_acc` table (read by `d_ausd_v_ta_inv_acc.sql`).
*   **Processing**: The `d_ausd_v_ta_inv_acc.sql` script performs the core data transformation. The KSH script orchestrates this execution.
*   **Outputs**: Potentially updated `ta_inv_acc` table (implied by SQL script), standard output messages, exit codes, and an inferred record count.

## 5. Transformation Logic

The KornShell script itself primarily handles orchestration and control flow, not direct data transformations. The core transformation logic resides within the invoked SQL script `d_ausd_v_ta_inv_acc.sql`.

The transformation logic for the orchestration will be:

*   **Parameter Handling**: The `getopts` logic for parsing `-j`, `-f`, and `-h` will be translated into Airflow DAG parameters or configuration.
*   **Environment Setup**: The sourcing of `.dw_init` and other utility scripts will be replaced by Airflow's environment management or explicit Python imports and function calls.
*   **Error Handling**: The `DWMSG_MeldeFehler` and parameter validation (`pruefeParameterGesetzt`) will be re-implemented as Python functions or Airflow tasks with appropriate logging and error management.
*   **SQL Execution**: The `starteSQLSkript` function call, which executes `d_ausd_v_ta_inv_acc.sql`, will be replaced by a BigQueryOperator or PythonOperator in Airflow, executing the migrated BigQuery SQL.
*   **Record Count**: The mechanism to read from `$tmpFile` will be replaced by capturing the row count from the BigQuery operation or by using Airflow XComs to pass metrics between tasks.

**Pseudo Code for Target Airflow DAG:**

```python
start_dag:
  load environment configurations (e.g., BERT_DIR_ROOT, DW_DIR_UTL from Airflow variables/connections)
  define utility functions for error reporting (e.g., DWMSG_MeldeFehler_python)

parse_and_validate_parameters_task:
  get p_JobKennung, p_EintragsNr from DAG parameters
  if p_JobKennung or p_EintragsNr are missing:
    log error and fail task

execute_sql_script_task:
  set v_TabName = 'ta_inv_acc'
  call BigQueryOperator to execute BQ-migrated 'd_ausd_v_ta_inv_acc.sql'
  pass p_EintragsNr, p_JobKennung as variables to the SQL script
  capture output (e.g., row count) from BigQuery operation

retrieve_record_count_task:
  get record_count from previous task's output (e.g., XCom)
  log "ENDE Datenverarbeitung" message
  log record_count

end_dag
```

## 6. External Dependencies

The original script has several dependencies:

*   **Sourced Shell Scripts**:
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus execution utility.
    These will need to be re-implemented in Python or integrated as part of the Airflow DAG's logic. Shared utility functions might be moved to a common Python package accessible by Cloud Composer.
*   **SQL Script**:
    *   `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_inv_acc.sql`: This is the core data processing component. It is currently executed via SQL*Plus by the `h_alis_sqlplus.ksh` utility. This SQL script will need to be migrated to BigQuery SQL. Its dependencies (e.g., the `ta_inv_acc` table) will also need to be migrated to BigQuery.
*   **Database Access**: The script implicitly depends on an Oracle or similar database where the `d_ausd_v_ta_inv_acc.sql` operates and `ta_inv_acc` resides. This will be replaced by BigQuery for both data storage and processing.
*   **Temporary File System**: The use of `$DW_DIR_UTL/bert_k_ausd_v_ta_inv_acc_$$.tmp` for intermediate record counts implies a shared file system. This will be replaced by internal Airflow mechanisms (XComs) or direct BigQuery result handling.

## 7. Unresolved / Risks

*   **Missing Lineage Details**: The `lineage_edges` query for this job returned no rows. This means the direct file-to-file dependencies and data flow were not explicitly captured in the lineage graph. The dependencies were inferred from the script's content and `file_analysis.references_out`. A deeper manual analysis of the referenced `.ksh` utilities (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) and `d_ausd_v_ta_inv_acc.sql` will be required to ensure complete understanding of their functions and to accurately re-platform them.
*   **SQL Script Migration**: The content and complexity of `d_ausd_v_ta_inv_acc.sql` are not analyzed here. This SQL script is a critical component and its migration to BigQuery SQL needs a separate, detailed design phase, including potential data type conversions, function replacements, and performance optimizations.
*   **Utility Script Re-platforming**: The various sourced KornShell utilities are common dependencies. Their re-platforming to Python for Cloud Composer needs to be consistent and potentially centralized to avoid duplication of effort.
*   **Security & Monitoring Gaps**: The original script has noted gaps in authentication, authorization, credential management, input sanitization, SQL injection protection, structured logging, audit logging, and metrics. These aspects must be explicitly designed and implemented in the target GCP environment using best practices (e.g., IAM for auth, Secret Manager for credentials, Cloud Logging/Monitoring for observability).
*   **Error Code Mapping**: The script uses specific error codes (192, 193). These will need to be mapped to appropriate Airflow task states and/or Cloud Logging severity levels.
*   **`semi_auto` Bucket**: The `semi_auto` migration bucket indicates that some manual effort will be required, likely due to the need to interpret the shell scripting logic, re-implement the utilities in Python, and manually craft the Airflow DAG and BigQuery SQL.

## 8. Build Plan

The build plan will proceed in the following stages:

1.  **Analyze and Migrate `d_ausd_v_ta_inv_acc.sql`**:
    *   **Action**: Analyze the SQL script content, identify tables, views, and functions used. Design and convert the SQL to BigQuery SQL.
    *   **Language**: BigQuery SQL
2.  **Design and Implement Python Utilities**:
    *   **Action**: Re-implement the functionality of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, and the environment setup from `.dw_init` into modular Python functions or classes.
    *   **Language**: Python
3.  **Develop Airflow DAG**:
    *   **Action**: Create an Airflow DAG in Python.
        *   Define tasks for parameter parsing and validation, leveraging the new Python utilities.
        *   Define a BigQueryOperator task to execute the migrated `d_ausd_v_ta_inv_acc.sql`.
        *   Implement error handling, logging, and metrics using Airflow and GCP services.
        *   Ensure passing of job identifier and entry number to the BigQuery task.
        *   Implement the retrieval and logging of record counts from the BigQuery operation.
    *   **Language**: Python (Airflow DAG)
4.  **Configuration and Deployment**:
    *   **Action**: Configure Airflow variables, connections, and service accounts in Cloud Composer. Deploy the Python DAG and utility scripts to the Cloud Composer environment.
    *   **Language**: YAML/JSON (for Airflow configurations), Python (deployment scripts)
5.  **Testing**:
    *   **Action**: Develop unit and integration tests for the Python utilities, the BigQuery SQL, and the complete Airflow DAG.
    *   **Language**: Python (Pytest/unittest), SQL (for BigQuery validation)