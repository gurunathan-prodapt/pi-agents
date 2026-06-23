# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh

## 1. Purpose & Scope
This Korn shell script (`r_ausd_adressen.ksh`) serves as an orchestrator for the initial, date-based extraction of address data from the CRS system. Its primary purpose is to prepare this data for downstream processing related to business partners and invoice recipients. The script sets up the execution environment, parses command-line parameters, performs validation and error handling, and then delegates the core processing to another script, `k_ausd_adressen.ksh`.

The business objective is to provide a consistent and controlled mechanism for extracting critical address data on a specified reference date, forming the foundation for further analytical and reporting activities.

## 2. Source Inventory
The job is comprised of a single KornShell script.

| File Name | Technology | Complexity Tier | Automation Bucket |
| :------------------------------------------------ | :--------- | :-------------- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh` | KornShell | Medium | semi_auto (B2) |

**Complexity Rationale:** The script is categorized as 'Medium' complexity due to its role as an orchestration script involving parameter parsing, environmental setup, sourcing multiple utility scripts, and conditional execution flow. While it doesn't contain complex data transformations itself, its dependency on and invocation of other scripts, coupled with its custom error handling and logging mechanisms, elevates it beyond simple. The analysis suggests a primary role as a "pipeline_orchestrator".

## 3. Target Architecture
The target platform for this migration is Google Cloud Platform (GCP), with BigQuery as the primary data warehouse. Given the script's orchestration nature, the recommended target for this specific component is:

*   **Cloud Composer (Apache Airflow):** The `r_ausd_adressen.ksh` script will be re-platformed into an Airflow DAG. This DAG will encapsulate the environment setup, parameter handling, and the sequential execution logic.
*   **BigQuery:** The data processing performed by the invoked core script (`k_ausd_adressen.ksh`) is expected to be migrated to BigQuery SQL, potentially orchestrated by the new Airflow DAG.

## 4. Data Flow & Lineage

The `r_ausd_adressen.ksh` script acts as an entry point and orchestrator.

**Inputs:**
*   **Command-line parameters:**
    *   `-s <Stichtag>` (DDMMYYYY): Reference date for the data extraction. If not provided, it defaults to the system date.
    *   `-l <Wiederanlaufwert>`: Restart value. If specified, it influences which contracts (`DWH_VERTRAG_ID`) are processed and can trigger deletion of existing entries before processing. Defaults to `0` if not provided.
    *   `-h`: Displays help and exits.
*   **Environment Variables & Configuration:** Sourced from `$HOME/.dw_init` and other utility scripts, providing global paths (`BERT_DIR_ROOT`) and functions.
*   **System Date:** Used as a fallback for `Stichtag`.

**Processing & Transformations (Orchestration Logic):**
1.  **Initialization:** Sets program name and version.
2.  **Environment Setup:** Sources `$HOME/.dw_init` and error/parameter/date utility scripts.
3.  **Parameter Parsing:** Uses `getopts` to process command-line arguments.
4.  **Defaulting Logic:** Initializes `p_wiederanlaufWert` to `0` if unset and `p_stichtag` to `v_sysdate` if unset.
5.  **Parameter Validation:** Calls `pruefeParameterGesetzt` to ensure `Stichtag` is set. Exits with error if validation fails.
6.  **Job Tracking & Logging Setup:** Initializes `JobKennung` (e.g., `BERT_P_ADRESSEN`), obtains a `DW_EintragsNr`, and sets up `LogDatei` using `DWMSG_` functions. This includes setting up traps for `INT`, `STOP`, `CONT`, and `ERR` signals for robust error handling and logging.
7.  **Core Script Invocation:** Executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_adressen.ksh` with the parsed parameters (`-j`, `-s`, `-f`, `-l`) redirected to the `LogDatei`.
8.  **Success/Error Reporting:** Records success status via `DWMSG_SetzeStatusOK` or `DWMSG_MeldeFehler` and prints messages to console/log.

**Outputs:**
*   **Log Files:** Detailed execution logs are written to `$LogDatei`.
*   **Console Output:** Basic job information and success/failure messages.
*   **Invocation of `k_ausd_adressen.ksh`:** This is the primary output, triggering the core data extraction and processing. The output of this core script (likely data to be loaded into tables) is captured within the `LogDatei`.

## 5. Transformation Logic
The transformation logic for `r_ausd_adressen.ksh` focuses on orchestration rather than data manipulation. The migration to an Airflow DAG would involve:

*   **DAG Definition:** A Python script defining the DAG, including its schedule, default arguments, and tasks.
*   **Parameter Handling:**
    *   Airflow's `params` mechanism or `DagRun` configuration can replace `getopts` for passing `Stichtag` and `Wiederanlaufwert`.
    *   The defaulting logic for `p_wiederanlaufWert` and `p_stichtag` will be translated into Python code within the DAG.
*   **Environment Setup:**
    *   `$HOME/.dw_init` dependencies need to be analyzed. If they set environment variables, these can be set in the Airflow environment or passed as `env` to specific operators.
    *   The utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) will need to be refactored. Functions like `DWDate_Gib_Zeitraum` and `pruefeParameterGesetzt` would be implemented as Python functions or custom Airflow operators.
*   **Job Tracking & Logging:** The `DWMSG_` functions would be replaced with Airflow's native logging capabilities and custom Python code for managing job-specific metadata and status updates (e.g., to a monitoring database or BigQuery logging table).
*   **Error Handling:** Airflow's robust error handling, retries, and alerting mechanisms will replace the `trap` commands and `DWMSG_Fehlerbehandlung`.
*   **Core Script Invocation:** The invocation of `k_ausd_adressen.ksh` will become a key task in the Airflow DAG.
    *   If `k_ausd_adressen.ksh` is itself a shell script, it can be executed using Airflow's `BashOperator`.
    *   Ideally, `k_ausd_adressen.ksh` would also be migrated to BigQuery SQL and orchestrated directly as BigQuery tasks within the Airflow DAG.

**Pseudo Code for Target Airflow DAG:**

```python
# Define DAG and default args
with DAG(
    dag_id='r_ausd_adressen_ksh_migration',
    start_date=days_ago(1),
    schedule_interval=None, # Or appropriate schedule
    catchup=False,
    params={
        "stichtag": {"type": "string", "pattern": "^\\d{8}$", "default": ""},
        "wiederanlaufwert": {"type": "integer", "default": 0}
    }
) as dag:

    # Task 1: Environment Setup / Parameter Defaulting
    def prepare_params(**context):
        stichtag = context['params'].get('stichtag')
        wiederanlaufwert = context['params'].get('wiederanlaufwert')

        # Implement defaulting logic from original script
        if not wiederanlaufwert:
            wiederanlaufwert = 0
        if not stichtag:
            # Get current system date
            stichtag = datetime.now().strftime('%d%m%Y')
        
        # Store processed parameters for downstream tasks
        context['ti'].xcom_push(key='processed_stichtag', value=stichtag)
        context['ti'].xcom_push(key='processed_wiederanlaufwert', value=wiederanlaufwert)
        # Implement other env setups as needed

    prepare_params_task = PythonOperator(
        task_id='prepare_parameters_and_env',
        python_callable=prepare_params,
        provide_context=True,
    )

    # Task 2: Parameter Validation (Python equivalent of pruefeParameterGesetzt)
    def validate_params(**context):
        stichtag = context['ti'].xcom_pull(task_ids='prepare_parameters_and_env', key='processed_stichtag')
        # Add validation logic for stichtag format/presence
        if not stichtag:
            raise ValueError("Stichtag parameter is missing or invalid.")
        # Other validations...

    validate_params_task = PythonOperator(
        task_id='validate_parameters',
        python_callable=validate_params,
        provide_context=True,
    )

    # Task 3: Job Tracking & Logging Initialization (replace DWMSG_ calls)
    def init_job_logging(**context):
        # Implement logic to get DW_EintragsNr, JobKennung, LogDatei, etc.
        # Store in XComs or a persistent logging store (e.g., BigQuery table)
        job_kennung = "BERT_P_ADRESSEN"
        entry_nr = generate_new_entry_number() # Custom function
        log_file_name = generate_log_filename(job_kennung, entry_nr) # Custom function
        context['ti'].xcom_push(key='job_kennung', value=job_kennung)
        context['ti'].xcom_push(key='entry_nr', value=entry_nr)
        context['ti'].xcom_push(key='log_file_name', value=log_file_name)
        # Call logging service to create new job entry
        log_job_entry(entry_nr, job_kennung, log_file_name) # Custom function
        # DWMSG_SetzeStichtagInfo equivalent
        log_stichtag_info(entry_nr, context['ti'].xcom_pull(task_ids='prepare_parameters_and_env', key='processed_stichtag'))

    init_logging_task = PythonOperator(
        task_id='initialize_job_logging',
        python_callable=init_job_logging,
        provide_context=True,
    )

    # Task 4: Execute Core Processing Script (k_ausd_adressen.ksh)
    # This task depends on the migration of k_ausd_adressen.ksh.
    # If k_ausd_adressen.ksh is migrated to BQ SQL:
    execute_core_script_task = BigQueryOperator(
        task_id='execute_k_ausd_adressen_bq',
        sql='''SELECT * FROM `project.dataset.k_ausd_adressen_logic`(...)''', # Placeholder
        params={
            'job_kennung': "{{ ti.xcom_pull(task_ids='initialize_job_logging', key='job_kennung') }}",
            'stichtag': "{{ ti.xcom_pull(task_ids='prepare_parameters_and_env', key='processed_stichtag') }}",
            'entry_nr': "{{ ti.xcom_pull(task_ids='initialize_job_logging', key='entry_nr') }}",
            'wiederanlaufwert': "{{ ti.xcom_pull(task_ids='prepare_parameters_and_env', key='processed_wiederanlaufwert') }}"
        },
        use_legacy_sql=False,
    )
    # If k_ausd_adressen.ksh remains a shell script temporarily:
    # execute_core_script_task = BashOperator(
    #     task_id='execute_k_ausd_adressen_ksh',
    #     bash_command='''
    #         /path/to/k_ausd_adressen.ksh \
    #         -j {{ ti.xcom_pull(task_ids='initialize_job_logging', key='job_kennung') }} \
    #         -s {{ ti.xcom_pull(task_ids='prepare_parameters_and_env', key='processed_stichtag') }} \
    #         -f {{ ti.xcom_pull(task_ids='initialize_job_logging', key='entry_nr') }} \
    #         -l {{ ti.xcom_pull(task_ids='prepare_parameters_and_env', key='processed_wiederanlaufwert') }} \
    #         >> {{ ti.xcom_pull(task_ids='initialize_job_logging', key='log_file_name') }} 2>&1
    #     ''',
    # )

    # Task 5: Final Status Update
    def update_final_status(**context):
        entry_nr = context['ti'].xcom_pull(task_ids='initialize_job_logging', key='entry_nr')
        # DWMSG_SetzeStatusOK equivalent
        log_job_status(entry_nr, "OK") # Custom function

    update_status_task = PythonOperator(
        task_id='update_final_status',
        python_callable=update_final_status,
        provide_context=True,
    )

    # Define Task Dependencies
    prepare_params_task >> validate_params_task >> init_logging_task >> execute_core_script_task >> update_status_task
```

## 6. External Dependencies
The script has the following external dependencies:

*   **CRS System:** The ultimate source of address data. This will need to be re-evaluated for direct ingestion into BigQuery (e.g., via CDC, batch extracts, or streaming).
*   **Filesystem-based utility scripts:**
    *   `$HOME/.dw_init`: Likely sets environment variables or sources other configuration. This will need to be translated to Airflow environment variables or Python configurations.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging functions.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities.
    These utility scripts will need to be refactored into Python modules or functions callable within the Airflow DAG.
*   **Invoked Core Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_adressen.ksh`. This is a critical dependency and must be migrated in conjunction with, or prior to, `r_ausd_adressen.ksh`. Its migration will likely involve converting its logic to BigQuery SQL and potentially encapsulating it in a separate Airflow DAG or a BigQuery stored procedure.

**Replacement Strategy:**
*   **CRS System:** A new data ingestion pipeline (e.g., using Cloud Data Fusion, Cloud Pub/Sub, or custom scripts) will be established to bring data from CRS into BigQuery.
*   **Utility Scripts:** The logic within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` will be re-implemented in Python to be called from the Airflow DAG. Custom Python modules for shared functions are recommended.
*   **`k_ausd_adressen.ksh`:** This script's logic will be fully re-implemented in BigQuery SQL, potentially as a series of SQL tasks within the same Airflow DAG or a separate, child DAG.

## 7. Unresolved / Risks

*   **Empty `file_complexity` table:** The `tier` for the `r_ausd_adressen.ksh` script was not directly available from `file_complexity` and was inferred as 'Medium'. This should be confirmed through manual review.
*   **Absence of `lineage_edges`:** The lack of explicit `lineage_edges` for this file indicates that the lineage tool could not fully parse the dynamic invocation of `k_ausd_adressen.ksh`. While the content and analysis explicitly confirm this invocation, future lineage efforts should aim to capture such dynamic dependencies.
*   **`semi_auto` migration bucket (B2):** This indicates that manual intervention and code refactoring will be required. The translation of shell scripting constructs (e.g., `getopts`, `trap`, custom `DWMSG_` functions) into Python and Airflow tasks will require development effort.
*   **Refactoring of Utility Scripts:** The detailed logic within the sourced utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) needs thorough analysis and re-implementation in Python.
*   **Migration of `k_ausd_adressen.ksh`:** The migration of this orchestrator is highly dependent on the migration strategy and progress of the core script it calls. This is the biggest dependency and potential blocker.
*   **No credential/secret management:** The original script lacks explicit secure handling of credentials. The migration must incorporate Google Cloud Secret Manager or similar best practices for any sensitive information.

## 8. Build Plan

The build plan focuses on re-platforming the orchestration logic to an Airflow DAG and migrating the core processing to BigQuery.

1.  **Analyze and Document `k_ausd_adressen.ksh`:**
    *   Perform a detailed analysis of the core script `k_ausd_adressen.ksh` to understand its data sources, transformations, and target tables.
    *   Generate a separate migration design document for `k_ausd_adressen.ksh` to define its BigQuery SQL implementation.
2.  **Refactor Utility Scripts to Python Modules:**
    *   Translate the logic of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` into a set of reusable Python functions or classes.
    *   Create a dedicated Python package for these shared utilities within the GCP project.
3.  **Design Airflow DAG Structure:**
    *   Define the Airflow DAG `r_ausd_adressen_ksh_migration.py` in Python.
    *   Outline the tasks: parameter parsing, environment setup, validation, logging initialization, core process invocation, and final status update.
4.  **Implement Parameter Handling Task:**
    *   Develop a PythonOperator to handle `stichtag` and `wiederanlaufwert` extraction and defaulting from Airflow DAG parameters.
5.  **Implement Validation Task:**
    *   Develop a PythonOperator for parameter validation (e.g., date format, presence), using the refactored utility functions.
6.  **Implement Logging and Job Tracking Task:**
    *   Develop a PythonOperator to initialize job-specific metadata and interact with a GCP logging service (e.g., Cloud Logging) or a BigQuery job status table, replacing the `DWMSG_` functions.
7.  **Implement Core Processing Task:**
    *   **Option A (Recommended):** If `k_ausd_adressen.ksh` is fully migrated to BigQuery SQL, implement this as a `BigQueryOperator` or a `DataprocBatchOperator` (if PySpark/Spark SQL is used).
    *   **Option B (Temporary):** If `k_ausd_adressen.ksh` is not yet migrated, use a `BashOperator` to call the original KornShell script (if it can execute in the Cloud Composer environment) or a containerized version of it.
8.  **Implement Final Status Update Task:**
    *   Develop a PythonOperator to record the final status of the DAG run (success/failure) in the centralized logging/monitoring system.
9.  **Testing:**
    *   Unit tests for refactored Python modules.
    *   Integration tests for the Airflow DAG on a Cloud Composer development environment.
    *   End-to-end tests to verify data accuracy and pipeline completeness.

**Language for Build Output:**
*   Airflow DAG: Python
*   Core Processing (from `k_ausd_adressen.ksh`): BigQuery SQL (preferred) or PySpark/Spark SQL.
*   Utility Functions: Python