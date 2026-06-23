# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh

## 1. Purpose & Scope

This job, `r_ausd_geschaeftspartner.ksh`, is a KornShell script designed to orchestrate the initial provisioning of contract caches for the demand scoring system (FOS - Forderungsscoring). Its primary function is to handle parameter parsing (Stichtag/processing date, Wiederanlaufwert/restart value), manage error logging, and invoke a core script, `k_ausd_geschaeftspartner.ksh`, which performs the actual data generation and transformation. The script is an ETL load process, taking `DWH_VERTRAG_ID` as input and producing data for `FOS-Tabelle`.

## 2. Source Inventory

*   **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh`
*   **Technology:** KornShell
*   **Complexity Tier:** Medium (inferred from `stage_distribution` and `semi_auto` migration bucket)
*   **Automation Bucket:** Semi-automatic (B2)
*   **Summary:** KornShell script orchestrating the initial provisioning of contract caches for the demand scoring system (FOS). It manages parameters, error logging, and delegates core logic to `k_ausd_geschaeftspartner.ksh`.

## 3. Target Architecture

The migration targets Google Cloud Platform (GCP).

*   **Orchestration:** Google Cloud Composer (Airflow). The current KornShell script's orchestration logic will be converted into an Airflow DAG.
*   **Transformation:** The core transformation logic within `k_ausd_geschaeftspartner.ksh` will be converted to PySpark and executed on Google Cloud Dataproc.
*   **Data Storage:** BigQuery will serve as the target data warehouse for the `FOS-Tabelle` and other related data.
*   **Logging:** Airflow's native logging integrated with Cloud Logging.

## 4. Data Flow & Lineage

The original KornShell script, `r_ausd_geschaeftspartner.ksh`, acts as an orchestrator.

1.  **Initialization:** The script sources environment variables (`. $HOME/.dw_init`) and utility scripts for error handling, parameter parsing, and date functions.
2.  **Parameter Acquisition:** It parses command-line arguments for `-s` (Stichtag/processing date) and `-l` (Wiederanlaufwert/restart value). If `Stichtag` is not provided, it defaults to `sysdate`. `Wiederanlaufwert` defaults to `0`.
3.  **Error Handling Setup:** It sets up `trap` commands for robust error handling and logging.
4.  **Core Logic Invocation:** The script then executes `k_ausd_geschaeftspartner.ksh`, passing the determined `Stichtag`, `JobKennung`, `DW_EintragsNr`, and `Wiederanlaufwert` as parameters.
    *   **Input:** `DWH_VERTRAG_ID` (likely a table or data source).
    *   **Output:** `FOS-Tabelle` (target table).
5.  **Logging & Exit:** Logs job status and exits.

In the target architecture, this flow will be represented by an Airflow DAG:

*   A single `DataprocSubmitJobOperator` task named `run_contract_cache_initial_load` will be responsible for executing the PySpark equivalent of `k_ausd_geschaeftspartner.ksh`.
*   Parameters (`stichtag`, `wiederanlaufwert`) will be passed from the Airflow DAG run configuration to the PySpark job.
*   The Airflow DAG will be manually triggered (schedule is currently `None`, as no legacy scheduling information was available).

## 5. Transformation Logic

The current script is primarily an orchestrator. The actual data transformation logic resides within the invoked script: `k_ausd_geschaeftspartner.ksh`.

*   **`r_ausd_geschaeftspartner.ksh` (Orchestrator):** This script's logic will be translated into the Airflow DAG structure. This includes parameter validation, default value assignments, error handling, and the invocation of the core transformation.
*   **`k_ausd_geschaeftspartner.ksh` (Core Transformation):** This script, which is currently a shell script, will need to be thoroughly analyzed to understand its data manipulation, filtering, and loading operations. It is currently speculated to be converted into a PySpark job that reads from source systems (e.g., `DWH_VERTRAG_ID`), performs the required transformations for the contract cache, and writes the results to `FOS-Tabelle` in BigQuery. The `analysis` of `r_ausd_geschaeftspartner.ksh` suggests an `ETL_LOAD` transformation type, with `DWH_VERTRAG_ID` as input and `FOS-Tabelle` as output.

## 6. External Dependencies

The source script has several external dependencies:

*   **Environment Initialization:** `. $HOME/.dw_init` – This initializes the environment. In Airflow, this will require setting appropriate environment variables or utilizing Airflow Connections/Secrets. The `BERT_DIR_ROOT` variable is sourced from here.
*   **Utility Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utility.
    These KornShell utilities will need to be refactored and reimplemented in Python to be compatible with the Airflow and PySpark environment.
*   **Called Script:** `k_ausd_geschaeftspartner.ksh` – This is a critical dependency that contains the main ETL logic. It will be migrated to a PySpark application.
*   **Data Sources/Targets:**
    *   `DWH_VERTRAG_ID`: Identified as a critical input. This will need to be mapped to an existing or new BigQuery table.
    *   `FOS-Tabelle`: Identified as a critical output. This will be mapped to a BigQuery table.

## 7. Unresolved / Risks

*   **Core Script (`k_ausd_geschaeftspartner.ksh`) Logic:** The precise transformation logic and internal dependencies of `k_ausd_geschaeftspartner.ksh` are currently unknown. A detailed analysis of this script is required.
*   **Legacy Scheduling:** No explicit scheduling information was provided for the `r_ausd_geschaeftspartner.ksh` job in the legacy environment. The Airflow DAG is currently designed for manual trigger only.
*   **File Complexity Details:** The `file_complexity` data for `r_ausd_geschaeftspartner.ksh` was not available, so specific migration flags and a detailed tier rationale are missing.
*   **Environment Variables & Secrets:** The exact content and implications of `. $HOME/.dw_init` and `BERT_DIR_ROOT` need to be fully understood and securely configured in the GCP environment.
*   **Dataproc Operator Choice:** The MCP tool suggested `DataprocSubmitJobOperator`. However, if `k_ausd_geschaeftspartner.ksh` turns out to be a simpler shell script without heavy data processing needs that warrant PySpark, then a `BashOperator` or `PythonOperator` might be more appropriate for its direct execution or Python-based reimplementation, respectively.
*   **Error Code Translation:** The legacy script uses specific error codes for different failure scenarios. These need to be effectively translated into Airflow's error handling and alerting mechanisms.

## 8. Build Plan

1.  **Analyze `k_ausd_geschaeftspartner.ksh`:**
    *   **Action:** Perform detailed static and dynamic analysis of `k_ausd_geschaeftspartner.ksh` to understand its data flow, transformations, and dependencies.
    *   **Language:** N/A (analysis phase)
2.  **Refactor Utility Functions:**
    *   **Action:** Convert the functionalities of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` into reusable Python modules or Airflow operators/hooks.
    *   **Language:** Python
3.  **Design `k_ausd_geschaeftspartner.py` (PySpark):**
    *   **Action:** Create a detailed design for the PySpark implementation of `k_ausd_geschaeftspartner.ksh`, including BigQuery table schemas, SQL transformations, and data loading strategy.
    *   **Language:** PySpark (design)
4.  **Implement `k_ausd_geschaeftspartner.py`:**
    *   **Action:** Develop the PySpark code for `k_ausd_geschaeftspartner.py` based on the design.
    *   **Language:** PySpark, BigQuery SQL (within PySpark)
5.  **Develop Airflow DAG (`r_ausd_geschaeftspartner_dag.py`):**
    *   **Action:** Create the Airflow DAG Python file using `DataprocSubmitJobOperator` to orchestrate the PySpark job. Incorporate parameter passing and error handling. Define `dag_id`, `start_date`, and `schedule` (if identified).
    *   **Language:** Python (Airflow)
6.  **BigQuery Schema Implementation:**
    *   **Action:** Create necessary BigQuery datasets and table schemas for `FOS-Tabelle` and ensure access to `DWH_VERTRAG_ID`.
    *   **Language:** BigQuery DDL
7.  **GCP Environment Configuration:**
    *   **Action:** Configure `BERT_DIR_ROOT` and any other required environment variables or secrets within the Cloud Composer environment. Set up Dataproc cluster and GCS buckets.
    *   **Language:** GCP Configuration (e.g., `gcloud`, Terraform)