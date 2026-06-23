# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

## 1. Purpose & Scope

This document outlines the migration design for the legacy KornShell script `r_ausd_v_ta_cntrct_crs3.ksh` to the Google Cloud Platform, targeting BigQuery for data processing and potentially Airflow for orchestration.

The original purpose of `r_ausd_v_ta_cntrct_crs3.ksh` is to act as a wrapper script for the synchronization of contract data into the `ta_cntrct_crs3` table. Its responsibilities include:
-   Parsing command-line parameters (`-h`, and general `getopts` defined `s:l:`).
-   Setting up the execution environment by sourcing shared utility scripts.
-   Implementing robust error handling and logging mechanisms.
-   Orchestrating the execution of a core data processing script, `k_ausd_v_ta_cntrct_crs3.ksh`, which is responsible for the actual data synchronization logic.
-   Reporting the job's completion status.

The scope of this migration covers transforming the orchestration, parameter handling, logging, and error management of this wrapper script, along with outlining the integration point for the core data processing.

## 2. Source Inventory

| File Path                                                                   | Technology | Complexity Tier | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| :-------------------------------------------------------------------------- | :--------- | :-------------- | :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh` | KornShell  | medium          | semi_auto         | This KornShell script acts as a wrapper for synchronizing contract data into the 'ta_cntrct_crs3' table. It handles parameter parsing, environment setup, error logging, and orchestrates the execution of a core data processing script. It relies on several sourced utility scripts for environment initialization, error handling, parameter parsing, and date functions. The script then invokes `k_ausd_v_ta_cntrct_crs3.ksh` with specific job identifiers and logs all output. |

## 3. Target Architecture

The migrated job will leverage Google Cloud services, specifically BigQuery for data storage and processing, and likely Cloud Composer (managed Airflow) for orchestration.

-   **Orchestration:** The wrapper script's control flow, parameter handling, and job execution logic will be re-implemented as a Python-based Airflow DAG in Cloud Composer.
-   **Core Data Processing:** The functionality of `k_ausd_v_ta_cntrct_crs3.ksh` will be migrated to BigQuery SQL, Python scripts utilizing the BigQuery client, or a Dataform pipeline, depending on its complexity and nature. The exact implementation will be determined after analyzing `k_ausd_v_ta_cntrct_crs3.ksh`.
-   **Logging and Monitoring:** Standard Python logging within the Airflow DAG will be configured to output to Google Cloud Logging. Cloud Monitoring will be used for alerts and operational visibility.
-   **Environment & Configuration:** Environment variables and parameters will be managed via Airflow connections, variables, and DAG parameters.
-   **Data Storage:** The `ta_cntrct_crs3` table will be provisioned as a native BigQuery table.

## 4. Data Flow & Lineage

The data flow primarily involves the orchestration of the core data synchronization process.

1.  **Airflow DAG Execution:** An Airflow DAG (migrated from `r_ausd_v_ta_cntrct_crs3.ksh`) is triggered, potentially on a schedule or manual request.
2.  **Environment Setup & Parameter Handling:** The DAG initializes necessary environment variables and processes any runtime parameters, mirroring the legacy script's `.dw_init` sourcing and `getopts` logic.
3.  **Logging & Error Handling Setup:** Cloud Logging is configured, and Python-based error handling mirrors the `DWMSG_*` functions and `trap` mechanisms.
4.  **Core Data Synchronization Task:** The Airflow DAG executes a task that triggers the migrated `k_ausd_v_ta_cntrct_crs3.ksh` logic. This task will pass the `JobKennung` and `DW_EintragsNr` equivalents as parameters.
5.  **Data Ingestion/Transformation:** The migrated `k_ausd_v_ta_cntrct_crs3.ksh` logic will:
    -   Read source data (specifics unknown at this stage).
    -   Perform data transformations/synchronization.
    -   Write/update data into the target BigQuery table `ta_cntrct_crs3`.
6.  **Status Reporting:** The Airflow DAG captures the success or failure of the core data synchronization task and records the overall job status in Cloud Logging.

## 5. Transformation Logic

The `r_ausd_v_ta_cntrct_crs3.ksh` script primarily provides control flow rather than direct data transformation. The migration will focus on replicating this control flow in Airflow:

-   **Wrapper Script to Airflow DAG:**
    -   The parameter parsing logic (`getopts`, `-h`) will be translated into Airflow DAG parameters or configuration.
    -   Environment sourcing (`. $HOME/.dw_init`) will be replaced by Airflow environment variables or Python-based configurations.
    -   The `DWMSG_*` logging and error handling functions will be re-implemented using Python's logging module, directed to Cloud Logging, and integrated into Airflow task failure/retry mechanisms.
    -   The `trap` statements will be implicitly handled by Airflow's robust task execution and error handling.
    -   The core invocation `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr}` will become an Airflow task that triggers the migrated `k_ausd_v_ta_cntrct_crs3.ksh` logic, passing the relevant parameters.
-   **Data Transformation:** The actual data transformation and synchronization logic within the `k_ausd_v_ta_cntrct_crs3.ksh` script itself is subject to a separate detailed design, but it will be implemented using BigQuery SQL, Python with BigQuery client libraries, or Dataform.

## 6. External Dependencies

The following external dependencies were identified:

-   **Sourced Utility Scripts:**
    -   `. $HOME/.dw_init`: Provides environment variables and initial setup.
        -   **Replacement:** Configuration in Airflow environment, or a dedicated Python module for environment initialization.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging and logging functions.
        -   **Replacement:** Python logging module integrated with Google Cloud Logging. Custom Python functions to replicate specific error reporting logic.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utilities.
        -   **Replacement:** Airflow DAG parameters and Python's `argparse` for specific script inputs.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities.
        -   **Replacement:** Python's `datetime` module.
-   **Core Data Processing Script:**
    -   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh`: This script performs the actual contract data synchronization.
        -   **Replacement:** This script will need to be analyzed and migrated to a BigQuery-compatible solution (e.g., BigQuery SQL, Python script, Dataform).
-   **Data Target:**
    -   `ta_cntrct_crs3`: The target table for contract data synchronization.
        -   **Replacement:** A new BigQuery table, ensuring schema and data type compatibility.

## 7. Unresolved / Risks

-   **Unknown Logic of `k_ausd_v_ta_cntrct_crs3.ksh`:** The most significant unresolved item is the exact functionality, data sources, and transformation logic contained within the core script `k_ausd_v_ta_cntrct_crs3.ksh`. A detailed analysis of this script is critical to complete the migration design.
-   **Complexity of Sourced Utilities:** The internal logic of the sourced `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` scripts is not fully known. Complex logic within these might require more involved re-engineering.
-   **Implicit Dependencies:** There might be implicit dependencies or side effects in the legacy environment that are not immediately apparent from the script itself. These could include specific system configurations or external tools used by the core script.
-   **"semi_auto" Automation Bucket:** The `semi_auto` designation suggests that some manual intervention or re-engineering will be required, reinforcing the need for careful review of the core script and utility functions.

## 8. Build Plan

The build plan will proceed in an iterative manner, with a strong focus on analyzing the core data processing script once this wrapper is designed.

1.  **BigQuery Target Table Definition:**
    -   **Action:** Create the `ta_cntrct_crs3` table in BigQuery.
    -   **Language:** DDL (Data Definition Language)
    -   **Note:** Schema will be derived from the legacy `ta_cntrct_crs3` table schema.

2.  **Migrate Utility Functions to Python:**
    -   **Action:** Create Python modules for environment initialization, parameter handling, and date utilities, replicating the functionality of `.dw_init`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`.
    -   **Language:** Python
    -   **Action:** Implement a Python logging module that mirrors the `DWMSG_*` error handling and logging, integrated with Google Cloud Logging.
    -   **Language:** Python

3.  **Design and Migrate `k_ausd_v_ta_cntrct_crs3.ksh` (Core Script):**
    -   **Action:** Analyze the `k_ausd_v_ta_cntrct_crs3.ksh` script to understand its data sources, transformation logic, and target writes.
    -   **Action:** Based on the analysis, design and implement the core data synchronization logic using BigQuery SQL, Python with BigQuery client, or Dataform.
    -   **Language:** BigQuery SQL / Python / Dataform

4.  **Develop Airflow DAG for Orchestration:**
    -   **Action:** Create an Airflow DAG that encapsulates the wrapper logic of `r_ausd_v_ta_cntrct_crs3.ksh`.
    -   **Language:** Python
    -   **Components:**
        -   A task to handle parameter ingestion (if any).
        -   Tasks to call the migrated core data processing logic (from step 3), passing necessary parameters.
        -   Error handling and retry mechanisms.
        -   Logging to Cloud Logging.
        -   Success/failure notifications.

5.  **Testing and Validation:**
    -   **Action:** Develop unit and integration tests for the migrated utility functions, core data processing logic, and the Airflow DAG.
    -   **Action:** Perform data validation to ensure the BigQuery `ta_cntrct_crs3` table is correctly populated and synchronized.