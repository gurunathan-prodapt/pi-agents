# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh

## 1. Purpose & Scope
This ETL job is designed to prepare selected basic products (e.g., FAX, Data24) for BERT. Its primary function is to extract contract cache data from the Data Warehouse (DWH), process it to aggregate APN (Access Point Name) and contract reference information, and then make this data available in a target table for downstream consumption by BERT. The job processes data based on a given key date, handling restart values and ensuring proper logging and error handling.

## 2. Source Inventory
This job is composed of three interconnected files:

| File Name                                                                     | Technology   | Role                        | Complexity Tier | Automation Bucket | Notes                                                                                                                                                                                                                                                                                        |
| :---------------------------------------------------------------------------- | :----------- | :-------------------------- | :-------------- | :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh` | KornShell    | Main Orchestrator           | Medium (Inferred) | semi_auto         | Entry point script, handles parameter parsing (`-s` for key date, `-l` for restart value), environment setup, and invokes the core control script. Includes error handling via `f_alis_msgerr.ksh` and date utilities via `h_alis_date.ksh`.                               |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh` | KornShell    | Core Control Script         | Medium (Inferred) | semi_auto         | Invoked by `r_ausd_bp_ta_apn_vertrag.ksh`. Further parses job parameters (`-j` JobKennung, `-f` EintragsNr, `-s` Stichtag, `-l` Wiederanlaufwert). Sets up SQL*Plus execution environment and invokes `d_ausd_bp_ta_apn_vertrag.sql`. Contains commented-out `sed`, `sort`, `join` commands. |
| `vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql` | Oracle SQL/PLSQL | Data Transformation Logic   | Complex (Inferred) | semi_auto         | Contains the core business logic. It truncates and inserts data into `sof$ta_apn_vertrag` by processing records from `sof$ta_bpr_apn` within a PL/SQL loop, aggregating APN and contract references. References utility package `isbert_schema.DWPA_UTIL_SKRIPT`. |

*Note: Complexity tiers are inferred as `file_complexity` data was not available for these files. `semi_auto` is the automation bucket for both ksh scripts and the SQL script.*

## 3. Target Architecture
The migration target platform is Google Cloud Platform (GCP), specifically:
*   **BigQuery:** For all data storage and SQL-based transformations. The target tables will reside here.
*   **Cloud Composer / Apache Airflow:** For orchestration of the ETL workflow, replacing the KornShell scripts and UC4 scheduling.
*   **Cloud Storage:** For temporary staging of data if required, and for storing logs before ingestion into Cloud Logging.
*   **Cloud Logging:** For centralized logging and monitoring of job execution.

## 4. Data Flow & Lineage
The original job execution flow is:
1.  A UC4 job (e.g., `DW.BERT_STAMMDATEN_JP` via `DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml`) invokes `r_ausd_bp_ta_apn_vertrag.ksh`.
2.  `r_ausd_bp_ta_apn_vertrag.ksh` sets up the environment and passes parameters to `k_ausd_bp_ta_apn_vertrag.ksh`.
3.  `k_ausd_bp_ta_apn_vertrag.ksh` further prepares the context, including an SQL*Plus environment, and executes `d_ausd_bp_ta_apn_vertrag.sql`.
4.  `d_ausd_bp_ta_apn_vertrag.sql` performs the data manipulation:
    *   **Reads from:** `sof$ta_bpr_apn` and `isbert_schema.dwtk_meldungen` (to determine `v_datum`).
    *   **Writes to:** `sof$ta_apn_vertrag` (after truncation).
    *   **Interacts with:** `isbert_schema.DWPA_UTIL_SKRIPT` (e.g., `runstatement` for `TRUNCATE`).

In the target BigQuery environment, this flow will be:
1.  An Airflow DAG (replacing the UC4 scheduler) will trigger the ETL process.
2.  The Airflow DAG will contain tasks:
    *   To prepare and validate parameters (similar to `r_ausd_bp_ta_apn_vertrag.ksh` and `k_ausd_bp_ta_apn_vertrag.ksh`).
    *   To execute the migrated BigQuery SQL (derived from `d_ausd_bp_ta_apn_vertrag.sql`).
    *   Potentially to read from source tables (e.g., `sof$ta_bpr_apn` which would be migrated to BigQuery) and write to a target table (`sof$ta_apn_vertrag` also migrated to BigQuery).

## 5. Transformation Logic
The core transformation logic resides in `d_ausd_bp_ta_apn_vertrag.sql`:
1.  **Truncation:** The target table `sof$ta_apn_vertrag` is truncated using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_apn_vertrag')`.
2.  **Date Determination:** A substitution variable `v_datum` is derived from the `timecreated` in `isbert_schema.dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`. This implies a dependency on a prior job's execution metadata.
3.  **Data Aggregation and Insertion:**
    *   The script iterates through records in `sof$ta_bpr_apn`, ordered by `cntrct_id`.
    *   For each unique `cntrct_id`, it aggregates all associated `access_point_name` values and `cntrct_id_ref` values into comma-separated strings (`v_apn` and `v_cntrct_ref` respectively), ensuring the concatenated strings do not exceed 100 characters.
    *   Once all APNs and contract references for a `cntrct_id` are collected, a single row is inserted into `sof$ta_apn_vertrag` with the `cntrct_id`, the aggregated `v_apn`, and `v_cntrct_ref`. This is done both within the loop (when `cntrct_id` changes) and after the loop (for the last `cntrct_id`).
    *   Error handling within the loop sets `v_apn = ' '` if an exception occurs during aggregation.
4.  **Committing Changes:** Changes are committed to the database after each block of insertions.

**Migration Considerations:**
*   The PL/SQL `DECLARE...BEGIN...END` block with explicit looping and variable concatenation will need to be re-engineered. BigQuery is set-based, so this will likely translate to a `GROUP BY` operation with string aggregation functions (e.g., `STRING_AGG`).
*   The `TRUNCATE TABLE` operation will map directly to BigQuery's `TRUNCATE TABLE` or a `DELETE FROM ... WHERE 1=1` followed by an `INSERT`.
*   The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call will need to be replaced by direct BigQuery DDL/DML.
*   The logic to get `v_datum` from `dwtk_meldungen` will need to be re-implemented to read from a corresponding BigQuery audit/metadata table.

## 6. External Dependencies
### Legacy Environment:
*   **Oracle Database:** Source tables (`sof$ta_bpr_apn`, `isbert_schema.dwtk_meldungen`) and target table (`sof$ta_apn_vertrag`) reside in an Oracle database. The `lineage_edges` indicated `EXT:DATABASE` and `HOST:DWHDWH2P`.
*   **Utility Scripts:**
    *   `.dw_init`: Initializes environment variables.
    *   `f_alis_msgerr.ksh`: Error messaging framework.
    *   `h_alis_parameter.ksh`: Parameter parsing helper.
    *   `h_alis_date.ksh`: Date handling utilities.
    *   `h_alis_sqlplus.ksh`: SQL*Plus wrapper script for executing SQL.
    *   `gestern.ksh`: Determines yesterday's and today's dates.
*   **Operating System Utilities:** `cat`, `print`, `sed`, `sort`, `join` (although `sed`, `sort`, `join` are commented out in `k_ausd_bp_ta_apn_vertrag.ksh`, they are present in the script).
*   **UC4 Scheduler:** Orchestrates the execution of `r_ausd_bp_ta_apn_vertrag.ksh`.

### Target Environment Replacement:
*   **BigQuery:** All Oracle tables will be migrated to BigQuery.
*   **Cloud Composer / Airflow:** The UC4 scheduling and KornShell orchestration will be replaced by an Airflow DAG written in Python. This DAG will manage tasks like parameter passing, BigQuery job execution, and logging.
*   **Cloud Functions / Python Operators:** Helper shell scripts (`h_alis_parameter.ksh`, `h_alis_date.ksh`, `gestern.ksh`) will either be re-implemented as Python functions within the Airflow DAG or as separate Cloud Functions if their logic is complex or reusable across multiple DAGs.
*   **Cloud Logging:** `f_alis_msgerr.ksh` and other logging mechanisms will be replaced by direct logging to Cloud Logging from the Airflow tasks.
*   **Secret Manager:** Database connection details and other sensitive configurations will be stored and retrieved from Secret Manager.

## 7. Unresolved / Risks
*   **Missing `file_complexity` data:** For all three files, `file_complexity` data was not available. Inferred complexity tiers were used for the design document.
*   **`UNRESOLVED:BERT_LOG.KSH`:** The `lineage_edges` showed a reference to `UNRESOLVED:BERT_LOG.KSH` from an XML job (`DW.BERT_LOG.xml`). While not directly invoked by our seed script, it indicates a related component that might need attention during a broader migration.
*   **Commented-out Post-processing:** `k_ausd_bp_ta_apn_vertrag.ksh` contains commented-out `sed`, `sort`, `join` commands for post-processing `.dat` files. This suggests a potential requirement for file manipulation that is currently inactive but could become active or indicative of a pattern in other similar jobs. If these become active, they would need to be migrated to PySpark, Dataflow, or appropriate BigQuery data loading strategies.
*   **`AL??` Comments:** Several lines are commented with `AL??` (e.g., `FOSHoleLadedatum`, `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`). These refer to potentially unused or conditional functionality related to a FOS (Forderungsscoring) job management system. Clarity is needed on whether this functionality is still required or can be safely ignored.

## 8. Build Plan
The migration build plan will proceed in the following steps:

1.  **Migrate Oracle Tables to BigQuery (DWH tables):**
    *   Migrate `sof$ta_bpr_apn` to `project.dataset.sof_ta_bpr_apn`.
    *   Migrate `isbert_schema.dwtk_meldungen` to `project.dataset.isbert_dwtk_meldungen`.
    *   Create the target table `sof$ta_apn_vertrag` as `project.dataset.sof_ta_apn_vertrag` in BigQuery, defining appropriate partitioning and clustering keys.
    *   *Language:* BigQuery DDL.

2.  **Rewrite `d_ausd_bp_ta_apn_vertrag.sql` to BigQuery SQL:**
    *   Convert the PL/SQL block into a standard BigQuery SQL query using `STRING_AGG` for concatenations and `GROUP BY` for aggregation.
    *   Replace `TRUNCATE TABLE` with BigQuery equivalent.
    *   Adjust date functions and variable handling to BigQuery syntax.
    *   Integrate the logic for `v_datum` into the main BigQuery query or a preceding query.
    *   *Language:* BigQuery SQL.

3.  **Design and Implement Airflow DAG:**
    *   Create a new Airflow DAG to orchestrate the entire process.
    *   **Task 1: Parameter Validation:** Replace the ksh parameter parsing and validation with Python logic in an Airflow task.
    *   **Task 2: Date Calculation:** Re-implement date calculation logic (from `h_alis_date.ksh` and `gestern.ksh`) in Python within an Airflow task or a reusable helper function.
    *   **Task 3: Execute BigQuery Transformation:** Use `BigQueryOperator` or `PythonOperator` calling `google-cloud-bigquery` client library to execute the rewritten BigQuery SQL from step 2.
    *   **Task 4: Logging and Monitoring:** Integrate Cloud Logging for all task outputs and use Airflow's built-in monitoring features.
    *   *Language:* Python (Airflow DAG).

4.  **Migrate Utility Scripts:**
    *   Review `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`.
    *   Identify reusable logic and rewrite them as Python functions or custom Airflow operators/hooks if needed for other DAGs. Otherwise, integrate their functionality directly into the DAG Python code.
    *   *Language:* Python.