# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

## 1. Purpose & Scope
This document outlines the migration strategy for the ETL job orchestrated by `r_ausd_v_ta_disc_zusgf.ksh` to Google Cloud Platform (GCP), targeting BigQuery as the data warehouse.

The original job, identified by `run_id: 5af228f1-3847-4cc6-9310-ed82ed19407c`, is a KornShell script (`r_ausd_v_ta_disc_zusgf.ksh`) that acts as a wrapper. Its primary purpose is to orchestrate the data reconciliation process for the `ta_disc_zusgf` table. It handles parameter parsing, environment setup, logging, and error handling, and then invokes a core processing script (`k_ausd_v_ta_disc_zusgf.ksh`). This core script, in turn, executes an Oracle PL/SQL script (`d_ausd_v_ta_disc_zusgf.sql`) that performs the actual data transformation and loading into the `SOF$TA_DISC_ZUSGF` table. The entire process is triggered by an upstream UC4 job (`DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml`).

The scope of this migration includes the orchestration logic from the KornShell scripts and the data transformation logic from the Oracle PL/SQL script, with the goal of replatforming to GCP.

## 2. Source Inventory
This job consists of three main components:

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh**
    *   **Technology**: KornShell
    *   **Category**: Shell (orchestration)
    *   **Complexity Tier**: Medium
    *   **Automation Bucket**: Retire (B0)
    *   **Summary**: Wrapper script managing execution, environment, logging, and error handling, invoking `k_ausd_v_ta_disc_zusgf.ksh`.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh**
    *   **Technology**: KornShell
    *   **Category**: Shell (orchestration)
    *   **Complexity Tier**: Medium
    *   **Automation Bucket**: Semi-Auto (B2)
    *   **Summary**: Control script managing job execution, handling parameters, and orchestrating the execution of `d_ausd_v_ta_disc_zusgf.sql` to update `ta_disc_zusgf`.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql**
    *   **Technology**: Oracle PL/SQL
    *   **Category**: SQL (ETL)
    *   **Complexity Tier**: Complex
    *   **Automation Bucket**: Manual (B3)
    *   **Summary**: Defines custom object types and a PL/SQL package with a pipelined table function to concatenate discount information, used to populate `SOF$TA_DISC_ZUSGF`.

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services:

*   **Orchestration**: Google Cloud Composer (Apache Airflow) for scheduling and managing the end-to-end workflow.
*   **Data Warehouse**: Google BigQuery for storing and querying the `ta_disc_zusgf` data.
*   **Data Transformation**:
    *   Core SQL logic (from `d_ausd_v_ta_disc_zusgf.sql`) will be refactored into BigQuery SQL.
    *   The complex Oracle PL/SQL constructs, particularly the pipelined table function, will require rewriting. The analysis suggests **PySpark on Dataproc Serverless** for this rewrite, indicating a potential hybrid approach where some transformation logic is handled by Spark, and the final load by BigQuery SQL.
*   **Logging and Monitoring**: Cloud Logging and Cloud Monitoring, integrated with Airflow for centralized observability.
*   **Environment Management**: Environment variables and configurations will be managed within the Airflow environment or via secret managers.

## 4. Data Flow & Lineage
The migrated job will follow this conceptual data flow and execution order:

1.  **UC4 Job (Original)**: The original UC4 job `DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml` which invokes `r_ausd_v_ta_disc_zusgf.ksh` will be migrated to an **Airflow DAG**.
2.  **Airflow DAG (New Orchestrator)**: This DAG will encapsulate the logic of `r_ausd_v_ta_disc_zusgf.ksh` and `k_ausd_v_ta_disc_zusgf.ksh`.
    *   **Environment Setup**: Initial tasks in the DAG will handle environment variable setup, equivalent to `. $HOME/.dw_init` and sourcing other utility scripts.
    *   **Parameter Handling**: Parameters (e.g., `p_JobKennung`, `p_EintragsNr`) will be passed as Airflow DAG parameters or XComs.
    *   **Pre-processing/Housekeeping**: Tasks for `TRUNCATE TABLE sof$ta_disc_zusgf` (currently handled by `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) will be implemented as BigQuery operators or Python operators executing DDL.
    *   **Core Transformation (PySpark/BigQuery)**:
        *   The complex Oracle pipelined table function logic from `d_ausd_v_ta_disc_zusgf.sql` will be rewritten. A PySpark job running on Dataproc Serverless is the recommended target for this part, which will transform `SOF$TA_DISCOUNT` and `isbert_schema.dwtk_meldungen` data.
        *   The output of the PySpark job (transformed discount information) will then be loaded into a staging table in BigQuery.
        *   A subsequent BigQuery SQL task will perform the final `INSERT INTO sof$ta_disc_zusgf` using the data from the staging table, potentially joining with other BigQuery tables.
    *   **Logging & Error Handling**: Airflow's native logging and error handling mechanisms will replace the custom `DWMSG_` functions. Alerts will be configured via Cloud Monitoring and Pub/Sub Notifications.
    *   **Post-processing**: Tasks for updating job status (equivalent to `DWMSG_SetzeStatusOK`) will be integrated.

**Lineage (Conceptual):**
*   **Input Tables**: `isbert_schema.dwtk_meldungen` (Oracle), `SOF$TA_DISCOUNT` (Oracle)
    *   *Migration Note*: These source tables must first be migrated to BigQuery.
*   **Transformation Logic**: `d_ausd_v_ta_disc_zusgf.sql` (Oracle PL/SQL with pipelined function)
    *   *Migration Note*: Rewritten as PySpark on Dataproc Serverless + BigQuery SQL.
*   **Output Table**: `SOF$TA_DISC_ZUSGF` (Oracle)
    *   *Migration Note*: Migrated to `SOF_TA_DISC_ZUSGF` in BigQuery.

## 5. Transformation Logic
The transformation logic from `d_ausd_v_ta_disc_zusgf.sql` is critical and presents the most significant migration challenge due to Oracle-specific features.

*   **Oracle Object Types and Pipelined Table Functions**:
    *   The `CREATE TYPE sof$ty_o_discount`, `CREATE TYPE sof$ty_t_discount`, and the `sof$sp_discount_functions` package with its `concat_discounts` pipelined table function are highly Oracle-specific.
    *   **Migration**: These constructs will be rewritten using **PySpark on Dataproc Serverless**. The PySpark job will read data from the BigQuery equivalent of `SOF$TA_DISCOUNT`, perform the grouping and concatenation logic (similar to the pipelined function's behavior), and then output the results to a temporary/staging BigQuery table. This addresses the `Stored_Proc_Transform` pattern.
*   **Bulk Load (INSERT SELECT)**:
    *   The `INSERT INTO sof$ta_disc_zusgf SELECT ... JOIN TABLE(sof$sp_discount_functions.concat_discounts(CURSOR(...)))` statement.
    *   **Migration**: Once the complex concatenation logic is handled by PySpark, the final `INSERT INTO` will be a straightforward **BigQuery SQL** statement, potentially reading from the PySpark job's output table and performing any final joins or transformations. This addresses the `Bulk_Load_Stage_To_Final` pattern.
*   **Dynamic SQL Substitution**:
    *   The use of `DEFINE v_carmen` and `SELECT ... NEW_VALUE v_datum` for substitution variables.
    *   **Migration**: These dynamic elements will be handled by **Cloud Composer (Airflow)**. Variables can be passed as DAG parameters, Airflow variables, or retrieved dynamically within Python operators. This addresses the `Dynamic_SQL_substitution` pattern.
*   **Housekeeping**:
    *   `DROP TYPE` commands and `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_disc_zusgf')`.
    *   **Migration**: `DROP TYPE` commands are irrelevant in BigQuery. The `TRUNCATE TABLE` operation will be performed using a BigQuery `TRUNCATE TABLE` DDL statement executed by an Airflow BigQuery operator. This addresses the `Housekeeping` pattern.

## 6. External Dependencies
The current job has the following external dependencies:

*   **Upstream Trigger (UC4)**: The job is invoked by an UC4 XML job (`DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml`).
    *   **Replacement**: This UC4 job will be migrated to a **Cloud Composer (Airflow) DAG**. This DAG will become the primary orchestrator, scheduling and managing the execution of the transformed Python/BigQuery components.
*   **Helper Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`)**: These are sourced by the KornShell scripts for environment setup, error handling, parameter parsing, date utilities, and SQLPlus interaction.
    *   **Replacement**:
        *   Environment setup (`.dw_init`): Replaced by Airflow environment configurations, environment variables, or Python code within Airflow operators.
        *   Error handling (`f_alis_msgerr.ksh`, `DWMSG_` functions): Replaced by Cloud Logging and Cloud Monitoring for error reporting and alerting. Airflow's built-in error handling and callbacks will manage task failures.
        *   Parameter parsing (`h_alis_parameter.ksh`): Replaced by Airflow's parameter passing mechanisms (e.g., DAG parameters).
        *   Date utilities (`h_alis_date.ksh`): Replaced by Python's `datetime` module or Airflow's date macros.
        *   SQLPlus interaction (`h_alis_sqlplus.ksh`, `starteSQLSkript`): Replaced by Airflow's BigQuery operators for direct SQL execution or Python operators for invoking PySpark jobs.
*   **Oracle Database (`isbert_schema.dwtk_meldungen`, `SOF$TA_DISCOUNT`, `SOF$TA_DISC_ZUSGF`)**: These are the source and target tables for the SQL script.
    *   **Replacement**: These tables must be migrated to **BigQuery**. The `isbert_schema.dwtk_meldungen` and `SOF$TA_DISCOUNT` will become source tables in BigQuery, and `SOF$TA_DISC_ZUSGF` will be the target table in BigQuery.
*   **Oracle PL/SQL Utility Package (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`)**: Used for `TRUNCATE TABLE`.
    *   **Replacement**: Direct **BigQuery DDL** commands executed via an Airflow BigQuery operator.

## 7. Unresolved / Risks
*   **Complex PL/SQL Rewrite (High Risk)**: The pipelined table function in `d_ausd_v_ta_disc_zusgf.sql` is a complex Oracle-specific feature. Its rewrite into PySpark will require thorough understanding of the original business logic and extensive testing. This is identified as a "Manual" migration bucket item with "High" estimated effort.
*   **Data Volume and Performance**: The original script uses `PARALLEL DML` and hints. The PySpark and BigQuery implementations must be designed with performance considerations for large datasets.
*   **Testing**: Comprehensive data validation and reconciliation will be required post-migration to ensure data integrity and functional equivalence.
*   **Cost Optimization**: Dataproc Serverless and BigQuery usage should be monitored and optimized for cost efficiency.

## 8. Build Plan
The migration will proceed in the following ordered steps:

1.  **Migrate Source Data to BigQuery**:
    *   Migrate Oracle tables `isbert_schema.dwtk_meldungen` and `SOF$TA_DISCOUNT` to BigQuery datasets and tables.
    *   Create the target `SOF_TA_DISC_ZUSGF` table in BigQuery.
    *   **Language/Tool**: GCP Data Migration Service, BigQuery Data Transfer Service, or custom data loading scripts (Python/Go).

2.  **Rewrite Oracle PL/SQL Pipelined Function to PySpark**:
    *   Develop a PySpark job that replicates the logic of the `concat_discounts` pipelined table function from `d_ausd_v_ta_disc_zusgf.sql`.
    *   This PySpark job will read from the BigQuery equivalent of `SOF$TA_DISCOUNT` and output to a staging BigQuery table.
    *   **Language/Tool**: Python (PySpark), Dataproc Serverless.

3.  **Develop Core BigQuery Transformation SQL**:
    *   Write BigQuery SQL statements that perform the final `INSERT INTO SOF_TA_DISC_ZUSGF` using data from the PySpark staging table and any other necessary BigQuery sources.
    *   **Language/Tool**: BigQuery SQL, Dataform for version control and orchestration of SQL assets.

4.  **Create Airflow DAG for Orchestration**:
    *   Design and implement an Airflow DAG in Python.
    *   Tasks in the DAG will include:
        *   Environment setup (if necessary).
        *   BigQuery operator for pre-processing DDL (e.g., `TRUNCATE TABLE SOF_TA_DISC_ZUSGF`).
        *   Dataproc Serverless hook/operator to execute the PySpark job (Step 2).
        *   BigQuery operator to execute the core transformation SQL (Step 3).
        *   Tasks for logging, monitoring, and error handling.
    *   **Language/Tool**: Python (Airflow DAGs), Cloud Composer.

5.  **Replace UC4 Scheduling**:
    *   Configure the newly created Airflow DAG to run on the desired schedule, replacing the UC4 job.
    *   **Language/Tool**: Cloud Composer scheduler.

6.  **Implement Logging and Alerting**:
    *   Configure Cloud Logging and Cloud Monitoring to capture logs and metrics from the Airflow DAG and associated BigQuery/Dataproc jobs.
    *   Set up alerts for job failures or performance issues using Cloud Monitoring and Pub/Sub.
    *   **Language/Tool**: Cloud Logging, Cloud Monitoring, Pub/Sub.

7.  **Testing and Validation**:
    *   Perform unit, integration, and end-to-end testing.
    *   Conduct data validation against the legacy system to ensure data accuracy.
    *   **Language/Tool**: Python (pytest), BigQuery.

8.  **Deployment**:
    *   Deploy the Airflow DAGs and PySpark code to production Cloud Composer and Cloud Storage/Artifact Registry.
    *   **Language/Tool**: Cloud Build, CI/CD pipelines.