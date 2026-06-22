# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier.ksh

## 1. Purpose & Scope
This job, assembled from a single component, serves as a control script for a data processing job related to `ta_barrier`. Its primary purpose is to orchestrate the execution of an SQL script (`d_ausd_v_ta_barrier.sql`), which populates a target table (`sof$ta_barrier`). The shell script handles parameter parsing, environment setup, error checking, and invokes the SQL script. Key functionalities include:
- Ignoring active jobs to prevent conflicts.
- Calling the SQL script (`d_ausd_v_ta_barrier.sql`) to perform data manipulation.
- Registering the job's execution in a job tracking system (implied).
- Deactivating old active jobs.

## 2. Source Inventory
The primary component of this job is a KornShell script, `k_ausd_v_ta_barrier.ksh`, which acts as an orchestrator for an Oracle SQL script, `d_ausd_v_ta_barrier.sql`.

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier.ksh`
- **Technology:** KornShell
- **Summary:** This script acts as a control script for a data processing job, handling parameter parsing, environment setup, error checking, and orchestrating the execution of a SQL script. It defines critical variables such as `v_TabName`, `Name_SQLskript`, `tmpFile`, `p_JobKennung`, and `p_EintragsNr`.
- **Referenced Scripts:** `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`
- **Referenced SQL Script:** `d_ausd_v_ta_barrier.sql`
- **Referenced Table (Logical):** `ta_barrier`
- **Complexity Tier:** Not available (data missing)
- **Automation Bucket:** Not available (data missing)

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_barrier.sql`
- **Technology:** Oracle SQL (PL/SQL)
- **Summary:** This SQL script performs data transformation and loading into the `sof$ta_barrier` table. It determines a date parameter (`v_datum`) from `isbert_schema.dwtk_meldungen` and then populates `sof$ta_barrier` by joining and transforming data from various `cds$ta_` tables (e.g., `cds$ta_barrier`, `cds$ta_barrier_class`, `cds$ta_barrier_kind`, `cds$ta_care_description`). It utilizes a DB link `v_carmen`.

## 3. Target Architecture
The target architecture in BigQuery will involve:
- **BigQuery Stored Procedure:** The core logic of `k_ausd_v_ta_barrier.ksh` and the orchestration of the SQL logic will be refactored into a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag_control`). This stored procedure will handle parameter validation, job control logic, and execute the SQL transformation.
- **BigQuery Tables:**
    - **Target Table:** `sof_ta_barrier` (or `ta_barrier` within a dedicated dataset) will be created in BigQuery to store the processed data.
    - **Source Tables:** The Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_barrier`, `cds$ta_barrier_class`, `cds$ta_barrier_kind`, `cds$ta_care_description`) will be ingested or replicated into corresponding BigQuery tables (e.g., `source_system_dataset.dwtk_meldungen`, `source_system_dataset.cds_ta_barrier`, etc.).
    - **Control/Logging Tables:** New BigQuery tables will be introduced for job tracking, logging, and parameter management to replace the implicit job management and temporary file handling of the legacy system.
- **Orchestration:** External orchestration (e.g., Cloud Composer/Airflow, Google Cloud Workflows) might be required if the `starteSQLSkript` or other sourced KSH scripts involve complex external system interactions or scheduling logic that cannot be fully encapsulated within a BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The current data flow is as follows:
1. **Initiation:** The `k_ausd_v_ta_barrier.ksh` shell script is invoked, likely by a scheduler or another control script, with parameters `p_JobKennung` and `p_EintragsNr`.
2. **Environment Setup & Parameter Handling:** The shell script sources several utility `.ksh` scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) for environment initialization, error handling, date functions, parameter parsing, and SQL*Plus utilities. It parses `p_JobKennung` and `p_EintragsNr` and performs basic validation.
3. **SQL Script Invocation:** The shell script calls the `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) which then executes `d_ausd_v_ta_barrier.sql`.
4. **SQL Data Processing (within Oracle):**
    - `v_datum` Determination: `d_ausd_v_ta_barrier.sql` queries `isbert_schema.dwtk_meldungen` to determine a critical date (`v_datum`).
    - Target Truncation: The script truncates the `sof$ta_barrier` table.
    - Data Transformation and Load: Data is selected from `cds$ta_barrier`, `cds$ta_barrier_class`, `cds$ta_barrier_kind`, and `cds$ta_care_description` (accessed via `v_carmen` DB link), joined, and transformed using `NVL`, `DECODE`, `GREATEST`, and `CASE WHEN` clauses. The result is inserted into `sof$ta_barrier`.
    - Record Count: The `starteSQLSkript` mechanism implicitly or explicitly captures the number of records processed, which the shell script then reads from a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_barrier_$$.tmp`) into `v_records`.
5. **Completion:** The shell script prints a completion message.

In BigQuery, this flow will be:
1. **Initiation:** A scheduling tool (e.g., Cloud Composer) invokes the BigQuery Stored Procedure `r_ausd_vertrag_control` with required parameters.
2. **Parameter Validation & Job Control:** The Stored Procedure validates input parameters and interacts with BigQuery control tables for job tracking and active job management.
3. **`v_datum` Determination:** The `v_datum` logic will be migrated to BigQuery SQL, querying the ingested `dwtk_meldungen` table.
4. **Data Transformation and Load:** The core SQL logic from `d_ausd_v_ta_barrier.sql` will be translated into BigQuery SQL. This will involve:
    - Truncating the BigQuery target table `sof_ta_barrier`.
    - Performing the `INSERT INTO ... SELECT FROM ...` operation, joining the ingested `cds_ta_barrier`, `cds_ta_barrier_class`, `cds_ta_barrier_kind`, and `cds_ta_care_description` tables.
    - The `NVL`, `DECODE`, `GREATEST`, and `CASE WHEN` functions will be translated to their BigQuery equivalents (`IFNULL`, `CASE` expressions, `GREATEST`).
5. **Record Count & Logging:** The number of processed records will be captured directly within the BigQuery Stored Procedure using `SELECT COUNT(*) INTO` and logged to BigQuery logging tables.

## 5. Transformation Logic
The primary transformation logic resides within `d_ausd_v_ta_barrier.sql`. This script performs the following key steps:
1. **`v_datum` Calculation:**
   - `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';`
   - This determines a cutoff date based on the maximum `timecreated` for a specific job in `dwtk_meldungen`. This will be translated to BigQuery's `FORMAT_DATE` and `IFNULL` functions, querying the ingested `dwtk_meldungen` table.
2. **Target Table Truncation:**
   - `TRUNCATE TABLE sof$ta_barrier;`
   - In BigQuery, this directly translates to `TRUNCATE TABLE \`project.dataset.sof_ta_barrier\`;` or a `DELETE FROM` statement if a physical truncate is not available/desired for partitioned tables.
3. **Data Insertion and Transformation:**
   - The `INSERT INTO sof$ta_barrier (...) SELECT ... FROM cds$ta_barrier b, cds$ta_barrier_class bc, cds$ta_barrier_kind bk, cds$ta_care_description dk ... WHERE ...` statement is the core transformation.
   - **Column Mappings & Transformations:**
     - `cntrct_id`, `barrier_kind_id`, `barrier_init_cv`, `barrier_reason_cv`, `bfc_age` (calculated as `GREATEST(b.insert_at, bc.insert_at)`) are directly mapped.
     - `sperrart`: Derived from `dk.cds_description`.
     - `sperr_beginn`: `nvl(b.net_barr_on_date, b.valid_from)`
     - `sperr_ende`: `nvl(b.net_barr_off_date, b.valid_to)`
     - `sperrgrund`: A complex `DECODE` statement based on `bc.barrier_reason_cv` mapping numeric codes to descriptive strings (e.g., 'Kartenverlust', 'Kundenwunsch', 'Betreiberinterne Sperre'). This will be translated to a `CASE` expression in BigQuery.
     - `ist_stillegung`: `CASE WHEN bc.closure = 1 THEN 1 ELSE 0 END`. This directly translates to BigQuery's `CASE` expression.
   - **Join Conditions:** The tables are joined on `b.barrier_class_id = bc.barrier_class_id`, `bk.barrier_kind_id = bc.barrier_kind_id`, and `dk.cds_description_id = bk.cds_description_id`. These will be translated to standard `JOIN` clauses in BigQuery.
   - **Filtering Conditions:** The `WHERE` clause includes date-based filtering using `TO_DATE(\'&v_datum\',\'YYYYMMDD\')` and `is_production = 1`. These will be translated to BigQuery date functions and standard filtering.

## 6. External Dependencies
The main external dependency is an **Oracle Database**.
- **`isbert_schema.dwtk_meldungen`**: This table is used to derive the `v_datum` parameter, which is crucial for data filtering.
- **`cds$ta_barrier`, `cds$ta_barrier_class`, `cds$ta_barrier_kind`, `cds$ta_care_description`**: These tables are the primary source for the data transformation, accessed through a DB link (`v_carmen`).

**Migration Strategy for External Dependencies:**
- **Data Ingestion:** Data from `isbert_schema.dwtk_meldungen` and all `cds$ta_` tables (including data from `v_carmen` DB link) must be continuously ingested or replicated into dedicated BigQuery datasets/tables.
    - **Method:** Options include batch loads (e.g., using Cloud Data Fusion, Dataflow, or custom scripts), change data capture (CDC) solutions, or BigQuery Data Transfer Service if direct connectors are available.
    - **Naming Convention:** Ingested tables should follow a clear naming convention (e.g., `oracle_raw.dwtk_meldungen`, `oracle_raw.cds_ta_barrier`).
- **DB Link (`v_carmen`) Replacement:** The concept of a DB link will be replaced by direct access to the ingested BigQuery tables. The `cds$ta_` tables accessed via `v_carmen` are assumed to be part of the same Oracle source system and will be ingested into BigQuery.

## 7. Unresolved / Risks
- **Missing Metadata:** The `file_complexity` and `automation_rate` data for `k_ausd_v_ta_barrier.ksh` was not available. This prevents a detailed assessment of migration effort and potential automation rates.
- **Utility Script Logic (`h_alis_sqlplus.ksh`, etc.):** The exact functionalities of `starteSQLSkript` (within `h_alis_sqlplus.ksh`) and other sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`) are not fully detailed. Their migration to BigQuery (e.g., error handling mechanisms, date calculations, parameter validation) needs a thorough analysis to ensure all edge cases are covered. Some of these might be implemented as helper functions in the BigQuery Stored Procedure, while complex OS-level orchestration might require Cloud Composer/Workflows.
- **Job Tracking/Control (`p_JobKennung`, `p_EintragsNr`):** The interaction with an implied "job table" for ignoring active jobs and deactivating old ones needs to be mapped to a BigQuery-based job control mechanism (e.g., a dedicated BigQuery table for job status and metadata).
- **Temporary File (`tmpFile`) Handling:** The shell script uses a temporary file to store record counts. This will be replaced by BigQuery scripting variables or direct logging to a BigQuery table.
- **Oracle-specific Functions/Syntax:** While the main SQL has been analyzed, potential subtle Oracle-specific syntax or behavior (e.g., `DEFINE` variables, `COLUMN ... new_value`, `WHENEVER SQLERROR`, `START ../trace.sql.cfg`, `SPOOL`) within `d_ausd_v_ta_barrier.sql` and `trace.sql.cfg` (if its content was critical) need careful review and translation to BigQuery equivalents or alternative BigQuery practices (e.g., logging, error handling).
- **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`**: The call to `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_barrier')` is an Oracle PL/SQL call. While `TRUNCATE TABLE` has a direct BigQuery equivalent, the `DWPA_UTIL_SKRIPT` context implies a utility package that might perform additional logic or logging. This interaction needs to be understood to ensure a faithful migration.

## 8. Build Plan
The migration build plan will consist of the following ordered steps:

1.  **Source Data Ingestion into BigQuery:**
    *   **Task:** Set up continuous data ingestion or replication for `isbert_schema.dwtk_meldungen`, `cds$ta_barrier`, `cds$ta_barrier_class`, `cds$ta_barrier_kind`, and `cds$ta_care_description` from the Oracle source system into a BigQuery dataset (e.g., `oracle_raw`).
    *   **Language/Tool:** Cloud Data Fusion, Dataflow, BigQuery Data Transfer Service, or custom Python/Java applications.

2.  **BigQuery Target Table DDL Creation:**
    *   **Task:** Create the Data Definition Language (DDL) for the target BigQuery table `sof_ta_barrier` in the designated target dataset (e.g., `project.dataset`). Ensure column types and structures are compatible with the transformed data.
    *   **Language/Tool:** BigQuery SQL.

3.  **BigQuery Control and Logging Tables DDL Creation (Optional but Recommended):**
    *   **Task:** Define DDLs for BigQuery tables to manage job status, parameters, and logs, replacing the legacy job tracking and temporary file mechanisms.
    *   **Language/Tool:** BigQuery SQL.

4.  **BigQuery Stored Procedure Development (`r_ausd_vertrag_control`):**
    *   **Task:** Develop the main BigQuery Stored Procedure that encapsulates the logic from `k_ausd_v_ta_barrier.ksh` and `d_ausd_v_ta_barrier.sql`.
        *   Implement parameter handling for `p_JobKennung` and `p_EintragsNr`.
        *   Translate the `v_datum` calculation.
        *   Migrate the job control logic (ignoring/deactivating jobs) to interact with BigQuery control tables.
        *   Translate the `TRUNCATE` statement.
        *   Translate the core `INSERT ... SELECT` transformation logic, including `NVL`, `DECODE`, `GREATEST`, and `CASE WHEN` to BigQuery SQL syntax.
        *   Implement logging of records processed to a BigQuery logging table.
        *   Replace calls to `f_alis_msgerr.ksh` with BigQuery error handling (`RAISE`, `ASSERT`) and logging.
    *   **Language/Tool:** BigQuery SQL (Scripting).

5.  **Orchestration and Scheduling:**
    *   **Task:** If external orchestration is needed (e.g., for complex sequencing, dependency management, or integration with other systems), create an Airflow DAG or Google Cloud Workflow to invoke the BigQuery Stored Procedure.
    *   **Language/Tool:** Python (for Airflow DAGs), YAML (for Cloud Workflows).

6.  **Testing and Validation:**
    *   **Task:** Thoroughly test the BigQuery components (ingestion pipelines, stored procedures) against sample and production data to ensure functional equivalence and performance.
    *   **Language/Tool:** BigQuery SQL, Python (for automated testing frameworks).