# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

## 1. Purpose & Scope
This job serves as an initial provisioning process for selected basic products (e.g., FAX, Data24) for the BERT system. Its primary business purpose is to generate a snapshot extraction of contract cache data from the Data Warehouse (DWH) for a specific cutoff date (`Stichtag`) and make it available for a downstream Forderungsscoring (FOS) process. The job supports restart/resume capabilities using a `Wiederanlaufwert` to prevent reprocessing of already delivered data. The main script `r_ausd_bp_ta_msisdn.ksh` acts as an orchestrator, handling parameter parsing and job control, while `k_ausd_bp_ta_msisdn.ksh` contains the core logic including parameter validation, date handling, and execution of a data extraction SQL script.

## 2. Source Inventory
The job is composed of two primary KornShell scripts:
- **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh`**
    - Technology: KornShell (Ksh)
    - Category: shell
    - Complexity Tier: *Not available in analysis*
    - Automation Bucket: *Not available in analysis*
    - Role: Main orchestrator script, responsible for top-level parameter parsing, environment setup, and invoking the core logic script.
- **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh`**
    - Technology: KornShell (Ksh)
    - Category: shell
    - Complexity Tier: *Not available in analysis*
    - Automation Bucket: *Not available in analysis*
    - Role: Core logic script, handles detailed parameter validation, date checks, and executes the primary SQL data extraction.

This job also relies on several sourced utility KornShell scripts and invokes an external SQL script, detailed in Section 4 and 6.

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services, primarily BigQuery for data storage and transformation, and Cloud Composer (or Cloud Workflows) for orchestration.

- **BigQuery Stored Procedures:**
    - A main BigQuery Stored Procedure (`r_ausd_bp_ta_msisdn`) will encapsulate the combined logic of the `r_ausd_bp_ta_msisdn.ksh` and `k_ausd_bp_ta_msisdn.ksh` scripts.
    - Helper BigQuery Stored Procedures or UDFs will be created for common utility functions (e.g., date validation, parameter checking).
- **BigQuery Tables:**
    - **Source Table:** A BigQuery table will represent the `contract_cache_source` (from which `d_ausd_bp_ta_msisdn.sql` extracts data).
    - **Target Table:** A BigQuery table (`PoolBasisprodukt_target` or similar) will store the processed output data.
    - **Audit Table:** A BigQuery table (`job_audit`) will log job status, errors, and execution details, replacing the shell-based logging mechanism.
    - **Job Result Counts Table:** A BigQuery table (`job_result_counts`) will store metrics like the number of records processed, replacing the temporary file-based count.
- **Orchestration:** Cloud Composer (Apache Airflow) or Cloud Workflows will be used to schedule and execute the main BigQuery Stored Procedure, handling parameters and monitoring.

## 4. Data Flow & Lineage
The original process flow is:
1.  **`r_ausd_bp_ta_msisdn.ksh` (Orchestrator):** Initializes the environment, parses command-line arguments for cutoff date (`-s`) and restart value (`-l`), determines the system date, and performs initial parameter validation.
2.  **`r_ausd_bp_ta_msisdn.ksh` invokes `k_ausd_bp_ta_msisdn.ksh`:** The orchestrator passes the parsed parameters to the core logic script.
3.  **`k_ausd_bp_ta_msisdn.ksh` (Core Logic):** Further parses job-specific parameters, validates the date format, sources SQL utility scripts, and executes the `d_ausd_bp_ta_msisdn.sql` script via a `starteSQLSkript` function. It then captures the record count from a temporary file and logs job status.
4.  **`d_ausd_bp_ta_msisdn.sql` (Data Extraction):** This SQL script (content not available, inferred) is expected to perform the actual data extraction and transformation. It reads from a source like `contract_cache_source` and writes to a target (implied to be `PoolBasisprodukt`).

**Lineage Observations:**
- The lineage analysis did not explicitly capture `INVOKES` edges between `r_ausd_bp_ta_msisdn.ksh` and `k_ausd_bp_ta_msisdn.ksh`, nor between `k_ausd_bp_ta_msisdn.ksh` and `d_ausd_bp_ta_msisdn.sql`. These relationships were inferred from static analysis of the source code.
- Data flows from an unspecified "contract cache" source (likely a DWH table) through `d_ausd_bp_ta_msisdn.sql` transformations, ultimately targeting a `PoolBasisprodukt` table. Logging and audit data are written to internal job management frameworks.

## 5. Transformation Logic
The transformation logic will be implemented within BigQuery Stored Procedures and SQL, following the flow outlined in the source KornShell scripts.

**Key Logic Points and BigQuery Mapping:**
-   **Parameter Handling:** Command-line arguments (`-s` for Stichtag, `-l` for Wiederanlaufwert, `-j` for JobKennung, `-f` for EintragsNr) will be mapped to `IN` parameters of the main BigQuery Stored Procedure.
-   **Defaulting Logic:**
    -   `p_wiederanlaufWert`: Defaults to `0` if not provided. This will be an `IF` condition in BQSP.
    -   `p_stichtag`: Defaults to the system date (`CURRENT_DATE()`) if not provided. This will also be an `IF` condition in BQSP.
-   **Date Operations:**
    -   `DWDate_Gib_Zeitraum`: Replaced by BigQuery's `CURRENT_DATE()` and `DATE_SUB` functions.
    -   `DWDate_Datum_Check`: Date format validation (`DDMMYYYY`) will be handled using `PARSE_DATE` and `SAFE.PARSE_DATE` with appropriate error handling (`RAISE USING MESSAGE`) in a BQSP.
-   **Error Handling & Logging:**
    -   The `f_alis_msgerr.ksh` framework, `DWMSG_*` functions, and `pruefeParameterGesetzt` will be replaced by custom error handling within BigQuery Stored Procedures (`BEGIN...EXCEPTION...END` blocks) and logging to the `job_audit` BigQuery table.
    -   Shell traps for `INT`, `STOP`, `CONT`, and `ERR` will be translated to BigQuery's `EXCEPTION WHEN ERROR THEN` clause for robust error capture and logging.
-   **Core Data Extraction/Transformation (`d_ausd_bp_ta_msisdn.sql`):** This critical component's logic is currently a placeholder. It is assumed to extract data from a source table (`contract_cache_source`) based on date ranges (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM` relative to `Stichtag`) and filter by `DWH_VERTRAG_ID` based on `p_wiederanlaufWert`. This logic will be translated into a BigQuery Standard SQL `SELECT` statement within the main stored procedure, potentially populating a temporary table (`tmp_result`) or directly inserting into the target.
-   **Record Count:** The `eval "v_records=`cat $tmpFile`"` mechanism for counting records will be replaced by a `SELECT COUNT(*)` query into a variable within the BigQuery Stored Procedure, and then inserted into the `job_result_counts` table.
-   **Post-processing (`sed`, `sort`, `join`):** The commented-out file manipulation steps in `k_ausd_bp_ta_msisdn.ksh` (sed, sort, join on `.dat` files) are considered legacy and will not be migrated unless explicitly required, as they represent operations better suited for direct SQL manipulation on tables in BigQuery.

## 6. External Dependencies
**Original Dependencies:**
-   **Sourced Utility Scripts:**
    -   `$HOME/.dw_init`
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
    -   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
-   **Invoked SQL Script:** `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_msisdn.sql`
-   **Database Access:** The presence of `h_alis_sqlplus.ksh` and execution of a `.sql` script implies an Oracle or similar SQL database.

**Replacement in BigQuery:**
-   **Utility Scripts:** These will be replaced by BigQuery Stored Procedure logic for parameter validation, date handling, and error logging. Environment initialization (`.dw_init`) will be managed by the BigQuery environment itself or Cloud Composer variables. `gestern.ksh` will be replaced by BigQuery's date functions.
-   **`d_ausd_bp_ta_msisdn.sql`:** The content of this SQL script will be directly integrated and translated into BigQuery Standard SQL within the main BigQuery Stored Procedure.
-   **Database Connection:** The direct database connection via SQL*Plus will be replaced by direct access to BigQuery tables. There were no explicit external systems (like SFTP, S3) identified in the `lineage_assembled_jobs` or `lineage_edges` for this specific job, indicating the primary data interaction is database-to-database.

## 7. Unresolved / Risks
-   **Missing SQL Script Content:** The most significant unresolved item is the actual SQL code within `d_ausd_bp_ta_msisdn.sql`. Without this, the exact data extraction and transformation logic cannot be fully translated. The provided BigQuery pseudocode serves as a structural outline, but the precise `SELECT` statements, `JOIN` conditions, and `WHERE` clauses need to be derived from the original SQL file.
-   **Empty Complexity and Automation Rate:** The `file_complexity` and `automation_rate` tables returned no rows for the source files. This means there's no automated assessment of migration difficulty or an estimated automation bucket (B0-B4), posing a risk for accurate effort estimation.
-   **Legacy Job Framework:** The original job control and messaging system (`DWMSG_*`, `FOSJob*`) is custom. While the design proposes BigQuery audit tables, full functional parity with all nuances of the original framework (e.g., historical data of job runs) may require deeper investigation into `f_alis_msgerr.ksh` and other related scripts.
-   **Oracle-Specific SQL:** If `d_ausd_bp_ta_msisdn.sql` contains Oracle-specific SQL constructs (e.g., `DECODE`, `ROWNUM`), these will require careful manual translation to BigQuery Standard SQL.

## 8. Build Plan
The migration will result in BigQuery Standard SQL artifacts.

**Ordered List of Files to Generate:**

1.  **`bq_schema_definition.sql`** (BigQuery Standard SQL DDL)
    -   `contract_cache_source` (input table schema)
    -   `PoolBasisprodukt_target` (output table schema)
    -   `job_audit` (audit log table schema)
    -   `job_result_counts` (record count table schema)

2.  **`bq_utility_procedures.sql`** (BigQuery Standard SQL Stored Procedures)
    -   `validate_ddmmyyyy(p_date_string STRING, OUT p_date DATE)`: Helper procedure for date format validation.
    -   Other small helper procedures as needed to replace sourced shell script functionalities (e.g., parameter validation `pruefeParameterGesetzt`).

3.  **`bq_d_ausd_bp_ta_msisdn_logic.sql`** (BigQuery Standard SQL Script)
    -   *This file will contain the translated content of the original `d_ausd_bp_ta_msisdn.sql`.* It will perform the core data extraction and transformation, likely as a `SELECT` statement inserting into a temporary table or directly into the target.

4.  **`bq_r_ausd_bp_ta_msisdn_sp.sql`** (BigQuery Standard SQL Stored Procedure)
    -   This will be the main orchestrating stored procedure (`CREATE OR REPLACE PROCEDURE project.dataset.r_ausd_bp_ta_msisdn(...)`).
    -   It will incorporate parameter handling, date logic, error handling, and invoke/embed the logic from `bq_d_ausd_bp_ta_msisdn_logic.sql`.
    -   It will include `INSERT` statements for the `job_audit` and `job_result_counts` tables.

5.  **`cloud_composer_dag.py`** (Python) or **`cloud_workflows_definition.yaml`** (YAML)
    -   An orchestration definition to schedule and execute the `r_ausd_bp_ta_msisdn` BigQuery Stored Procedure, passing required parameters.
    -   Includes error handling and monitoring configurations.

**Build Steps:**
1.  Define the schemas for all BigQuery tables based on the understood data model.
2.  **Crucially, obtain and translate the full content of `d_ausd_bp_ta_msisdn.sql` into BigQuery Standard SQL.** This step is manual and requires access to the original SQL file.
3.  Develop and test the utility BigQuery Stored Procedures.
4.  Implement and test the main orchestrator BigQuery Stored Procedure, integrating the translated SQL logic.
5.  Develop and deploy the Cloud Composer DAG or Cloud Workflows definition to schedule and execute the BigQuery job.