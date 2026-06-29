# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh

## 1. Purpose & Scope
The shell script `k_ausd_bp_ta_bpr_basis.ksh` serves as a controller/orchestrator within the legacy ISBERT/ISRPT database extraction framework. It acts as a wrapper around the SQL script `d_ausd_bp_ta_bpr_basis.sql`, providing operational checks and setup.

### Core Functions:
- **Environment Setup**: Sourcing environment properties and central helper scripts.
- **Parameter Validation**: Ensuring that the job identifier (`p_JobKennung`), the run sequence/entry number (`p_EintragsNr`), and the reporting date (`p_Stichtag`) are present and correctly formatted.
- **Date Check**: Ensuring the reporting date adheres to the `DDMMYYYY` format.
- **Date Derivation**: Resolving "today" and "yesterday" using an external script (`gestern.ksh`).
- **SQL*Plus Execution**: Launching `d_ausd_bp_ta_bpr_basis.sql` via a SQL\*Plus wrapper, passing context variables, and capturing runtime metadata (such as processed records count) into a temporary workspace file.
- **Audit Logging**: Registering job statuses in operational metadata tables (commented out in the legacy source but part of the logical design).

---

## 2. Source Inventory

The physical scope of the migration for this specific job consists of the following components:

| Source File Path | Tech/Language | Complexity Tier | Automation Bucket | Est. Effort | Role / Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `k_ausd_bp_ta_bpr_basis.ksh` | KornShell (KSH) | Medium | Semi-auto (65% rate) | Medium | Primary Orchestrator & Parameter Validator |
| `d_ausd_bp_ta_bpr_basis.sql` | Oracle SQL | Medium | Semi-auto (70% rate) | Medium | Target DB extraction and load script (downstream dependency) |

---

## 3. Target Architecture

The modernized architecture on Google Cloud Platform (GCP) replaces legacy operating system orchestration with managed serverless services.

```
       [ Cloud Composer / Apache Airflow ]
                       │
       ┌───────────────┴───────────────┐
       ▼                               ▼
[ Cloud Storage ]            [ Google BigQuery ]
 (DAG / Configs)             ┌─────────┴─────────┐
                             ▼                   ▼
                     [Stored Procedures]   [Target Tables]
```

### Target Components:
1. **Orchestrator — Cloud Composer (Apache Airflow)**:
   - The shell execution and parameter-parsing logic is migrated to a Python-based **Apache Airflow DAG**.
   - Input arguments (`JobKennung`, `Stichtag`, `EintragsNr`, `wiederanlaufWert`) are defined as DAG run configuration parameters (via `dag_run.conf`).
2. **Execution — BigQuery Stored Procedure**:
   - The Oracle SQL logic in `d_ausd_bp_ta_bpr_basis.sql` is migrated to a BigQuery Stored Procedure named `project.dataset.sp_d_ausd_bp_ta_bpr_basis`.
3. **Audit/Control Table**:
   - Instead of reading/writing temporary OS-level files (e.g., `bert_k_ausd_bp_ta_bpr_basis.tmp`), record counts and task metrics are recorded directly to a centralized BigQuery audit log table: `project.dataset.job_control_log`.

---

## 4. Data Flow & Lineage

### Sequential Process Flow:
1. **Trigger**: The Cloud Composer DAG is triggered (manually or via scheduler) with JSON configuration parameters.
2. **Validation**:
   - Validates that `p_JobKennung`, `p_EintragsNr`, and `p_Stichtag` are passed.
   - Parses `p_Stichtag` (format `DDMMYYYY`) into an Airflow date context. If parsing fails, the DAG exits gracefully.
3. **Date Derivation**:
   - Python `datetime` logic derives the equivalent variables for `p_datum_heute` (today) and `p_datum_gestern` (yesterday), replacing `gestern.ksh`.
4. **Procedure Execution**:
   - Airflow executes a `BigQueryInsertJobOperator` to call the BigQuery Stored Procedure `sp_d_ausd_bp_ta_bpr_basis`, passing the calculated date variables and identifiers.
5. **Post-Processing & Logging**:
   - The stored procedure runs the main query, loads the target table `PoolBasisprodukt` (or staging equivalent), and outputs the row count.
   - An entry is inserted into the BigQuery metadata table `job_control_log` registering the successful execution.

### Lineage Diagram:
```
[Airflow Trigger (Parameters)]
         │
         ▼
[Validate Date & Derive Today/Yesterday (Python)]
         │
         ▼
[Execute BigQuery SP: sp_d_ausd_bp_ta_bpr_basis]
         │
         ├───> [Read: Source Tables / Staging Area]
         └───> [Write: project.dataset.PoolBasisprodukt]
         │
         ▼
[Log Run & Row Count to project.dataset.job_control_log]
```

---

## 5. Transformation Logic

### 5.1 Parameter and Orchestration Mapping
The KornShell parameter validation and environment checking are mapped to Python logic in the Airflow DAG below:

```python
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.exceptions import AirflowFailException

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime.datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

def validate_parameters(**kwargs):
    """
    Replaces getopts, pruefeParameterGesetzt, and DWDate_Datum_Check
    """
    conf = kwargs.get('dag_run').conf or {}
    
    # Extract variables
    job_kennung = conf.get('p_JobKennung')
    eintrags_nr = conf.get('p_EintragsNr')
    stichtag_str = conf.get('p_Stichtag')
    wiederanlauf = conf.get('p_wiederanlaufWert', '0')
    
    # Check if necessary parameters are set
    missing = []
    if not job_kennung: missing.append("p_JobKennung")
    if not eintrags_nr: missing.append("p_EintragsNr")
    if not stichtag_str: missing.append("p_Stichtag")
    
    if missing:
        raise AirflowFailException(f"Missing required parameters: {', '.join(missing)}")
    
    # Validate date format (DDMMYYYY)
    try:
        stichtag_date = datetime.datetime.strptime(stichtag_str, "%d%m%Y").date()
    except ValueError:
        raise AirflowFailException(f"Stichtag '{stichtag_str}' does not match format 'DDMMYYYY'")
    
    # Derive yesterday and today (replaces gestern.ksh)
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    
    # Push validated values to XComs
    kwargs['ti'].xcom_push(key='p_JobKennung', value=job_kennung)
    kwargs['ti'].xcom_push(key='p_EintragsNr', value=eintrags_nr)
    kwargs['ti'].xcom_push(key='p_Stichtag', value=stichtag_date.isoformat())
    kwargs['ti'].xcom_push(key='p_wiederanlaufWert', value=wiederanlauf)
    kwargs['ti'].xcom_push(key='p_datum_heute', value=today.isoformat())
    kwargs['ti'].xcom_push(key='p_datum_gestern', value=yesterday.isoformat())
    print("Parameter and date validations successful.")

with DAG(
    dag_id='k_ausd_bp_ta_bpr_basis_orchestrator',
    default_args=default_args,
    schedule_interval=None, # Triggered via framework or composer REST API
    catchup=False,
) as dag:

    validate_inputs = PythonOperator(
        task_id='validate_inputs_and_dates',
        python_callable=validate_parameters,
        provide_context=True,
    )

    # BigQuery call executing migrated SQL procedure
    execute_stored_procedure = BigQueryInsertJobOperator(
        task_id='execute_d_ausd_bp_ta_bpr_basis_sp',
        configuration={
            "query": {
                "query": """
                    CALL `project.dataset.sp_d_ausd_bp_ta_bpr_basis`(
                        '{{ task_instance.xcom_pull(task_ids="validate_inputs_and_dates", key="p_EintragsNr") }}',
                        '{{ task_instance.xcom_pull(task_ids="validate_inputs_and_dates", key="p_JobKennung") }}',
                        '{{ task_instance.xcom_pull(task_ids="validate_inputs_and_dates", key="p_Stichtag") }}',
                        '{{ task_instance.xcom_pull(task_ids="validate_inputs_and_dates", key="p_wiederanlaufWert") }}',
                        '{{ task_instance.xcom_pull(task_ids="validate_inputs_and_dates", key="p_datum_heute") }}',
                        '{{ task_instance.xcom_pull(task_ids="validate_inputs_and_dates", key="p_datum_gestern") }}'
                    )
                """,
                "useLegacySql": False,
            }
        }
    )

    validate_inputs >> execute_stored_procedure
```

### 5.2 Legacy Post-Processing Code Mapping
The shell script contains legacy commented-out commands that perform parsing and joining of raw data files (`sed`, `sort`, `join`). If these operations must be restored as part of the modern migration, they must be implemented directly in BigQuery rather than as local files.

Below is the SQL mapping to achieve the equivalent logic inside BigQuery:

```sql
-- Replacement for commented-out sed/sort/join file processing workflow.
-- Replaces processing of: cibasis_data24.dat, cibasis_data96.dat, cibasis_fax.dat

WITH raw_data24 AS (
  -- Remove blanks (equivalent to sed s/\ //g) and distinct (equivalent to sort -u)
  SELECT DISTINCT
    SPLIT(REGEXP_REPLACE(raw_line, r'\s+', ''), ';')[SAFE_OFFSET(0)] AS key,
    SPLIT(REGEXP_REPLACE(raw_line, r'\s+', ''), ';')[SAFE_OFFSET(1)] AS value_24
  FROM `project.dataset.cibasis_data24_raw`
),

raw_data96 AS (
  SELECT DISTINCT
    SPLIT(REGEXP_REPLACE(raw_line, r'\s+', ''), ';')[SAFE_OFFSET(0)] AS key,
    SPLIT(REGEXP_REPLACE(raw_line, r'\s+', ''), ';')[SAFE_OFFSET(1)] AS value_96
  FROM `project.dataset.cibasis_data96_raw`
),

raw_fax AS (
  SELECT DISTINCT
    SPLIT(REGEXP_REPLACE(raw_line, r'\s+', ''), ';')[SAFE_OFFSET(0)] AS key,
    SPLIT(REGEXP_REPLACE(raw_line, r'\s+', ''), ';')[SAFE_OFFSET(1)] AS value_fax
  FROM `project.dataset.cibasis_fax_raw`
),

-- join -j1 1 -j2 1 -o 2.1,1.2,2.2 -a 2 (Full join/Left join mapping)
joined_24_96 AS (
  SELECT
    COALESCE(d24.key, d96.key) AS key,
    d24.value_24,
    d96.value_96
  FROM raw_data24 d24
  FULL OUTER JOIN raw_data96 d96
  ON d24.key = d96.key
),

-- Final join combining fax data (analogous to the second join statement in KSH)
final_cibasis_product AS (
  SELECT
    COALESCE(j.key, f.key) AS key,
    j.value_24,
    j.value_96,
    f.value_fax
  FROM joined_24_96 j
  LEFT JOIN raw_fax f
  ON j.key = f.key
)

-- Insert or overwrite into Target Table 'PoolBasisprodukt'
SELECT 
  key, 
  value_24, 
  value_96, 
  value_fax,
  CURRENT_TIMESTAMP() as loaded_at
FROM final_cibasis_product;
```

---

## 6. External Dependencies

| Legacy Dependency / Utility | Functionality Description | Modern Replacement Platform / Strategy |
| :--- | :--- | :--- |
| `$HOME/.dw_init` | Sourced profile to initialize variables | Standard Composer/Airflow Variables and Connection Configuration |
| `f_alis_msgerr.ksh` | Operational error concepts | Native Airflow DAG Exception raising and execution logs |
| `h_alis_date.ksh` | Custom date validation logic | Python standard `datetime` checks inside the Python Airflow Task |
| `h_alis_parameter.ksh`| Parameter handling & extraction | DAG execution configuration context (`dag_run.conf`) |
| `h_alis_sqlplus.ksh`  | SQL*Plus launcher & context helper | `BigQueryInsertJobOperator` executing SQL scripts directly |
| `gestern.ksh` | Resolves yesterday/today dates | Python-native date arithmetic (`datetime.timedelta`) |
| Temporary File `.tmp` | Output storage of SQL row counts | Direct query mapping (`SELECT COUNT(*)`) or storing metrics in `job_control_log` table |

---

## 7. Unresolved / Risks

1. **Downstream SQL Syntax Conversion**:
   - The contents of `d_ausd_bp_ta_bpr_basis.sql` are outside the scope of this file context. This SQL script must be separately analyzed and its Oracle-specific SQL (e.g. `(+)` outer joins, Oracle-native date casting, specific session set statements) migrated to standard BigQuery syntax.
2. **Deactivated Operational Logging**:
   - The original script contains commented calls to framework operations (`FOSJobDeaktivate` and `FOSJobErzeugeEintrag`). If these control frameworks are required in the target platform, equivalent table records must be written to standard BigQuery logging schemas.
3. **Recovery / Wiederanlaufwert Handling**:
   - The `-l` parameter (restart parameter) is initialized to `0` but not heavily integrated within the orchestrator shell script. How this variable is utilized inside `d_ausd_bp_ta_bpr_basis.sql` needs functional verification to ensure correct recovery behavior in BigQuery.

---

## 8. Build Plan

The migration implementation should proceed in the following ordered steps:

1. **Phase 1: Setup Metadata Schema**
   - Create the BigQuery logging and tracking audit table `project.dataset.job_control_log` using standard DDL:
     ```sql
     CREATE TABLE IF NOT EXISTS `project.dataset.job_control_log` (
       job_kennung STRING,
       eintrags_nr STRING,
       tab_name STRING,
       stichtag DATE,
       status STRING,
       record_count INT64,
       message STRING,
       created_at TIMESTAMP
     );
     ```
2. **Phase 2: Migrate Database Logic (SQL SP)**
   - Translate and compile `d_ausd_bp_ta_bpr_basis.sql` as a BigQuery stored procedure `sp_d_ausd_bp_ta_bpr_basis` taking inputs: `p_EintragsNr`, `p_JobKennung`, `p_Stichtag`, `p_wiederanlaufWert`, `p_datum_heute`, `p_datum_gestern`.
3. **Phase 3: Deploy Orchestration**
   - Save the proposed DAG code as `k_ausd_bp_ta_bpr_basis_orchestrator.py` and upload it to the `/dags` directory of the Cloud Composer GCS bucket.
4. **Phase 4: Testing & Validation**
   - Execute the DAG via standard trigger config passing mock run parameters:
     ```json
     {
       "p_JobKennung": "TEST_JOB_01",
       "p_EintragsNr": "100249",
       "p_Stichtag": "31122022",
       "p_wiederanlaufWert": "0"
     }
     ```
   - Validate task logging outputs, date arithmetic values, and proper audit writing in BigQuery.