# Migration Validation Test Suite: `ausd_bp_ta_cntrct_dist`

This document defines the migration-validation test suite to verify that the migrated BigQuery/Airflow pipeline `ausd_bp_ta_cntrct_dist` is behaviorally equivalent to the legacy Oracle PL/SQL implementation.

---

## Test Case 1: End-to-End Output Parity (Oracle vs. BigQuery)

### Purpose
Verify that given the exact same input dataset in the source table, the migrated BigQuery job produces the identical distinct set of contract IDs as the legacy Oracle job.

### Setup
1. **Oracle Environment (Legacy):**
   * Clear the source table: `TRUNCATE TABLE isbert_schema.sof$ta_bpr_basis;`
   * Clear the target table: `TRUNCATE TABLE isbert_schema.sof$ta_cntrct_dist;`
   * Populate the source table with a controlled test dataset containing duplicates, alphanumeric IDs, and boundary values:
     ```sql
     INSERT INTO isbert_schema.sof$ta_bpr_basis (cntrct_id, other_col) VALUES ('CON_001', 'A');
     INSERT INTO isbert_schema.sof$ta_bpr_basis (cntrct_id, other_col) VALUES ('CON_001', 'B'); -- Duplicate ID
     INSERT INTO isbert_schema.sof$ta_bpr_basis (cntrct_id, other_col) VALUES ('CON_002', 'C');
     INSERT INTO isbert_schema.sof$ta_bpr_basis (cntrct_id, other_col) VALUES ('CON_003', 'D');
     COMMIT;
     ```

2. **BigQuery Environment (Target):**
   * Clear the source table: `TRUNCATE TABLE \`project_id.dataset_id.sof_ta_bpr_basis\`;`
   * Clear the target table: `TRUNCATE TABLE \`project_id.dataset_id.sof_ta_cntrct_dist\`;`
   * Populate the source table with the exact same dataset:
     ```sql
     INSERT INTO `project_id.dataset_id.sof_ta_bpr_basis` (cntrct_id) 
     VALUES ('CON_001'), ('CON_001'), ('CON_002'), ('CON_003');
     ```

### Action
1. Execute the legacy Oracle PL/SQL job (`d_ausd_bp_ta_cntrct_dist.sql`).
2. Trigger the migrated Airflow DAG `ausd_bp_ta_cntrct_dist` in Cloud Composer.

### Pass/Fail Criterion
* **Pass:** The target tables in both environments contain the exact same set of distinct contract IDs (`CON_001`, `CON_002`, `CON_003`). Row counts must be identical (exactly 3 rows), and a symmetric difference check between the Oracle target and BigQuery target yields zero differences.
* **Fail:** Row counts differ, or the set of contract IDs does not match.

### Validation Script (PyTest)
```python
import os
import pytest
from google.cloud import bigquery
import cx_Oracle

def test_output_parity():
    # 1. Fetch from Oracle Target
    orcl_conn = cx_Oracle.connect(os.environ["ORACLE_CONN_STR"])
    orcl_cursor = orcl_conn.cursor()
    orcl_cursor.execute("SELECT cntrct_id FROM isbert_schema.sof$ta_cntrct_dist ORDER BY cntrct_id")
    oracle_results = [row[0] for row in orcl_cursor.fetchall()]
    orcl_cursor.close()
    orcl_conn.close()

    # 2. Fetch from BigQuery Target
    bq_client = bigquery.Client()
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_DATASET"]
    query = f"SELECT cntrct_id FROM `{project}.{dataset}.sof_ta_cntrct_dist` ORDER BY cntrct_id"
    bq_results = [row.cntrct_id for row in bq_client.query(query).result()]

    # 3. Assert Equivalence
    assert len(oracle_results) == len(bq_results), f"Row count mismatch: Oracle ({len(oracle_results)}) vs BQ ({len(bq_results)})"
    assert oracle_results == bq_results, f"Data mismatch! Oracle: {oracle_results}, BQ: {bq_results}"
```

---

## Test Case 2: Transformation Correctness & NULL Handling

### Purpose
Verify that the BigQuery SQL transformation correctly handles duplicate values, filters out `NULL` values (as specified in the migrated `WHERE cntrct_id IS NOT NULL` clause), and preserves empty strings if they are present.

### Setup
Populate the BigQuery source table `sof_ta_bpr_basis` with the following edge-case records:
```sql
TRUNCATE TABLE `project_id.dataset_id.sof_ta_bpr_basis`;

INSERT INTO `project_id.dataset_id.sof_ta_bpr_basis` (cntrct_id) VALUES 
('CTR_999'),
('CTR_999'),  -- Duplicate
(NULL),       -- Explicit NULL
(''),         -- Empty String
('CTR_100');
```

### Action
Run the BigQuery SQL script:
```sql
TRUNCATE TABLE `project_id.dataset_id.sof_ta_cntrct_dist`;

INSERT INTO `project_id.dataset_id.sof_ta_cntrct_dist` (cntrct_id)
SELECT DISTINCT
  cntrct_id
FROM `project_id.dataset_id.sof_ta_bpr_basis`
WHERE cntrct_id IS NOT NULL;
```

### Pass/Fail Criterion
* **Pass:** 
  * The target table `sof_ta_cntrct_dist` contains exactly 3 rows.
  * The values in the target table are exactly `'CTR_999'`, `'CTR_100'`, and `''` (empty string).
  * No `NULL` value is present in the target table.
* **Fail:** The target table contains duplicate `'CTR_999'` values, contains a `NULL` value, or misses the empty string.

### Validation Query (SQL Assertion)
```sql
-- This query should return 0 rows if the test passes
WITH expected_data AS (
  SELECT 'CTR_999' AS cntrct_id UNION ALL
  SELECT 'CTR_100' AS cntrct_id UNION ALL
  SELECT '' AS cntrct_id
),
actual_data AS (
  SELECT cntrct_id FROM `project_id.dataset_id.sof_ta_cntrct_dist`
)
SELECT cntrct_id, 'Missing in Target' AS issue FROM expected_data
WHERE cntrct_id NOT IN (SELECT cntrct_id FROM actual_data)
UNION ALL
SELECT cntrct_id, 'Unexpected in Target' AS issue FROM actual_data
WHERE cntrct_id NOT IN (SELECT cntrct_id FROM expected_data);
```

---

## Test Case 3: Idempotency and Restartability (Truncate Safety)

### Purpose
Prove that the target table is fully truncated before insertion, ensuring that running the job multiple times sequentially does not duplicate or accumulate records.

### Setup
1. Populate the target table `sof_ta_cntrct_dist` with "stale" or "orphan" records that do not exist in the source:
   ```sql
   INSERT INTO `project_id.dataset_id.sof_ta_cntrct_dist` (cntrct_id) VALUES ('ORPHAN_01'), ('ORPHAN_02');
   ```
2. Populate the source table `sof_ta_bpr_basis` with active records:
   ```sql
   TRUNCATE TABLE `project_id.dataset_id.sof_ta_bpr_basis`;
   INSERT INTO `project_id.dataset_id.sof_ta_bpr_basis` (cntrct_id) VALUES ('ACTIVE_01');
   ```

### Action
1. Execute the BigQuery migration script once.
2. Execute the BigQuery migration script a second time immediately after.

### Pass/Fail Criterion
* **Pass:** 
  * After the first run, the target table contains exactly 1 row (`ACTIVE_01`). The orphan records are completely removed.
  * After the second run, the target table still contains exactly 1 row (`ACTIVE_01`), proving idempotency.
* **Fail:** Orphan records remain in the target table, or the target table accumulates duplicate `ACTIVE_01` records (e.g., 2 rows of `ACTIVE_01`).

### Validation Script (PyTest)
```python
def test_idempotency_and_truncate(bq_client, project_id, dataset_id):
    target_table = f"`{project_id}.{dataset_id}.sof_ta_cntrct_dist`"
    source_table = f"`{project_id}.{dataset_id}.sof_ta_bpr_basis`"
    
    # 1. Setup stale data in target, fresh data in source
    bq_client.query(f"INSERT INTO {target_table} (cntrct_id) VALUES ('ORPHAN_01')").result()
    bq_client.query(f"TRUNCATE TABLE {source_table}").result()
    bq_client.query(f"INSERT INTO {source_table} (cntrct_id) VALUES ('ACTIVE_01')").result()
    
    # 2. Run execution 1
    run_job(bq_client, project_id, dataset_id)
    res1 = list(bq_client.query(f"SELECT cntrct_id FROM {target_table}").result())
    
    # 3. Run execution 2
    run_job(bq_client, project_id, dataset_id)
    res2 = list(bq_client.query(f"SELECT cntrct_id FROM {target_table}").result())
    
    # Assertions
    assert len(res1) == 1, f"Expected 1 row after Run 1, got {len(res1)}"
    assert res1[0].cntrct_id == 'ACTIVE_01', f"Expected 'ACTIVE_01', got '{res1[0].cntrct_id}'"
    assert len(res2) == 1, f"Expected 1 row after Run 2 (idempotency), got {len(res2)}"
    assert res2[0].cntrct_id == 'ACTIVE_01'

def run_job(client, project, dataset):
    sql = f"""
    TRUNCATE TABLE `{project}.{dataset}.sof_ta_cntrct_dist`;
    INSERT INTO `{project}.{dataset}.sof_ta_cntrct_dist` (cntrct_id)
    SELECT DISTINCT cntrct_id FROM `{project}.{dataset}.sof_ta_bpr_basis`
    WHERE cntrct_id IS NOT NULL;
    """
    client.query(sql).result()
```

---

## Test Case 4: Airflow DAG Integration & Variable Substitution

### Purpose
Verify that the Cloud Composer DAG parses successfully, resolves all environment-specific Jinja variables (`gcp_project_id`, `bq_dataset`, `bq_location`, `gcp_conn_id`), and executes the BigQuery task using the correct connection parameters.

### Setup
1. Import the DAG file `dags/ausd_bp_ta_cntrct_dist.py` into an Airflow environment (or a local unit-testing environment using `dagbag`).
2. Define the following Airflow Variables in the test environment:
   * `gcp_project_id` = `test-gcp-project`
   * `bq_dataset` = `test_dataset`
   * `bq_location` = `europe-west3`
   * `gcp_conn_id` = `google_cloud_default`

### Action
1. Run a DAG parsing test to check for syntax errors.
2. Render the Jinja templates for the `load_contract_distinct` task.

### Pass/Fail Criterion
* **Pass:** 
  * The DAG parses without any `DagBag` import errors.
  * The rendered SQL query correctly substitutes the variables, resulting in:
    `TRUNCATE TABLE test-gcp-project.test_dataset.sof_ta_cntrct_dist;`
    and
    `FROM test-gcp-project.test_dataset.sof_ta_bpr_basis`
  * The task configuration uses connection `google_cloud_default` and location `europe-west3`.
* **Fail:** The DAG fails to parse, or the rendered SQL contains unresolved `{{ var.value... }}` placeholders.

### Validation Script (PyTest)
```python
from airflow.models import DagBag, Variable
from airflow.models.taskinstance import TaskInstance
from datetime import datetime

def test_dag_parsing_and_template_rendering():
    # Set up mock Airflow variables
    Variable.set("gcp_project_id", "test-gcp-project")
    Variable.set("bq_dataset", "test_dataset")
    Variable.set("bq_location", "europe-west3")
    Variable.set("gcp_conn_id", "google_cloud_default")

    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(dag_id="ausd_bp_ta_cntrct_dist")
    
    # Assert DAG loaded successfully
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"
    assert dag is not None, "DAG 'ausd_bp_ta_cntrct_dist' not found"

    # Get the BigQuery task
    task = dag.get_task("load_contract_distinct")
    
    # Render templates for a mock execution date
    ti = TaskInstance(task=task, execution_date=datetime(2024, 1, 1))
    ti.render_templates()

    rendered_sql = task.configuration["query"]["query"]
    
    # Assertions on variable substitutions
    assert "test-gcp-project.test_dataset.sof_ta_cntrct_dist" in rendered_sql
    assert "test-gcp-project.test_dataset.sof_ta_bpr_basis" in rendered_sql
    assert task.location == "europe-west3"
    assert task.gcp_conn_id == "google_cloud_default"
```