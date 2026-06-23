As a senior data-migration QA engineer, I've analyzed the migration design and the generated code for `.dw_global`. The migration re-platforms a KornShell script to a BigQuery stored procedure and an Airflow DAG.

A critical behavioral discrepancy has been identified in the `PATH` environment variable derivation:
*   **Legacy Script:** Appends `$ORACLE_HOME/bin` to the existing `PATH`.
*   **Generated BigQuery Stored Procedure:** Prepends `$ORACLE_HOME/bin` to the existing `PATH`.

This is a deviation from behavioral equivalence and should be addressed in the migration design or the generated code. The tests below will highlight this difference.

---

# Migration Validation Tests for `.dw_global`

## 1. Output Parity - Successful Execution (All variables provided)

**Purpose:** To verify that when all required input parameters are provided, the BigQuery stored procedure and Airflow DAG successfully execute and return derived configuration values that are equivalent to the legacy script's output, accounting for documented design changes (e.g., NLS settings).

**Setup:**
1.  Ensure the BigQuery stored procedure `project.dataset.dw_global_init` is deployed.
2.  Ensure the Airflow DAG `dw_global_init_dag` is deployed and accessible.
3.  Prepare a test environment for the legacy KornShell script where environment variables can be set and captured.

**Action:**
1.  **Legacy Script:**
    *   Set all required environment variables with valid, non-empty values.
    *   Set `LD_LIBRARY_PATH` and `PATH` to initial values.
    *   Ensure `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` does *not* exist to isolate the core logic.
    *   Execute the legacy script by sourcing it: `source .dw_global`.
    *   Capture the final values of `LD_LIBRARY_PATH`, `PATH`, `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`.
2.  **Migrated Code (BigQuery & Airflow):**
    *   Trigger the `dw_global_init_dag` Airflow DAG.
    *   Provide the same input values as parameters to the `get_configuration_values` task (e.g., via Airflow Variables or `os.getenv` for the test).
    *   Set `ENABLE_COGNOS_CLOUD_SETUP` to `False` for the `handle_cognos_setup_logic` task.
    *   Capture the output of the `call_dw_global_init_procedure` task (the `SELECT` statement result).

**Pass/Fail Criterion:**
*   The Airflow DAG completes successfully.
*   The derived `ld_library_path` from BigQuery exactly matches the `LD_LIBRARY_PATH` from the legacy script.
*   The derived `path` from BigQuery **does NOT match** the `PATH` from the legacy script due to the prepend vs. append difference. This is a **FAIL** for behavioral equivalence, but a **PASS** if the design explicitly allowed this change. (As a QA, I'd flag this as a defect or require design clarification).
*   The derived `nls_lang` from BigQuery is `'AMERICAN_AMERICA.WE8ISO8859P1'` (as per generated code), which **does NOT match** the legacy `'GERMAN_GERMANY.WE8ISO8859P1'`. This is a **PASS** if the design explicitly allowed this change.
*   The derived `nls_date_format` from BigQuery is `'YYYY-MM-DD HH24:MI:SS'` (as per generated code), which **does NOT match** the legacy `'DD-MON-YY'`. This is a **PASS** if the design explicitly allowed this change.
*   The derived `nls_date_language` from BigQuery is `'AMERICAN'` (as per generated code), which **matches** the legacy `'AMERICAN'`.

**Runnable Test Code (Conceptual Python for Airflow/BigQuery interaction):**

```python
import pytest
from unittest.mock import patch, MagicMock
from airflow.models.dagbag import DagBag
from airflow.utils.state import State
from google.cloud import bigquery

# Assume these are the values used for testing
TEST_CONFIG = {
    'dw_dir_root': '/test/dw',
    'dw_dir_prot': '/test/dw/prot',
    'dw_dir_cubes': '/test/dw/cubes',
    'dw_dir_imp_d1': '/test/dw/imp/d1',
    'dw_dir_imp_xtra': '/test/dw/imp/xtra',
    'dw_dir_imp_ctel': '/test/dw/imp/ctel',
    'oracle_home': '/usr/oracle/19c',
    'initial_ld_library_path': '/usr/local/lib:/opt/app/lib',
    'initial_path': '/usr/local/bin:/usr/bin',
    'project_id': 'your-gcp-project-id',
    'dataset_id': 'your_dataset_name',
}

# Expected values from legacy script (assuming TEST_CONFIG inputs)
LEGACY_EXPECTED = {
    'LD_LIBRARY_PATH': f"{TEST_CONFIG['oracle_home']}/lib:{TEST_CONFIG['initial_ld_library_path']}",
    'PATH': f"{TEST_CONFIG['initial_path']}:{TEST_CONFIG['oracle_home']}/bin:", # Note the trailing colon
    'NLS_LANG': 'GERMAN_GERMANY.WE8ISO8859P1',
    'NLS_DATE_FORMAT': 'DD-MON-YY',
    'NLS_DATE_LANGUAGE': 'AMERICAN',
}

# Expected values from BigQuery SP (assuming TEST_CONFIG inputs)
BQ_GENERATED_EXPECTED = {
    'ld_library_path': f"{TEST_CONFIG['oracle_home']}/lib:{TEST_CONFIG['initial_ld_library_path']}",
    'path': f"{TEST_CONFIG['oracle_home']}/bin:{TEST_CONFIG['initial_path']}", # NOTE: PREPENDED in BQ
    'nls_lang': 'AMERICAN_AMERICA.WE8ISO8859P1', # NOTE: DIFFERENT from legacy
    'nls_date_format': 'YYYY-MM-DD HH24:MI:SS', # NOTE: DIFFERENT from legacy
    'nls_date_language': 'AMERICAN',
}

@pytest.fixture(scope="module")
def dag():
    dag_bag = DagBag(dag_folder='src/dags', include_examples=False)
    return dag_bag.get_dag('dw_global_init_dag')

@patch.dict('os.environ', {
    'DW_DIR_ROOT': TEST_CONFIG['dw_dir_root'],
    'DW_DIR_PROT': TEST_CONFIG['dw_dir_prot'],
    'DW_DIR_CUBES': TEST_CONFIG['dw_dir_cubes'],
    'DW_DIR_IMP_D1': TEST_CONFIG['dw_dir_imp_d1'],
    'DW_DIR_IMP_XTRA': TEST_CONFIG['dw_dir_imp_xtra'],
    'DW_DIR_IMP_CTEL': TEST_CONFIG['dw_dir_imp_ctel'],
    'ORACLE_HOME': TEST_CONFIG['oracle_home'],
    'LD_LIBRARY_PATH_INITIAL': TEST_CONFIG['initial_ld_library_path'],
    'PATH_INITIAL': TEST_CONFIG['initial_path'],
    'GCP_PROJECT_ID': TEST_CONFIG['project_id'],
    'BQ_DATASET_ID': TEST_CONFIG['dataset_id'],
    'ENABLE_COGNOS_CLOUD_SETUP': 'False', # Disable Cognos for this test
})
def test_full_successful_execution(dag):
    # Mock BigQuery client to capture the SQL call and return a predefined result
    with patch('airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client') as mock_get_client:
        mock_client = MagicMock(spec=bigquery.Client)
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            MagicMock(
                dw_dir_root=TEST_CONFIG['dw_dir_root'],
                dw_dir_prot=TEST_CONFIG['dw_dir_prot'],
                dw_dir_cubes=TEST_CONFIG['dw_dir_cubes'],
                dw_dir_imp_d1=TEST_CONFIG['dw_dir_imp_d1'],
                dw_dir_imp_xtra=TEST_CONFIG['dw_dir_imp_xtra'],
                dw_dir_imp_ctel=TEST_CONFIG['dw_dir_imp_ctel'],
                oracle_home=TEST_CONFIG['oracle_home'],
                ld_library_path=BQ_GENERATED_EXPECTED['ld_library_path'],
                path=BQ_GENERATED_EXPECTED['path'],
                nls_lang=BQ_GENERATED_EXPECTED['nls_lang'],
                nls_date_format=BQ_GENERATED_EXPECTED['nls_date_format'],
                nls_date_language=BQ_GENERATED_EXPECTED['nls_date_language'],
            )
        ]
        mock_client.query.return_value = mock_query_job
        mock_get_client.return_value = mock_client

        # Run the DAG
        dr = dag.create_dagrun(
            run_id=f"test_run_{datetime.now().isoformat()}",
            state=State.RUNNING,
            execution_date=datetime.now(),
            start_date=datetime.now(),
            data_interval_start=datetime.now(),
            data_interval_end=datetime.now(),
        )
        dr.task_instances[0].run(ignore_ti_state=True) # get_configuration_values
        dr.task_instances[1].run(ignore_ti_state=True) # call_dw_global_init_procedure
        dr.task_instances[2].run(ignore_ti_state=True) # handle_cognos_setup

        # Assert DAG run state
        assert dr.task_instances[0].current_state() == State.SUCCESS
        assert dr.task_instances[1].current_state() == State.SUCCESS
        assert dr.task_instances[2].current_state() == State.SUCCESS

        # Assert BigQuery call parameters (simplified, full assertion would be complex)
        mock_client.query.assert_called_once()
        called_sql = mock_client.query.call_args[0][0]
        assert f"p_dw_dir_root => '{TEST_CONFIG['dw_dir_root']}'" in called_sql
        assert f"p_oracle_home => '{TEST_CONFIG['oracle_home']}'" in called_sql
        assert f"p_initial_ld_library_path => '{TEST_CONFIG['initial_ld_library_path']}'" in called_sql
        assert f"p_initial_path => '{TEST_CONFIG['initial_path']}'" in called_sql

        # Assert derived values (from the mocked BQ result)
        # For a real test, you'd query the BQ job results or mock the BQ hook to return them.
        # Here, we're asserting against the *expected* BQ output based on the generated code.
        # This part would typically involve retrieving the actual results from BQ.
        # For this example, we're assuming the mock returns the BQ_GENERATED_EXPECTED.
        # The key is comparing BQ_GENERATED_EXPECTED against LEGACY_EXPECTED.

        # LD_LIBRARY_PATH comparison
        assert BQ_GENERATED_EXPECTED['ld_library_path'] == LEGACY_EXPECTED['LD_LIBRARY_PATH']

        # PATH comparison - EXPECTED FAILURE for behavioral equivalence
        # This assertion will fail if the BQ code is not changed to append.
        # If the design accepts the prepend, this test needs to be adjusted.
        assert BQ_GENERATED_EXPECTED['path'] == LEGACY_EXPECTED['PATH'] # This will FAIL with current BQ code

        # NLS_LANG comparison - EXPECTED FAILURE for behavioral equivalence (design change)
        assert BQ_GENERATED_EXPECTED['nls_lang'] == LEGACY_EXPECTED['NLS_LANG'] # This will FAIL with current BQ code

        # NLS_DATE_FORMAT comparison - EXPECTED FAILURE for behavioral equivalence (design change)
        assert BQ_GENERATED_EXPECTED['nls_date_format'] == LEGACY_EXPECTED['NLS_DATE_FORMAT'] # This will FAIL with current BQ code

        # NLS_DATE_LANGUAGE comparison - EXPECTED SUCCESS
        assert BQ_GENERATED_EXPECTED['nls_date_language'] == LEGACY_EXPECTED['NLS_DATE_LANGUAGE']

```

## 2. Transformation Correctness - Missing Required Variables (Single)

**Purpose:** To verify that the BigQuery stored procedure correctly identifies and raises an error when a single required input parameter is missing or empty.

**Setup:**
1.  Ensure the BigQuery stored procedure `project.dataset.dw_global_init` is deployed.

**Action:**
1.  Call the `dw_global_init` stored procedure, intentionally omitting one required parameter (e.g., `p_dw_dir_root`).
2.  **Legacy Script:**
    *   Unset `DW_DIR_ROOT` and set all other variables.
    *   Source the script and capture output.

**Pass/Fail Criterion:**
*   **BigQuery:** The stored procedure call `RAISE`s an error. The error message contains "ERROR: Procedure aborted due to missing environment configuration variables." and specifically mentions `DW_DIR_ROOT`.
*   **Legacy:** The script prints "Fehler in .dw_global:", "Umgebungsvariable DW_DIR_ROOT ist nicht gesetzt !", and "Breche ab ..". The script terminates.
*   The error message content (variable name) from BigQuery matches the legacy script's output.

**Runnable Test Code (BigQuery SQL):**

```sql
-- Test Case: Missing DW_DIR_ROOT
DECLARE expected_error_message STRING DEFAULT "ERROR: Procedure aborted due to missing environment configuration variables. The following parameters were not set or were empty: DW_DIR_ROOT";
BEGIN
    CALL `project.dataset.dw_global_init`(
        p_dw_dir_root => NULL, -- Intentionally missing
        p_dw_dir_prot => '/test/dw/prot',
        p_dw_dir_cubes => '/test/dw/cubes',
        p_dw_dir_imp_d1 => '/test/dw/imp/d1',
        p_dw_dir_imp_xtra => '/test/dw/imp/xtra',
        p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
        p_oracle_home => '/usr/oracle/19c',
        p_initial_ld_library_path => '/usr/local/lib',
        p_initial_path => '/usr/local/bin'
    );
EXCEPTION WHEN ERROR THEN
    SELECT
        CASE
            WHEN CONTAINS_SUBSTR(@@error.message, expected_error_message) THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result,
        @@error.message AS actual_error_message,
        expected_error_message AS expected_error_substring;
END;

-- Repeat for each required variable (DW_DIR_PROT, DW_DIR_CUBES, etc., and ORACLE_HOME)
```

## 3. Transformation Correctness - Missing Required Variables (Multiple)

**Purpose:** To verify that the BigQuery stored procedure correctly identifies and raises an error when multiple required input parameters are missing or empty, and lists all of them.

**Setup:**
1.  Ensure the BigQuery stored procedure `project.dataset.dw_global_init` is deployed.

**Action:**
1.  Call the `dw_global_init` stored procedure, intentionally omitting several required parameters (e.g., `p_dw_dir_root`, `p_oracle_home`).
2.  **Legacy Script:**
    *   Unset `DW_DIR_ROOT` and `ORACLE_HOME`.
    *   Source the script and capture output.

**Pass/Fail Criterion:**
*   **BigQuery:** The stored procedure call `RAISE`s an error. The error message contains "ERROR: Procedure aborted due to missing environment configuration variables." and lists all missing parameters (e.g., "DW_DIR_ROOT, ORACLE_HOME"). The order of listed variables might differ, but all should be present.
*   **Legacy:** The script prints "Fehler in .dw_global:", "Umgebungsvariable DW_DIR_ROOT ist nicht gesetzt !", "Umgebungsvariable ORACLE_HOME ist nicht gesetzt !", and "Breche ab ..". The script terminates.
*   The error message content (all missing variable names) from BigQuery matches the legacy script's output (ignoring order).

**Runnable Test Code (BigQuery SQL):**

```sql
-- Test Case: Missing DW_DIR_ROOT and ORACLE_HOME
DECLARE expected_error_substring_1 STRING DEFAULT "DW_DIR_ROOT";
DECLARE expected_error_substring_2 STRING DEFAULT "ORACLE_HOME";
BEGIN
    CALL `project.dataset.dw_global_init`(
        p_dw_dir_root => '', -- Empty string, treated as missing
        p_dw_dir_prot => '/test/dw/prot',
        p_dw_dir_cubes => '/test/dw/cubes',
        p_dw_dir_imp_d1 => '/test/dw/imp/d1',
        p_dw_dir_imp_xtra => '/test/dw/imp/xtra',
        p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
        p_oracle_home => NULL, -- Intentionally missing
        p_initial_ld_library_path => '/usr/local/lib',
        p_initial_path => '/usr/local/bin'
    );
EXCEPTION WHEN ERROR THEN
    SELECT
        CASE
            WHEN CONTAINS_SUBSTR(@@error.message, expected_error_substring_1) AND CONTAINS_SUBSTR(@@error.message, expected_error_substring_2) THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result,
        @@error.message AS actual_error_message,
        CONCAT('Expected to contain: ', expected_error_substring_1, ' and ', expected_error_substring_2) AS expected_content;
END;
```

## 4. Transformation Correctness - Empty String Inputs

**Purpose:** To verify that the BigQuery stored procedure correctly treats empty string inputs for required parameters as missing, consistent with the legacy shell script's `-z` check.

**Setup:**
1.  Ensure the BigQuery stored procedure `project.dataset.dw_global_init` is deployed.

**Action:**
1.  Call the `dw_global_init` stored procedure, providing an empty string `''` for one required parameter (e.g., `p_dw_dir_root`).
2.  **Legacy Script:**
    *   Set `DW_DIR_ROOT=""` (empty string) and all other variables.
    *   Source the script and capture output.

**Pass/Fail Criterion:**
*   **BigQuery:** The stored procedure call `RAISE`s an error, specifically mentioning the parameter provided as an empty string.
*   **Legacy:** The script prints an error message for the variable set to an empty string.
*   The behavior (error raised) is consistent between BigQuery and legacy.

**Runnable Test Code (BigQuery SQL):**

```sql
-- Test Case: Empty string for DW_DIR_ROOT
DECLARE expected_error_message STRING DEFAULT "ERROR: Procedure aborted due to missing environment configuration variables. The following parameters were not set or were empty: DW_DIR_ROOT";
BEGIN
    CALL `project.dataset.dw_global_init`(
        p_dw_dir_root => '', -- Empty string
        p_dw_dir_prot => '/test/dw/prot',
        p_dw_dir_cubes => '/test/dw/cubes',
        p_dw_dir_imp_d1 => '/test/dw/imp/d1',
        p_dw_dir_imp_xtra => '/test/dw/imp/xtra',
        p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
        p_oracle_home => '/usr/oracle/19c',
        p_initial_ld_library_path => '/usr/local/lib',
        p_initial_path => '/usr/local/bin'
    );
EXCEPTION WHEN ERROR THEN
    SELECT
        CASE
            WHEN CONTAINS_SUBSTR(@@error.message, expected_error_message) THEN 'PASS'
            ELSE 'FAIL'
        END AS test_result,
        @@error.message AS actual_error_message,
        expected_error_message AS expected_error_substring;
END;
```

## 5. Transformation Correctness - `LD_LIBRARY_PATH` Derivation

**Purpose:** To verify that the BigQuery stored procedure correctly constructs the `ld_library_path` by prepending `$ORACLE_HOME/lib` to the `p_initial_ld_library_path`, handling both empty and non-empty initial values.

**Setup:**
1.  Ensure the BigQuery stored procedure `project.dataset.dw_global_init` is deployed.

**Action:**
1.  Call the `dw_global_init` stored procedure with:
    *   Case A: `p_initial_ld_library_path` as a non-empty string.
    *   Case B: `p_initial_ld_library_path` as an empty string `''`.
    *   Case C: `p_initial_ld_library_path` as `NULL`.
2.  **Legacy Script:**
    *   Case A: Set `LD_LIBRARY_PATH` to a non-empty string.
    *   Case B: Set `LD_LIBRARY_PATH=""`.
    *   Case C: Unset `LD_LIBRARY_PATH`.
    *   Source the script and capture the resulting `LD_LIBRARY_PATH`.

**Pass/Fail Criterion:**
*   The `ld_library_path` returned by BigQuery for all cases (A, B, C) exactly matches the `LD_LIBRARY_PATH` derived by the legacy script.
    *   Case A: `ORACLE_HOME/lib:initial_ld_library_path`
    *   Case B & C: `ORACLE_HOME/lib` (no trailing colon if initial was empty/null)

**Runnable Test Code (BigQuery SQL):**

```sql
-- Test Case: LD_LIBRARY_PATH derivation with non-empty initial path
CALL `project.dataset.dw_global_init`(
    p_dw_dir_root => '/test/dw', p_dw_dir_prot => '/test/dw/prot', p_dw_dir_cubes => '/test/dw/cubes',
    p_dw_dir_imp_d1 => '/test/dw/imp/d1', p_dw_dir_imp_xtra => '/test/dw/imp/xtra', p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
    p_oracle_home => '/usr/oracle/19c',
    p_initial_ld_library_path => '/usr/local/lib:/opt/app/lib', -- Non-empty
    p_initial_path => '/usr/local/bin'
);
-- Expected result: ld_library_path = '/usr/oracle/19c/lib:/usr/local/lib:/opt/app/lib'

-- Test Case: LD_LIBRARY_PATH derivation with empty initial path
CALL `project.dataset.dw_global_init`(
    p_dw_dir_root => '/test/dw', p_dw_dir_prot => '/test/dw/prot', p_dw_dir_cubes => '/test/dw/cubes',
    p_dw_dir_imp_d1 => '/test/dw/imp/d1', p_dw_dir_imp_xtra => '/test/dw/imp/xtra', p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
    p_oracle_home => '/usr/oracle/19c',
    p_initial_ld_library_path => '', -- Empty string
    p_initial_path => '/usr/local/bin'
);
-- Expected result: ld_library_path = '/usr/oracle/19c/lib'

-- Test Case: LD_LIBRARY_PATH derivation with NULL initial path
CALL `project.dataset.dw_global_init`(
    p_dw_dir_root => '/test/dw', p_dw_dir_prot => '/test/dw/prot', p_dw_dir_cubes => '/test/dw/cubes',
    p_dw_dir_imp_d1 => '/test/dw/imp/d1', p_dw_dir_imp_xtra => '/test/dw/imp/xtra', p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
    p_oracle_home => '/usr/oracle/19c',
    p_initial_ld_library_path => NULL, -- NULL
    p_initial_path => '/usr/local/bin'
);
-- Expected result: ld_library_path = '/usr/oracle/19c/lib'
```

## 6. Transformation Correctness - `PATH` Derivation

**Purpose:** To verify that the BigQuery stored procedure correctly constructs the `path` variable. **NOTE: This test will highlight the behavioral difference (prepend vs. append) between the generated BigQuery code and the legacy script.**

**Setup:**
1.  Ensure the BigQuery stored procedure `project.dataset.dw_global_init` is deployed.

**Action:**
1.  Call the `dw_global_init` stored procedure with:
    *   Case A: `p_initial_path` as a non-empty string.
    *   Case B: `p_initial_path` as an empty string `''`.
    *   Case C: `p_initial_path` as `NULL`.
2.  **Legacy Script:**
    *   Case A: Set `PATH` to a non-empty string.
    *   Case B: Set `PATH=""`.
    *   Case C: Unset `PATH`.
    *   Source the script and capture the resulting `PATH`.

**Pass/Fail Criterion:**
*   **BigQuery:** The `path` returned by BigQuery for all cases (A, B, C) **does NOT match** the `PATH` derived by the legacy script.
    *   Legacy Case A: `initial_path:ORACLE_HOME/bin:` (appends)
    *   Legacy Case B & C: `ORACLE_HOME/bin:` (appends, potentially just `/usr/bin:/bin:ORACLE_HOME/bin:` if PATH was truly unset and system default was used)
    *   BQ Generated Case A: `ORACLE_HOME/bin:initial_path` (prepends)
    *   BQ Generated Case B & C: `ORACLE_HOME/bin` (prepends)
*   This test should **FAIL** if strict behavioral equivalence is required, indicating a defect in the generated BigQuery code's `PATH` derivation logic. If the design explicitly allows this change, the test criterion should be adjusted to reflect the *new* expected behavior.

**Runnable Test Code (BigQuery SQL):**

```sql
-- Test Case: PATH derivation with non-empty initial path
CALL `project.dataset.dw_global_init`(
    p_dw_dir_root => '/test/dw', p_dw_dir_prot => '/test/dw/prot', p_dw_dir_cubes => '/test/dw/cubes',
    p_dw_dir_imp_d1 => '/test/dw/imp/d1', p_dw_dir_imp_xtra => '/test/dw/imp/xtra', p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
    p_oracle_home => '/usr/oracle/19c',
    p_initial_ld_library_path => '/usr/local/lib',
    p_initial_path => '/usr/local/bin:/usr/bin' -- Non-empty
);
-- Legacy Expected: '/usr/local/bin:/usr/bin:/usr/oracle/19c/bin:'
-- BQ Generated:    '/usr/oracle/19c/bin:/usr/local/bin:/usr/bin' (FAIL for equivalence)

-- Test Case: PATH derivation with empty initial path
CALL `project.dataset.dw_global_init`(
    p_dw_dir_root => '/test/dw', p_dw_dir_prot => '/test/dw/prot', p_dw_dir_cubes => '/test/dw/cubes',
    p_dw_dir_imp_d1 => '/test/dw/imp/d1', p_dw_dir_imp_xtra => '/test/dw/imp/xtra', p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
    p_oracle_home => '/usr/oracle/19c',
    p_initial_ld_library_path => '/usr/local/lib',
    p_initial_path => '' -- Empty string
);
-- Legacy Expected: '/usr/oracle/19c/bin:' (assuming initial PATH was empty)
-- BQ Generated:    '/usr/oracle/19c/bin' (FAIL for equivalence)

-- Test Case: PATH derivation with NULL initial path
CALL `project.dataset.dw_global_init`(
    p_dw_dir_root => '/test/dw', p_dw_dir_prot => '/test/dw/prot', p_dw_dir_cubes => '/test/dw/cubes',
    p_dw_dir_imp_d1 => '/test/dw/imp/d1', p_dw_dir_imp_xtra => '/test/dw/imp/xtra', p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
    p_oracle_home => '/usr/oracle/19c',
    p_initial_ld_library_path => '/usr/local/lib',
    p_initial_path => NULL -- NULL
);
-- Legacy Expected: '/usr/oracle/19c/bin:' (assuming initial PATH was unset)
-- BQ Generated:    '/usr/oracle/19c/bin' (FAIL for equivalence)
```

## 7. Transformation Correctness - NLS Settings

**Purpose:** To verify that the NLS settings (`nls_lang`, `nls_date_format`, `nls_date_language`) are hardcoded in the BigQuery stored procedure as specified in the generated code, acknowledging the design change from the legacy script.

**Setup:**
1.  Ensure the BigQuery stored procedure `project.dataset.dw_global_init` is deployed.

**Action:**
1.  Call the `dw_global_init` stored procedure with all valid required parameters.
2.  **Legacy Script:**
    *   Source the script and capture the final `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`.

**Pass/Fail Criterion:**
*   **BigQuery:** The `nls_lang` returned is `'AMERICAN_AMERICA.WE8ISO8859P1'`.
*   **BigQuery:** The `nls_date_format` returned is `'YYYY-MM-DD HH24:MI:SS'`.
*   **BigQuery:** The `nls_date_language` returned is `'AMERICAN'`.
*   **Legacy:** `NLS_LANG` is `GERMAN_GERMANY.WE8ISO8859P1`, `NLS_DATE_FORMAT` is `DD-MON-YY`, `NLS_DATE_LANGUAGE` is `AMERICAN`.
*   This test will show that `nls_lang` and `nls_date_format` **do NOT match** the legacy values, which is a **PASS** if this design change was intentional and documented. `nls_date_language` should match.

**Runnable Test Code (BigQuery SQL):**

```sql
-- Test Case: NLS settings verification
SELECT
    nls_lang,
    nls_date_format,
    nls_date_language
FROM
    (
        CALL `project.dataset.dw_global_init`(
            p_dw_dir_root => '/test/dw', p_dw_dir_prot => '/test/dw/prot', p_dw_dir_cubes => '/test/dw/cubes',
            p_dw_dir_imp_d1 => '/test/dw/imp/d1', p_dw_dir_imp_xtra => '/test/dw/imp/xtra', p_dw_dir_imp_ctel => '/test/dw/imp/ctel',
            p_oracle_home => '/usr/oracle/19c',
            p_initial_ld_library_path => '/usr/local/lib',
            p_initial_path => '/usr/local/bin'
        )
    );
-- Expected BQ Output:
-- nls_lang: 'AMERICAN_AMERICA.WE8ISO8859P1'
-- nls_date_format: 'YYYY-MM-DD HH24:MI:SS'
-- nls_date_language: 'AMERICAN'

-- Assertions would be done in the calling environment (e.g., Python)
-- assert result.nls_lang == 'AMERICAN_AMERICA.WE8ISO8859P1'
-- assert result.nls_date_format == 'YYYY-MM-DD HH24:MI:SS'
-- assert result.nls_date_language == 'AMERICAN'
```

## 8. External System Replacements - Cognos Sourcing (Enabled)

**Purpose:** To verify that the Airflow DAG's `handle_cognos_setup_logic` task correctly simulates the conditional Cognos setup when the enabling condition is met, pushing the expected XCom value.

**Setup:**
1.  Ensure the Airflow DAG `dw_global_init_dag` is deployed.
2.  Set the environment variable `ENABLE_COGNOS_CLOUD_SETUP` to `'True'` for the Airflow worker executing the DAG.

**Action:**
1.  Trigger the `dw_global_init_dag` Airflow DAG.
2.  Observe the logs for the `handle_cognos_setup` task.
3.  Inspect the XComs pushed by the `handle_cognos_setup` task.

**Pass/Fail Criterion:**
*   The `handle_cognos_setup` task completes successfully.
*   The task logs indicate that Cognos setup is enabled and proceeding.
*   An XCom with key `'cognos_env_ready'` and value `True` is pushed by the task.
*   **Legacy Script:** If `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` exists, it would be sourced. The migrated behavior is to signal readiness.

**Runnable Test Code (Python for Airflow DAG testing):**

```python
import pytest
from unittest.mock import patch, MagicMock
from airflow.models.dagbag import DagBag
from airflow.utils.state import State
from datetime import datetime

@pytest.fixture(scope="module")
def dag():
    dag_bag = DagBag(dag_folder='src/dags', include_examples=False)
    return dag_bag.get_dag('dw_global_init_dag')

@patch.dict('os.environ', {
    'DW_DIR_ROOT': '/test/dw', # Required for BQ call, but not directly for this task
    'ORACLE_HOME': '/usr/oracle/19c', # Required for BQ call
    'GCP_PROJECT_ID': 'your-gcp-project-id',
    'BQ_DATASET_ID': 'your_dataset_name',
    'ENABLE_COGNOS_CLOUD_SETUP': 'True', # Enable Cognos for this test
})
@patch('airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client')
def test_cognos_setup_enabled(mock_get_client, dag):
    # Mock BQ client to prevent actual BQ calls, as this test focuses on Cognos task
    mock_get_client.return_value = MagicMock(spec=bigquery.Client)
    mock_get_client.return_value.query.return_value.result.return_value = [MagicMock()] # Dummy result

    dr = dag.create_dagrun(
        run_id=f"test_run_cognos_enabled_{datetime.now().isoformat()}",
        state=State.RUNNING,
        execution_date=datetime.now(),
        start_date=datetime.now(),
        data_interval_start=datetime.now(),
        data_interval_end=datetime.now(),
    )
    # Run only the Cognos task, assuming previous tasks would pass
    ti = dr.get_task_instance('handle_cognos_setup')
    ti.run(ignore_ti_state=True)

    assert ti.current_state() == State.SUCCESS
    assert ti.xcom_pull(key='cognos_env_ready') is True
    # Further assertions could check logs for specific messages
```

## 9. External System Replacements - Cognos Sourcing (Disabled)

**Purpose:** To verify that the Airflow DAG's `handle_cognos_setup_logic` task correctly skips the conditional Cognos setup when the enabling condition is not met, pushing the expected XCom value.

**Setup:**
1.  Ensure the Airflow DAG `dw_global_init_dag` is deployed.
2.  Set the environment variable `ENABLE_COGNOS_CLOUD_SETUP` to `'False'` (or omit it) for the Airflow worker executing the DAG.

**Action:**
1.  Trigger the `dw_global_init_dag` Airflow DAG.
2.  Observe the logs for the `handle_cognos_setup` task.
3.  Inspect the XComs pushed by the `handle_cognos_setup` task.

**Pass/Fail Criterion:**
*   The `handle_cognos_setup` task completes successfully.
*   The task logs indicate that Cognos setup is skipped.
*   An XCom with key `'cognos_env_ready'` and value `False` is pushed by the task.
*   **Legacy Script:** If `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh` does not exist, the sourcing would be skipped. The migrated behavior is to signal non-readiness.

**Runnable Test Code (Python for Airflow DAG testing):**

```python
import pytest
from unittest.mock import patch, MagicMock
from airflow.models.dagbag import DagBag
from airflow.utils.state import State
from datetime import datetime

@pytest.fixture(scope="module")
def dag():
    dag_bag = DagBag(dag_folder='src/dags', include_examples=False)
    return dag_bag.get_dag('dw_global_init_dag')

@patch.dict('os.environ', {
    'DW_DIR_ROOT': '/test/dw', # Required for BQ call, but not directly for this task
    'ORACLE_HOME': '/usr/oracle/19c', # Required for BQ call
    'GCP_PROJECT_ID': 'your-gcp-project-id',
    'BQ_DATASET_ID': 'your_dataset_name',
    'ENABLE_COGNOS_CLOUD_SETUP': 'False', # Disable Cognos for this test
})
@patch('airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client')
def test_cognos_setup_disabled(mock_get_client, dag):
    # Mock BQ client to prevent actual BQ calls, as this test focuses on Cognos task
    mock_get_client.return_value = MagicMock(spec=bigquery.Client)
    mock_get_client.return_value.query.return_value.result.return_value = [MagicMock()] # Dummy result

    dr = dag.create_dagrun(
        run_id=f"test_run_cognos_disabled_{datetime.now().isoformat()}",
        state=State.RUNNING,
        execution_date=datetime.now(),
        start_date=datetime.now(),
        data_interval_start=datetime.now(),
        data_interval_end=datetime.now(),
    )
    # Run only the Cognos task, assuming previous tasks would pass
    ti = dr.get_task_instance('handle_cognos_setup')
    ti.run(ignore_ti_state=True)

    assert ti.current_state() == State.SUCCESS
    assert ti.xcom_pull(key='cognos_env_ready') is False
    # Further assertions could check logs for specific messages
```

## 10. Data Quality / Schema Assertions - Output Structure

**Purpose:** To verify that the BigQuery stored procedure consistently returns a structured output with the expected column names and data types, ensuring downstream consumers can reliably parse the configuration.

**Setup:**
1.  Ensure the BigQuery stored procedure `project.dataset.dw_global_init` is deployed.

**Action:**
1.  Call the `dw_global_init` stored procedure with all valid required parameters.
2.  Examine the schema and data types of the `SELECT` statement result.

**Pass/Fail Criterion:**
*   The output of the stored procedure is a single row.
*   The output contains exactly 10 columns with the following names and types:
    *   `dw_dir_root` (STRING)
    *   `dw_dir_prot` (STRING)
    *   `dw_dir_cubes` (STRING)
    *   `dw_dir_imp_d1` (STRING)
    *   `dw_dir_imp_xtra` (STRING)
    *   `dw_dir_imp_ctel` (STRING)
    *   `oracle_home` (STRING)
    *   `ld_library_path` (STRING)
    *   `path` (STRING)
    *   `nls_lang` (STRING)
    *   `nls_date_format` (STRING)
    *   `nls_date_language` (STRING)

**Runnable Test Code (BigQuery SQL):**

```sql
-- Test Case: Output schema and data types
SELECT
    COUNT(1) AS row_count,
    ARRAY_AGG(STRUCT(column_name, data_type) ORDER BY ordinal_position) AS schema_details
FROM
    (
        SELECT
            column_name,
            data_type,
            ordinal_position
        FROM
            INFORMATION_SCHEMA.ROUTINE_COLUMNS
        WHERE
            table_catalog = 'your-gcp-project-id'
            AND table_schema = 'your_dataset_name'
            AND routine_name = 'dw_global_init'
            AND column_name IS NOT NULL -- Filter out parameters, only select results
    );
-- Expected result (after running the CALL statement and then querying INFORMATION_SCHEMA):
-- row_count: 1 (if querying the result of a CALL, or 12 if querying INFORMATION_SCHEMA for output columns)
-- schema_details:
-- [
--   {column_name: 'dw_dir_root', data_type: 'STRING'},
--   {column_name: 'dw_dir_prot', data_type: 'STRING'},
--   {column_name: 'dw_dir_cubes', data_type: 'STRING'},
--   {column_name: 'dw_dir_imp_d1', data_type: 'STRING'},
--   {column_name: 'dw_dir_imp_xtra', data_type: 'STRING'},
--   {column_name: 'dw_dir_imp_ctel', data_type: 'STRING'},
--   {column_name: 'oracle_home', data_type: 'STRING'},
--   {column_name: 'ld_library_path', data_type: 'STRING'},
--   {column_name: 'path', data_type: 'STRING'},
--   {column_name: 'nls_lang', data_type: 'STRING'},
--   {column_name: 'nls_date_format', data_type: 'STRING'},
--   {column_name: 'nls_date_language', data_type: 'STRING'}
-- ]

-- Note: Directly querying INFORMATION_SCHEMA.ROUTINE_COLUMNS for a stored procedure
-- will show both input parameters and output columns. To specifically test the output
-- of the SELECT statement, you'd typically capture the result of the CALL in a temporary
-- table or use a client library to inspect the result schema.
```