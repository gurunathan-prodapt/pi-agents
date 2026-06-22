As a senior data-migration QA engineer, I've designed a suite of migration validation tests for the `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job. These tests aim to ensure the migrated Google Cloud Platform (GCP) solution, leveraging BigQuery and Airflow, is functionally equivalent to the legacy Oracle/KornShell system.

The tests are categorized to cover output parity, transformation correctness, external system replacements, and data quality assertions. Each test case includes its purpose, setup, action, and a concrete pass/fail criterion, with runnable Python (pytest) code blocks demonstrating the testing approach.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

### Test Environment Configuration (Conceptual)

Before running these tests, ensure:
*   A GCP project (`gcp_project`) and BigQuery dataset (`dataset`) are configured.
*   BigQuery tables (`sof_ta_iccid_einzeln`, `sof_ta_iccid_vertrag`, `dwtk_meldungen`, `pool_basisprodukt`) exist with the defined schemas.
*   The Airflow DAG (`bert_ausd_bp_ta_iccid_vertrag_dag.py`) is deployed to a Cloud Composer environment or accessible for local testing.
*   `google-cloud-bigquery` and `pandas` Python libraries are installed.
*   For Airflow task simulation, `apache-airflow` and `pytest-airflow` (or similar) might be needed, along with a configured Airflow metadata database for `TaskInstance` and `DagRun` objects.

```python
# Common imports and configurations for all test cases
import pytest
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import os
from airflow.models.dagbag import DagBag
from airflow.models import DagRun, TaskInstance
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.exceptions import AirflowFailException

# --- Configuration ---
GCP_PROJECT_ID = 'gcp_project'
BIGQUERY_DATASET = 'dataset'
SOURCE_TABLE_BQ = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.sof_ta_iccid_einzeln"
TARGET_TABLE_BQ = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.sof_ta_iccid_vertrag"
DWTK_MELDUNGEN_TABLE_BQ = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dwtk_meldungen"
POOL_BASISPRODUKT_TABLE_BQ = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.pool_basisprodukt"

# Path to your DAG file (adjust as necessary for your test setup)
# This assumes the DAG file is in a 'dags' directory relative to where pytest is run.
DAG_FILE_PATH = "dags/bert_ausd_bp_ta_iccid_vertrag_dag.py"

# --- Helper Functions for BigQuery Interaction ---
def load_bq_data(table_id, data_df):
    """Loads a Pandas DataFrame into a BigQuery table, truncating existing data."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    # Infer schema for basic types, but explicitly define for DATE if needed
    schema_fields = []
    for col in data_df.columns:
        if data_df[col].dtype == 'object': # Likely string
            schema_fields.append(bigquery.SchemaField(col, 'STRING'))
        elif data_df[col].dtype == 'datetime64[ns]': # Pandas datetime for dates
            schema_fields.append(bigquery.SchemaField(col, 'TIMESTAMP')) # BigQuery TIMESTAMP for datetime
        elif data_df[col].dtype == 'int64':
            schema_fields.append(bigquery.SchemaField(col, 'INTEGER'))
        elif data_df[col].dtype == 'float64':
            schema_fields.append(bigquery.SchemaField(col, 'FLOAT'))
        # Handle datetime.date objects specifically for VALID_TO
        if col.endswith('_VALID_TO') and not data_df[col].empty and isinstance(data_df[col].iloc[0], datetime.date):
             schema_fields.append(bigquery.SchemaField(col, 'DATE'))
    
    job_config = bigquery.LoadJobConfig(
        schema=schema_fields,
        write_disposition="WRITE_TRUNCATE",
    )
    
    # Convert datetime.date objects to string for BigQuery load_table_from_dataframe
    # as it expects serializable types. BigQuery will then parse to DATE.
    df_to_load = data_df.copy()
    for col in df_to_load.columns:
        if not df_to_load[col].empty and isinstance(df_to_load[col].iloc[0], datetime.date):
            df_to_load[col] = df_to_load[col].astype(str)

    job = client.load_table_from_dataframe(df_to_load, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded {len(data_df)} rows into {table_id}")

def fetch_bq_data(table_id, order_by_col='CNTRCT_ID'):
    """Fetches all data from a BigQuery table into a Pandas DataFrame."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"SELECT * FROM `{table_id}` ORDER BY {order_by_col}"
    query_job = client.query(query)
    return query_job.to_dataframe()

def trigger_airflow_dag(dag_id, params=None):
    """
    Simulates triggering an Airflow DAG. For actual end-to-end tests,
    this would typically involve using Airflow's REST API or CLI.
    For local pytest, we assume the DAG's core logic is executed.
    """
    print(f"Simulating Airflow DAG trigger for: {dag_id} with params: {params}")
    # In a real test, you'd use Airflow's TestKit or trigger a deployed DAG.
    # For simplicity here, we're just acknowledging the trigger.
    # The actual data population and checks happen via BigQuery queries.
    pass

# Fixture to load the DAG for task-level tests
@pytest.fixture(scope="module")
def dag_fixture():
    dagbag = DagBag(dag_folder=os.path.dirname(DAG_FILE_PATH), include_examples=False)
    dag = dagbag.get_dag("bert_ausd_bp_ta_iccid_vertrag_dag")
    if not dag:
        raise ValueError(f"DAG bert_ausd_bp_ta_iccid_vertrag_dag not found in {DAG_FILE_PATH}")
    return dag

```

---

### 1. Output Parity: End-to-End Data Validation

**Purpose:** To verify that the migrated job produces identical output data in BigQuery as the legacy Oracle job for a representative set of input data, ensuring overall behavioral equivalence. This covers all aspects of transformation, aggregation, and pivoting.

**Setup:**
1.  **Legacy System:**
    *   Populate the Oracle `sof$ta_iccid_einzeln` table with a diverse dataset covering various `CNTRCT_ID`s, `ICCID_TYPE`s (TN, TC, TB, MS1-MS10), and attribute values (including NULLs, valid dates, different string lengths).
    *   Ensure `isbert_schema.dwtk_meldungen` contains an entry for `BERT_DROP_TEMP_TABLE` to allow `s_datum` to be derived (e.g., `timecreated` = '2023-12-31').
    *   Run the legacy `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job.
    *   Export the content of the Oracle `sof$ta_iccid_vertrag` table to a CSV or JSON file (e.g., `legacy_golden_output.csv`).
2.  **Migrated System (BigQuery/Airflow):**
    *   Load the *exact same* input data used in Oracle into the BigQuery `gcp_project.dataset.sof_ta_iccid_einzeln` table.
    *   Load the *exact same* `dwtk_meldungen` data into BigQuery.
    *   Ensure the `gcp_project.dataset.sof_ta_iccid_vertrag` table is empty before running the DAG.

**Action:**
1.  Trigger the Airflow DAG `bert_ausd_bp_ta_iccid_vertrag_dag` with default parameters (or specific `p_stichtag` if the legacy run used one).
2.  After the DAG completes successfully, query the `gcp_project.dataset.sof_ta_iccid_vertrag` table in BigQuery.
3.  Export the BigQuery output to a comparable format (e.g., `migrated_output.csv`).

**Pass/Fail Criterion:**
The exported `migrated_output.csv` must be identical to `legacy_golden_output.csv` after sorting both by `CNTRCT_ID` and all other columns to ensure consistent ordering. All column values, including NULLs and data types, must match.

**Runnable Test Code (Conceptual Pytest with DataFrames):**

```python
# This fixture and test would typically be in a separate test file, e.g., test_output_parity.py

@pytest.fixture(scope="module")
def setup_end_to_end_test_data():
    """
    Fixture to set up test data in BigQuery and provide the expected (golden) DataFrame.
    In a real scenario, `legacy_golden_output_df` would be loaded from an actual
    export of the legacy system's output. Here, it's generated for demonstration.
    """
    # --- Input Data for sof_ta_iccid_einzeln ---
    input_data = [
        {'CNTRCT_ID': 'C1', 'ICCID_TYPE': 'TN', 'ICCID': 'TN1', 'IMSI_MCC': '100', 'IMSI_MNC': '01', 'IMSI_HLR': 'HLR1', 'IMSI_SI': 'SI1', 'STATUS': 'A', 'VALID_TO': datetime(2024, 12, 31).date(), 'E_ID': 'E1', 'CARD_TYPE_NAME': 'Primary'},
        {'CNTRCT_ID': 'C1', 'ICCID_TYPE': 'MS1', 'ICCID': 'MS1A', 'IMSI_MCC': '101', 'IMSI_MNC': '02', 'IMSI_HLR': 'HLR2', 'IMSI_SI': 'SI2', 'STATUS': 'B', 'VALID_TO': datetime(2025, 1, 1).date(), 'E_ID': 'E2', 'CARD_TYPE_NAME': 'MultiSIM'},
        {'CNTRCT_ID': 'C2', 'ICCID_TYPE': 'TN', 'ICCID': 'TN2', 'IMSI_MCC': '200', 'IMSI_MNC': '04', 'IMSI_HLR': 'HLR4', 'IMSI_SI': 'SI4', 'STATUS': 'D', 'VALID_TO': datetime(2024, 11, 30).date(), 'E_ID': 'E4', 'CARD_TYPE_NAME': 'Primary'},
        {'CNTRCT_ID': 'C2', 'ICCID_TYPE': 'TC', 'ICCID': 'TC2', 'IMSI_MCC': '201', 'IMSI_MNC': '05', 'IMSI_HLR': 'HLR5', 'IMSI_SI': 'SI5', 'STATUS': 'E', 'VALID_TO': datetime(2024, 10, 31).date(), 'E_ID': 'E5', 'CARD_TYPE_NAME': 'TwinCard'},
        {'CNTRCT_ID': 'C3', 'ICCID_TYPE': 'TB', 'ICCID': 'TB3', 'IMSI_MCC': '300', 'IMSI_MNC': '06', 'IMSI_HLR': 'HLR6', 'IMSI_SI': 'SI6', 'STATUS': 'F', 'VALID_TO': datetime(2023, 9, 30).date(), 'E_ID': 'E6', 'CARD_TYPE_NAME': 'Broadband'},
        {'CNTRCT_ID': 'C4', 'ICCID_TYPE': 'TN', 'ICCID': 'TN4', 'IMSI_MCC': '400', 'IMSI_MNC': '07', 'IMSI_HLR': 'HLR7', 'IMSI_SI': 'SI7', 'STATUS': 'G', 'VALID_TO': datetime(2026, 1, 1).date(), 'E_ID': 'E7', 'CARD_TYPE_NAME': 'Primary'},
        {'CNTRCT_ID': 'C4', 'ICCID_TYPE': 'MS10', 'ICCID': 'MS10A', 'IMSI_MCC': '410', 'IMSI_MNC': '17', 'IMSI_HLR': 'HLR17', 'IMSI_SI': 'SI17', 'STATUS': 'H', 'VALID_TO': datetime(2026, 2, 1).date(), 'E_ID': 'E17', 'CARD_TYPE_NAME': 'MultiSIM'},
        {'CNTRCT_ID': 'C5', 'ICCID_TYPE': 'TN', 'ICCID': 'TN5', 'IMSI_MCC': '500', 'IMSI_MNC': '08', 'IMSI_HLR': 'HLR8', 'IMSI_SI': 'SI8', 'STATUS': 'I', 'VALID_TO': datetime(2024, 7, 1).date(), 'E_ID': 'E8', 'CARD_TYPE_NAME': 'Primary'},
        {'CNTRCT_ID': 'C5', 'ICCID_TYPE': 'MS1', 'ICCID': None, 'IMSI_MCC': '501', 'IMSI_MNC': '09', 'IMSI_HLR': 'HLR9', 'IMSI_SI': 'SI9', 'STATUS': 'J', 'VALID_TO': datetime(2024, 8, 1).date(), 'E_ID': 'E9', 'CARD_TYPE_NAME': 'MultiSIM'}, # NULL ICCID
        {'CNTRCT_ID': 'C6', 'ICCID_TYPE': 'TN', 'ICCID': 'TN6', 'IMSI_MCC': None, 'IMSI_MNC': '10', 'IMSI_HLR': 'HLR10', 'IMSI_SI': 'SI10', 'STATUS': 'K', 'VALID_TO': datetime(2024, 6, 1).date(), 'E_ID': 'E10', 'CARD_TYPE_NAME': 'Primary'}, # NULL IMSI_MCC
        {'CNTRCT_ID': 'C7', 'ICCID_TYPE': 'TN', 'ICCID': 'TN7', 'IMSI_MCC': '700', 'IMSI_MNC': '11', 'IMSI_HLR': 'HLR11', 'IMSI_SI': 'SI11', 'STATUS': 'L', 'VALID_TO': None, 'E_ID': 'E11', 'CARD_TYPE_NAME': 'Primary'}, # NULL VALID_TO
        {'CNTRCT_ID': 'C8', 'ICCID_TYPE': 'TN', 'ICCID': 'TN8', 'IMSI_MCC': '800', 'IMSI_MNC': '12', 'IMSI_HLR': 'HLR12', 'IMSI_SI': 'SI12', 'STATUS': 'M', 'VALID_TO': datetime(2024, 5, 1).date(), 'E_ID': None, 'CARD_TYPE_NAME': 'Primary'}, # NULL E_ID
        {'CNTRCT_ID': 'C9', 'ICCID_TYPE': 'TN', 'ICCID': 'TN9', 'IMSI_MCC': '900', 'IMSI_MNC': '13', 'IMSI_HLR': 'HLR13', 'IMSI_SI': 'SI13', 'STATUS': 'N', 'VALID_TO': datetime(2024, 4, 1).date(), 'E_ID': 'E13', 'CARD_TYPE_NAME': None}, # NULL CARD_TYPE_NAME
        {'CNTRCT_ID': 'C10', 'ICCID_TYPE': 'MS1', 'ICCID': 'MS1B', 'IMSI_MCC': '101', 'IMSI_MNC': '02', 'IMSI_HLR': 'HLR2', 'IMSI_SI': 'SI2', 'STATUS': 'B', 'VALID_TO': datetime(2025, 1, 1).date(), 'E_ID': 'E2', 'CARD_TYPE_NAME': 'MultiSIM'}, # Duplicate ICCID_TYPE for C10 (MAX will pick 'MS1C' for strings, '2025-03-01' for date)
        {'CNTRCT_ID': 'C10', 'ICCID_TYPE': 'MS1', 'ICCID': 'MS1C', 'IMSI_MCC': '103', 'IMSI_MNC': '04', 'IMSI_HLR': 'HLR4', 'IMSI_SI': 'SI4', 'STATUS': 'D', 'VALID_TO': datetime(2025, 3, 1).date(), 'E_ID': 'E4', 'CARD_TYPE_NAME': 'MultiSIM'},
    ]
    input_df = pd.DataFrame(input_data)

    # --- DWTK Meldungen Data ---
    dwtk_data = [{'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 12, 31, 10, 0, 0)}]
    dwtk_df = pd.DataFrame(dwtk_data)

    # Expected output (golden data) - This would typically come from the legacy system.
    # For this example, it's a pre-calculated DataFrame based on the transformation logic.
    expected_output_data = [
        {'CNTRCT_ID': 'C1', 'TN_ICCID': 'TN1', 'TN_IMSI_MCC': '100', 'TN_IMSI_MNC': '01', 'TN_IMSI_HLR': 'HLR1', 'TN_IMSI_SI': 'SI1', 'TN_STATUS': 'A', 'TN_VALID_TO': datetime(2024, 12, 31).date(), 'TN_E_ID': 'E1', 'TN_CARD_TYPE_NAME': 'Primary', 'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None, 'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None, 'MS1_ICCID': 'MS1A', 'MS1_IMSI_MCC': '101', 'MS1_IMSI_MNC': '02', 'MS1_IMSI_HLR': 'HLR2', 'MS1_IMSI_SI': 'SI2', 'MS1_STATUS': 'B', 'MS1_VALID_TO': datetime(2025, 1, 1).date(), 'MS1_E_ID': 'E2', 'MS1_CARD_TYPE_NAME': 'MultiSIM', 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C2', 'TN_ICCID': 'TN2', 'TN_IMSI_MCC': '200', 'TN_IMSI_MNC': '04', 'TN_IMSI_HLR': 'HLR4', 'TN_IMSI_SI': 'SI4', 'TN_STATUS': 'D', 'TN_VALID_TO': datetime(2024, 11, 30).date(), 'TN_E_ID': 'E4', 'TN_CARD_TYPE_NAME': 'Primary', 'TC_ICCID': 'TC2', 'TC_IMSI_MCC': '201', 'TC_IMSI_MNC': '05', 'TC_IMSI_HLR': 'HLR5', 'TC_IMSI_SI': 'SI5', 'TC_STATUS': 'E', 'TC_VALID_TO': datetime(2024, 10, 31).date(), 'TC_E_ID': 'E5', 'TC_CARD_TYPE_NAME': 'TwinCard', 'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None, 'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID': None, 'MS1_CARD_TYPE_NAME': None, 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C3', 'TN_ICCID': None, 'TN_IMSI_MCC': None, 'TN_IMSI_MNC': None, 'TN_IMSI_HLR': None, 'TN_IMSI_SI': None, 'TN_STATUS': None, 'TN_VALID_TO': None, 'TN_E_ID': None, 'TN_CARD_TYPE_NAME': None, 'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None, 'TB_ICCID': 'TB3', 'TB_IMSI_MCC': '300', 'TB_IMSI_MNC': '06', 'TB_IMSI_HLR': 'HLR6', 'TB_IMSI_SI': 'SI6', 'TB_STATUS': 'F', 'TB_VALID_TO': datetime(2023, 9, 30).date(), 'TB_E_ID': 'E6', 'TB_CARD_TYPE_NAME': 'Broadband', 'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID': None, 'MS1_CARD_TYPE_NAME': None, 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C4', 'TN_ICCID': 'TN4', 'TN_IMSI_MCC': '400', 'TN_IMSI_MNC': '07', 'TN_IMSI_HLR': 'HLR7', 'TN_IMSI_SI': 'SI7', 'TN_STATUS': 'G', 'TN_VALID_TO': datetime(2026, 1, 1).date(), 'TN_E_ID': 'E7', 'TN_CARD_TYPE_NAME': 'Primary', 'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None, 'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None, 'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID': None, 'MS1_CARD_TYPE_NAME': None, 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': 'MS10A', 'MS10_IMSI_MCC': '410', 'MS10_IMSI_MNC': '17', 'MS10_IMSI_HLR': 'HLR17', 'MS10_IMSI_SI': 'SI17', 'MS10_STATUS': 'H', 'MS10_VALID_TO': datetime(2026, 2, 1).date(), 'MS10_E_ID': 'E17', 'MS10_CARD_TYPE_NAME': 'MultiSIM'},
        {'CNTRCT_ID': 'C5', 'TN_ICCID': 'TN5', 'TN_IMSI_MCC': '500', 'TN_IMSI_MNC': '08', 'TN_IMSI_HLR': 'HLR8', 'TN_IMSI_SI': 'SI8', 'TN_STATUS': 'I', 'TN_VALID_TO': datetime(2024, 7, 1).date(), 'TN_E_ID': 'E8', 'TN_CARD_TYPE_NAME': 'Primary', 'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None, 'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None, 'MS1_ICCID': None, 'MS1_IMSI_MCC': '501', 'MS1_IMSI_MNC': '09', 'MS1_IMSI_HLR': 'HLR9', 'MS1_IMSI_SI': 'SI9', 'MS1_STATUS': 'J', 'MS1_VALID_TO': datetime(2024, 8, 1).date(), 'MS1_E_ID': 'E9', 'MS1_CARD_TYPE_NAME': 'MultiSIM', 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C6', 'TN_ICCID': 'TN6', 'TN_IMSI_MCC': None, 'TN_IMSI_MNC': '10', 'TN_IMSI_HLR': 'HLR10', 'TN_IMSI_SI': 'SI10', 'TN_STATUS': 'K', 'TN_VALID_TO': datetime(2024, 6, 1).date(), 'TN_E_ID': 'E10', 'TN_CARD_TYPE_NAME': 'Primary', 'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None, 'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None, 'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID': None, 'MS1_CARD_TYPE_NAME': None, 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C7', 'TN_ICCID': 'TN7', 'TN_IMSI_MCC': '700', 'TN_IMSI_MNC': '11', 'TN_IMSI_HLR': 'HLR11', 'TN_IMSI_SI': 'SI11', 'TN_STATUS': 'L', 'TN_VALID_TO': None, 'TN_E_ID': 'E11', 'TN_CARD_TYPE_NAME': 'Primary', 'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None, 'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None, 'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID': None, 'MS1_CARD_TYPE_NAME': None, 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C8', 'TN_ICCID': 'TN8', 'TN_IMSI_MCC': '800', 'TN_IMSI_MNC': '12', 'TN_IMSI_HLR': 'HLR12', 'TN_IMSI_SI': 'SI12', 'TN_STATUS': 'M', 'TN_VALID_TO': datetime(2024, 5, 1).date(), 'TN_E_ID': None, 'TN_CARD_TYPE_NAME': 'Primary', 'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None, 'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None, 'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID': None, 'MS1_CARD_TYPE_NAME': None, 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C9', 'TN_ICCID': 'TN9', 'TN_IMSI_MCC': '900', 'TN_IMSI_MNC': '13', 'TN_IMSI_HLR': 'HLR13', 'TN_IMSI_SI': 'SI13', 'TN_STATUS': 'N', 'TN_VALID_TO': datetime(2024, 4, 1).date(), 'TN_E_ID': 'E13', 'TN_CARD_TYPE_NAME': None, 'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None, 'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None, 'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID': None, 'MS1_CARD_TYPE_NAME': None, 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C10', 'TN_ICCID': None, 'TN_IMSI_MCC': None, 'TN_IMSI_MNC': None, 'TN_IMSI_HLR': None, 'TN_IMSI_SI': None, 'TN_STATUS': None, 'TN_VALID_TO': None, 'TN_E_ID': None, 'TN_CARD_TYPE_NAME': None, 'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None, 'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None, 'MS1_ICCID': 'MS1C', 'MS1_IMSI_MCC': '103', 'MS1_IMSI_MNC': '04', 'MS1_IMSI_HLR': 'HLR4', 'MS1_IMSI_SI': 'SI4', 'MS1_STATUS': 'D', 'MS1_VALID_TO': datetime(2025, 3, 1).date(), 'MS1_E_ID': 'E4', 'MS1_CARD_TYPE_NAME': 'MultiSIM', 'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None, 'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None, 'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None, 'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None, 'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None, 'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None, 'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None, 'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None, 'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
    ]
    # Define all possible target columns to ensure consistency
    all_target_cols = [
        'CNTRCT_ID',
        'TN_ICCID', 'TN_IMSI_MCC', 'TN_IMSI_MNC', 'TN_IMSI_HLR', 'TN_IMSI_SI', 'TN_STATUS', 'TN_VALID_TO', 'TN_E_ID', 'TN_CARD_TYPE_NAME',
        'TC_ICCID', 'TC_IMSI_MCC', 'TC_IMSI_MNC', 'TC_IMSI_HLR', 'TC_IMSI_SI', 'TC_STATUS', 'TC_VALID_TO', 'TC_E_ID', 'TC_CARD_TYPE_NAME',
        'TB_ICCID', 'TB_IMSI_MCC', 'TB_IMSI_MNC', 'TB_IMSI_HLR', 'TB_IMSI_SI', 'TB_STATUS', 'TB_VALID_TO', 'TB_E_ID', 'TB_CARD_TYPE_NAME',
        'MS1_ICCID', 'MS1_IMSI_MCC', 'MS1_IMSI_MNC', 'MS1_IMSI_HLR', 'MS1_IMSI_SI', 'MS1_STATUS', 'MS1_VALID_TO', 'MS1_E_ID', 'MS1_CARD_TYPE_NAME',
        'MS2_ICCID', 'MS2_IMSI_MCC', 'MS2_IMSI_MNC', 'MS2_IMSI_HLR', 'MS2_IMSI_SI', 'MS2_STATUS', 'MS2_VALID_TO', 'MS2_E_ID', 'MS2_CARD_TYPE_NAME',
        'MS3_ICCID', 'MS3_IMSI_MCC', 'MS3_IMSI_MNC', 'MS3_IMSI_HLR', 'MS3_IMSI_SI', 'MS3_STATUS', 'MS3_VALID_TO', 'MS3_E_ID', 'MS3_CARD_TYPE_NAME',
        'MS4_ICCID', 'MS4_IMSI_MCC', 'MS4_IMSI_MNC', 'MS4_IMSI_HLR', 'MS4_IMSI_SI', 'MS4_STATUS', 'MS4_VALID_TO', 'MS4_E_ID', 'MS4_CARD_TYPE_NAME',
        'MS5_ICCID', 'MS5_IMSI_MCC', 'MS5_IMSI_MNC', 'MS5_IMSI_HLR', 'MS5_IMSI_SI', 'MS5_STATUS', 'MS5_VALID_TO', 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None,
        'MS6_ICCID', 'MS6_IMSI_MCC', 'MS6_IMSI_MNC', 'MS6_IMSI_HLR', 'MS6_IMSI_SI', 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None,
        'MS7_ICCID', 'MS7_IMSI_MCC', 'MS7_IMSI_MNC', 'MS7_IMSI_HLR', 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None,
        'MS8_ICCID', 'MS8_IMSI_MCC', 'MS8_IMSI_MNC', 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None,
        'MS9_ICCID', 'MS9_IMSI_MCC', 'MS9_IMSI_MNC', 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None,
        'MS10_ICCID', 'MS10_IMSI_MCC', 'MS10_IMSI_MNC', 'MS10_IMSI_HLR', 'MS10_IMSI_SI', 'MS10_STATUS', 'MS10_VALID_TO', 'MS10_E_ID', 'MS10_CARD_TYPE_NAME'
    ]
    expected_df = pd.DataFrame(expected_output_data, columns=all_target_cols)
    for col in expected_df.columns:
        if 'VALID_TO' in col:
            expected_df[col] = pd.to_datetime(expected_df[col]).dt.date # Ensure date type for comparison

    expected_df = expected_df.sort_values(by='CNTRCT_ID').reset_index(drop=True)

    # Load data into BigQuery
    load_bq_data(SOURCE_TABLE_BQ, input_df)
    load_bq_data(DWTK_MELDUNGEN_TABLE_BQ, dwtk_df)

    yield expected_df

    # --- Teardown ---
    print("Cleaning up BigQuery tables...")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    client.delete_table(SOURCE_TABLE_BQ, not_found_ok=True)
    client.delete_table(TARGET_TABLE_BQ, not_found_ok=True)
    client.delete_table(DWTK_MELDUNGEN_TABLE_BQ, not_found_ok=True)
    client.delete_table(POOL_BASISPRODUKT_TABLE_BQ, not_found_ok=True)


def test_end_to_end_output_parity(setup_end_to_end_test_data):
    """
    Test case for end-to-end output parity.
    """
    expected_df = setup_end_to_end_test_data

    # Action: Trigger the Airflow DAG (simulated for this example)
    # In a real test, you'd use an Airflow test harness or trigger a deployed DAG.
    # For this example, we assume the DAG runs successfully and populates the target.
    # A more robust test would use Airflow's test utilities to run the DAG.
    # For now, we'll assume the DAG execution is handled by an external Airflow instance.
    # If running locally, you might manually execute the BigQuery SQL.
    # For a true end-to-end test, you'd trigger the DAG via Airflow's API/CLI.
    # Example (conceptual):
    # trigger_airflow_dag("bert_ausd_bp_ta_iccid_vertrag_dag")
    # For this test to pass, you'd need to manually run the DAG or mock its execution.
    # For demonstration, we'll directly execute the transformation SQL.
    client = bigquery.Client(project=GCP_PROJECT_ID)
    client.query(f"TRUNCATE TABLE `{TARGET_TABLE_BQ}`;").result()
    with open("sql/transform/d_ausd_bp_ta_iccid_vertrag.sql", "r") as f:
        transform_sql = f.read()
    # Replace placeholders in SQL
    transform_sql = transform_sql.replace("`gcp_project.dataset.sof_ta_iccid_vertrag`", f"`{TARGET_TABLE_BQ}`")
    transform_sql = transform_sql.replace("`gcp_project.dataset.sof_ta_iccid_einzeln`", f"`{SOURCE_TABLE_BQ}`")
    client.query(transform_sql).result()
    print("Simulated DAG execution by running BigQuery transformation SQL.")

    # Fetch actual output from BigQuery
    actual_df = fetch_bq_data(TARGET_TABLE_BQ)

    # Ensure column order is the same for comparison
    actual_df = actual_df[expected_df.columns]

    # Convert all columns to string type for consistent comparison, handling None/NaT
    for col in expected_df.columns:
        if 'VALID_TO' in col:
            expected_df[col] = expected_df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
            actual_df[col] = actual_df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
        else:
            expected_df[col] = expected_df[col].astype(str).replace('NaT', None).replace('None', None)
            actual_df[col] = actual_df[col].astype(str).replace('NaT', None).replace('None', None)

    # Pass/Fail Criterion: Compare DataFrames
    pd.testing.assert_frame_equal(
        expected_df,
        actual_df,
        check_dtype=False, # Data types might differ slightly (e.g., int vs object for NULLable numbers)
        check_like=True # Ignore column order if not explicitly sorted
    )
    print("End-to-end output parity test passed!")

```

---

### 2. Transformation Correctness: Pivoting, Aggregation, and NULL Handling

**Purpose:** To specifically verify the correctness of the `MAX(CASE WHEN ...)` pivoting logic and `GROUP BY CNTRCT_ID` aggregation, including handling of various `ICCID_TYPE`s and NULL values.

**Setup:**
1.  **BigQuery:**
    *   Ensure `gcp_project.dataset.sof_ta_iccid_einzeln` and `gcp_project.dataset.dwtk_meldungen` are populated with specific test data designed to exercise pivoting and aggregation.
    *   Ensure `gcp_project.dataset.sof_ta_iccid_vertrag` is empty.

**Action:**
1.  Trigger the Airflow DAG `bert_ausd_bp_ta_iccid_vertrag_dag`.
2.  Query the `gcp_project.dataset.sof_ta_iccid_vertrag` table.

**Pass/Fail Criterion:**
The output in `gcp_project.dataset.sof_ta_iccid_vertrag` must match the pre-calculated expected results for the specific test data, verifying:
*   Correct pivoting of all `ICCID_TYPE`s (TN, TC, TB, MS1-MS10).
*   Correct aggregation using `MAX()` for each attribute.
*   Accurate handling of NULL values in source attributes, resulting in NULLs in target pivoted columns where expected.
*   Correct `VALID_TO` date transformation.

**Runnable Test Code (Pytest with DataFrames):**

```python
# This fixture and test would typically be in a separate test file, e.g., test_transformation_logic.py

@pytest.fixture(scope="function")
def setup_transformation_test_data():
    """Fixture for transformation correctness test."""
    # Test data focusing on specific pivoting and aggregation scenarios
    input_data = [
        # Scenario 1: Single CNTRCT_ID with multiple ICCID_TYPEs
        {'CNTRCT_ID': 'C_TRANS_01', 'ICCID_TYPE': 'TN', 'ICCID': 'TN_X', 'IMSI_MCC': '111', 'IMSI_MNC': '01', 'IMSI_HLR': 'H1', 'IMSI_SI': 'S1', 'STATUS': 'A', 'VALID_TO': datetime(2023, 1, 1).date(), 'E_ID': 'E1', 'CARD_TYPE_NAME': 'P'},
        {'CNTRCT_ID': 'C_TRANS_01', 'ICCID_TYPE': 'TC', 'ICCID': 'TC_Y', 'IMSI_MCC': '222', 'IMSI_MNC': '02', 'IMSI_HLR': 'H2', 'IMSI_SI': 'S2', 'STATUS': 'B', 'VALID_TO': datetime(2023, 2, 1).date(), 'E_ID': 'E2', 'CARD_TYPE_NAME': 'T'},
        {'CNTRCT_ID': 'C_TRANS_01', 'ICCID_TYPE': 'MS1', 'ICCID': 'MS1_Z', 'IMSI_MCC': '333', 'IMSI_MNC': '03', 'IMSI_HLR': 'H3', 'IMSI_SI': 'S3', 'STATUS': 'C', 'VALID_TO': datetime(2023, 3, 1).date(), 'E_ID': 'E3', 'CARD_TYPE_NAME': 'M1'},
        {'CNTRCT_ID': 'C_TRANS_01', 'ICCID_TYPE': 'MS10', 'ICCID': 'MS10_W', 'IMSI_MCC': '444', 'IMSI_MNC': '04', 'IMSI_HLR': 'H4', 'IMSI_SI': 'S4', 'STATUS': 'D', 'VALID_TO': datetime(2023, 4, 1).date(), 'E_ID': 'E4', 'CARD_TYPE_NAME': 'M10'},
        # Scenario 2: CNTRCT_ID with only one ICCID_TYPE
        {'CNTRCT_ID': 'C_TRANS_02', 'ICCID_TYPE': 'TB', 'ICCID': 'TB_SINGLE', 'IMSI_MCC': '555', 'IMSI_MNC': '05', 'IMSI_HLR': 'H5', 'IMSI_SI': 'S5', 'STATUS': 'E', 'VALID_TO': datetime(2023, 5, 1).date(), 'E_ID': 'E5', 'CARD_TYPE_NAME': 'B'},
        # Scenario 3: CNTRCT_ID with NULL values for some attributes
        {'CNTRCT_ID': 'C_TRANS_03', 'ICCID_TYPE': 'TN', 'ICCID': 'TN_NULLS', 'IMSI_MCC': None, 'IMSI_MNC': '06', 'IMSI_HLR': None, 'IMSI_SI': 'S6', 'STATUS': 'F', 'VALID_TO': None, 'E_ID': 'E6', 'CARD_TYPE_NAME': None},
        # Scenario 4: CNTRCT_ID with no matching ICCID_TYPEs (should result in all NULLs for pivoted columns)
        {'CNTRCT_ID': 'C_TRANS_04', 'ICCID_TYPE': 'UNKNOWN', 'ICCID': 'UNKNOWN_ICCID', 'IMSI_MCC': '999', 'IMSI_MNC': '99', 'IMSI_HLR': 'H99', 'IMSI_SI': 'S99', 'STATUS': 'Z', 'VALID_TO': datetime(2023, 12, 31).date(), 'E_ID': 'E99', 'CARD_TYPE_NAME': 'U'},
        # Scenario 5: Duplicate ICCID_TYPE for a CNTRCT_ID (MAX behavior)
        {'CNTRCT_ID': 'C_TRANS_05', 'ICCID_TYPE': 'TN', 'ICCID': 'TN_FIRST', 'IMSI_MCC': '100', 'IMSI_MNC': '01', 'IMSI_HLR': 'H1', 'IMSI_SI': 'S1', 'STATUS': 'A', 'VALID_TO': datetime(2023, 1, 1).date(), 'E_ID': 'E1', 'CARD_TYPE_NAME': 'P'},
        {'CNTRCT_ID': 'C_TRANS_05', 'ICCID_TYPE': 'TN', 'ICCID': 'TN_SECOND', 'IMSI_MCC': '101', 'IMSI_MNC': '02', 'IMSI_HLR': 'H2', 'IMSI_SI': 'S2', 'STATUS': 'B', 'VALID_TO': datetime(2023, 2, 1).date(), 'E_ID': 'E2', 'CARD_TYPE_NAME': 'Q'}, # MAX should pick 'TN_SECOND', '101', 'H2', 'S2', 'B', '2023-02-01', 'E2', 'Q'
    ]
    input_df = pd.DataFrame(input_data)

    dwtk_data = [{'job_kennung': 'BERT_DROP_TEMP_TABLE', 'timecreated': datetime(2023, 12, 31, 10, 0, 0)}]
    dwtk_df = pd.DataFrame(dwtk_data)

    # Expected output based on the transformation logic
    expected_output_data = [
        {'CNTRCT_ID': 'C_TRANS_01',
         'TN_ICCID': 'TN_X', 'TN_IMSI_MCC': '111', 'TN_IMSI_MNC': '01', 'TN_IMSI_HLR': 'H1', 'TN_IMSI_SI': 'S1', 'TN_STATUS': 'A', 'TN_VALID_TO': datetime(2023, 1, 1).date(), 'TN_E_ID': 'E1', 'TN_CARD_TYPE_NAME': 'P',
         'TC_ICCID': 'TC_Y', 'TC_IMSI_MCC': '222', 'TC_IMSI_MNC': '02', 'TC_IMSI_HLR': 'H2', 'TC_IMSI_SI': 'S2', 'TC_STATUS': 'B', 'TC_VALID_TO': datetime(2023, 2, 1).date(), 'TC_E_ID': 'E2', 'TC_CARD_TYPE_NAME': 'T',
         'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None,
         'MS1_ICCID': 'MS1_Z', 'MS1_IMSI_MCC': '333', 'MS1_IMSI_MNC': '03', 'MS1_IMSI_HLR': 'H3', 'MS1_IMSI_SI': 'S3', 'MS1_STATUS': 'C', 'MS1_VALID_TO': datetime(2023, 3, 1).date(), 'MS1_E_ID': 'E3', 'MS1_CARD_TYPE_NAME': 'M1',
         'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None,
         'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None,
         'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None,
         'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None,
         'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None,
         'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None,
         'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None,
         'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None,
         'MS10_ICCID': 'MS10_W', 'MS10_IMSI_MCC': '444', 'MS10_IMSI_MNC': '04', 'MS10_IMSI_HLR': 'H4', 'MS10_IMSI_SI': 'S4', 'MS10_STATUS': 'D', 'MS10_VALID_TO': datetime(2023, 4, 1).date(), 'MS10_E_ID': 'E4', 'MS10_CARD_TYPE_NAME': 'M10'},
        {'CNTRCT_ID': 'C_TRANS_02',
         'TN_ICCID': None, 'TN_IMSI_MCC': None, 'TN_IMSI_MNC': None, 'TN_IMSI_HLR': None, 'TN_IMSI_SI': None, 'TN_STATUS': None, 'TN_VALID_TO': None, 'TN_E_ID': None, 'TN_CARD_TYPE_NAME': None,
         'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None,
         'TB_ICCID': 'TB_SINGLE', 'TB_IMSI_MCC': '555', 'TB_IMSI_MNC': '05', 'TB_IMSI_HLR': 'H5', 'TB_IMSI_SI': 'S5', 'TB_STATUS': 'E', 'TB_VALID_TO': datetime(2023, 5, 1).date(), 'TB_E_ID': 'E5', 'TB_CARD_TYPE_NAME': 'B',
         'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID': None, 'MS1_CARD_TYPE_NAME': None,
         'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None,
         'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None,
         'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None,
         'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None,
         'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None,
         'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None,
         'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None,
         'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None,
         'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C_TRANS_03',
         'TN_ICCID': 'TN_NULLS', 'TN_IMSI_MCC': None, 'TN_IMSI_MNC': '06', 'TN_IMSI_HLR': None, 'TN_IMSI_SI': 'S6', 'TN_STATUS': 'F', 'TN_VALID_TO': None, 'TN_E_ID': 'E6', 'TN_CARD_TYPE_NAME': None,
         'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None,
         'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None,
         'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID': None, 'MS1_CARD_TYPE_NAME': None,
         'MS2_ICCID': None, 'MS2_IMSI_MCC': None, 'MS2_IMSI_MNC': None, 'MS2_IMSI_HLR': None, 'MS2_IMSI_SI': None, 'MS2_STATUS': None, 'MS2_VALID_TO': None, 'MS2_E_ID': None, 'MS2_CARD_TYPE_NAME': None,
         'MS3_ICCID': None, 'MS3_IMSI_MCC': None, 'MS3_IMSI_MNC': None, 'MS3_IMSI_HLR': None, 'MS3_IMSI_SI': None, 'MS3_STATUS': None, 'MS3_VALID_TO': None, 'MS3_E_ID': None, 'MS3_CARD_TYPE_NAME': None,
         'MS4_ICCID': None, 'MS4_IMSI_MCC': None, 'MS4_IMSI_MNC': None, 'MS4_IMSI_HLR': None, 'MS4_IMSI_SI': None, 'MS4_STATUS': None, 'MS4_VALID_TO': None, 'MS4_E_ID': None, 'MS4_CARD_TYPE_NAME': None,
         'MS5_ICCID': None, 'MS5_IMSI_MCC': None, 'MS5_IMSI_MNC': None, 'MS5_IMSI_HLR': None, 'MS5_IMSI_SI': None, 'MS5_STATUS': None, 'MS5_VALID_TO': None, 'MS5_E_ID': None, 'MS5_CARD_TYPE_NAME': None,
         'MS6_ICCID': None, 'MS6_IMSI_MCC': None, 'MS6_IMSI_MNC': None, 'MS6_IMSI_HLR': None, 'MS6_IMSI_SI': None, 'MS6_STATUS': None, 'MS6_VALID_TO': None, 'MS6_E_ID': None, 'MS6_CARD_TYPE_NAME': None,
         'MS7_ICCID': None, 'MS7_IMSI_MCC': None, 'MS7_IMSI_MNC': None, 'MS7_IMSI_HLR': None, 'MS7_IMSI_SI': None, 'MS7_STATUS': None, 'MS7_VALID_TO': None, 'MS7_E_ID': None, 'MS7_CARD_TYPE_NAME': None,
         'MS8_ICCID': None, 'MS8_IMSI_MCC': None, 'MS8_IMSI_MNC': None, 'MS8_IMSI_HLR': None, 'MS8_IMSI_SI': None, 'MS8_STATUS': None, 'MS8_VALID_TO': None, 'MS8_E_ID': None, 'MS8_CARD_TYPE_NAME': None,
         'MS9_ICCID': None, 'MS9_IMSI_MCC': None, 'MS9_IMSI_MNC': None, 'MS9_IMSI_HLR': None, 'MS9_IMSI_SI': None, 'MS9_STATUS': None, 'MS9_VALID_TO': None, 'MS9_E_ID': None, 'MS9_CARD_TYPE_NAME': None,
         'MS10_ICCID': None, 'MS10_IMSI_MCC': None, 'MS10_IMSI_MNC': None, 'MS10_IMSI_HLR': None, 'MS10_IMSI_SI': None, 'MS10_STATUS': None, 'MS10_VALID_TO': None, 'MS10_E_ID': None, 'MS10_CARD_TYPE_NAME': None},
        {'CNTRCT_ID': 'C_TRANS_04',
         'TN_ICCID': None, 'TN_IMSI_MCC': None, 'TN_IMSI_MNC': None, 'TN_IMSI_HLR': None, 'TN_IMSI_SI': None, 'TN_STATUS': None, 'TN_VALID_TO': None, 'TN_E_ID': None, 'TN_CARD_TYPE_NAME': None,
         'TC_ICCID': None, 'TC_IMSI_MCC': None, 'TC_IMSI_MNC': None, 'TC_IMSI_HLR': None, 'TC_IMSI_SI': None, 'TC_STATUS': None, 'TC_VALID_TO': None, 'TC_E_ID': None, 'TC_CARD_TYPE_NAME': None,
         'TB_ICCID': None, 'TB_IMSI_MCC': None, 'TB_IMSI_MNC': None, 'TB_IMSI_HLR': None, 'TB_IMSI_SI': None, 'TB_STATUS': None, 'TB_VALID_TO': None, 'TB_E_ID': None, 'TB_CARD_TYPE_NAME': None,
         'MS1_ICCID': None, 'MS1_IMSI_MCC': None, 'MS1_IMSI_MNC': None, 'MS1_IMSI_HLR': None, 'MS1_IMSI_SI': None, 'MS1_STATUS': None, 'MS1_VALID_TO': None, 'MS1_E_ID