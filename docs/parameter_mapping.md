# Parameter Mapping for k_ausd_bp_ta_bpr_beschr.ksh Migration

This document outlines the mapping of parameters from the legacy KornShell script
`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`
to the new BigQuery Stored Procedure `dataset.r_ausd_bp_ta_bpr_beschr` and
its orchestration via a Cloud Composer (Airflow) DAG.

## Legacy Parameters (from k_ausd_bp_ta_bpr_beschr.ksh)

The original KornShell script accepted parameters typically via command-line arguments.

| Legacy Parameter | Description                               | Example Value |
| :--------------- | :---------------------------------------- | :------------ |
| `-j p_JobKennung`  | Identifier for the job.                   | `k_ausd_bp_ta_bpr_beschr` |
| `-f p_EintragsNr`  | Entry number, potentially for uniqueness or tracking. | `1`           |
| `-s p_Stichtag`    | Reference date for data processing. Expected format 'DDMMYYYY'. | `31122023`    |
| `-l p_wiederanlaufWert` | Restart value or run indicator.         | `0`           |

## Target BigQuery Stored Procedure Parameters (`r_ausd_bp_ta_bpr_beschr`)

The BigQuery Stored Procedure `CREATE OR REPLACE PROCEDURE <project_id>.<dataset>.r_ausd_bp_ta_bpr_beschr(...)`
accepts the following `IN` parameters, which directly map from the legacy script's arguments:

| BQ Stored Procedure Parameter | Type   | Description                                                                 | Maps From Legacy |
| :---------------------------- | :----- | :-------------------------------------------------------------------------- | :--------------- |
| `job_kennung`                 | STRING | Identifier for the job, e.g., 'k_ausd_bp_ta_bpr_beschr'.                    | `p_JobKennung`   |
| `eintrags_nr`                 | STRING | Entry number for the job run.                                               | `p_EintragsNr`   |
| `stichtag_str`                | STRING | Reference date for data processing, passed as 'DDMMYYYY' string.            | `p_Stichtag`     |
| `wiederanlauf_wert`           | STRING | Value indicating restart or run condition.                                  | `p_wiederanlaufWert` |

## Cloud Composer (Airflow) DAG Configuration

The Airflow DAG `k_ausd_bp_ta_bpr_beschr_dag` will be responsible for scheduling and executing the BigQuery Stored Procedure, passing the necessary parameters.

*   **`job_kennung`**:
    *   **Source:** Airflow DAG `params` argument or Airflow Variable.
    *   **Default in DAG:** `'k_ausd_bp_ta_bpr_beschr'`
    *   **Notes:** Can be overridden at runtime or dynamically set.

*   **`eintrags_nr`**:
    *   **Source:** Airflow DAG `params` argument or Airflow Variable.
    *   **Default in DAG:** `'1'`
    *   **Notes:** Review if this value needs to be dynamic or sequential based on legacy behavior.

*   **`stichtag_str`**:
    *   **Source:** Dynamically generated using Airflow macros based on the DAG's `data_interval_start`.
    *   **Airflow Macro:** `{{ data_interval_start.strftime('%d%m%Y') }}`
    *   **Notes:** `data_interval_start` represents the logical start date of the data interval the DAG is processing. This is a common pattern for 'Stichtag'. If the logical date should be different (e.g., day before `data_interval_start`), the macro needs adjustment (e.g., `(data_interval_start - macros.timedelta(days=1)).strftime('%d%m%Y')`).

*   **`wiederanlauf_wert`**:
    *   **Source:** Airflow DAG `params` argument or Airflow Variable.
    *   **Default in DAG:** `'0'`
    *   **Notes:** Review if this needs to be dynamic based on specific restart logic.

### Example Airflow Task Parameter Configuration within DAG:

```python
BigQueryExecuteStoredProcedureOperator(
    task_id='execute_r_ausd_bp_ta_bpr_beschr',
    project_id=GCP_PROJECT_ID, # Replace with your GCP project ID
    dataset_id=BQ_DATASET_ID, # Replace with your BigQuery dataset ID
    procedure_id='r_ausd_bp_ta_bpr_beschr',
    parameters={
        'job_kennung': '{{ params.job_kennung }}',
        'eintrags_nr': '{{ params.eintrags_nr }}',
        'stichtag_str': "{{ data_interval_start.strftime('%d%m%Y') }}",
        'wiederanlauf_wert': '{{ params.wiederanlauf_wert }}'
    },
    gcp_conn_id='google_cloud_default', # Ensure 'google_cloud_default' connection is configured in Airflow
)
```

**Important Notes:**
*   **BigQuery Project and Dataset IDs:** The placeholders `<project_id>`, `<dataset>`, `GCP_PROJECT_ID`, and `BQ_DATASET_ID` must be replaced with your actual Google Cloud Project ID and BigQuery Dataset ID. These are typically managed through Airflow Variables, environment variables, or explicit configuration.
*   **Airflow `gcp_conn_id`:** Ensure the `google_cloud_default` (or your custom) Airflow connection to Google Cloud is properly configured.