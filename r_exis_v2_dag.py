"""
Airflow DAG for r_exis_v2 migration.
Replaces: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2 KornShell script.
Orchestrates data extraction, transformation, and distribution using BigQuery, Dataform, Dataflow, and GCS.
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

# Import DataformOperator if available, otherwise use BigQueryExecuteQueryOperator
try:
    from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator
    from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
    dataform_available = True
except ImportError:
    print("Dataform operators not available. Falling back to BigQueryExecuteQueryOperator for SQL tasks.")
    dataform_available = False

# [START common_variables]
# Replace with your actual GCP project ID and dataset
GCP_PROJECT_ID = "your_gcp_project_id"
BIGQUERY_DATASET = "your_bigquery_dataset"
GCS_LANDING_BUCKET = "your-gcs-landing-bucket"
GCS_TEMP_BUCKET = "your-gcs-temp-bucket"
GCP_REGION = "your-gcp-region" # e.g., us-central1

# External services
SFTP_CLOUD_RUN_SERVICE_URL = "https://your-sftp-cloud-run-service-url"
EMAIL_CLOUD_RUN_SERVICE_URL = "https://your-email-cloud-run-service-url"
# [END common_variables]

with DAG(
    dag_id="r_exis_v2_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["exporter", "bigquery", "dataform"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    }
) as dag:

    def _log_job_status(job_name, run_id, status, message, **context):
        """Helper function to log job status to BigQuery."""
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        bq_hook.run_query(
            sql=f"""
            MERGE INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.exporter_status` AS T
            USING (SELECT '{job_name}' AS job_name) AS S
            ON T.job_name = S.job_name
            WHEN MATCHED THEN
                UPDATE SET
                    last_run_id = '{run_id}',
                    status = '{status}',
                    start_time = COALESCE(T.start_time, CURRENT_TIMESTAMP()),
                    end_time = CASE WHEN '{status}' IN ('SUCCESS', 'FAILED') THEN CURRENT_TIMESTAMP() ELSE T.end_time END,
                    duration_seconds = TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), T.start_time, SECOND),
                    error_message = CASE WHEN '{status}' = 'FAILED' THEN '{message}' ELSE NULL END,
                    last_updated = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (job_name, last_run_id, status, start_time, last_updated)
                VALUES ('{job_name}', '{run_id}', '{status}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
            """
        )
        bq_hook.run_query(
            sql=f"""
            INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.exporter_log`
            (job_name, run_id, log_timestamp, log_level, task_id, message)
            VALUES (
                '{job_name}',
                '{run_id}',
                CURRENT_TIMESTAMP(),
                'INFO',
                '{context["task"].task_id}',
                '{message}'
            );
            """
        )

    def _get_job_config(job_name, **context):
        """Fetches job configuration from BigQuery exporter_config table."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        query = f"""
            SELECT config_key, config_value
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.exporter_config`
            WHERE job_name = '{job_name}';
        """
        records = bq_hook.get_records(query)
        config = {rec[0]: json.loads(rec[1]) for rec in records}
        context["ti"].xcom_push(key="job_config", value=config)
        return config

    def _resolve_parameters(job_config, ds, **context):
        """Resolves dynamic parameters like FROM, TO, SEQ based on config and execution date."""
        # This function would contain complex logic from 'fillattribs' and 'handletimestamps'
        # For simplicity, we'll use a basic date range here.
        import datetime

        from_date = context["data_interval_start"].in_tz("UTC").format("YYYY-MM-DD")
        to_date = context["data_interval_end"].in_tz("UTC").format("YYYY-MM-DD")
        
        # Example of using the BQ UDF for date formatting if needed
        # from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        # bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        # formatted_from_date = bq_hook.run_query(f"SELECT `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.handletimestamps_bq`(DATE '{from_date}', '%Y%m%d')").fetchone()[0]

        resolved_params = {
            "JOB_NAME": "r_exis_v2",
            "FROM_DATE": from_date,
            "TO_DATE": to_date,
            "EXECUTION_DATE_YYYYMMDD": ds.replace("-", ""),
            "SYSDATE": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            # Add other dynamic parameters as needed based on original script logic
        }
        context["ti"].xcom_push(key="resolved_params", value=resolved_params)
        return resolved_params

    def _determine_file_partitions(job_config, resolved_params, **context):
        """Determines file partitions using BigQuery Stored Procedure."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")

        start_date = resolved_params["FROM_DATE"]
        end_date = resolved_params["TO_DATE"]
        partition_unit = job_config.get("META", {}).get("partition_unit", "DAY") # Default to DAY

        call_sp_sql = f"""
            DECLARE p_details ARRAY<STRUCT<partition_start_date DATE, partition_end_date DATE, partition_label STRING>>;
            CALL `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.get_file_partitions_bq`(DATE '{start_date}', DATE '{end_date}', '{partition_unit}', p_details);
            SELECT * FROM UNNEST(p_details);
        """
        records = bq_hook.get_records(call_sp_sql)
        partitions = [
            {"start_date": rec[0].strftime("%Y-%m-%d"), "end_date": rec[1].strftime("%Y-%m-%d"), "label": rec[2]}
            for rec in records
        ]
        context["ti"].xcom_push(key="file_partitions", value=partitions)
        return partitions

    start_dag_status = PythonOperator(
        task_id="start_dag_status",
        python_callable=_log_job_status,
        op_kwargs={
            "job_name": "r_exis_v2",
            "run_id": "{{ run_id }}",
            "status": "RUNNING",
            "message": "DAG started",
        },
    )

    get_job_config = PythonOperator(
        task_id="get_job_config",
        python_callable=_get_job_config,
        op_kwargs={"job_name": "r_exis_v2"},
    )

    resolve_parameters = PythonOperator(
        task_id="resolve_parameters",
        python_callable=_resolve_parameters,
        op_kwargs={
            "job_config": "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config') }}"
        },
    )

    determine_file_partitions = PythonOperator(
        task_id="determine_file_partitions",
        python_callable=_determine_file_partitions,
        op_kwargs={
            "job_config": "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config') }}",
            "resolved_params": "{{ task_instance.xcom_pull(task_ids='resolve_parameters', key='resolved_params') }}"
        },
    )

    # Dynamic task generation for file partitions
    # This will be done in a later step if the number of partitions is significant,
    # or if we need to iterate over partitions in the DAG.
    # For now, let's assume a single partition for simplicity or that Dataform handles partitioning internally.

    # [START pre_sql_task]
    if dataform_available:
        pre_sql_task = DataformCreateCompilationResultOperator(
            task_id="pre_sql_task",
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            repository_id="your_dataform_repository", # Replace with your Dataform repository ID
            workspace_id="your_dataform_workspace",   # Replace with your Dataform workspace ID
            compilation_result={
                "git_commitish": "main", # Or specific branch/tag
                "workflow_config": {
                    "included_targets": [{"name": "r_exis_v2_sql_model_pre"}] # Dataform model for PRE_SQL
                }
            }
        ) >> DataformCreateWorkflowInvocationOperator(
            task_id="run_pre_sql",
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            repository_id="your_dataform_repository",
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull(task_ids='pre_sql_task', key='return_value')['name'] }}"
            }
        )
    else:
        pre_sql_content = "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config')['PRE_SQL']['sql'] }}"
        # Use fillattribs_bq or Python string formatting to substitute parameters
        pre_sql_task = BigQueryExecuteQueryOperator(
            task_id="pre_sql_task",
            sql=f"SELECT `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fillattribs_bq`('{pre_sql_content}', TO_JSON((SELECT AS STRUCT * FROM UNNEST([{{ task_instance.xcom_pull(task_ids='resolve_parameters', key='resolved_params') | tojson }}]))))",
            use_legacy_sql=False,
            gcp_conn_id="google_cloud_default",
            destination_dataset_table=None, # PRE_SQL might not write to a table
        )
    # [END pre_sql_task]

    # [START output_sql_task]
    if dataform_available:
        output_sql_task = DataformCreateCompilationResultOperator(
            task_id="output_sql_task",
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            repository_id="your_dataform_repository",
            workspace_id="your_dataform_workspace",
            compilation_result={
                "git_commitish": "main",
                "workflow_config": {
                    "included_targets": [{"name": "r_exis_v2_sql_model_output"}] # Dataform model for OUTPUT_SQL
                }
            }
        ) >> DataformCreateWorkflowInvocationOperator(
            task_id="run_output_sql",
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            repository_id="your_dataform_repository",
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull(task_ids='output_sql_task', key='return_value')['name'] }}"
            }
        )
    else:
        output_sql_content = "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config')['OUTPUT_SQL']['sql'] }}"
        # This assumes OUTPUT_SQL creates a temporary table or view that will be exported.
        # For actual implementation, Dataform is highly recommended for managing parameterized SQL.
        output_sql_task = BigQueryExecuteQueryOperator(
            task_id="output_sql_task",
            sql=f"CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.temp_r_exis_output_{{{{ run_id | replace('-', '_') }}}}` AS "
                f"SELECT `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fillattribs_bq`('{output_sql_content}', TO_JSON((SELECT AS STRUCT * FROM UNNEST([{{ task_instance.xcom_pull(task_ids='resolve_parameters', key='resolved_params') | tojson }}]))))",
            use_legacy_sql=False,
            gcp_conn_id="google_cloud_default",
            destination_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.temp_r_exis_output_{{{{ run_id | replace('-', '_') }}}}",
            write_disposition="WRITE_TRUNCATE",
        )
    # [END output_sql_task]

    # [START dataflow_complex_transformations]
    # This task is conditional based on whether complex text processing is required.
    # The template_gcs_path should point to a Dataflow template stored in GCS.
    # The actual job parameters for Dataflow will depend on the pipeline logic.
    dataflow_pipeline_params = {
        "inputFile": f"gs://{GCS_LANDING_BUCKET}/temp_r_exis_output_{{{{ run_id | replace('-', '_') }}}}/*", # If BQ to GCS is done first
        "outputFile": f"gs://{GCS_LANDING_BUCKET}/processed_r_exis_output_{{{{ run_id | replace('-', '_') }}}}/data.csv",
        # ... other Dataflow specific parameters
    }
    dataflow_complex_transformations = DataflowTemplatedJobStartOperator(
        task_id="dataflow_complex_transformations",
        template_gcs_path=f"gs://{GCS_TEMP_BUCKET}/dataflow_templates/r_exis_v2_dataflow_pipeline", # Path to your Dataflow template
        parameters=dataflow_pipeline_params,
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        gcp_conn_id="google_cloud_default",
        # Only run if `r_exis_v2_dataflow_pipeline.py` is necessary based on config
        # depends_on_past=False,
        # trigger_rule=TriggerRule.ALL_SUCCESS,
        # wait_for_downstream=False,
    )
    # [END dataflow_complex_transformations]

    # [START export_to_gcs]
    export_to_gcs = BigQueryToGCSOperator(
        task_id="export_to_gcs",
        source_project_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.temp_r_exis_output_{{{{ run_id | replace('-', '_') }}}}",
        destination_cloud_storage_uris=[
            f"gs://{GCS_LANDING_BUCKET}/r_exis_v2/output_data_{{{{ ds_nodash }}}}_{{{{ run_id | replace('-', '_') }}}}.csv"
        ],
        compression="GZIP", # Based on design document mentioning 'compress'
        export_format="CSV",
        field_delimiter=(
            "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config')['META']['delimiter'] }}"
            if 'META' in "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config') }}"
            else ","
        ),
        print_header=(
            "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config')['META']['header'] }}"
            if 'META' in "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config') }}"
            else True
        ),
        gcp_conn_id="google_cloud_default",
    )
    # [END export_to_gcs]

    # [START post_sql_task]
    if dataform_available:
        post_sql_task = DataformCreateCompilationResultOperator(
            task_id="post_sql_task",
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            repository_id="your_dataform_repository",
            workspace_id="your_dataform_workspace",
            compilation_result={
                "git_commitish": "main",
                "workflow_config": {
                    "included_targets": [{"name": "r_exis_v2_sql_model_post"}] # Dataform model for POST_SQL
                }
            }
        ) >> DataformCreateWorkflowInvocationOperator(
            task_id="run_post_sql",
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            repository_id="your_dataform_repository",
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull(task_ids='post_sql_task', key='return_value')['name'] }}"
            }
        )
    else:
        post_sql_content = "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config')['POST_SQL']['sql'] }}"
        post_sql_task = BigQueryExecuteQueryOperator(
            task_id="post_sql_task",
            sql=f"SELECT `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fillattribs_bq`('{post_sql_content}', TO_JSON((SELECT AS STRUCT * FROM UNNEST([{{ task_instance.xcom_pull(task_ids='resolve_parameters', key='resolved_params') | tojson }}]))))",
            use_legacy_sql=False,
            gcp_conn_id="google_cloud_default",
            destination_dataset_table=None, # POST_SQL might not write to a table
        )
    # [END post_sql_task]

    # [START queue_distribution_task]
    def _queue_external_distribution(job_name, run_id, gcs_file_uri, job_config, **context):
        """Adds a record to the exporter_distribution_queue for external services."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        import uuid
        import json

        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        distribution_config = job_config.get("DISTRIBUTION", {})
        
        if not distribution_config:
            print("No distribution configuration found. Skipping external distribution.")
            return

        distribution_method = distribution_config.get("method", "UNKNOWN")
        target_details = json.dumps(distribution_config) # Store full config for target service

        queue_id = str(uuid.uuid4())
        insert_sql = f"""
            INSERT INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.exporter_distribution_queue`
            (queue_id, job_name, run_id, file_path_gcs, distribution_method, target_details, status)
            VALUES (
                '{queue_id}',
                '{job_name}',
                '{run_id}',
                '{gcs_file_uri}',
                '{distribution_method}',
                PARSE_JSON('{target_details}'),
                'PENDING'
            );
        """
        bq_hook.run_query(sql=insert_sql)
        context["ti"].xcom_push(key="distribution_queue_id", value=queue_id)


    queue_external_distribution = PythonOperator(
        task_id="queue_external_distribution",
        python_callable=_queue_external_distribution,
        op_kwargs={
            "job_name": "r_exis_v2",
            "run_id": "{{ run_id }}",
            "gcs_file_uri": (
                f"gs://{GCS_LANDING_BUCKET}/r_exis_v2/output_data_{{{{ ds_nodash }}}}_{{{{ run_id | replace('-', '_') }}}}.csv"
            ),
            "job_config": "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config') }}"
        },
    )
    # [END queue_distribution_task]

    # [START cloud_run_sftp_email_tasks]
    # Example for invoking a Cloud Run service for SFTP/SCP.
    # Requires a custom Airflow operator for Cloud Run or using a simple PythonOperator with requests.
    # from airflow.providers.http.operators.http import SimpleHttpOperator # If Cloud Run is exposed via HTTP
    def _call_cloud_run_service(service_url, payload, **context):
        import requests
        import google.auth
        import google.auth.transport.requests

        credentials, project = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        headers = {"Authorization": f"Bearer {credentials.id_token}"}
        
        response = requests.post(service_url, json=payload, headers=headers)
        response.raise_for_status()
        print(f"Cloud Run service responded: {response.text}")
        return response.json()

    call_sftp_cloud_run = PythonOperator(
        task_id="call_sftp_cloud_run",
        python_callable=_call_cloud_run_service,
        op_kwargs={
            "service_url": SFTP_CLOUD_RUN_SERVICE_URL,
            "payload": {
                "queue_id": "{{ task_instance.xcom_pull(task_ids='queue_external_distribution', key='distribution_queue_id') }}",
                "file_path_gcs": (
                    f"gs://{GCS_LANDING_BUCKET}/r_exis_v2/output_data_{{{{ ds_nodash }}}}_{{{{ run_id | replace('-', '_') }}}}.csv"
                ),
                "distribution_config": "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config')['DISTRIBUTION'] }}"
            }
        },
        # Only run if SFTP is the method from config
        trigger_rule=TriggerRule.ONE_SUCCESS, # Will only run if queueing succeeded
    )

    call_email_cloud_run = PythonOperator(
        task_id="call_email_cloud_run",
        python_callable=_call_cloud_run_service,
        op_kwargs={
            "service_url": EMAIL_CLOUD_RUN_SERVICE_URL,
            "payload": {
                "job_name": "r_exis_v2",
                "run_id": "{{ run_id }}",
                "status": "SUCCESS",
                "message": "Exporter job completed successfully.",
                "recipients": "{{ task_instance.xcom_pull(task_ids='get_job_config', key='job_config')['DISTRIBUTION']['email_recipients'] }}"
            }
        },
        # Only run if email_on_success is true in config
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    # [END cloud_run_sftp_email_tasks]

    # [START cleanup_temp_files]
    cleanup_temp_bq_table = BigQueryExecuteQueryOperator(
        task_id="cleanup_temp_bq_table",
        sql=f"DROP TABLE IF EXISTS `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.temp_r_exis_output_{{{{ run_id | replace('-', '_') }}}}`;",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ALL_DONE, # Always run even if upstream fails
    )

    cleanup_temp_gcs_files = GCSDeleteObjectsOperator(
        task_id="cleanup_temp_gcs_files",
        bucket_name=GCS_TEMP_BUCKET,
        prefix=f"dataflow_temp/r_exis_v2_{{{{ run_id | replace('-', '_') }}}}/", # Prefix for Dataflow temp files
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END cleanup_temp_files]

    end_dag_status_success = PythonOperator(
        task_id="end_dag_status_success",
        python_callable=_log_job_status,
        op_kwargs={
            "job_name": "r_exis_v2",
            "run_id": "{{ run_id }}",
            "status": "SUCCESS",
            "message": "DAG completed successfully",
        },
        trigger_rule=TriggerRule.ALL_SUCCESS, # Only runs if all upstream tasks succeed
    )

    end_dag_status_failure = PythonOperator(
        task_id="end_dag_status_failure",
        python_callable=_log_job_status,
        op_kwargs={
            "job_name": "r_exis_v2",
            "run_id": "{{ run_id }}",
            "status": "FAILED",
            "message": "DAG failed",
        },
        trigger_rule=TriggerRule.ONE_FAILED, # Runs if any upstream task fails
    )

    # Define task dependencies
    start_dag_status >> get_job_config >> resolve_parameters >> determine_file_partitions

    # If Dataform is available, use it for SQL tasks. Otherwise, use BigQueryExecuteQueryOperator.
    if dataform_available:
        determine_file_partitions >> pre_sql_task >> output_sql_task
    else:
        # Note: Dynamic SQL using fillattribs_bq in BigQueryExecuteQueryOperator might be complex
        # and prone to injection if not handled carefully. Dataform or Python templating is preferred.
        determine_file_partitions >> pre_sql_task
        pre_sql_task >> output_sql_task # Assuming pre_sql doesn't directly influence output_sql table creation
    
    # Conditional Dataflow task
    # To properly implement conditional execution of Dataflow based on config,
    # you'd use a BranchPythonOperator or check config directly within this DAG.
    # For now, it's chained directly as an example.
    output_sql_task >> dataflow_complex_transformations >> export_to_gcs

    # If Dataflow is not needed, then:
    # output_sql_task >> export_to_gcs

    export_to_gcs >> post_sql_task >> queue_external_distribution
    queue_external_distribution >> [call_sftp_cloud_run, call_email_cloud_run]

    # Cleanup tasks should run regardless of upstream success/failure
    [call_sftp_cloud_run, call_email_cloud_run, post_sql_task] >> cleanup_temp_bq_table
    [export_to_gcs, dataflow_complex_transformations] >> cleanup_temp_gcs_files

    # Final status tasks
    [cleanup_temp_bq_table, cleanup_temp_gcs_files] >> end_dag_status_success
    [cleanup_temp_bq_table, cleanup_temp_gcs_files] >> end_dag_status_failure