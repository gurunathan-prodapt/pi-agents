import pendulum

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time import TimeSensor

# Header comment
# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml
# Job: DW.BERT_ABLAUFSTEUERUNG

# --- Global Configuration ---
# Placeholder for project-specific values. Replace these.
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"
# The original UC4 scheduler typically runs daily.
# PLACEHOLDER_START_DATE should be adjusted to when you want this DAG to start running.
PLACEHOLDER_START_DATE = pendulum.datetime(2023, 1, 1, tz="UTC")


# --- Task Definitions ---

def _guard_active_run(**context):
    """
    Checks for concurrently running DAGs. If another active run is detected,
    raises AirflowSkipException to implement UC4's Else=Skip behavior.
    This function skips the current run if any other run of the same DAG
    is currently in a 'running' state.
    """
    dag_id = context['dag'].dag_id
    current_run_id = context['dag_run'].run_id

    # Find all active runs for this DAG
    # Exclude the current run itself to prevent self-referential skipping
    concurrent_runs = DagRun.find(dag_id=dag_id, state="running")
    concurrent_runs = [run for run in concurrent_runs if run.run_id != current_run_id]

    if concurrent_runs:
        concurrent_run_ids = [run.run_id for run in concurrent_runs]
        raise AirflowSkipException(
            f"Skipping this DAG run ({current_run_id}) as other active run(s) "
            f"for DAG '{dag_id}' are already running: {concurrent_run_ids}"
        )
    print(f"No concurrent active runs found for DAG '{dag_id}'. Proceeding with run {current_run_id}.")


def _calendar_check_dw_bert_monatlich_jp(**context):
    """
    PLACEHOLDER: Implement the UC4 calendar logic for DW.NEW_CALENDAR.
    This calendar uses keys DAY_OF_MONTH_25 and DAY_OF_MONTH_05.
    Manual analysis of UC4 calendar definitions is required here.
    If the current date does not match the calendar conditions, raise AirflowSkipException.
    """
    logical_date = context['logical_date']
    print(f"Placeholder: Checking calendar DW.NEW_CALENDAR for DW.BERT_MONATLICH_JP on {logical_date.to_date_string()}.")
    #
    # REAL LOGIC GOES HERE.
    # Example:
    # current_day = logical_date.day
    # if current_day not in [5, 25]:
    #     raise AirflowSkipException(
    #         f"Skipping DW.BERT_MONATLICH_JP: Not a calendar day (current day: {current_day}). "
    #         f"Expected days: 5, 25 based on DW.NEW_CALENDAR description."
    #     )
    # print(f"Calendar check passed for DW.BERT_MONATLICH_JP on {logical_date.to_date_string()}.")
    #
    pass  # Remove this and add actual calendar logic.


def _calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt(**context):
    """
    PLACEHOLDER: Implement the UC4 calendar logic for DW.KALENDER.
    This calendar uses key BERT_NICHT.
    Manual analysis of UC4 calendar definitions is required here.
    If the current date does not match the calendar conditions, raise AirflowSkipException.
    """
    logical_date = context['logical_date']
    print(f"Placeholder: Checking calendar DW.KALENDER for DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT on {logical_date.to_date_string()}.")
    #
    # REAL LOGIC GOES HERE.
    # Example: Assuming "BERT_NICHT" means not on weekends.
    # if logical_date.weekday() in [pendulum.SATURDAY, pendulum.SUNDAY]:
    #     raise AirflowSkipException(
    #         f"Skipping DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT: "
    #         f"Not a calendar day (current day is {logical_date.day_name()}). "
    #         f"Based on 'BERT_NICHT' calendar, assumed not to run on weekends."
    #     )
    # print(f"Calendar check passed for DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT on {logical_date.to_date_string()}.")
    #
    pass  # Remove this and add actual calendar logic.


with DAG(
    dag_id='dw_bert_ablaufsteuerung',
    start_date=PLACEHOLDER_START_DATE,
    schedule=None,  # Set schedule to None if triggered externally or via TimeSensors,
                    # or specify a cron expression (e.g., '0 0 * * *' for daily midnight).
    catchup=False,  # Set to True if historical DAG runs should be backfilled.
    tags=['bert', 'uc4', 'scheduler'],
    doc_md="""
    ### DW.BERT_ABLAUFSTEUERUNG Airflow DAG
    This DAG orchestrates various productive processes related to 'Bert',
    migrated from a UC4 Job Scheduler (JSCH). It defines a sequence of child
    job plans (JOBP) and events (EVNT) with specific start times and calendar
    dependencies, triggering other downstream Airflow DAGs.

    **Original UC4 Source**: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml`
    """,
) as dag:

    # Task to ensure only one instance of the DAG is running at a time (UC4 Else=Skip behavior)
    guard_active_run = PythonOperator(
        task_id='guard_active_run',
        python_callable=_guard_active_run,
        provide_context=True,
    )

    # 1. DW.BERT_MONATLICH_JP (JOBP): Monthly workflow. Earliest start time: 20:00. Calendar dependent (DW.NEW_CALENDAR).
    wait_until_20_00_for_dw_bert_monatlich_jp = TimeSensor(
        task_id='wait_until_20_00_for_dw_bert_monatlich_jp',
        target_time="20:00:00",
        poke_interval=60,  # Check every minute
    )

    calendar_check_dw_bert_monatlich_jp = PythonOperator(
        task_id='calendar_check_dw_bert_monatlich_jp',
        python_callable=_calendar_check_dw_bert_monatlich_jp,
        provide_context=True,
    )

    trigger_dw_bert_monatlich_jp = TriggerDagRunOperator(
        task_id='trigger_dw_bert_monatlich_jp',
        trigger_dag_id='dw_bert_monatlich_jp',  # Ensure this DAG ID matches the actual child DAG
        wait_for_completion=True,  # Wait for the triggered DAG to complete
        conf={"message": "Triggered by dw_bert_ablaufsteuerung"},
    )

    # 2. DW.BERT_RUN_ADM_CHECK_JP_EVT (EVNT): Event task for admin check workflow. Earliest start time: 07:00.
    wait_until_07_00_for_dw_bert_run_adm_check_jp_evt = TimeSensor(
        task_id='wait_until_07_00_for_dw_bert_run_adm_check_jp_evt',
        target_time="07:00:00",
        poke_interval=60,
    )

    trigger_dw_bert_run_adm_check_jp_evt = TriggerDagRunOperator(
        task_id='trigger_dw_bert_run_adm_check_jp_evt',
        trigger_dag_id='dw_bert_run_adm_check_jp_evt',  # Ensure this DAG ID matches the actual child DAG
        wait_for_completion=True,
        conf={"message": "Triggered by dw_bert_ablaufsteuerung"},
    )

    # 3. DW.BERT_ADM_HOUSEKEEPING_JP (JOBP): Admin housekeeping workflow. Earliest start time: 04:03.
    wait_until_04_03_for_dw_bert_adm_housekeeping_jp = TimeSensor(
        task_id='wait_until_04_03_for_dw_bert_adm_housekeeping_jp',
        target_time="04:03:00",
        poke_interval=60,
    )

    trigger_dw_bert_adm_housekeeping_jp = TriggerDagRunOperator(
        task_id='trigger_dw_bert_adm_housekeeping_jp',
        trigger_dag_id='dw_bert_adm_housekeeping_jp',  # Ensure this DAG ID matches the actual child DAG
        wait_for_completion=True,
        conf={"message": "Triggered by dw_bert_ablaufsteuerung"},
    )

    # 4. DW.DWH_APT_EXPORT_TAEGLICH_JP (JOBP): Daily APT export workflow. Earliest start time: 01:30.
    wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp = TimeSensor(
        task_id='wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp',
        target_time="01:30:00",
        poke_interval=60,
    )

    trigger_dw_dwh_apt_export_taeglich_jp = TriggerDagRunOperator(
        task_id='trigger_dw_dwh_apt_export_taeglich_jp',
        trigger_dag_id='dw_dwh_apt_export_taeglich_jp',  # Ensure this DAG ID matches the actual child DAG
        wait_for_completion=True,
        conf={"message": "Triggered by dw_bert_ablaufsteuerung"},
    )

    # 5. DW.BERT_STAMMDATEN_JP (JOBP): Master data workflow. Earliest start time: 01:00.
    wait_until_01_00_for_dw_bert_stammdaten_jp = TimeSensor(
        task_id='wait_until_01_00_for_dw_bert_stammdaten_jp',
        target_time="01:00:00",
        poke_interval=60,
    )

    trigger_dw_bert_stammdaten_jp = TriggerDagRunOperator(
        task_id='trigger_dw_bert_stammdaten_jp',
        trigger_dag_id='dw_bert_stammdaten_jp',  # Ensure this DAG ID matches the actual child DAG
        wait_for_completion=True,
        conf={"message": "Triggered by dw_bert_ablaufsteuerung"},
    )

    # 6. DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT (EVNT): Event task for monthly APT export workflow. Earliest start time: 01:00. Calendar dependent (DW.KALENDER).
    wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt = TimeSensor(
        task_id='wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt',
        target_time="01:00:00",
        poke_interval=60,
    )

    calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt = PythonOperator(
        task_id='calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt',
        python_callable=_calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt,
        provide_context=True,
    )

    trigger_dw_dwh_run_apt_export_monatlich_jp_evt = TriggerDagRunOperator(
        task_id='trigger_dw_dwh_run_apt_export_monatlich_jp_evt',
        trigger_dag_id='dw_dwh_run_apt_export_monatlich_jp_evt',  # Ensure this DAG ID matches the actual child DAG
        wait_for_completion=True,
        conf={"message": "Triggered by dw_bert_ablaufsteuerung"},
    )

    # --- Task Dependencies ---
    guard_active_run >> wait_until_20_00_for_dw_bert_monatlich_jp
    wait_until_20_00_for_dw_bert_monatlich_jp >> calendar_check_dw_bert_monatlich_jp
    calendar_check_dw_bert_monatlich_jp >> trigger_dw_bert_monatlich_jp
    trigger_dw_bert_monatlich_jp >> wait_until_07_00_for_dw_bert_run_adm_check_jp_evt
    wait_until_07_00_for_dw_bert_run_adm_check_jp_evt >> trigger_dw_bert_run_adm_check_jp_evt
    trigger_dw_bert_run_adm_check_jp_evt >> wait_until_04_03_for_dw_bert_adm_housekeeping_jp
    wait_until_04_03_for_dw_bert_adm_housekeeping_jp >> trigger_dw_bert_adm_housekeeping_jp
    trigger_dw_bert_adm_housekeeping_jp >> wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp
    wait_until_01_30_for_dw_dwh_apt_export_taeglich_jp >> trigger_dw_dwh_apt_export_taeglich_jp
    trigger_dw_dwh_apt_export_taeglich_jp >> wait_until_01_00_for_dw_bert_stammdaten_jp
    wait_until_01_00_for_dw_bert_stammdaten_jp >> trigger_dw_bert_stammdaten_jp
    trigger_dw_bert_stammdaten_jp >> wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt
    wait_until_01_00_for_dw_dwh_run_apt_export_monatlich_jp_evt >> calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt
    calendar_check_dw_dwh_run_apt_export_monatlich_jp_evt >> trigger_dw_dwh_run_apt_export_monatlich_jp_evt