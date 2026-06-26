# Airflow DAG for DW.BERT_ABLAUFSTEUERUNG
# Migrated from legacy UC4 Job Scheduler: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/03_SCHEDULER/DW.BERT_ABLAUFSTEUERUNG.xml

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
import pendulum

def _check_monthly_run_day(**kwargs):
    """
    Checks if the current execution day is the 5th or 25th of the month.
    This corresponds to the DW.NEW_CALENDAR with DAY_OF_MONTH_25 and DAY_OF_MONTH_05 keys.
    """
    execution_date = pendulum.parse(kwargs["ds"])
    if execution_date.day not in [5, 25]:
        raise AirflowSkipException(f"Skipping DW.BERT_MONATLICH_JP as it's not the 5th or 25th of the month (today is {execution_date.day}).")
    print(f"Executing DW.BERT_MONATLICH_JP on day {execution_date.day}.")

def _check_bert_nicht_exclusion(**kwargs):
    """
    Checks if the current execution day is excluded by the BERT_NICHT calendar key.
    The specific logic for BERT_NICHT needs to be implemented here.
    For demonstration, we assume BERT_NICHT excludes runs on the 10th of every month.
    """
    execution_date = pendulum.parse(kwargs["ds"])
    # TODO: Implement actual BERT_NICHT calendar logic from DW.KALENDER.
    # This might involve querying a database or an external system for specific exclusion dates.
    # For now, as a placeholder, we'll exclude the 10th day of every month.
    if execution_date.day == 10:
        raise AirflowSkipException(f"Skipping DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT due to BERT_NICHT exclusion (today is {execution_date.day}).")
    print(f"Executing DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT on day {execution_date.day}.")

with DAG(
    dag_id='dw_bert_ablaufsteuerung',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['bert', 'uc4_migration'],
    description='Orchestrates various productive processes related to Bert, migrated from UC4 JSCH.',
) as dag:
    # Task 1: DW.BERT_MONATLICH_JP (JOBP)
    # Scheduled to run on the 5th and 25th of the month with an earliest start time of 20:00.
    # The time constraint is handled by the overall DAG schedule and task dependencies.
    check_bert_monatlich_run_day = PythonOperator(
        task_id='check_bert_monatlich_jp_run_day',
        python_callable=_check_monthly_run_day,
        provide_context=True,
    )

    dw_bert_monatlich_jp = BashOperator(
        task_id='dw_bert_monatlich_jp',
        bash_command='echo "Executing DW.BERT_MONATLICH_JP (monthly job plan)..."',
        # This task would trigger a sub-DAG or run BigQuery/Python code for DW.BERT_MONATLICH_JP
    )

    # Task 2: DW.BERT_RUN_ADM_CHECK_JP_EVT (EVNT)
    # Event-driven administrative check with an earliest start time of 07:00.
    dw_bert_run_adm_check_jp_evt = BashOperator(
        task_id='dw_bert_run_adm_check_jp_evt',
        bash_command='echo "Executing DW.BERT_RUN_ADM_CHECK_JP_EVT (admin check event)..."',
        # This task would typically be an Airflow Sensor if waiting for an external event.
    )

    # Task 3: DW.BERT_ADM_HOUSEKEEPING_JP (JOBP)
    # Administrative housekeeping job plan with an earliest start time of 04:03.
    dw_bert_adm_housekeeping_jp = BashOperator(
        task_id='dw_bert_adm_housekeeping_jp',
        bash_command='echo "Executing DW.BERT_ADM_HOUSEKEEPING_JP (housekeeping job plan)..."',
    )

    # Task 4: DW.DWH_APT_EXPORT_TAEGLICH_JP (JOBP)
    # Daily APT export job plan with an earliest start time of 01:30.
    dw_dwh_apt_export_taeglich_jp = BashOperator(
        task_id='dw_dwh_apt_export_taeglich_jp',
        bash_command='echo "Executing DW.DWH_APT_EXPORT_TAEGLICH_JP (daily APT export job plan)..."',
    )

    # Task 5: DW.BERT_STAMMDATEN_JP (JOBP)
    # Master data processing job plan with an earliest start time of 01:00.
    dw_bert_stammdaten_jp = BashOperator(
        task_id='dw_bert_stammdaten_jp',
        bash_command='echo "Executing DW.BERT_STAMMDATEN_JP (master data job plan)..."',
    )

    # Task 6: DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT (EVNT)
    # Monthly APT export event with an earliest start time of 01:00,
    # explicitly excluded by the BERT_NICHT key in DW.KALENDER.
    check_bert_nicht_exclusion = PythonOperator(
        task_id='check_bert_nicht_exclusion',
        python_callable=_check_bert_nicht_exclusion,
        provide_context=True,
    )

    dw_dwh_run_apt_export_monatlich_jp_evt = BashOperator(
        task_id='dw_dwh_run_apt_export_monatlich_jp_evt',
        bash_command='echo "Executing DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT (monthly APT export event)..."',
        # This task would trigger a sub-DAG or run specific export logic.
    )

    # Define task dependencies based on the observed sequence in the UC4 XML
    check_bert_monatlich_run_day >> dw_bert_monatlich_jp
    dw_bert_monatlich_jp >> dw_bert_run_adm_check_jp_evt
    dw_bert_run_adm_check_jp_evt >> dw_bert_adm_housekeeping_jp
    dw_bert_adm_housekeeping_jp >> dw_dwh_apt_export_taeglich_jp
    dw_dwh_apt_export_taeglich_jp >> dw_bert_stammdaten_jp
    dw_bert_stammdaten_jp >> check_bert_nicht_exclusion
    check_bert_nicht_exclusion >> dw_dwh_run_apt_export_monatlich_jp_evt