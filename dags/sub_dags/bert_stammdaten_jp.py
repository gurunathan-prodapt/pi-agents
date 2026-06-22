# Legacy Source: JOBP:DW.BERT_STAMMDATEN_JP (child of DW.BERT_ABLAUFSTEUERUNG)
# Job: DW.BERT_ABLAUFSTEUERUNG
# Airflow TaskGroup for Bert master data processes.

from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

def create_bert_stammdaten_jp_taskgroup(dag):
    with TaskGroup("bert_stammdaten_jp", dag=dag) as bert_stammdaten_jp_group:
        start_stammdaten = DummyOperator(task_id='start_stammdaten')

        # DW.BERT_DROP_TEMP_TABLE
        bert_drop_temp_table = BigQueryExecuteQueryOperator(
            task_id='bert_drop_temp_table',
            sql="DROP TABLE IF EXISTS `{{ var.value.TARGET_BQ_PROJECT }}.{{ var.value.TARGET_BQ_DATASET }}.temp_bert_table`;", # Placeholder SQL
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default',
        )

        # DW.BERT_P_ADRESSEN (Placeholder for a Python/SQL task)
        bert_p_adressen = PythonOperator(
            task_id='bert_p_adressen',
            python_callable=lambda: print("Processing BERT_P_ADRESSEN..."),
        )

        # DW.BERT_P_AUSTAUSCH (Placeholder for a Python/SQL task)
        bert_p_austausch = PythonOperator(
            task_id='bert_p_austausch',
            python_callable=lambda: print("Processing BERT_P_AUSTAUSCH..."),
        )

        # JOBP:DW.BERT_P_BASISPRODUKT_JP (Nested TaskGroup)
        with TaskGroup("bert_p_basisprodukt_jp") as bert_p_basisprodukt_jp_group:
            # DW.BERT_AUSD_BP_TA_APN_CARMEN
            bert_ausd_bp_ta_apn_carmen = PythonOperator(
                task_id='bert_ausd_bp_ta_apn_carmen',
                python_callable=lambda: print("Processing BERT_AUSD_BP_TA_APN_CARMEN..."),
            )
            # DW.BERT_AUSD_BP_TA_APN_VERTRAG
            bert_ausd_bp_ta_apn_vertrag = PythonOperator(
                task_id='bert_ausd_bp_ta_apn_vertrag',
                python_callable=lambda: print("Processing BERT_AUSD_BP_TA_APN_VERTRAG..."),
            )
            # DW.BERT_AUSD_BP_TA_BCP_ICCID
            bert_ausd_bp_ta_bcc_iccid = PythonOperator(
                task_id='bert_ausd_bp_ta_bcc_iccid',
                python_callable=lambda: print("Processing BERT_AUSD_BP_TA_BCP_ICCID..."),
            )
            # Define dependencies within the nested TaskGroup if any, otherwise they run in parallel
            [
                bert_ausd_bp_ta_apn_carmen,
                bert_ausd_bp_ta_apn_vertrag,
                bert_ausd_bp_ta_bcc_iccid
            ]

        start_stammdaten >> bert_drop_temp_table >> bert_p_adressen >> bert_p_austausch >> bert_p_basisprodukt_jp_group

    return bert_stammdaten_jp_group