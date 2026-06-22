"""
Airflow DAG for migrating DW.BERT_AUSD_V_TA_VERTRAG_TMP.

This DAG runs on a manual or scheduled basis and performs three steps:
1) initialize_job_parameters: computes the v_datum cutoff date from the source
   dwtk_meldungen table and prepares runtime parameters.
2) execute_contract_transformation: runs the BigQuery SQL transformation that
   truncates bert_staging.ta_vertrag_tmp and reloads it with transformed contract
   data from the source BigQuery tables.
3) handle_job_completion: final success/logging step to mirror legacy completion
   handling.

Schedule: None (manual trigger only, per design document pseudocode).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# GCP configuration placeholders - replace with your actual values.
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"  # TODO: replace with your GCP project ID
BIGQUERY_REGION = "YOUR_BIGQUERY_REGION"  # TODO: replace with your BigQuery region
SOURCE_DATASET = "bert_source"
TARGET_DATASET = "bert_staging"
TARGET_TABLE = "ta_vertrag_tmp"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def initialize_job_parameters(**context):
    """
    Computes the v_datum cutoff date from the source dwtk_meldungen table
    and pushes it to XCom for downstream use.
    """
    from google.cloud import bigquery
    client = bigquery.Client()

    # Construct the fully qualified table name for dwtk_meldungen
    dwtk_meldungen_table_ref = f"{GCP_PROJECT_ID}.{SOURCE_DATASET}.dwtk_meldungen"

    query = f"""
    SELECT COALESCE(MAX(FORMAT_DATE('%Y%m%d', DATE(m.timecreated))), '19000101')
    FROM `{dwtk_meldungen_table_ref}` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    query_job = client.query(query)
    results = query_job.result()
    v_datum = [row[0] for row in results][0]
    context['ti'].xcom_push(key='v_datum', value=v_datum)


def handle_job_completion(**context):
    """
    Placeholder for legacy completion logging/status update handling.
    """
    # TODO: implement success logging or status update logic
    pass


with DAG(
    dag_id="dw_bert_ausd_v_ta_vertrag_tmp",
    default_args=default_args,
    description="Migrated contract transformation job for bert_staging.ta_vertrag_tmp",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bert", "bigquery", "migration"],
) as dag:
    # Task 1: Initialize runtime parameters and compute v_datum.
    initialize_job_parameters_task = PythonOperator(
        task_id="initialize_job_parameters",
        python_callable=initialize_job_parameters,
    )

    # Task 2: Truncate and reload the target staging table using BigQuery SQL.
    # The SQL template uses {{ ti.xcom_pull(...) }} to retrieve v_datum from the previous task.
    execute_contract_transformation = BigQueryInsertJobOperator(
        task_id="execute_contract_transformation",
        project_id=GCP_PROJECT_ID,
        location=BIGQUERY_REGION,
        configuration={
            "query": {
                "query": """
TRUNCATE TABLE `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`;

INSERT INTO `{GCP_PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` (
        vertrag_id_carmen,
        partner_id_carmen,
        rechdef_id_carmen,
        kundenkonto,
        mwst_kennzeichen,
        rahmenvertrag_id,
        rechnungslauf,
        vo_kenn,
        order_number,
        geplant_kuend,
        eingang_kuend,
        vertragsbeginn,
        vertragsstatus,
        sperrart,
        sperrgrund,
        stillegungszeitraum,
        twincard,
        dwh_tarifgr_text,
        bindefrist,
        letztes_upgrade,
        vertragsbindung,
        vertragsbindungseinheit,
        rechnungszahlart,
        rechnungsmedium,
        twin_vertrag_id,
        upgradeberechtigt,
        apn,
        upgradegrund,
        SV_Id,
        VDA,
        cost_centre,
        cost_centre_user,
        cntrct_ty,
        segment_id,
        rv_action_id,
        rechn_inh_konfig_text,
        commitment_reference_date,
        cntrct_validity_id)
SELECT
       c.cntrct_id                      AS vertrag_id_carmen,
       bp.bp_id                         AS partner_id_carmen,
       ia.inv_definition_id             AS rechdef_id_carmen,
       ia.account_reference             AS kundenkonto,
       ia.sales_tax_freed               AS mwst_kennzeichen,
       c.rv_num                         AS rahmenvertrag_id,
       ia.billcycle_id                  AS rechnungslauf,
       c.vo_code                        AS vo_kenn,
       c.order_number                   AS order_number,
       n.valid_from                     AS geplant_kuend,
       n.entry_date_of_notice           AS eingang_kuend,
       c.cntrct_start_date              AS vertragsbeginn,
       CASE c.cntrct_st
                           WHEN 5 THEN 'A'
                           WHEN 6 THEN 'L'
                             ELSE ''
                             END    AS vertragsstatus,
       b.sperrart_alle                  AS sperrart,
       b.sperrgrund_alle                AS sperrgrund,
       b.stillegungszeitraum_alle      AS stillegungszeitraum,
       c.twinbill                       AS twincard,
       ct.cds_description               AS dwh_tarifgr_text,
       bf.bindefrist                    AS bindefrist,
       vvl.upgradedatum                 AS letztes_upgrade,
       p.number_time_measurement        AS vertragsbindung,
       p.einheit                        AS vertragsbindungseinheit,
       CASE ia.inv_pay_ty_cv
              WHEN 1 THEN 'U'
              WHEN 2 THEN 'E'
              WHEN 3 THEN 'K'
              WHEN 4 THEN 'B'
              ELSE ''
              END    AS rechnungszahlart,
       CASE ia.inv_media_cv
              WHEN 1 THEN 'Papier'
              WHEN 2 THEN 'ELMO'
              WHEN 3 THEN 'E-Mail'
              WHEN 4 THEN 'Fax'
              WHEN 5 THEN 'Inline/Papier'
              WHEN 6 THEN 'ELMO/Papier'
              ELSE ''
              END AS rechnungsmedium,
       c.twin_vertrag_id                AS twin_vertrag_id,
       CASE
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN 'J'
         WHEN      p.number_time_measurement = 12
               AND DATE_DIFF(PARSE_DATE('%Y%m%d', '{{ ti.xcom_pull(key='v_datum', task_ids='initialize_job_parameters') }}'),
                                   COALESCE(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 9
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN 'J'
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
               AND DATE_DIFF(PARSE_DATE('%Y%m%d', '{{ ti.xcom_pull(key='v_datum', task_ids='initialize_job_parameters') }}'),
                                   COALESCE(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 23
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN 'J'
         ELSE 'N'
       END  AS upgradeberechtigt,
        ap.access_point_name    AS apn,
        vvl.upgradegrund        AS upgradegrund,
        ct.cntrct_template_id   AS SV_Id,
        CASE
           WHEN (ct.cntrct_template_id in (5104,5105,5106) or
                (ct.cntrct_template_id >= 5155 and
                 ct.cntrct_template_id <= 5161)
                )
           THEN c.contract_number
           ELSE NULL
        END  AS VDA,
        c.cost_centre           AS cost_centre,
        c.cost_centre_user      AS cost_centre_user,
        c.cntrct_ty             AS cntrct_ty,
        rd.segment_id           AS segment_id,
        ac.rv_action_id         AS rv_action_id,
        ia.rechn_inh_konfig_text AS rechn_inh_konfig_text,
        c.commitment_reference_date AS commitment_reference_date,
        c.cntrct_validity_id    AS cntrct_validity_id
  FROM  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_cntrct_crs3`     AS c
  JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_bp_ref`          AS bp
    ON bp.cntrct_cp2_id = c.cntrct_id AND c.cntrct_ty <> 20
  JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_inv_acc`         AS ia
    ON ia.cntrct_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_notice`          AS n
    ON n.cntrct_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_barrier_zusgf`   AS b
    ON b.cntrct_id = c.cntrct_id
  JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_cntrct_templ`    AS ct
    ON ct.cntrct_template_id = c.cntrct_template_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_cntrct_valid`    AS cv
    ON cv.cntrct_validity_id = c.cntrct_validity_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_period`          AS p
    ON p.period_id = cv.first_period_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_vvl_upgrade`     AS vvl
    ON vvl.vertrags_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_apn_ve`          AS ap
    ON ap.cntrct_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.vi_s_rd_segment`    AS rd
    ON ia.inv_definition_id = rd.rechdef_id_carmen
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_action_assoc`    AS ac
    ON ac.cntrct_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.vi_c_bfc`             AS bf
    ON bf.cntrct_id = c.cntrct_id
  WHERE c.cntrct_ty <> 20

UNION ALL

SELECT
       c.cntrct_id                      AS vertrag_id_carmen,
       bp.bp_id                         AS partner_id_carmen,
       ia.inv_definition_id             AS rechdef_id_carmen,
       ia.account_reference             AS kundenkonto,
       ia.sales_tax_freed               AS mwst_kennzeichen,
       c.rv_num                         AS rahmenvertrag_id,
       ia.billcycle_id                  AS rechnungslauf,
       c.vo_code                        AS vo_kenn,
       c.order_number                   AS order_number,
       n.valid_from                     AS geplant_kuend,
       n.entry_date_of_notice           AS eingang_kuend,
       c.cntrct_start_date              AS vertragsbeginn,
       CASE c.cntrct_st
                           WHEN 5 THEN 'A'
                           WHEN 6 THEN 'L'
                             ELSE ''
                             END    AS vertragsstatus,
       b.sperrart_alle                  AS sperrart,
       b.sperrgrund_alle                AS sperrgrund,
       b.stillegungszeitraum_alle      AS stillegungszeitraum,
       c.twinbill                       AS twincard,
       ct.cds_description               AS dwh_tarifgr_text,
       bf.bindefrist                    AS bindefrist,
       vvl.upgradedatum                 AS letztes_upgrade,
       p.number_time_measurement        AS vertragsbindung,
       p.einheit                        AS vertragsbindungseinheit,
       CASE ia.inv_pay_ty_cv
              WHEN 1 THEN 'U'
              WHEN 2 THEN 'E'
              WHEN 3 THEN 'K'
              WHEN 4 THEN 'B'
              ELSE ''
              END    AS rechnungszahlart,
       CASE ia.inv_media_cv
              WHEN 1 THEN 'Papier'
              WHEN 2 THEN 'ELMO'
              WHEN 3 THEN 'E-Mail'
              WHEN 4 THEN 'Fax'
              WHEN 5 THEN 'Inline/Papier'
              WHEN 6 THEN 'ELMO/Papier'
              ELSE ''
              END AS rechnungsmedium,
       c.twin_vertrag_id                AS twin_vertrag_id,
       CASE
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement = 0)
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN 'J'
         WHEN      p.number_time_measurement = 12
               AND DATE_DIFF(PARSE_DATE('%Y%m%d', '{{ ti.xcom_pull(key='v_datum', task_ids='initialize_job_parameters') }}'),
                                   COALESCE(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 9
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN 'J'
         WHEN      (p.number_time_measurement IS NULL OR p.number_time_measurement IN (0, 24))
               AND DATE_DIFF(PARSE_DATE('%Y%m%d', '{{ ti.xcom_pull(key='v_datum', task_ids='initialize_job_parameters') }}'),
                                   COALESCE(c.commitment_reference_date, c.cntrct_start_date), MONTH) > 23
               AND (
                        b.sperrart_alle IS     NULL
                    OR (b.sperrart_alle IS NOT NULL AND b.sperrgrund_zusgf = 2)
                   )
         THEN 'J'
         ELSE 'N'
       END  AS upgradeberechtigt,
        ap.access_point_name    AS apn,
        vvl.upgradegrund        AS upgradegrund,
        ct.cntrct_template_id   AS SV_Id,
        CASE
           WHEN (ct.cntrct_template_id in (5104,5105,5106) or
                (ct.cntrct_template_id >= 5155 and
                 ct.cntrct_template_id <= 5161)
                )
           THEN c.contract_number
           ELSE NULL
        END  AS VDA,
        c.cost_centre           AS cost_centre,
        c.cost_centre_user      AS cost_centre_user,
        c.cntrct_ty             AS cntrct_ty,
        rd.segment_id           AS segment_id,
        ac.rv_action_id         AS rv_action_id,
        ia.rechn_inh_konfig_text AS rechn_inh_konfig_text,
        c.commitment_reference_date AS commitment_reference_date,
        c.cntrct_validity_id    AS cntrct_validity_id
  FROM  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_cntrct_crs3`     AS c
  JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_bp_ref`          AS bp
    ON bp.cntrct_cp2_id = c.cntrct_parent AND c.cntrct_ty = 20
  JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_inv_acc`         AS ia
    ON ia.cntrct_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_notice`          AS n
    ON n.cntrct_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_barrier_zusgf`   AS b
    ON b.cntrct_id = c.cntrct_id
  JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_cntrct_templ`    AS ct
    ON ct.cntrct_template_id = c.cntrct_template_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_cntrct_valid`    AS cv
    ON cv.cntrct_validity_id = c.cntrct_validity_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_period`          AS p
    ON p.period_id = cv.first_period_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_vvl_upgrade`     AS vvl
    ON vvl.vertrags_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_apn_ve`          AS ap
    ON ap.cntrct_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.vi_s_rd_segment`    AS rd
    ON ia.inv_definition_id = rd.rechdef_id_carmen
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.ta_action_assoc`    AS ac
    ON ac.cntrct_id = c.cntrct_id
  LEFT JOIN  `{GCP_PROJECT_ID}.{SOURCE_DATASET}.vi_c_bfc`             AS bf
    ON bf.cntrct_id = c.cntrct_id;
""",
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "v_datum",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ti.xcom_pull(key='v_datum', task_ids='initialize_job_parameters') }}"},
                    },
                ],
            }
        },
        params={"v_datum": "{{ ti.xcom_pull(key='v_datum', task_ids='initialize_job_parameters') }}"}, # Also add to params for templating outside query string
    )

    # Task 3: Final completion handling.
    handle_job_completion_task = PythonOperator(
        task_id="handle_job_completion",
        python_callable=handle_job_completion,
    )

    initialize_job_parameters_task >> execute_contract_transformation >> handle_job_completion_task

---