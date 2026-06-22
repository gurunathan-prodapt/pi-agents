# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
# Description: Airflow DAG to migrate the r_ausd_adressen.ksh ETL job to BigQuery.

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
    'project_id': 'your-gcp-project-id', # Replace with your GCP project ID
    'stg_dataset': 'staging',
    'temp_dataset': 'temp_address_processing',
    'target_dataset': 'reporting_address_data',
}

def _get_processing_dates(logical_date, **kwargs):
    """
    Derives stichtag (snapshot date), today's date, and yesterday's date.
    This replaces the logic from isbert_schema.dwtk_meldungen and gestern.ksh.
    
    In a production scenario, the stichtag logic for 'BERT_DROP_TEMP_TABLE'
    from 'isbert_schema.dwtk_meldungen' would involve a BigQuery query
    to a metadata table or an equivalent BigQuery staging table to determine
    the correct processing date. For this example, we default to the DAG's
    logical date or a manual override.
    """
    # Assuming logical_date is passed as YYYY-MM-DD string
    stichtag_date = pendulum.parse(logical_date).date()
    stichtag_yyyymmdd = stichtag_date.strftime('%Y%m%d')

    # Get today and yesterday dates (replacing gestern.ksh and h_alis_date.ksh logic)
    # Using the logical date context for consistent daily processing.
    today_date = stichtag_date
    yesterday_date = today_date.subtract(days=1)

    today_yyyymmdd = today_date.strftime('%Y%m%d')
yesterday_yyyymmdd = yesterday_date.strftime('%Y%m%d')

    print(f"Processing Stichtag (YYYYMMDD): {stichtag_yyyymmdd}")
    print(f"Processing Today (YYYYMMDD): {today_yyyymmdd}")
    print(f"Processing Yesterday (YYYYMMDD): {yesterday_yyyymmdd}")

    kwargs['ti'].xcom_push(key='stichtag_yyyymmdd', value=stichtag_yyyymmdd)
    kwargs['ti'].xcom_push(key='today_yyyymmdd', value=today_yyyymmdd)
    kwargs['ti'].xcom_push(key='yesterday_yyyymmdd', value=yesterday_yyyymmdd)

    # The original script had a `wiederanlaufwert` parameter. If this were to
    # be implemented, its value would be pushed to XCom here.
    # For now, it's assumed not directly used in the BigQuery SQL transformation.


with DAG(
    dag_id='r_ausd_adressen_ksh_to_bq_dag',
    default_args=default_args,
    description='Migrates vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh to BigQuery and Airflow',
    schedule_interval='@daily', # Example schedule, adjust as needed
    catchup=False,
    tags=['address_data', 'etl', 'bigquery', 'isbert'],
) as dag:
    start = DummyOperator(
        task_id='start',
    )

    get_processing_dates = PythonOperator(
        task_id='get_processing_dates',
        python_callable=_get_processing_dates,
        op_kwargs={'logical_date': "{{ ds }}"}, # Pass logical date as YYYY-MM-DD string
    )

    # XCom pull for the stichtag parameter to be used in BigQuery queries
    stichtag_param = "{{ task_instance.xcom_pull(task_ids='get_processing_dates', key='stichtag_yyyymmdd') }}"

    # Step 01: Truncate all tables
    # This replaces the PL/SQL calls to DWPA_UTIL_SKRIPT.runstatement for truncations.
    truncate_tables = BigQueryExecuteQueryOperator(
        task_id='truncate_tables',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 01 - Truncate all temporary and target tables.
            -- This ensures a clean slate for each run, mimicking Oracle's TRUNCATE behavior.

            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp_nodp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re_nodp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev_nodp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn_nodp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country_desc`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_gp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_re`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_dn`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_ev`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_gp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_re`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_ev`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_dn`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_regulierer`;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
            'target_dataset': default_args['target_dataset'],
        },
        gcp_conn_id='google_cloud_default', # Ensure this connection exists
    )

    # Step 02: Populate bp_ref tables (contract partners, invoice recipients, EVN, service users)
    populate_bp_ref_tables = BigQueryExecuteQueryOperator(
        task_id='populate_bp_ref_tables',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 02 - Populate temporary bp_ref tables.
            -- Replaces Oracle Step 02 (2a-2d) sections from original SQL*Plus script.

            -- step02a: contract partners (Vertragspartner 2)
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp`
            (
              BP_ID,
              REACHABILITY_ID,
              CNTRCT_CP2_ID,
              INV_DEF_INVREC_ID,
              BPR_INST_EVNREC_ID,
              BPR_INST_SRVUSR_ID
            )
            SELECT
              bpr.bp_id,
              bpr.reachability_id,
              bpr.cntrct_cp2_id,
              bpr.inv_def_invrec_id,
              bpr.bpr_inst_evnrec_id,
              bpr.bpr_inst_srvusr_id
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_cds_bp_ref` AS bpr
            WHERE
              (bpr.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.modified_at IS NULL
                  OR bpr.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND (bpr.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.valid_to IS NULL
                  OR bpr.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND bpr.is_production = 1
              AND bpr.bp_ref_ty = 4
              AND bpr.address_ref_ty = 6
            ;


            -- step02b: invoice recipients (Rechnungsempfänger)
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re`
            (
              BP_ID,
              REACHABILITY_ID,
              CNTRCT_CP2_ID,
              INV_DEF_INVREC_ID,
              BPR_INST_EVNREC_ID,
              BPR_INST_SRVUSR_ID
            )
            SELECT
              bpr.bp_id,
              bpr.reachability_id,
              bpr.cntrct_cp2_id,
              bpr.inv_def_invrec_id,
              bpr.bpr_inst_evnrec_id,
              bpr.bpr_inst_srvusr_id
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_cds_bp_ref` AS bpr
            WHERE
              (bpr.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.modified_at IS NULL
                  OR bpr.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND (bpr.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.valid_to IS NULL
                  OR bpr.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND bpr.is_production = 1
              AND bpr.bp_ref_ty = 1
              AND bpr.address_ref_ty = 5
            UNION ALL
            SELECT
              id.rdndnt_cp2_bp_id AS bp_id,
              id.rdndnt_cp2_reachability_id AS reachability_id,
              NULL AS cntrct_cp2_id,
              id.inv_definition_id AS inv_def_invrec_id,
              NULL AS bpr_inst_evnrec_id,
              NULL AS bpr_inst_srvusr_id
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_cds_inv_definition` AS id
            WHERE
              (id.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (id.modified_at IS NULL
                  OR id.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND (id.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (id.valid_to IS NULL
                  OR id.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND id.is_production = 1
              AND id.rdndant_invrec = 0
            ;


            -- step02c: separate EVN recipients
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev`
            (
              BP_ID,
              REACHABILITY_ID,
              CNTRCT_CP2_ID,
              INV_DEF_INVREC_ID,
              BPR_INST_EVNREC_ID,
              BPR_INST_SRVUSR_ID
            )
            SELECT
              bpr.bp_id,
              bpr.reachability_id,
              bpr.cntrct_cp2_id,
              bpr.inv_def_invrec_id,
              bpr.bpr_inst_evnrec_id,
              bpr.bpr_inst_srvusr_id
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_cds_bp_ref` AS bpr
            WHERE
              (bpr.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.modified_at IS NULL
                  OR bpr.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND (bpr.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.valid_to IS NULL
                  OR bpr.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND bpr.is_production = 1
              AND bpr.bp_ref_ty = 1
              AND bpr.address_ref_ty = 7
            ;


            -- step02d: service users (Dienstenutzer)
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn`
            (
              BP_ID,
              REACHABILITY_ID,
              CNTRCT_CP2_ID,
              INV_DEF_INVREC_ID,
              BPR_INST_EVNREC_ID,
              BPR_INST_SRVUSR_ID
            )
            SELECT
              bpr.bp_id,
              bpr.reachability_id,
              bpr.cntrct_cp2_id,
              bpr.inv_def_invrec_id,
              bpr.bpr_inst_evnrec_id,
              bpr.bpr_inst_srvusr_id
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_cds_bp_ref` AS bpr
            WHERE
              (bpr.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.modified_at IS NULL
                  OR bpr.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND (bpr.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.valid_to IS NULL
                  OR bpr.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND bpr.is_production = 1
              AND bpr.bp_ref_ty = 1
              AND bpr.address_ref_ty = 8
            ;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'stg_dataset': default_args['stg_dataset'],
            'temp_dataset': default_args['temp_dataset'],
        },
        query_params=[
            {
                'name': 'stichtag_yyyymmdd',
                'parameterType': {'type': 'STRING'},
                'parameterValue': {'value': stichtag_param}
            }
        ],
        gcp_conn_id='google_cloud_default',
    )

    # Step 03 - Part 1: Populate country and reachability intermediates
    populate_country_reachability_part1 = BigQueryExecuteQueryOperator(
        task_id='populate_country_reachability_part1',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 03 - Part 1 - Populate temporary country and reachability tables.
            -- Replaces Oracle Step 03 (3a, 3b, 3c, 3e) sections.

            -- step03a: country
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country`
            (
              COUNTRY_CODE,
              DESCRIPTION_ID,
              PARENT_COUNTRY_CODE,
              EU_INDICATOR,
              SAP_CODE,
              CORR_CODE,
              VALID
            )
            SELECT
              country.COUNTRY_CODE,
              country.DESCRIPTION_ID,
              country.PARENT_COUNTRY_CODE,
              country.EU_INDICATOR,
              country.SAP_CODE,
              country.CORR_CODE,
              country.VALID
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_glv_country` AS country
            ;


            -- step03b: country description
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country_desc`
            (
              DESCRIPTION_ID,
              LANGUAGE,
              SHORT_DESCRIPTION,
              DESCRIPTION,
              LONG_DESCRIPTION
            )
            SELECT
              des.DESCRIPTION_ID,
              des.LANGUAGE,
              des.SHORT_DESCRIPTION,
              des.DESCRIPTION,
              des.LONG_DESCRIPTION
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_glv_description` AS des
            ;


            -- step03c: country identification (Länderkennungen)
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng`
            (
              COUNTRY_CODE,
              DESCRIPTION_ID,
              LANGUAGE,
              SHORT_DESCRIPTION,
              DESCRIPTION,
              LONG_DESCRIPTION
            )
            SELECT
              co.country_code,
              de.DESCRIPTION_ID,
              de.LANGUAGE,
              de.SHORT_DESCRIPTION,
              de.DESCRIPTION,
              de.LONG_DESCRIPTION
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country` AS co
            JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country_desc` AS de
            ON
              co.description_id = de.description_id
            WHERE
              co.valid = 1
            ;


            -- step03e: reachability
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability`
            (
              BP_ID,
              REACHABILITY_ID,
              OBJ_VERSION,
              COUNTRY_CODE,
              FOR_THE_ATTENTION_OF,
              ADDRESS_ATTACHMENT,
              ADDRESS_ATTACHMENT_ORG,
              CORP_UNIT,
              SURNAME_S,
              FIRST_NAME_G,
              ZIP_CODE,
              CITY,
              POBOX,
              STREET,
              HOUSE_NR,
              PUBLIC_AREA_A,
              PRIVATE_AREA_P,
              CORP_UNIT_OU1,
              ADDRESS_LINE_1,
              ADDRESS_LINE_2,
              REACHABLE_FROM,
              REACHABLE_THRU
            )
            SELECT
              re.bp_id,
              re.reachability_id,
              re.obj_version,
              re.country_code,
              re.for_the_attention_of,
              re.address_attachment,
              re.address_attachment_org,
              re.corp_unit,
              re.surname_s,
              re.first_name_g,
              re.zip_code,
              re.city,
              re.pobox,
              re.street,
              re.house_nr,
              re.public_area_a,
              re.private_area_p,
              re.corp_unit_ou1,
              re.address_line_1,
              re.address_line_2,
              re.reachable_from,
              re.reachable_thru
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_bpd_reachability` AS re
            WHERE
              (re.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (re.modified_at IS NULL
                  OR re.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND (re.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (re.valid_to IS NULL
                  OR re.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND re.is_production = 1
            ;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'stg_dataset': default_args['stg_dataset'],
            'temp_dataset': default_args['temp_dataset'],
        },
        query_params=[
            {
                'name': 'stichtag_yyyymmdd',
                'parameterType': {'type': 'STRING'},
                'parameterValue': {'value': stichtag_param}
            }
        ],
        gcp_conn_id='google_cloud_default',
    )

    # Step 03 - Part 2: Populate final reachability tables
    populate_reach_final = BigQueryExecuteQueryOperator(
        task_id='populate_reach_final',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 03 - Part 2 - Populate final reachability tables.
            -- Replaces Oracle Step 03 (3f, 3g, 3h, 3i) sections.

            -- step03f: separate reachability tables for contract partners (Geschaeftspartner)
            INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_gp`
            (
              BP_ID,
              REACHABILITY_ID,
              OBJ_VERSION,
              COUNTRY_CODE,
              FOR_THE_ATTENTION_OF,
              ADDRESS_ATTACHMENT,
              ADDRESS_ATTACHMENT_ORG,
              CORP_UNIT,
              SURNAME_S,
              FIRST_NAME_G,
              ZIP_CODE,
              CITY,
              POBOX,
              STREET,
              HOUSE_NR,
              PUBLIC_AREA_A,
              PRIVATE_AREA_P,
              CORP_UNIT_OU1,
              ADDRESS_LINE_1,
              ADDRESS_LINE_2,
              REACHABLE_FROM,
              REACHABLE_THRU,
              CNTRCT_CP2_ID,
              INV_DEF_INVREC_ID,
              BPR_INST_EVNREC_ID,
              BPR_INST_SRVUSR_ID,
              LAND_SD
            )
            SELECT
              re.BP_ID,
              re.REACHABILITY_ID,
              re.OBJ_VERSION,
              re.COUNTRY_CODE,
              re.FOR_THE_ATTENTION_OF,
              re.ADDRESS_ATTACHMENT,
              re.ADDRESS_ATTACHMENT_ORG,
              re.CORP_UNIT,
              re.SURNAME_S,
              re.FIRST_NAME_G,
              re.ZIP_CODE,
              re.CITY,
              re.POBOX,
              re.STREET,
              re.HOUSE_NR,
              re.PUBLIC_AREA_A,
              re.PRIVATE_AREA_P,
              re.CORP_UNIT_OU1,
              re.ADDRESS_LINE_1,
              re.ADDRESS_LINE_2,
              re.REACHABLE_FROM,
              re.REACHABLE_THRU,
              br.cntrct_cp2_id,
              br.inv_def_invrec_id,
              br.bpr_inst_evnrec_id,
              br.bpr_inst_srvusr_id,
              SUBSTR(lk.short_description, 1, 3) AS land_sd
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp` AS br
            JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability` AS re
            ON
              br.bp_id = re.bp_id
              AND br.reachability_id = re.reachability_id
            LEFT JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng` AS lk
            ON
              re.country_code = lk.country_code
            ;


            -- step03g: separate reachability tables for invoice recipients (Rechnungsempfänger)
            INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_re`
            (
              BP_ID,
              REACHABILITY_ID,
              OBJ_VERSION,
              COUNTRY_CODE,
              FOR_THE_ATTENTION_OF,
              ADDRESS_ATTACHMENT,
              ADDRESS_ATTACHMENT_ORG,
              CORP_UNIT,
              SURNAME_S,
              FIRST_NAME_G,
              ZIP_CODE,
              CITY,
              POBOX,
              STREET,
              HOUSE_NR,
              PUBLIC_AREA_A,
              PRIVATE_AREA_P,
              CORP_UNIT_OU1,
              ADDRESS_LINE_1,
              ADDRESS_LINE_2,
              REACHABLE_FROM,
              REACHABLE_THRU,
              CNTRCT_CP2_ID,
              INV_DEF_INVREC_ID,
              BPR_INST_EVNREC_ID,
              BPR_INST_SRVUSR_ID,
              LAND_SD
            )
            SELECT
              re.BP_ID,
              re.REACHABILITY_ID,
              re.OBJ_VERSION,
              re.COUNTRY_CODE,
              re.FOR_THE_ATTENTION_OF,
              re.ADDRESS_ATTACHMENT,
              re.ADDRESS_ATTACHMENT_ORG,
              re.CORP_UNIT,
              re.SURNAME_S,
              re.FIRST_NAME_G,
              re.ZIP_CODE,
              re.CITY,
              re.POBOX,
              re.STREET,
              re.HOUSE_NR,
              re.PUBLIC_AREA_A,
              re.PRIVATE_AREA_P,
              re.CORP_UNIT_OU1,
              re.ADDRESS_LINE_1,
              re.ADDRESS_LINE_2,
              re.REACHABLE_FROM,
              re.REACHABLE_THRU,
              br.cntrct_cp2_id,
              br.inv_def_invrec_id,
              br.bpr_inst_evnrec_id,
              br.bpr_inst_srvusr_id,
              SUBSTR(lk.short_description, 1, 3) AS land_sd
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re` AS br
            JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability` AS re
            ON
              br.bp_id = re.bp_id
              AND br.reachability_id = re.reachability_id
            LEFT JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng` AS lk
            ON
              re.country_code = lk.country_code
            ;


            -- step03h: separate reachability tables for EVN recipients
            INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_ev`
            (
              BP_ID,
              REACHABILITY_ID,
              OBJ_VERSION,
              COUNTRY_CODE,
              FOR_THE_ATTENTION_OF,
              ADDRESS_ATTACHMENT,
              ADDRESS_ATTACHMENT_ORG,
              CORP_UNIT,
              SURNAME_S,
              FIRST_NAME_G,
              ZIP_CODE,
              CITY,
              POBOX,
              STREET,
              HOUSE_NR,
              PUBLIC_AREA_A,
              PRIVATE_AREA_P,
              CORP_UNIT_OU1,
              ADDRESS_LINE_1,
              ADDRESS_LINE_2,
              REACHABLE_FROM,
              REACHABLE_THRU,
              CNTRCT_CP2_ID,
              INV_DEF_INVREC_ID,
              BPR_INST_EVNREC_ID,
              BPR_INST_SRVUSR_ID,
              LAND_SD
            )
            SELECT
              re.BP_ID,
              re.REACHABILITY_ID,
              re.OBJ_VERSION,
              re.COUNTRY_CODE,
              re.FOR_THE_ATTENTION_OF,
              re.ADDRESS_ATTACHMENT,
              re.ADDRESS_ATTACHMENT_ORG,
              re.CORP_UNIT,
              re.SURNAME_S,
              re.FIRST_NAME_G,
              re.ZIP_CODE,
              re.CITY,
              re.POBOX,
              re.STREET,
              re.HOUSE_NR,
              re.PUBLIC_AREA_A,
              re.PRIVATE_AREA_P,
              re.CORP_UNIT_OU1,
              re.ADDRESS_LINE_1,
              re.ADDRESS_LINE_2,
              re.REACHABLE_FROM,
              re.REACHABLE_THRU,
              br.cntrct_cp2_id,
              br.inv_def_invrec_id,
              br.bpr_inst_evnrec_id,
              br.bpr_inst_srvusr_id,
              SUBSTR(lk.short_description, 1, 3) AS land_sd
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev` AS br
            JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability` AS re
            ON
              br.bp_id = re.bp_id
              AND br.reachability_id = re.reachability_id
            LEFT JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng` AS lk
            ON
              re.country_code = lk.country_code
            ;


            -- step03i: separate reachability tables for service users (Dienstenutzer)
            INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_reach_dn`
            (
              BP_ID,
              REACHABILITY_ID,
              OBJ_VERSION,
              COUNTRY_CODE,
              FOR_THE_ATTENTION_OF,
              ADDRESS_ATTACHMENT,
              ADDRESS_ATTACHMENT_ORG,
              CORP_UNIT,
              SURNAME_S,
              FIRST_NAME_G,
              ZIP_CODE,
              CITY,
              POBOX,
              STREET,
              HOUSE_NR,
              PUBLIC_AREA_A,
              PRIVATE_AREA_P,
              CORP_UNIT_OU1,
              ADDRESS_LINE_1,
              ADDRESS_LINE_2,
              REACHABLE_FROM,
              REACHABLE_THRU,
              CNTRCT_CP2_ID,
              INV_DEF_INVREC_ID,
              BPR_INST_EVNREC_ID,
              BPR_INST_SRVUSR_ID,
              LAND_SD
            )
            SELECT
              re.BP_ID,
              re.REACHABILITY_ID,
              re.OBJ_VERSION,
              re.COUNTRY_CODE,
              re.FOR_THE_ATTENTION_OF,
              re.ADDRESS_ATTACHMENT,
              re.ADDRESS_ATTACHMENT_ORG,
              re.CORP_UNIT,
              re.SURNAME_S,
              re.FIRST_NAME_G,
              re.ZIP_CODE,
              re.CITY,
              re.POBOX,
              re.STREET,
              re.HOUSE_NR,
              re.PUBLIC_AREA_A,
              re.PRIVATE_AREA_P,
              re.CORP_UNIT_OU1,
              re.ADDRESS_LINE_1,
              re.ADDRESS_LINE_2,
              re.REACHABLE_FROM,
              re.REACHABLE_THRU,
              br.cntrct_cp2_id,
              br.inv_def_invrec_id,
              br.bpr_inst_evnrec_id,
              br.bpr_inst_srvusr_id,
              SUBSTR(lk.short_description, 1, 3) AS land_sd
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn` AS br
            JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability` AS re
            ON
              br.bp_id = re.bp_id
              AND br.reachability_id = re.reachability_id
            LEFT JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng` AS lk
            ON
              re.country_code = lk.country_code
            ;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
            'target_dataset': default_args['target_dataset'],
        },
        query_params=[
            {
                'name': 'stichtag_yyyymmdd',
                'parameterType': {'type': 'STRING'},
                'parameterValue': {'value': stichtag_param}
            }
        ],
        gcp_conn_id='google_cloud_default',
    )

    # Step 03 - Part 3: Cleanup intermediate country and reachability tables
    cleanup_country_reachability_part2 = BigQueryExecuteQueryOperator(
        task_id='cleanup_country_reachability_part2',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 03 - Part 3 - Cleanup intermediate country and reachability tables.
            -- Replaces Oracle Step 03j section.

            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_reachability`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_country_desc`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_laender_kng`;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
        },
        gcp_conn_id='google_cloud_default',
    )

    # Step 04 - Part 1: Populate business_partner and related GP tables
    populate_business_partner_part1_gp = BigQueryExecuteQueryOperator(
        task_id='populate_business_partner_part1_gp',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 04 - Part 1 - Populate business partner and related contract partner tables.
            -- Replaces Oracle Step 04 (4a, 4b_nodp, 4b_final) sections.

            -- step04a: business_partner
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt`
            (
              BP_ID,
              ORGANISATION_NAME,
              TITLE,
              SURNAME,
              FIRST_NAME,
              SALES_TAX_FREED,
              TM_CUSTOMERID
            )
            SELECT
              bp.bp_id,
              bp.organisation_name,
              bp.title,
              bp.surname,
              bp.first_name,
              bp.sales_tax_freed,
              bp.tm_customerid
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_bpd_business_partner` AS bp
            WHERE
              (bp.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bp.modified_at IS NULL
                  OR bp.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
            ;


            -- step04b: separate business-partner tables for contract partners (Geschaeftspartner) - nodp
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp_nodp`
            (
              BP_ID
            )
            SELECT DISTINCT
              ref_gp.bp_id
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp` AS ref_gp
            ;


            -- step04b: separate business-partner tables for contract partners (Geschaeftspartner) - final
            INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_gp`
            (
              BP_ID,
              ORGANISATION_NAME,
              TITLE,
              SURNAME,
              FIRST_NAME,
              SALES_TAX_FREED,
              TM_CUSTOMERID
            )
            SELECT
              bp.bp_id,
              bp.organisation_name,
              bp.title,
              bp.surname,
              bp.first_name,
              bp.sales_tax_freed,
              bp.tm_customerid
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp_nodp` AS br
            JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt` AS bp
            ON
              br.bp_id = bp.bp_id
            ;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'stg_dataset': default_args['stg_dataset'],
            'temp_dataset': default_args['temp_dataset'],
            'target_dataset': default_args['target_dataset'],
        },
        query_params=[
            {
                'name': 'stichtag_yyyymmdd',
                'parameterType': {'type': 'STRING'},
                'parameterValue': {'value': stichtag_param}
            }
        ],
        gcp_conn_id='google_cloud_default',
    )

    # Step 04 - Part 2: Cleanup GP tables
    cleanup_gp_tables = BigQueryExecuteQueryOperator(
        task_id='cleanup_gp_tables',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 04 - Part 2 - Cleanup intermediate tables for contract partners.
            -- Replaces Oracle Step 04c section.

            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp_nodp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_gp`;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
        },
        gcp_conn_id='google_cloud_default',
    )

    # Step 04 - Part 3: Populate business_partner and related RE tables
    populate_business_partner_part2_re = BigQueryExecuteQueryOperator(
        task_id='populate_business_partner_part2_re',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 04 - Part 3 - Populate business partner and related invoice recipient tables.
            -- Replaces Oracle Step 04 (4d_nodp, 4d_final) sections.

            -- step04d: separate business-partner tables for invoice recipients (Rechnungsempfänger) - nodp
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re_nodp`
            (
              BP_ID
            )
            SELECT DISTINCT
              ref_re.bp_id
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re` AS ref_re
            ;


            -- step04d: separate business-partner tables for invoice recipients (Rechnungsempfänger) - final
            INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_re`
            (
              BP_ID,
              ORGANISATION_NAME,
              TITLE,
              SURNAME,
              FIRST_NAME,
              SALES_TAX_FREED,
              TM_CUSTOMERID
            )
            SELECT
              bp.bp_id,
              bp.organisation_name,
              bp.title,
              bp.surname,
              bp.first_name,
              bp.sales_tax_freed,
              bp.tm_customerid
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re_nodp` AS br
            JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt` AS bp
            ON
              br.bp_id = bp.bp_id
            ;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
            'target_dataset': default_args['target_dataset'],
        },
        gcp_conn_id='google_cloud_default',
    )

    # Step 04 - Part 4: Cleanup RE tables
    cleanup_re_tables = BigQueryExecuteQueryOperator(
        task_id='cleanup_re_tables',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 04 - Part 4 - Cleanup intermediate tables for invoice recipients.
            -- Replaces Oracle Step 04e section.

            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re_nodp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_re`;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
        },
        gcp_conn_id='google_cloud_default',
    )

    # Step 04 - Part 5: Populate business_partner and related EV tables
    populate_business_partner_part3_ev = BigQueryExecuteQueryOperator(
        task_id='populate_business_partner_part3_ev',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 04 - Part 5 - Populate business partner and related EVN recipient tables.
            -- Replaces Oracle Step 04 (4f_nodp, 4f_final) sections.

            -- step04f: separate business-partner tables for EVN recipients - nodp
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev_nodp`
            (
              BP_ID
            )
            SELECT DISTINCT
              ref_ev.bp_id
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev` AS ref_ev
            ;


            -- step04f: separate business-partner tables for EVN recipients - final
            INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_ev`
            (
              BP_ID,
              ORGANISATION_NAME,
              TITLE,
              SURNAME,
              FIRST_NAME,
              SALES_TAX_FREED,
              TM_CUSTOMERID
            )
            SELECT
              bp.bp_id,
              bp.organisation_name,
              bp.title,
              bp.surname,
              bp.first_name,
              bp.sales_tax_freed,
              bp.tm_customerid
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev_nodp` AS br
            JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt` AS bp
            ON
              br.bp_id = bp.bp_id
            ;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
            'target_dataset': default_args['target_dataset'],
        },
        gcp_conn_id='google_cloud_default',
    )

    # Step 04 - Part 6: Cleanup EV tables
    cleanup_ev_tables = BigQueryExecuteQueryOperator(
        task_id='cleanup_ev_tables',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 04 - Part 6 - Cleanup intermediate tables for EVN recipients.
            -- Replaces Oracle Step 04g section.

            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev_nodp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_ev`;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
        },
        gcp_conn_id='google_cloud_default',
    )

    # Step 04 - Part 7: Populate business_partner and related DN tables
    populate_business_partner_part4_dn = BigQueryExecuteQueryOperator(
        task_id='populate_business_partner_part4_dn',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 04 - Part 7 - Populate business partner and related service user tables.
            -- Replaces Oracle Step 04 (4h_nodp, 4h_final) sections.

            -- step04h: separate business-partner tables for service users (Dienstenutzer) - nodp
            INSERT INTO `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn_nodp`
            (
              BP_ID
            )
            SELECT DISTINCT
              ref_dn.bp_id
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn` AS ref_dn
            ;


            -- step04h: separate business-partner tables for service users (Dienstenutzer) - final
            INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_business_dn`
            (
              BP_ID,
              ORGANISATION_NAME,
              TITLE,
              SURNAME,
              FIRST_NAME,
              SALES_TAX_FREED,
              TM_CUSTOMERID
            )
            SELECT
              bp.bp_id,
              bp.organisation_name,
              bp.title,
              bp.surname,
              bp.first_name,
              bp.sales_tax_freed,
              bp.tm_customerid
            FROM
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn_nodp` AS br
            JOIN
              `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt` AS bp
            ON
              br.bp_id = bp.bp_id
            ;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
            'target_dataset': default_args['target_dataset'],
        },
        gcp_conn_id='google_cloud_default',
    )

    # Step 04 - Part 8: Cleanup DN and business_partner tables
    cleanup_dn_tables = BigQueryExecuteQueryOperator(
        task_id='cleanup_dn_tables',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 04 - Part 8 - Cleanup intermediate tables for service users and main business partner.
            -- Replaces Oracle Step 04i section.

            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_business_pt`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn_nodp`;
            TRUNCATE TABLE `{{ params.project_id }}.{{ params.temp_dataset }}.sof_ta_bp_ref_dn`;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'temp_dataset': default_args['temp_dataset'],
        },
        gcp_conn_id='google_cloud_default',
    )

    # Step 05: Populate Regulierer table
    populate_regulierer = BigQueryExecuteQueryOperator(
        task_id='populate_regulierer',
        sql="""
            -- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_adressen.sql
            -- BigQuery Standard SQL: Step 05 - Populate the final regulierer table.
            -- Replaces Oracle Step 05 section.

            INSERT INTO `{{ params.project_id }}.{{ params.target_dataset }}.sof_ta_e_regulierer`
            (
              INV_DEF_MOPREF_ID,
              MOP_BP_ID,
              MEANS_OF_PAYMENT_ID
            )
            SELECT
              bpr.inv_def_mopref_id,
              bpr.mop_bp_id,
              bpr.means_of_payment_id
            FROM
              `{{ params.project_id }}.{{ params.stg_dataset }}.stg_cds_bp_ref` AS bpr
            WHERE
              (bpr.insert_at <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.modified_at IS NULL
                  OR bpr.modified_at > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND (bpr.valid_from <= PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)
                AND (bpr.valid_to IS NULL
                  OR bpr.valid_to > PARSE_DATE('%Y%m%d', @stichtag_yyyymmdd)))
              AND bpr.is_production = 1
              AND bpr.bp_ref_ty = 2
              AND bpr.mop_ref_ty = 1
            ;
        """,
        use_legacy_sql=False,
        params={
            'project_id': default_args['project_id'],
            'stg_dataset': default_args['stg_dataset'],
            'target_dataset': default_args['target_dataset'],
        },
        query_params=[
            {
                'name': 'stichtag_yyyymmdd',
                'parameterType': {'type': 'STRING'},
                'parameterValue': {'value': stichtag_param}
            }
        ],
        gcp_conn_id='google_cloud_default',
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_SUCCESS, # Ensure all upstream tasks are successful
    )

    # Define task dependencies
    start >> get_processing_dates
    get_processing_dates >> truncate_tables
    truncate_tables >> populate_bp_ref_tables
    populate_bp_ref_tables >> populate_country_reachability_part1
    populate_country_reachability_part1 >> populate_reach_final
    populate_reach_final >> cleanup_country_reachability_part2
    cleanup_country_reachability_part2 >> populate_business_partner_part1_gp
    populate_business_partner_part1_gp >> cleanup_gp_tables
    cleanup_gp_tables >> populate_business_partner_part2_re
    populate_business_partner_part2_re >> cleanup_re_tables
    cleanup_re_tables >> populate_business_partner_part3_ev
    populate_business_partner_part3_ev >> cleanup_ev_tables
    cleanup_ev_tables >> populate_business_partner_part4_dn
    populate_business_partner_part4_dn >> cleanup_dn_tables
    cleanup_dn_tables >> populate_regulierer
    populate_regulierer >> end