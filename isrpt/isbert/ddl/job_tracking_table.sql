-- DDL for job tracking table, replacing legacy job management.
-- For job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_tracking_table`
(
    job_kennung         STRING      NOT NULL OPTIONS(description="Identifier for the job, e.g., 'BERT_K_AUSD_GESCHAEFTSP'"),
    entry_nr            STRING      NOT NULL OPTIONS(description="Entry number or run ID for the job, e.g., 'A123'"),
    table_name          STRING              OPTIONS(description="Target table name associated with the job run, e.g., 'PoolVertrag'"),
    status_code_1       STRING              OPTIONS(description="Generic status code 1, from legacy 'A'"),
    status_code_2       STRING              OPTIONS(description="Generic status code 2, from legacy 'I'"),
    stichtag            DATE        NOT NULL OPTIONS(description="Reference date for the data processed by the job"),
    records_processed   INT64               OPTIONS(description="Number of records processed or affected by the job run"),
    notes               STRING              OPTIONS(description="Additional notes or description for the job entry"),
    created_at          TIMESTAMP   NOT NULL OPTIONS(description="Timestamp when the job entry was created")
);