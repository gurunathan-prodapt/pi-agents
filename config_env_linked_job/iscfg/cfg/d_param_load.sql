-- d_param_load.sql — merge staged parameters into the DWH parameter table
MERGE INTO `{GCP_PROJECT}.{BQ_DATASET_ADM}.JOB_PARAMS` tgt
USING (
    SELECT param_key, param_value, loaded_at
    FROM   `{GCP_PROJECT}.{BQ_DATASET_STG}.PARAM_LOAD`
) src
ON (tgt.param_key = src.param_key)
WHEN MATCHED THEN UPDATE SET
    param_value = src.param_value,
    updated_at  = src.loaded_at
WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
VALUES (src.param_key, src.param_value, src.loaded_at);