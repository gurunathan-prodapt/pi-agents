```python
# finaltestingrepo_gcp_migration.py
# Modular PySpark code for GCP/BigQuery migration patterns described in the design.
# Focus: reusable utilities, SCD Type 2, dynamic SQL, aggregations, DQ checks,
# BigQuery read/write helpers, and domain-specific transformation functions.

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ------------------------------------------------------------------------------
# 1) Spark / BigQuery Utilities
# ------------------------------------------------------------------------------

@dataclass
class BQConfig:
    project_id: str
    temp_gcs_bucket: str
    materialization_dataset: Optional[str] = None


def build_spark(app_name: str = "finaltestingrepo_migration") -> SparkSession:
    """
    Build SparkSession configured for BigQuery connector usage.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def read_bigquery(
    spark: SparkSession,
    table: str,
    project_id: Optional[str] = None,
    query: Optional[str] = None
) -> DataFrame:
    """
    Read from BigQuery table or query using Spark BigQuery connector.
    """
    reader = spark.read.format("bigquery")
    if project_id:
        reader = reader.option("project", project_id)
    if query:
        return reader.option("query", query).load()
    return reader.option("table", table).load()


def write_bigquery(
    df: DataFrame,
    table: str,
    mode: str = "append",
    temporary_gcs_bucket: Optional[str] = None
) -> None:
    """
    Write DataFrame to BigQuery using Spark BigQuery connector.
    """
    writer = df.write.format("bigquery").mode(mode).option("table", table)
    if temporary_gcs_bucket:
        writer = writer.option("temporaryGcsBucket", temporary_gcs_bucket)
    writer.save()


def add_load_metadata(
    df: DataFrame,
    load_date_col: str = "load_date",
    batch_id_col: str = "load_batch_id"
) -> DataFrame:
    """
    Add standard load metadata columns.
    """
    return (
        df.withColumn(load_date_col, F.current_date())
          .withColumn(batch_id_col, F.unix_timestamp(F.current_timestamp()).cast("long"))
    )


def standardize_string_cols(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Trim and upper-case selected string columns.
    """
    for c in cols:
        df = df.withColumn(c, F.upper(F.trim(F.col(c))))
    return df


def safe_coalesce(*cols):
    """
    Reusable COALESCE wrapper.
    """
    return F.coalesce(*cols)


# ------------------------------------------------------------------------------
# 2) Generic SCD Type 2 Utilities
# ------------------------------------------------------------------------------

def prepare_scd2_source(
    df: DataFrame,
    natural_key_cols: List[str],
    tracked_cols: List[str],
    effective_date_col: str,
    hash_col_name: str = "record_hash"
) -> DataFrame:
    """
    Create a deterministic hash for SCD2 change detection.
    """
    hash_expr = F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]),
        256
    )
    return df.withColumn(hash_col_name, hash_expr)


def scd2_merge_sql(
    target_table: str,
    source_view: str,
    natural_key_cols: List[str],
    tracked_cols: List[str],
    surrogate_key_col: str,
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    current_flag_col: str = "is_current",
    version_col: str = "version_num",
    default_valid_to: str = "DATE '9999-12-31'"
) -> str:
    """
    Generate BigQuery MERGE SQL for SCD Type 2.
    Assumes source_view contains columns for natural keys + tracked cols + effective date.
    """
    nk_join = " AND ".join([f"T.{c} = S.{c}" for c in natural_key_cols])
    tracked_compare = " OR ".join([f"COALESCE(T.{c}, '') <> COALESCE(S.{c}, '')" for c in tracked_cols])

    insert_cols = natural_key_cols + tracked_cols + [valid_from_col, valid_to_col, current_flag_col, version_col, surrogate_key_col]
    insert_vals = [f"S.{c}" for c in natural_key_cols + tracked_cols] + [
        f"S.{valid_from_col}",
        default_valid_to,
        "'Y'",
        f"COALESCE((SELECT MAX({version_col}) + 1 FROM `{target_table}` WHERE { ' AND '.join([f'{c} = S.{c}' for c in natural_key_cols]) }), 1)",
        f"COALESCE((SELECT MAX({surrogate_key_col}) + 1 FROM `{target_table}`), 1)"
    ]

    return f"""
    MERGE `{target_table}` T
    USING `{source_view}` S
    ON {nk_join} AND T.{current_flag_col} = 'Y'
    WHEN MATCHED AND ({tracked_compare}) THEN
      UPDATE SET
        T.{valid_to_col} = DATE_SUB(S.{valid_from_col}, INTERVAL 1 DAY),
        T.{current_flag_col} = 'N',
        T.updated_date = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT ({", ".join(insert_cols)})
      VALUES ({", ".join(insert_vals)})
    """


# ------------------------------------------------------------------------------
# 3) Finance Domain Transformations
# ------------------------------------------------------------------------------

def build_dim_account_source(stg_account_master: DataFrame) -> DataFrame:
    return (
        stg_account_master
        .select(
            "ACCOUNT_CODE", "ENTITY_CODE", "ACCOUNT_NAME", "ACCOUNT_TYPE",
            "ACCOUNT_SUBTYPE", "PARENT_ACCOUNT_CODE", "NATURAL_BALANCE",
            "IS_CONTROL_ACCOUNT", "IS_RECONCILABLE", "IS_ACTIVE", "LOAD_DATE"
        )
        .withColumn("VALID_FROM", F.col("LOAD_DATE"))
        .withColumn("VALID_TO", F.lit(None).cast("date"))
        .withColumn("IS_CURRENT", F.lit("Y"))
        .withColumn("VERSION_NUM", F.lit(1))
        .withColumn("CREATED_DATE", F.current_timestamp())
        .withColumn("UPDATED_DATE", F.lit(None).cast("timestamp"))
    )


def derive_period_dimension(stg_period_rates: DataFrame) -> DataFrame:
    """
    Build dim_period from period rates or a calendar source.
    """
    return (
        stg_period_rates
        .select("PERIOD_NAME")
        .distinct()
        .withColumn("DIM_PERIOD_KEY", F.monotonically_increasing_id().cast("long"))
        .withColumn("PERIOD_YEAR", F.regexp_extract("PERIOD_NAME", r"(\d{4})", 1).cast("int"))
        .withColumn("PERIOD_NUM", F.regexp_extract("PERIOD_NAME", r"(\d{1,2})$", 1).cast("int"))
        .withColumn("PERIOD_START_DATE", F.current_date())
        .withColumn("PERIOD_END_DATE", F.current_date())
        .withColumn("FISCAL_QUARTER", F.lit(None).cast("string"))
        .withColumn("FISCAL_HALF", F.lit(None).cast("string"))
        .withColumn("IS_CLOSED", F.lit("N"))
        .withColumn("CLOSE_DATE", F.lit(None).cast("timestamp"))
    )


def build_fact_gl_balances(
    stg_gl_transactions: DataFrame,
    dim_account: DataFrame,
    dim_period: DataFrame,
    dim_cost_centre: Optional[DataFrame] = None
) -> DataFrame:
    """
    Aggregate GL transactions into balances.
    """
    acct = dim_account.filter(F.col("IS_CURRENT") == "Y").select(
        F.col("ACCOUNT_CODE").alias("D_ACCOUNT_CODE"),
        F.col("ENTITY_CODE").alias("D_ENTITY_CODE"),
        F.col("DIM_ACCOUNT_KEY")
    )
    per = dim_period.select(F.col("PERIOD_NAME").alias("D_PERIOD_NAME"), "DIM_PERIOD_KEY")
    txn = stg_gl_transactions.alias("T")

    joined = txn.join(acct, (txn.ACCOUNT_CODE == acct.D_ACCOUNT_CODE) & (txn.ENTITY_CODE == acct.D_ENTITY_CODE), "left") \
                .join(per, txn.PERIOD_NAME == per.D_PERIOD_NAME, "left")

    if dim_cost_centre is not None:
        cc = dim_cost_centre.filter(F.col("IS_CURRENT") == "Y").select(
            F.col("COST_CENTRE_CODE").alias("D_CC_CODE"),
            F.col("ENTITY_CODE").alias("D_CC_ENTITY"),
            F.col("DIM_CC_KEY")
        )
        joined = joined.join(cc, (txn.COST_CENTRE_CODE == cc.D_CC_CODE) & (txn.ENTITY_CODE == cc.D_CC_ENTITY), "left")

    return (
        joined.groupBy(
            F.col("DIM_ACCOUNT_KEY"),
            F.col("DIM_PERIOD_KEY"),
            F.col("DIM_CC_KEY") if dim_cost_centre is not None else F.lit(None).cast("long").alias("DIM_CC_KEY"),
            F.col("ENTITY_CODE"),
            F.col("PERIOD_NAME"),
            F.col("CURRENCY_CODE")
        )
        .agg(
            F.sum("DEBIT_AMOUNT").alias("PERIOD_DEBITS"),
            F.sum("CREDIT_AMOUNT").alias("PERIOD_CREDITS"),
            F.sum("NET_AMOUNT").alias("FUNCTIONAL_BALANCE")
        )
        .withColumn("OPENING_BALANCE", F.lit(None).cast("decimal(38,9)"))
        .withColumn("CLOSING_BALANCE", F.col("FUNCTIONAL_BALANCE"))
        .withColumn("LOAD_DATE", F.current_timestamp())
        .withColumn("LOAD_BATCH_ID", F.unix_timestamp(F.current_timestamp()).cast("long"))
        .withColumn("FACT_GL_KEY", F.monotonically_increasing_id().cast("long"))
    )


def build_period_reconciliation(
    fact_gl_balances: DataFrame,
    subledger_df: DataFrame
) -> DataFrame:
    """
    Reconcile GL vs subledger balances.
    """
    gl = fact_gl_balances.select("ENTITY_CODE", "PERIOD_NAME", "ACCOUNT_CODE", F.col("CLOSING_BALANCE").alias("GL_BALANCE"))
    sl = subledger_df.select("ENTITY_CODE", "PERIOD_NAME", "ACCOUNT_CODE", F.col("SUB_LEDGER_BALANCE"))
    joined = gl.join(sl, ["ENTITY_CODE", "PERIOD_NAME", "ACCOUNT_CODE"], "full_outer")
    return (
        joined.withColumn("VARIANCE_AMOUNT", F.coalesce(F.col("GL_BALANCE"), F.lit(0)) - F.coalesce(F.col("SUB_LEDGER_BALANCE"), F.lit(0)))
              .withColumn("VARIANCE_PCT",
                          F.when(F.coalesce(F.col("SUB_LEDGER_BALANCE"), F.lit(0)) == 0, F.lit(None))
                           .otherwise(F.col("VARIANCE_AMOUNT") / F.col("SUB_LEDGER_BALANCE") * 100))
              .withColumn("RECON_STATUS",
                          F.when(F.abs(F.col("VARIANCE_AMOUNT")) < F.lit(0.01), F.lit("MATCH"))
                           .otherwise(F.lit("MISMATCH")))
              .withColumn("RECON_KEY", F.monotonically_increasing_id().cast("long"))
              .withColumn("LOAD_DATE", F.current_timestamp())
    )


def build_fin_account_hierarchy_snapshot(dim_account: DataFrame) -> DataFrame:
    """
    Build hierarchy snapshot with root account and level.
    """
    base = dim_account.select(
        "DIM_ACCOUNT_KEY", "ACCOUNT_CODE", "ACCOUNT_NAME", "ACCOUNT_TYPE",
        "ENTITY_CODE", "PARENT_ACCOUNT_CODE"
    )
    return (
        base.withColumn("ROOT_ACCOUNT_CODE", F.coalesce(F.col("PARENT_ACCOUNT_CODE"), F.col("ACCOUNT_CODE")))
            .withColumn("LEVEL", F.lit(1))
            .withColumn("SNAPSHOT_DATE", F.current_date())
    )


# ------------------------------------------------------------------------------
# 4) Retail Domain Transformations
# ------------------------------------------------------------------------------

def build_dim_product_source(stg_product_master: DataFrame) -> DataFrame:
    return (
        stg_product_master
        .select(
            "PRODUCT_ID", "PRODUCT_CODE", "PRODUCT_NAME", "CATEGORY_CODE",
            "SUBCATEGORY_CODE", "SUPPLIER_ID", "COST_PRICE", "LIST_PRICE",
            "TAX_CLASS_CODE", "IS_ACTIVE", "EFFECTIVE_DATE", "EXPIRY_DATE", "LOAD_DATE"
        )
        .withColumn("VALID_FROM", F.col("EFFECTIVE_DATE"))
        .withColumn("VALID_TO", F.col("EXPIRY_DATE"))
        .withColumn("IS_CURRENT", F.when(F.col("EXPIRY_DATE").isNull(), F.lit("Y")).otherwise(F.lit("N")))
        .withColumn("VERSION_NUM", F.lit(1))
        .withColumn("CREATED_BY", F.lit("ETL"))
        .withColumn("CREATED_DATE", F.current_timestamp())
        .withColumn("UPDATED_BY", F.lit(None).cast("string"))
        .withColumn("UPDATED_DATE", F.lit(None).cast("timestamp"))
    )


def build_dim_customer_retail_source(stg_customer_sales: DataFrame) -> DataFrame:
    return (
        stg_customer_sales
        .select(
            "CUSTOMER_ID", "CUSTOMER_CODE", "FIRST_NAME", "LAST_NAME",
            "EMAIL", "PHONE", "REGION_CODE", "LOYALTY_TIER",
            "REGISTRATION_DATE", "LAST_PURCHASE_DATE", "LOAD_DATE"
        )
        .withColumn("FULL_NAME", F.concat_ws(" ", F.col("FIRST_NAME"), F.col("LAST_NAME")))
        .withColumn("VALID_FROM", F.col("REGISTRATION_DATE"))
        .withColumn("VALID_TO", F.lit(None).cast("date"))
        .withColumn("IS_CURRENT", F.lit("Y"))
        .withColumn("VERSION_NUM", F.lit(1))
        .withColumn("CREATED_DATE", F.current_timestamp())
        .withColumn("UPDATED_DATE", F.lit(None).cast("timestamp"))
    )


def build_fact_daily_sales(
    stg_sales_transactions: DataFrame,
    dim_product: DataFrame,
    dim_customer: Optional[DataFrame] = None,
    dim_store: Optional[DataFrame] = None
) -> DataFrame:
    """
    Build daily sales fact.
    """
    p = dim_product.filter(F.col("IS_CURRENT") == "Y").select("PRODUCT_ID", F.col("DIM_PRODUCT_KEY"))
    df = stg_sales_transactions.join(p, "PRODUCT_ID", "left")

    if dim_customer is not None:
        c = dim_customer.filter(F.col("IS_CURRENT") == "Y").select("CUSTOMER_ID", F.col("DIM_CUSTOMER_KEY"))
        df = df.join(c, "CUSTOMER_ID", "left")
    if dim_store is not None:
        s = dim_store.filter(F.col("IS_CURRENT") == "Y").select("STORE_ID", F.col("DIM_STORE_KEY"))
        df = df.join(s, "STORE_ID", "left")

    return (
        df.withColumn("GROSS_AMOUNT", F.col("QUANTITY") * F.col("UNIT_PRICE"))
          .withColumn("NET_AMOUNT", F.col("GROSS_AMOUNT") - F.coalesce(F.col("DISCOUNT_AMT"), F.lit(0)))
          .withColumn("TAX_AMOUNT", F.lit(None).cast("decimal(38,9)"))
          .withColumn("FISCAL_YEAR", F.year("TRANSACTION_DATE"))
          .withColumn("FISCAL_MONTH", F.month("TRANSACTION_DATE"))
          .withColumn("FISCAL_WEEK", F.weekofyear("TRANSACTION_DATE"))
          .withColumn("FACT_SALES_KEY", F.monotonically_increasing_id().cast("long"))
          .withColumn("LOAD_DATE", F.current_timestamp())
          .withColumn("LOAD_BATCH_ID", F.unix_timestamp(F.current_timestamp()).cast("long"))
    )


def build_fact_regional_summary(fact_daily_sales: DataFrame) -> DataFrame:
    return (
        fact_daily_sales.groupBy("REGION_CODE", F.col("TRANSACTION_DATE").alias("SUMMARY_DATE"))
        .agg(
            F.count("*").alias("TOTAL_TRANSACTIONS"),
            F.sum("QUANTITY").alias("TOTAL_QUANTITY"),
            F.sum("NET_AMOUNT").alias("TOTAL_REVENUE"),
            F.avg("QUANTITY").alias("AVG_BASKET_SIZE"),
            F.countDistinct("DIM_CUSTOMER_KEY").alias("DISTINCT_CUSTOMERS"),
            F.countDistinct("DIM_PRODUCT_KEY").alias("DISTINCT_PRODUCTS")
        )
        .withColumn("SUMMARY_KEY", F.monotonically_increasing_id().cast("long"))
        .withColumn("LOAD_DATE", F.current_timestamp())
        .withColumn("LOAD_BATCH_ID", F.unix_timestamp(F.current_timestamp()).cast("long"))
    )


def build_retail_product_rankings(fact_daily_sales: DataFrame) -> DataFrame:
    agg = (
        fact_daily_sales.groupBy("REGION_CODE", "DIM_PRODUCT_KEY", "FISCAL_YEAR", "FISCAL_MONTH")
        .agg(
            F.sum("NET_AMOUNT").alias("TOTAL_REVENUE"),
            F.sum("QUANTITY").alias("TOTAL_UNITS"),
            F.count("*").alias("TRANSACTION_COUNT"),
            F.avg("NET_AMOUNT").alias("AVG_TRANSACTION_VALUE")
        )
    )
    w = Window.partitionBy("REGION_CODE", "FISCAL_YEAR", "FISCAL_MONTH").orderBy(F.desc("TOTAL_REVENUE"))
    total_w = Window.partitionBy("REGION_CODE", "FISCAL_YEAR", "FISCAL_MONTH")
    return (
        agg.withColumn("REVENUE_RANK", F.rank().over(w))
           .withColumn("REVENUE_DENSE_RANK", F.dense_rank().over(w))
           .withColumn("REVENUE_PCT_SHARE", F.col("TOTAL_REVENUE") / F.sum("TOTAL_REVENUE").over(total_w))
           .withColumn("LOAD_DATE", F.current_timestamp())
    )


def build_retail_daily_analytical_summary(fact_daily_sales: DataFrame) -> DataFrame:
    daily = (
        fact_daily_sales.groupBy("REGION_CODE", "TRANSACTION_DATE", "FISCAL_YEAR", "FISCAL_MONTH")
        .agg(
            F.count("*").alias("TOTAL_TRANSACTIONS"),
            F.sum("NET_AMOUNT").alias("DAILY_REVENUE"),
            F.sum("QUANTITY").alias("DAILY_UNITS"),
            F.avg("NET_AMOUNT").alias("AVG_BASKET"),
            F.sum("DISCOUNT_AMT").alias("TOTAL_DISCOUNTS"),
            F.countDistinct("DIM_CUSTOMER_KEY").alias("UNIQUE_CUSTOMERS"),
            F.countDistinct("DIM_PRODUCT_KEY").alias("UNIQUE_PRODUCTS")
        )
    )
    w = Window.partitionBy("REGION_CODE", "FISCAL_YEAR").orderBy("TRANSACTION_DATE").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    prev_w = Window.partitionBy("REGION_CODE").orderBy("TRANSACTION_DATE")
    return (
        daily.withColumn("RUNNING_REVENUE_YTD", F.sum("DAILY_REVENUE").over(w))
             .withColumn("REVENUE_VS_PREV_DAY", F.col("DAILY_REVENUE") - F.lag("DAILY_REVENUE", 1).over(prev_w))
             .withColumn("LOAD_DATE", F.current_timestamp())
    )


# ------------------------------------------------------------------------------
# 5) Customer Domain Transformations
# ------------------------------------------------------------------------------

def build_dim_customer_crm_source(stg_customer_profile: DataFrame) -> DataFrame:
    return (
        stg_customer_profile
        .withColumn("FULL_NAME", F.concat_ws(" ", F.col("FIRST_NAME"), F.col("LAST_NAME")))
        .withColumn("VALID_FROM", F.col("REGISTRATION_DATE"))
        .withColumn("VALID_TO", F.lit(None).cast("date"))
        .withColumn("IS_CURRENT", F.lit("Y"))
        .withColumn("VERSION_NUM", F.lit(1))
        .withColumn("CREATED_DATE", F.current_timestamp())
        .withColumn("UPDATED_DATE", F.lit(None).cast("timestamp"))
    )


def build_customer_scores(
    crm_customer_dim: DataFrame,
    feature_config: DataFrame
) -> DataFrame:
    """
    Example scoring framework using configurable weights.
    """
    base = crm_customer_dim.select(
        "customer_id", "customer_code", "region_code", "customer_segment",
        "is_opted_in_email"
    )
    return (
        base.withColumn("clv_score", F.lit(50.0))
            .withColumn("churn_risk_score", F.lit(20.0))
            .withColumn("propensity_buy_score", F.lit(60.0))
            .withColumn("engagement_score", F.lit(70.0))
            .withColumn("segment_code", F.coalesce(F.col("customer_segment"), F.lit("UNKNOWN")))
            .withColumn("segment_label", F.col("segment_code"))
            .withColumn("score_date", F.current_date())
            .withColumn("batch_run_date", F.current_date())
            .withColumn("model_version", F.lit("v1"))
            .withColumn("load_batch_id", F.unix_timestamp(F.current_timestamp()).cast("long"))
            .withColumn("score_key", F.monotonically_increasing_id().cast("long"))
    )


def build_crm_micro_segments(customer_scores: DataFrame) -> DataFrame:
    return (
        customer_scores.withColumn(
            "composite_score",
            F.round(
                F.col("clv_score") * F.lit(0.4) +
                F.col("churn_risk_score") * F.lit(0.3) +
                F.col("engagement_score") * F.lit(0.3),
                2
            )
        )
        .withColumn(
            "micro_segment",
            F.when(F.col("composite_score") >= 75, F.lit("HIGH_VALUE"))
             .when(F.col("composite_score") >= 50, F.lit("MID_VALUE"))
             .otherwise(F.lit("LOW_VALUE"))
        )
        .withColumn(
            "intervention_recommended",
            F.when(F.col("churn_risk_score") >= 70, F.lit("YES")).otherwise(F.lit("NO"))
        )
        .select(
            "customer_id", "segment_code", "micro_segment", "composite_score",
            "churn_risk_score", "clv_score", "engagement_score",
            "intervention_recommended", "region_code"
        )
        .withColumn("load_date", F.current_date())
    )


def build_crm_region_segment_dist(crm_micro_segments: DataFrame) -> DataFrame:
    total_by_region = crm_micro_segments.groupBy("region_code").agg(F.count("*").alias("region_total"))
    agg = (
        crm_micro_segments.groupBy("region_code", "micro_segment", "segment_code")
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("clv_score").alias("avg_clv"),
            F.avg("churn_risk_score").alias("avg_churn_risk"),
            F.avg("composite_score").alias("avg_composite_score"),
            F.sum(F.when(F.col("intervention_recommended") == "YES", 1).otherwise(0)).alias("email_opted_in"),
            F.avg("clv_score").alias("avg_retail_lifetime_value")
        )
    )
    return (
        agg.join(total_by_region, "region_code", "left")
           .withColumn("pct_of_region", F.col("customer_count") / F.col("region_total"))
           .drop("region_total")
           .withColumn("load_date", F.current_date())
    )


def build_crm_segment_weekly_snapshot(crm_micro_segments: DataFrame) -> DataFrame:
    return (
        crm_micro_segments.groupBy("segment_code", "region_code")
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("clv_score").alias("avg_clv_score"),
            F.avg("churn_risk_score").alias("avg_churn_risk"),
            F.sum(F.when(F.col("micro_segment") == "HIGH_VALUE", 1).otherwise(0)).alias("high_value_count"),
            F.sum(F.when(F.col("micro_segment") == "LOW_VALUE", 1).otherwise(0)).alias("at_risk_count"),
            F.avg("clv_score").alias("avg_retail_spend")
        )
        .withColumn("load_date", F.current_date())
    )


# ------------------------------------------------------------------------------
# 6) Data Quality Framework
# ------------------------------------------------------------------------------

def run_dq_rule(df: DataFrame, rule_name: str, rule_sql: str) -> Tuple[int, int]:
    """
    Generic DQ rule runner using Spark SQL expression.
    Returns total rows and failed rows.
    """
    total_rows = df.count()
    failed_rows = df.filter(~F.expr(rule_sql)).count()
    return total_rows, failed_rows


def dq_null_check(df: DataFrame, cols: List[str]) -> DataFrame:
    expr = None
    for c in cols:
        cond = F.col(c).isNull()
        expr = cond if expr is None else (expr | cond)
    return df.withColumn("dq_failed", expr.cast("int"))


# ------------------------------------------------------------------------------
# 7) Example Orchestration Entry Points
# ------------------------------------------------------------------------------

def finance_pipeline(
    spark: SparkSession,
    stg_gl_transactions: DataFrame,
    dim_account: DataFrame,
    dim_period: DataFrame,
    dim_cost_centre: Optional[DataFrame] = None
) -> Dict[str, DataFrame]:
    fact_gl = build_fact_gl_balances(stg_gl_transactions, dim_account, dim_period, dim_cost_centre)
    recon = build_period_reconciliation(
        fact_gl.select(
            "ENTITY_CODE", "PERIOD_NAME",
            F.lit(None).cast("string").alias("ACCOUNT_CODE"),
            F.col("CLOSING_BALANCE")
        ),
        spark.createDataFrame([], "ENTITY_CODE STRING, PERIOD_NAME STRING, ACCOUNT_CODE STRING, SUB_LEDGER_BALANCE DECIMAL(38,9)")
    )
    return {"fact_gl_balances": fact_gl, "fact_period_reconciliation": recon}


def retail_pipeline(
    stg_sales_transactions: DataFrame,
    dim_product: DataFrame,
    dim_customer: Optional[DataFrame] = None,
    dim_store: Optional[DataFrame] = None
) -> Dict[str, DataFrame]:
    fact_sales = build_fact_daily_sales(stg_sales_transactions, dim_product, dim_customer, dim_store)
    regional = build_fact_regional_summary(fact_sales)
    rankings = build_retail_product_rankings(fact_sales)
    daily_summary = build_retail_daily_analytical_summary(fact_sales)
    return {
        "fact_daily_sales": fact_sales,
        "fact_regional_summary": regional,
        "retail_product_rankings": rankings,
        "retail_daily_analytical_summary": daily_summary
    }


def customer_pipeline(
    crm_customer_profile: DataFrame
) -> Dict[str, DataFrame]:
    crm_dim = build_dim_customer_crm_source(crm_customer_profile)
    scores = build_customer_scores(crm_dim, feature_config=None)  # feature_config can be wired in later
    micro = build_crm_micro_segments(scores)
    dist = build_crm_region_segment_dist(micro)
    weekly = build_crm_segment_weekly_snapshot(micro)
    return {
        "dim_customer_crm_source": crm_dim,
        "fact_customer_scores": scores,
        "crm_micro_segments": micro,
        "crm_region_segment_dist": dist,
        "crm_segment_weekly_snapshot": weekly
    }


# ------------------------------------------------------------------------------
# 8) Main Example
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    spark = build_spark()

    # Example placeholders; in production, read from BigQuery staging tables.
    # stg_gl = read_bigquery(spark, "project.stg_finance.stg_gl_transactions")
    # dim_account = read_bigquery(spark, "project.dw_finance.dim_account")
    # dim_period = read_bigquery(spark, "project.dw_finance.dim_period")

    # finance_outputs = finance_pipeline(spark, stg_gl, dim_account, dim_period)
    # write_bigquery(finance_outputs["fact_gl_balances"], "project.dw_finance.fact_gl_balances", mode="append", temporary_gcs_bucket="your-temp-bucket")

    spark.stop()
```

### Notes
- This is a reusable PySpark foundation for the migration design.
- It includes modular functions for:
  - BigQuery read/write
  - SCD Type 2 preparation
  - Finance, Retail, and Customer transformations
  - Data quality checks
  - Pipeline entry points
- For production, wire these functions into Cloud Composer DAGs and replace placeholders with actual BigQuery table names and configs.

If you want, I can next generate:
1. **Cloud Composer DAG code**
2. **BigQuery MERGE SQL for each SCD Type 2 dimension**
3. **Dataproc PySpark job files split by domain**
4. **A reusable project folder structure**