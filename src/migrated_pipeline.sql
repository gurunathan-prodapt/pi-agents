Below is a modular PySpark implementation that mirrors the migration design, but keeps the code reusable and organized by domain and function. Since the design is primarily a BigQuery migration target, I’ve structured this as a PySpark-based orchestration and transformation layer that can be used for:

- interim migration validation,
- source-to-target transformation parity,
- reusable ETL logic,
- and a thin orchestration wrapper for unsupported logic.

If you want, I can also convert this into:
1. pure BigQuery SQL/stored procedures, or  
2. a Databricks-style PySpark job package.

---

## Project structure

```text
finaltestingrepo/
├── config/
│   ├── app_config.py
│   └── schemas.py
├── core/
│   ├── spark_session.py
│   ├── io_utils.py
│   ├── audit_utils.py
│   ├── dq_utils.py
│   └── common_transformations.py
├── sales/
│   ├── sales_extract.py
│   ├── sales_rollup.py
│   ├── sales_historization.py
│   ├── sales_aggregation.py
│   └── sales_dq.py
├── finance/
│   ├── gl_extract.py
│   ├── gl_transform.py
│   ├── gl_reconciliation.py
│   ├── account_processor.py
│   └── gl_aggregation.py
├── customer/
│   ├── customer_extract.py
│   ├── customer_transform.py
│   ├── customer_historization.py
│   ├── customer_scoring.py
│   ├── customer_segmentation.py
│   └── lineage_tracker.py
├── orchestration/
│   ├── event_waiter.py
│   ├── runner.py
│   └── retry_utils.py
└── main.py
```

---

# 1) Core reusable utilities

## `config/app_config.py`

```python
from dataclasses import dataclass

@dataclass
class AppConfig:
    app_name: str = "finaltestingrepo"
    env: str = "dev"

    # source systems
    sales_source_table: str = "source_ops.sales_txn"
    customer_source_table: str = "source_ops.customer"
    loyalty_source_table: str = "source_ops.loyalty_profile"
    gl_source_table: str = "source_fin.gl_jnl_lines"
    gl_ledgers_table: str = "source_fin.gl_ledgers"
    legal_entities_table: str = "source_fin.legal_entities"

    # target tables
    stg_sales_transactions: str = "sales.stg_sales_transactions"
    stg_customer_sales: str = "sales.stg_customer_sales"
    fact_regional_summary: str = "sales.fact_regional_summary"
    dim_product: str = "sales.dim_product"
    dim_customer: str = "sales.dim_customer"

    stg_gl_transactions: str = "finance.stg_gl_transactions"
    stg_period_rates: str = "finance.stg_period_rates"
    fact_gl_balances: str = "finance.fact_gl_balances"
    fact_period_reconciliation: str = "finance.fact_period_reconciliation"
    dim_account: str = "finance.dim_account"
    dim_period: str = "finance.dim_period"

    stg_customer_profile: str = "customer.stg_customer_profile"
    stg_campaign_events: str = "customer.stg_campaign_events"
    stg_customer_interactions: str = "customer.stg_customer_interactions"
    fact_customer_scores: str = "customer.fact_customer_scores"
    dim_customer_crm: str = "customer.dim_customer_crm"

    dq_results: str = "audit.dq_results"
    job_audit: str = "audit.job_audit"
```

---

## `core/spark_session.py`

```python
from pyspark.sql import SparkSession

def get_spark(app_name: str = "finaltestingrepo") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )
```

---

## `core/common_transformations.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_load_metadata(df: DataFrame, load_date_col: str, batch_id: str, source_file: str = None) -> DataFrame:
    df = df.withColumn("load_date", F.lit(load_date_col))
    df = df.withColumn("load_batch_id", F.lit(batch_id))
    if source_file is not None:
        df = df.withColumn("source_file_name", F.lit(source_file))
    return df

def standardize_string_cols(df: DataFrame, cols: list[str]) -> DataFrame:
    for c in cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df

def safe_divide(numerator, denominator):
    return F.when(denominator.isNull() | (denominator == 0), F.lit(0)).otherwise(numerator / denominator)

def hash_key(*cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)

def current_ts_col():
    return F.current_timestamp()
```

---

## `core/audit_utils.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_job_audit_df(job_name: str, run_date: str, status: str, rows_processed: int, message: str = None) -> DataFrame:
    return (
        spark.createDataFrame(
            [(job_name, run_date, status, rows_processed, message)],
            ["job_name", "run_date", "status", "rows_processed", "message"]
        )
        .withColumn("audit_ts", F.current_timestamp())
    )

def write_audit(df: DataFrame, target_table: str, mode: str = "append"):
    df.write.mode(mode).saveAsTable(target_table)
```

---

## `core/dq_utils.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def dq_null_check(df: DataFrame, column_name: str, rule_name: str, severity: str = "CRITICAL") -> DataFrame:
    total_cnt = df.count()
    null_cnt = df.filter(F.col(column_name).isNull()).count()
    fail_pct = 0 if total_cnt == 0 else round((null_cnt / total_cnt) * 100, 2)
    status = "PASS" if null_cnt == 0 else "FAIL"

    return df.sparkSession.createDataFrame(
        [(rule_name, column_name, total_cnt, null_cnt, fail_pct, status, severity)],
        ["rule_name", "column_name", "total_count", "failed_count", "failed_pct", "status", "severity"]
    )

def dq_row_count_check(source_df: DataFrame, target_df: DataFrame, rule_name: str, severity: str = "HIGH") -> DataFrame:
    src_cnt = source_df.count()
    tgt_cnt = target_df.count()
    status = "PASS" if src_cnt == tgt_cnt else "FAIL"

    return source_df.sparkSession.createDataFrame(
        [(rule_name, src_cnt, tgt_cnt, status, severity)],
        ["rule_name", "source_count", "target_count", "status", "severity"]
    )
```

---

# 2) Sales domain

## `sales/sales_extract.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from core.common_transformations import add_load_metadata

def extract_sales_transactions(source_df: DataFrame, load_date: str, region_code: str, batch_id: str) -> DataFrame:
    df = (
        source_df
        .filter(
            (F.to_date("txn_datetime") == F.lit(load_date)) &
            (F.col("store_region_cd") == F.lit(region_code)) &
            (~F.col("txn_status_cd").isin("VOID", "CANCELLED", "TEST")) &
            (F.col("sold_qty") > 0) &
            (F.col("unit_sell_price") >= 0)
        )
        .select(
            F.col("txn_id").alias("transaction_id"),
            F.col("cust_id").alias("customer_id"),
            F.col("prod_id").alias("product_id"),
            F.col("store_id").alias("store_id"),
            F.to_date("txn_datetime").alias("transaction_date"),
            F.col("sold_qty").alias("quantity"),
            F.col("unit_sell_price").alias("unit_price"),
            F.coalesce(F.col("disc_amount"), F.lit(0)).alias("discount_amt"),
            F.when(F.col("unit_sell_price") > 0,
                   F.round((F.coalesce(F.col("disc_amount"), F.lit(0)) / F.col("unit_sell_price")) * 100, 2)
            ).otherwise(F.lit(0)).alias("discount_pct"),
            F.col("store_region_cd").alias("region_code"),
            F.coalesce(F.col("currency"), F.lit("GBP")).alias("currency_code"),
            F.col("payment_type_cd").alias("payment_method"),
            F.col("terminal_ref").alias("pos_terminal_id")
        )
    )
    return add_load_metadata(df, load_date, batch_id, f"{region_code}_txn_{load_date}")
```

---

## `sales/sales_rollup.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_sales_rollup(fact_sales_df: DataFrame, load_date: str, region_code: str) -> DataFrame:
    return (
        fact_sales_df
        .filter((F.col("transaction_date") == F.lit(load_date)) & (F.col("region_code") == F.lit(region_code)))
        .groupBy("region_code", F.col("transaction_date").alias("summary_date"))
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("net_amount").alias("total_revenue"),
            F.avg("net_amount").alias("avg_basket_size"),
            F.countDistinct("customer_id").alias("distinct_customers"),
            F.countDistinct("product_id").alias("distinct_products")
        )
        .withColumn("summary_key", F.sha2(F.concat_ws("||", F.col("region_code"), F.col("summary_date").cast("string")), 256))
    )
```

---

## `sales/sales_historization.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def scd2_merge_dimension(current_dim_df: DataFrame, staging_df: DataFrame, business_key: str, compare_cols: list[str]) -> DataFrame:
    joined = current_dim_df.alias("t").join(
        staging_df.alias("s"),
        on=F.col(f"t.{business_key}") == F.col(f"s.{business_key}"),
        how="fullouter"
    )

    changed_expr = None
    for c in compare_cols:
        expr = F.coalesce(F.col(f"t.{c}").cast("string"), F.lit("X")) != F.coalesce(F.col(f"s.{c}").cast("string"), F.lit("X"))
        changed_expr = expr if changed_expr is None else (changed_expr | expr)

    return joined.withColumn(
        "action",
        F.when(F.col(f"t.{business_key}").isNull(), F.lit("INSERT"))
         .when(F.col(f"s.{business_key}").isNull(), F.lit("NOOP"))
         .when(changed_expr, F.lit("EXPIRE_AND_INSERT"))
         .otherwise(F.lit("NOOP"))
    )
```

---

## `sales/sales_aggregation.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_sales_aggregation(fact_sales_df: DataFrame) -> DataFrame:
    return (
        fact_sales_df
        .withColumn("txn_day", F.to_date("transaction_date"))
        .groupBy("txn_day", "region_code", "product_id")
        .agg(
            F.sum("quantity").alias("total_qty"),
            F.sum("net_amount").alias("total_net_amount"),
            F.rank().over(
                __import__("pyspark").sql.window.Window.partitionBy("txn_day", "region_code").orderBy(F.desc("net_amount"))
            ).alias("product_rank")
        )
    )
```

---

## `sales/sales_dq.py`

```python
from pyspark.sql import DataFrame
from core.dq_utils import dq_null_check

def run_sales_dq(fact_sales_df: DataFrame) -> DataFrame:
    return dq_null_check(fact_sales_df, "product_id", "NULL_PRODUCT_KEY", "CRITICAL")
```

---

# 3) Finance domain

## `finance/gl_extract.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def extract_gl_transactions(gl_df: DataFrame, ledgers_df: DataFrame, legal_entities_df: DataFrame,
                            rates_df: DataFrame, period_name: str, entity_code: str, period_date: str) -> DataFrame:
    joined = (
        gl_df.alias("g")
        .join(ledgers_df.alias("l"), F.col("l.ledger_id") == F.col("g.ledger_id"), "inner")
        .join(legal_entities_df.alias("le"), F.col("le.legal_entity_id") == F.col("l.legal_entity_id"), "inner")
        .join(
            rates_df.alias("r"),
            (F.coalesce(F.col("g.txn_currency"), F.lit("GBP")) == F.col("r.from_currency")) &
            (F.col("r.to_currency") == F.lit("GBP")) &
            (F.col("r.period_name") == F.lit(period_name)) &
            (F.col("r.rate_type") == F.lit("AVERAGE")),
            "left"
        )
        .filter(
            (F.col("g.period_name") == F.lit(period_name)) &
            (F.col("g.status") == F.lit("POSTED")) &
            (F.col("g.amount") != 0)
        )
    )

    return joined.select(
        F.col("g.jnl_line_id").alias("journal_line_id"),
        F.col("le.entity_short_code").alias("entity_code"),
        F.col("g.ledger_id"),
        F.lit(period_name).alias("period_name"),
        F.year(F.lit(period_date)).alias("period_year"),
        F.month(F.lit(period_date)).alias("period_month"),
        F.col("g.account_segment"),
        F.col("g.cc_segment"),
        F.when(F.col("g.dr_cr_flag") == "D", F.abs(F.col("g.amount"))).otherwise(F.lit(0)).alias("debit_amt"),
        F.when(F.col("g.dr_cr_flag") == "C", F.abs(F.col("g.amount"))).otherwise(F.lit(0)).alias("credit_amt"),
        F.when(F.col("g.dr_cr_flag") == "D", F.abs(F.col("g.amount"))).otherwise(-F.abs(F.col("g.amount"))).alias("signed_amt"),
        F.coalesce(F.col("g.txn_currency"), F.lit("GBP")).alias("txn_currency"),
        F.coalesce(F.col("r.exchange_rate"), F.lit(1)).alias("exchange_rate"),
        F.when(F.coalesce(F.col("g.txn_currency"), F.lit("GBP")) == "GBP", F.abs(F.col("g.amount")))
         .otherwise(F.abs(F.col("g.amount")) * F.coalesce(F.col("r.exchange_rate"), F.lit(1))).alias("base_currency_amt"),
        F.col("g.txn_date"),
        F.col("g.journal_category"),
        F.col("g.journal_source_name"),
        F.substring(F.col("g.description"), 1, 240).alias("description"),
        F.col("g.created_by"),
        F.col("g.creation_date")
    )
```

---

## `finance/gl_transform.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_gl_balances(stg_gl_df: DataFrame, dim_account_df: DataFrame, rates_df: DataFrame) -> DataFrame:
    return (
        stg_gl_df.alias("g")
        .join(dim_account_df.alias("a"), F.col("g.account_segment") == F.col("a.account_code"), "left")
        .join(rates_df.alias("r"), F.col("g.txn_currency") == F.col("r.from_currency"), "left")
        .groupBy("g.period_name", "g.entity_code", "g.account_segment", "g.cc_segment")
        .agg(
            F.sum("g.debit_amt").alias("total_debit"),
            F.sum("g.credit_amt").alias("total_credit"),
            F.sum("g.signed_amt").alias("net_amount"),
            F.sum("g.base_currency_amt").alias("base_currency_amount")
        )
    )
```

---

## `finance/gl_reconciliation.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_period_reconciliation(gl_balances_df: DataFrame, source_balances_df: DataFrame, period_name: str) -> DataFrame:
    return (
        gl_balances_df.alias("gl")
        .join(
            source_balances_df.alias("src"),
            (F.col("gl.account_segment") == F.col("src.account_code")) &
            (F.col("gl.entity_code") == F.col("src.entity_code")) &
            (F.col("gl.period_name") == F.col("src.period_name")),
            "left"
        )
        .select(
            F.col("gl.period_name"),
            F.col("gl.entity_code"),
            F.col("gl.account_segment"),
            F.col("gl.cc_segment"),
            F.col("gl.net_amount").alias("gl_amount"),
            F.coalesce(F.col("src.source_amount"), F.lit(0)).alias("source_amount"),
            (F.col("gl.net_amount") - F.coalesce(F.col("src.source_amount"), F.lit(0))).alias("variance"),
            F.when((F.col("gl.net_amount") - F.coalesce(F.col("src.source_amount"), F.lit(0))) == 0, "PASS").otherwise("FAIL").alias("status")
        )
    )
```

---

## `finance/account_processor.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def flatten_account_hierarchy(account_df: DataFrame) -> DataFrame:
    return (
        account_df
        .withColumn("parent_account_code", F.coalesce(F.col("parent_account_code"), F.lit("ROOT")))
        .withColumn("hierarchy_path", F.concat_ws("/", F.col("parent_account_code"), F.col("account_code")))
    )

def eliminate_intercompany(df: DataFrame) -> DataFrame:
    return df.filter(~F.col("account_code").startswith("IC_"))
```

---

## `finance/gl_aggregation.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_gl_aggregation(gl_balances_df: DataFrame) -> DataFrame:
    return (
        gl_balances_df
        .groupBy("period_name", "entity_code", "cc_segment")
        .agg(
            F.sum("net_amount").alias("ytd_balance"),
            F.sum("base_currency_amount").alias("base_ytd_balance"),
            F.variance("net_amount").alias("variance_amount")
        )
    )
```

---

# 4) Customer domain

## `customer/customer_extract.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def extract_customer_sources(profile_df: DataFrame, events_df: DataFrame, interactions_df: DataFrame, load_date: str) -> dict:
    return {
        "profile": profile_df.filter(F.col("load_date") == F.lit(load_date)),
        "events": events_df.filter(F.col("event_date") <= F.lit(load_date)),
        "interactions": interactions_df.filter(F.col("interaction_date") <= F.lit(load_date))
    }
```

---

## `customer/customer_transform.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_customer_transform(profile_df: DataFrame, events_df: DataFrame, interactions_df: DataFrame, retail_spend_df: DataFrame) -> DataFrame:
    event_rollup = events_df.groupBy("customer_id").agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "CONVERTED", 1).otherwise(0)).alias("converted_count")
    )

    interaction_rollup = interactions_df.groupBy("customer_id").agg(
        F.count("*").alias("interaction_count")
    )

    return (
        profile_df.alias("p")
        .join(event_rollup.alias("e"), "customer_id", "left")
        .join(interaction_rollup.alias("i"), "customer_id", "left")
        .join(retail_spend_df.alias("r"), "customer_id", "left")
        .withColumn("conversion_rate", F.when(F.col("event_count") > 0, F.col("converted_count") / F.col("event_count")).otherwise(F.lit(0)))
        .withColumn("at_risk_flag",
                    F.when(F.col("days_since_last_purchase") > 180, F.lit("Y"))
                     .otherwise(F.lit("N")))
    )
```

---

## `customer/customer_historization.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def historize_customer_dim(current_dim_df: DataFrame, staging_df: DataFrame) -> DataFrame:
    return (
        current_dim_df.alias("t")
        .join(staging_df.alias("s"), "customer_id", "fullouter")
        .withColumn(
            "action",
            F.when(F.col("t.customer_id").isNull(), "INSERT")
             .when(F.col("s.customer_id").isNull(), "NOOP")
             .otherwise("UPSERT")
        )
    )
```

---

## `customer/customer_scoring.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_customer_scores(dim_customer_df: DataFrame, customer_transform_df: DataFrame, run_date: str, model_version: str) -> DataFrame:
    return (
        dim_customer_df.alias("dc")
        .join(customer_transform_df.alias("cs"), "customer_id", "left")
        .select(
            F.expr("uuid()").alias("score_id"),
            F.col("dc.customer_id"),
            F.col("dc.dim_crm_customer_key"),
            F.lit(run_date).cast("date").alias("score_date"),
            F.round(F.least(F.coalesce(F.col("cs.lifetime_value"), F.lit(0)) / 1000.0, F.lit(10.0)), 4).alias("value_score"),
            F.round(
                F.when(F.col("cs.last_purchase_date").isNull(), 0.95)
                 .when(F.datediff(F.current_date(), F.col("cs.last_purchase_date")) > 180, 0.80)
                 .when(F.datediff(F.current_date(), F.col("cs.last_purchase_date")) > 90, 0.50)
                 .when(F.datediff(F.current_date(), F.col("cs.last_purchase_date")) > 30, 0.20)
                 .otherwise(0.05), 4
            ).alias("recency_score"),
            F.lit(model_version).alias("model_version")
        )
    )
```

---

## `customer/customer_segmentation.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def assign_customer_segments(scores_df: DataFrame) -> DataFrame:
    return (
        scores_df
        .withColumn("composite_score", F.round((F.col("value_score") + F.col("recency_score")) / 2, 4))
        .withColumn(
            "micro_segment",
            F.when(F.col("composite_score") >= 0.8, "VIP")
             .when(F.col("composite_score") >= 0.5, "RETAIL")
             .otherwise("WHOLESALE")
        )
    )
```

---

## `customer/lineage_tracker.py`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_lineage_metadata(job_name: str, source_system: str, target_table: str, run_date: str) -> DataFrame:
    spark = __import__("pyspark").sql.SparkSession.getActiveSession()
    return spark.createDataFrame(
        [(job_name, source_system, target_table, run_date)],
        ["job_name", "source_system", "target_table", "run_date"]
    ).withColumn("tracked_ts", F.current_timestamp())
```

---

# 5) Orchestration and retry logic

## `orchestration/retry_utils.py`

```python
import time

def retry_command(fn, max_retries=3, base_sleep=2, *args, **kwargs):
    last_exc = None
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            time.sleep(base_sleep * (2 ** attempt))
    raise last_exc
```

---

## `orchestration/event_waiter.py`

```python
import time

def wait_for_event(event_checker_fn, timeout_seconds=3600, poll_seconds=30):
    start = time.time()
    while time.time() - start < timeout_seconds:
        if event_checker_fn():
            return True
        time.sleep(poll_seconds)
    return False
```

---

## `orchestration/runner.py`

```python
from orchestration.retry_utils import retry_command

def run_pipeline(steps: list):
    results = []
    for step_fn, args, kwargs in steps:
        result = retry_command(step_fn, *args, **kwargs)
        results.append(result)
    return results
```

---

# 6) Main pipeline entrypoint

## `main.py`

```python
from config.app_config import AppConfig
from core.spark_session import get_spark

from sales.sales_extract import extract_sales_transactions
from sales.sales_rollup import build_sales_rollup
from sales.sales_dq import run_sales_dq

from finance.gl_extract import extract_gl_transactions
from finance.gl_transform import build_gl_balances
from finance.gl_reconciliation import build_period_reconciliation

from customer.customer_extract import extract_customer_sources
from customer.customer_transform import build_customer_transform
from customer.customer_scoring import build_customer_scores
from customer.customer_segmentation import assign_customer_segments

def main():
    cfg = AppConfig()
    spark = get_spark(cfg.app_name)

    # Example source reads
    sales_src = spark.table(cfg.sales_source_table)
    customer_src = spark.table(cfg.customer_source_table)
    loyalty_src = spark.table(cfg.loyalty_source_table)
    gl_src = spark.table(cfg.gl_source_table)
    ledgers_src = spark.table(cfg.gl_ledgers_table)
    legal_entities_src = spark.table(cfg.legal_entities_table)

    # SALES
    sales_stg = extract_sales_transactions(sales_src, "2024-01-01", "UK", "1001")
    sales_rollup_df = build_sales_rollup(sales_stg, "2024-01-01", "UK")
    sales_dq_df = run_sales_dq(sales_stg)

    # FINANCE
    gl_stg = extract_gl_transactions(
        gl_src, ledgers_src, legal_entities_src,
        rates_df=spark.table(cfg.stg_period_rates),
        period_name="2024-01",
        entity_code="UK01",
        period_date="2024-01-31"
    )
    gl_balances_df = build_gl_balances(gl_stg, spark.table(cfg.dim_account), spark.table(cfg.stg_period_rates))
    recon_df = build_period_reconciliation(gl_balances_df, spark.table("finance.source_balances"), "2024-01")

    # CUSTOMER
    customer_sources = extract_customer_sources(
        customer_src, spark.table(cfg.stg_campaign_events), spark.table(cfg.stg_customer_interactions), "2024-01-01"
    )
    customer_transform_df = build_customer_transform(
        customer_sources["profile"],
        customer_sources["events"],
        customer_sources["interactions"],
        spark.table("sales.retail_spend")
    )
    scores_df = build_customer_scores(spark.table(cfg.dim_customer_crm), customer_transform_df, "2024-01-01", "v1")
    segments_df = assign_customer_segments(scores_df)

    # Example writes
    sales_rollup_df.write.mode("append").saveAsTable(cfg.fact_regional_summary)
    sales_dq_df.write.mode("append").saveAsTable(cfg.dq_results)
    gl_balances_df.write.mode("append").saveAsTable(cfg.fact_gl_balances)
    recon_df.write.mode("append").saveAsTable(cfg.fact_period_reconciliation)
    segments_df.write.mode("append").saveAsTable(cfg.fact_customer_scores)

if __name__ == "__main__":
    main()
```

---

# 7) Notes on how this maps to the migration design

This PySpark package is intentionally modular so each design item can be implemented independently:

- **Sales extract / rollup / historization / aggregation / DQ**
  - implemented as separate functions
- **Finance extract / transform / reconciliation / aggregation**
  - implemented as separate functions
- **Customer extract / transform / scoring / segmentation / lineage**
  - implemented as separate functions
- **Unsupported orchestration**
  - isolated into `orchestration/`
- **Reusable logic**
  - centralized in `core/`

---

# 8) Recommended next step

If you want this to be production-ready, I can generate the following next:

1. **PySpark DDL scripts** for all target tables  
2. **Unit tests** for each transformation function  
3. **A full Airflow/Composer DAG** for orchestration  
4. **A pure BigQuery SQL/stored procedure version** of the same design  
5. **A migration mapping document** from Oracle/Ab Initio/Spark to BigQuery

If you want, I can now produce the **BigQuery SQL/stored procedure implementation** for the same design, domain by domain.