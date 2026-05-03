# Databricks notebook source
storage_account = "olistetlstga"
container_name = "olistdata"
application_id = "f827b9f3-98f6-4c39-a694-810245f03070"
directory_id = "5d25a949-9d59-4659-90df-82c00195d214"
client_secret = "Num8Q~e3lLzKWjZssYZe~pCMp549r8mtmaOGVakg"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

bronze_base = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/bronze"
silver_base = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/silver"
gold_base = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, sum, avg, count, max, round, year, month, concat,
    monotonically_increasing_id, first, countDistinct
)
from delta.tables import DeltaTable

# COMMAND ----------

spark = SparkSession.builder.appName("SilverToGold_Facts").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper function for OPTIMIZE

# COMMAND ----------

def optimize_table(path, zorder_cols=None):
    if zorder_cols and len(zorder_cols) > 0:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver tables

# COMMAND ----------

df_orders = spark.read.format("delta").load(f"{silver_base}/orders")
df_order_items = spark.read.format("delta").load(f"{silver_base}/order_items")
df_order_payments = spark.read.format("delta").load(f"{silver_base}/order_payments")
df_order_reviews = spark.read.format("delta").load(f"{silver_base}/order_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold dimensions

# COMMAND ----------

dim_customer = spark.read.format("delta").load(f"{gold_base}/dim_customer")
dim_product = spark.read.format("delta").load(f"{gold_base}/dim_product")
dim_seller = spark.read.format("delta").load(f"{gold_base}/dim_seller")
dim_orders = spark.read.format("delta").load(f"{gold_base}/dim_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC # FACTS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare payment aggregate by order

# COMMAND ----------

payments_agg = df_order_payments.groupBy("order_id").agg(
    round(sum(col("payment_value")), 2).alias("payment_amount"),
    max(col("payment_installments")).alias("max_payment_installments"),
    count(lit(1)).alias("payment_transaction_count"),
    first(col("payment_type"), ignorenulls=True).alias("primary_payment_type"),
    first(col("payment_group"), ignorenulls=True).alias("primary_payment_group"),
    max(col("is_installment_payment")).alias("has_installment_payment"),
    max(col("is_high_value_payment")).alias("has_high_value_payment")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare review aggregate by order

# COMMAND ----------

df_order_reviews.printSchema()

# COMMAND ----------

#avg(col("response_time_days")).alias("avg_response_time_days")
reviews_agg = df_order_reviews.groupBy("order_id").agg(
    avg(col("review_score")).alias("avg_review_score"),
    first(col("review_sentiment"), ignorenulls=True).alias("review_sentiment"),
    max(col("has_comment")).alias("has_comment"),
    avg(col("response_time_days")).alias("avg_response_time_days"),
    count(lit(1)).alias("review_count")
)

reviews_agg = reviews_agg.withColumn(
    "avg_review_score",
    round(col("avg_review_score"), 2)
).withColumn(
    "avg_response_time_days",
    round(col("avg_response_time_days"), 2)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build fact_sales base at order-item grain

# COMMAND ----------

fact_sales_base = df_order_items.alias("oi") \
    .join(
        df_orders.alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "inner"
    ) \
    .join(
        payments_agg.alias("p"),
        col("oi.order_id") == col("p.order_id"),
        "left"
    ) \
    .join(
        reviews_agg.alias("r"),
        col("oi.order_id") == col("r.order_id"),
        "left"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join surrogate keys from dimensions

# COMMAND ----------

fact_sales_base = fact_sales_base \
    .join(
        dim_customer.select(
            col("customer_sk"),
            col("natural_customer_key")
        ).alias("dc"),
        col("o.customer_id") == col("dc.natural_customer_key"),
        "left"
    ) \
    .join(
        dim_product.select(
            col("product_sk"),
            col("natural_product_key")
        ).alias("dp"),
        col("oi.product_id") == col("dp.natural_product_key"),
        "left"
    ) \
    .join(
        dim_seller.select(
            col("seller_sk"),
            col("natural_seller_key")
        ).alias("ds"),
        col("oi.seller_id") == col("ds.natural_seller_key"),
        "left"
    ) \
    .join(
        dim_orders.select(
            col("order_sk"),
            col("natural_order_key")
        ).alias("do"),
        col("oi.order_id") == col("do.natural_order_key"),
        "left"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create fact_sales

# COMMAND ----------

fact_sales_df = fact_sales_base.select(
    monotonically_increasing_id().alias("fact_sales_sk"),

    # surrogate keys
    col("do.order_sk"),
    col("dc.customer_sk"),
    col("dp.product_sk"),
    col("ds.seller_sk"),

    # degenerate business keys
    col("oi.order_id"),
    col("oi.order_item_id"),
    col("oi.product_id").alias("natural_product_id"),
    col("oi.seller_id").alias("natural_seller_id"),
    col("o.customer_id").alias("natural_customer_id"),

    # order info
    col("o.order_status"),
    col("o.order_status_group"),
    col("o.order_purchase_date").alias("purchase_date"),
    col("o.order_delivered_customer_date").alias("delivery_date"),
    col("o.order_estimated_delivery_date").alias("estimated_delivery_date"),
    col("o.purchase_year"),
    col("o.purchase_month"),
    col("o.purchase_day"),
    col("o.delivery_days"),
    col("o.approval_days"),
    col("o.shipping_days"),
    col("o.is_delayed"),

    # item metrics
    round(col("oi.price"), 2).alias("sales_amount"),
    round(col("oi.freight_value"), 2).alias("shipping_cost"),
    round(col("oi.item_total_amount"), 2).alias("gross_item_amount"),

    # payment metrics
    round(col("p.payment_amount"), 2).alias("payment_amount"),
    col("p.primary_payment_type").alias("payment_type"),
    col("p.primary_payment_group").alias("payment_group"),
    col("p.max_payment_installments").alias("payment_installments"),
    col("p.payment_transaction_count"),
    col("p.has_installment_payment"),
    col("p.has_high_value_payment"),

    # review metrics
    col("r.avg_review_score").alias("review_score"),
    col("r.review_sentiment"),
    col("r.has_comment"),
    col("r.avg_response_time_days"),
    col("r.review_count")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle missing values in fact_sales

# COMMAND ----------

fact_sales_df = fact_sales_df.fillna({
    "order_status": "unknown",
    "order_status_group": "other",
    "sales_amount": 0.0,
    "shipping_cost": 0.0,
    "gross_item_amount": 0.0,
    "payment_amount": 0.0,
    "payment_type": "unknown",
    "payment_group": "other",
    "payment_installments": 0,
    "payment_transaction_count": 0,
    "has_installment_payment": False,
    "has_high_value_payment": False,
    "review_score": 0.0,
    "review_sentiment": "negative",
    "has_comment": False,
    "avg_response_time_days": 0.0,
    "review_count": 0,
    "delivery_days": 0,
    "approval_days": 0,
    "shipping_days": 0,
    "is_delayed": False
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write fact_sales + OPTIMIZE

# COMMAND ----------

fact_sales_df.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/fact_sales") \
    .save()

optimize_table(f"{gold_base}/fact_sales", ["customer_sk", "product_sk", "seller_sk"])

# COMMAND ----------

# MAGIC %md
# MAGIC # FACT SALES AGGREGATION

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reload fact_sales

# COMMAND ----------

fact_sales_df = spark.read.format("delta").load(f"{gold_base}/fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build fact_sales_agg (monthly)

# COMMAND ----------

fact_sales_agg_df = fact_sales_df.groupBy(
    year(col("purchase_date")).alias("year"),
    month(col("purchase_date")).alias("month"),
    concat(year(col("purchase_date")), lit("-"), month(col("purchase_date"))).alias("year_month"),
    col("product_sk"),
    col("seller_sk")
).agg(
    round(sum(col("sales_amount")), 2).alias("total_sales"),
    round(sum(col("shipping_cost")), 2).alias("total_shipping_cost"),
    round(sum(col("gross_item_amount")), 2).alias("total_gross_amount"),
    round(avg(col("review_score")), 2).alias("avg_review_score"),
    count(col("fact_sales_sk")).alias("total_order_items"),
    countDistinct(col("order_id")).alias("total_orders")
).withColumn(
    "fact_sales_agg_sk",
    monotonically_increasing_id()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write fact_sales_agg + OPTIMIZE

# COMMAND ----------

fact_sales_agg_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year_month") \
    .option("path", f"{gold_base}/fact_sales_agg") \
    .save()

optimize_table(f"{gold_base}/fact_sales_agg", ["product_sk", "seller_sk"])

# COMMAND ----------

# MAGIC %md
# MAGIC # BRIDGE ORDER ITEMS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build bridge_order_items

# COMMAND ----------

bridge_order_items_df = fact_sales_df.select(
    monotonically_increasing_id().alias("bridge_order_items_sk"),
    col("order_sk"),
    col("order_id"),
    col("order_item_id"),
    col("product_sk"),
    col("seller_sk"),
    col("sales_amount").alias("unit_price"),
    col("shipping_cost"),
    col("gross_item_amount").alias("total_price"),
    lit(1).alias("quantity")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write bridge_order_items + OPTIMIZE

# COMMAND ----------

bridge_order_items_df.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/bridge_order_items") \
    .save()

optimize_table(f"{gold_base}/bridge_order_items", ["order_sk", "product_sk", "seller_sk"])

# COMMAND ----------

# MAGIC %md
# MAGIC # FACT ORDER PAYMENTS PARTITIONED

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build payment-focused fact

# COMMAND ----------

fact_order_payments_df = fact_sales_df.select(
    monotonically_increasing_id().alias("fact_order_payment_sk"),
    col("order_sk"),
    col("order_id"),
    col("customer_sk"),
    col("purchase_date"),
    col("payment_type"),
    col("payment_group"),
    col("payment_installments"),
    col("payment_transaction_count"),
    col("payment_amount"),
    col("has_installment_payment"),
    col("has_high_value_payment")
).dropDuplicates([
    "order_id",
    "payment_type",
    "payment_amount",
    "payment_installments"
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write fact_order_payments_partitioned + OPTIMIZE

# COMMAND ----------

fact_order_payments_df = fact_order_payments_df.fillna({
    "payment_type": "unknown",
    "payment_group": "other",
    "payment_installments": 0,
    "payment_transaction_count": 0,
    "payment_amount": 0.0,
    "has_installment_payment": False,
    "has_high_value_payment": False
})

# COMMAND ----------

fact_order_payments_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("payment_type") \
    .option("path", f"{gold_base}/fact_order_payments_partitioned") \
    .save()

optimize_table(f"{gold_base}/fact_order_payments_partitioned", ["order_sk", "customer_sk"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## PERFORMANCE OPTIMIZATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write partitioned fact_sales

# COMMAND ----------

fact_sales_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("purchase_year", "purchase_month") \
    .option("path", f"{gold_base}/fact_sales_partitioned") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE partitioned fact_sales

# COMMAND ----------

optimize_table(
    f"{gold_base}/fact_sales_partitioned",
    ["customer_sk", "product_sk", "seller_sk"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE dimensions used heavily

# COMMAND ----------

optimize_table(f"{gold_base}/dim_customer", ["natural_customer_key"])
optimize_table(f"{gold_base}/dim_product", ["natural_product_key"])
optimize_table(f"{gold_base}/dim_orders", ["natural_order_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vacuum

# COMMAND ----------

fact_sales_partitioned_table = DeltaTable.forPath(spark, f"{gold_base}/fact_sales_partitioned")
dim_customer_table = DeltaTable.forPath(spark, f"{gold_base}/dim_customer")

fact_sales_partitioned_table.vacuum(retentionHours=168)
dim_customer_table.vacuum(retentionHours=168)

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDATION

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate outputs

# COMMAND ----------

gold_tables = [
    "fact_sales",
    "fact_sales_agg",
    "bridge_order_items",
    "fact_order_payments_partitioned",
    "fact_sales_partitioned"
]

for table_name in gold_tables:
    df_tmp = spark.read.format("delta").load(f"{gold_base}/{table_name}")
    print(f"{table_name}: {df_tmp.count()} rows")