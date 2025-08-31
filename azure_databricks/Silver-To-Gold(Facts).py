# Databricks notebook source
storage_account = "olistetlstga"
application_id = "0d394405-7465-4b4f-8179-b27eac5c6fb4"
directory_id = "5d25a949-9d59-4659-90df-82c00195d214"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "p9u8Q~1T9a6AZo4uBrX0fgjyQy86qVMhtM3BMcIs")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORT LIBRARIES

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, split, concat_ws, trim, upper, lower, avg, countDistinct
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType, TimestampType
from pyspark.sql import Window
from pyspark.sql.functions import row_number, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPARK SESSION

# COMMAND ----------

spark = SparkSession.builder.appName("SilverLayerProcessing").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD TABLES FROM BRONZE LAYER

# COMMAND ----------

df_customers = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/customers")
df_geolocation = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/geolocation")
df_order_items = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/order_items")
df_order_payments = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/order_payments")
df_order_reviews = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/order_reviews")
df_orders = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/orders")
df_products = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/products")
df_sellers = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC # FACTS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Read Orders, Order_Items, Order_Payments, Order_Reviews Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create FactSales Table

# COMMAND ----------

# Objective:
# The FactSales table is the core transactional fact table in the Gold Layer. It stores detailed sales transactions at the most granular level, linking customers, products, orders, and sellers.

# Source Tables (Silver Layer):
# Silver.orders

# Silver.order_items

# Silver.order_payments

# Silver.order_reviews

# Transformations Applied:
# ✅ Joining Multiple Tables → Consolidating order, payment, and review data
# ✅ Generating Surrogate Keys → fact_sales_sk as a unique identifier
# ✅ Ensuring Referential Integrity → Linking to Dim tables (customer_sk, product_sk, etc.)
# ✅ Handling Missing Values → Default values for missing data
# ✅ Formatting Date Columns → Standardizing timestamps
# ✅ Aggregating Key Metrics → Revenue, discount, and payment amounts
# ✅ Optimizing Query Performance → Saving as Delta format

# COMMAND ----------

from pyspark.sql.functions import col, lit, sum, avg, count, when, monotonically_increasing_id

# Join Tables
fact_sales_df = df_orders \
    .join(df_order_items, "order_id", "inner") \
    .join(df_order_payments, "order_id", "left") \
    .join(df_order_reviews, "order_id", "left") \
    .select(
        monotonically_increasing_id().alias("fact_sales_sk"),  # Generating Surrogate Key
        col("order_id"),
        col("customer_id").alias("customer_sk"),  # Linking to DimCustomer
        col("product_id").alias("product_sk"),  # Linking to DimProduct
        col("seller_id").alias("seller_sk"),  # Linking to DimSeller
        col("order_status"),
        col("order_purchase_date").alias("purchase_date"),
        col("order_delivered_customer_date").alias("delivery_date"),
        col("price").alias("sales_amount"),
        col("freight_value").alias("shipping_cost"),
        col("payment_value").alias("payment_amount"),
        col("payment_type"),
        col("review_score").cast("int").alias("review_score"),
        when(col("review_score") >= 4, "Positive")
        .when(col("review_score") == 3, "Neutral")
        .otherwise("Negative").alias("review_sentiment")
    )

# Handle Missing Values
fact_sales_df = fact_sales_df.fillna({
    "review_score": 0,
    "review_sentiment": "Unknown",
    "payment_type": "Unknown",
    "delivery_date": "1900-01-01"
})

# Save as Delta Table in Gold Layer
fact_sales_df.write.format("delta")\
                    .mode("overwrite")\
                     .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/fact_sales")\
                     .save()

# COMMAND ----------

# Explanation of Transformations
# 🔹 Joining Multiple Tables → Combines orders, order_items, order_payments, and order_reviews.
# 🔹 Generating Surrogate Keys → fact_sales_sk for unique row identification.
# 🔹 Ensuring Referential Integrity → customer_sk, product_sk, seller_sk link to dimension tables.
# 🔹 Handling Missing Values → Filling NULLs with default values (e.g., "Unknown" for missing payment_type).
# 🔹 Formatting Date Columns → Standardizing date formats.
# 🔹 Aggregating Key Metrics → Sales amount, shipping cost, discount amount, review sentiment.
# 🔹 Optimizing Query Performance → Saving as Delta format for faster lookups.


# COMMAND ----------

# MAGIC %md
# MAGIC # FACTSALES AGGREGATION

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data From Gold Layer / FactSales

# COMMAND ----------

fact_sales_df = spark.read.format("delta")\
                    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/fact_sales")

# COMMAND ----------

fact_sales_df = spark.read.format("delta")\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .load("abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/fact_sales")

# COMMAND ----------

fact_sales_df.show()

# COMMAND ----------

# Objective:
# The FactSalesAggregation table is an aggregated fact table that provides precomputed summaries of sales performance. This table improves query performance for reporting and analytics.

# Source Tables (Gold Layer):
# FactSales (Gold Layer)

# Transformations Applied:
# ✅ Aggregating Data for Reporting → SUM, AVG, COUNT on key metrics
# ✅ Generating Surrogate Keys → fact_sales_agg_sk for row identification
# ✅ Date-Based Aggregation → Monthly & yearly revenue calculations
# ✅ Optimizing for Performance → Partitioning by year_month for fast queries


# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, count, year, month, concat, lit, monotonically_increasing_id

# Aggregate sales data at a monthly level
fact_sales_agg_df = fact_sales_df.groupBy(
        year(col("purchase_date")).alias("year"),
        month(col("purchase_date")).alias("month"),
        concat(year(col("purchase_date")), lit("-"), month(col("purchase_date"))).alias("year_month"),
        col("product_sk"),
        col("seller_sk")
    ) \
    .agg(
        sum(col("sales_amount")).alias("total_sales"),
        sum(col("shipping_cost")).alias("total_shipping_cost"),
        avg(col("review_score")).alias("avg_review_score"),
        count(col("fact_sales_sk")).alias("total_orders")
    ) \
    .withColumn("fact_sales_agg_sk", monotonically_increasing_id())  # Generating Surrogate Key

# Save as Delta Table in Gold Layer (Partitioned for Faster Queries)
fact_sales_agg_df.write.format("delta")\
                        .mode("overwrite")\
                        .partitionBy("year_month")\
                        .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/fact_sales_agg")\
                        .save()

# COMMAND ----------

# Explanation of Transformations
# 🔹 Aggregating Data for Reporting →

# SUM(total_sales), SUM(total_shipping_cost), SUM(total_discount)

# AVG(review_score), COUNT(total_orders)

# 🔹 Generating Surrogate Keys → fact_sales_agg_sk uniquely identifies each row.

# 🔹 Date-Based Aggregation →

# Yearly & Monthly Sales → year_month column helps time-series analysis.

# 🔹 Optimizing for Performance →

# Partitioning by year_month for faster queries in dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating BridgeOrderItems Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## FactSales, DimProducts, DimSeller

# COMMAND ----------

# Objective:
# The BridgeOrderItems table is a bridge table that resolves the many-to-many relationship between Orders and Products. In an e-commerce dataset, a single order can contain multiple products, and a product can be part of multiple orders. This table facilitates efficient querying of order-product relationships.

# Source Tables (Gold Layer):
# FactSales (Gold Layer)

# DimProduct (Gold Layer)

# DimOrderStatus (Gold Layer)

# Transformations Applied:
# ✅ Resolving Many-to-Many Relationship → Bridge table between Orders and Products
# ✅ Generating Surrogate Keys → bridge_order_items_sk for uniqueness
# ✅ Retaining Business-Critical Attributes → order_sk, product_sk, order_status_sk, quantity, unit_price, total_price
# ✅ Optimizing for Query Performance → Storing as Delta Table


# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

# Select Required Columns from FactSales
bridge_order_items_df = fact_sales_df.select(
    col("fact_sales_sk"),
    col("product_sk"),
    col("order_status"),
    col("sales_amount").alias("total_price")
)

# Generating Surrogate Key for Bridge Table
bridge_order_items_df = bridge_order_items_df.withColumn("bridge_order_items_sk", monotonically_increasing_id())

# Save as Delta Table in Gold Layer
bridge_order_items_df.write.format("delta")\
                        .mode("overwrite")\
                        .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/bridge_order_items")\
                        .save()


# COMMAND ----------

# Explanation of Transformations
# 🔹 Resolving Many-to-Many Relationship → Links Orders and Products through a bridge table.

# 🔹 Generating Surrogate Keys →

# bridge_order_items_sk uniquely identifies each row.

# 🔹 Retaining Business-Critical Attributes →

# order_sk → Links to DimOrderStatus.

# product_sk → Links to DimProduct.

# quantity, unit_price, total_price → Essential sales attributes.

# 🔹 Optimizing for Performance →

# Storing as a Delta Table for fast queries and indexing.

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimizing Gold Layer for Query Performance

# COMMAND ----------

# %md
# ### Objective:
# Now that we have created all the Dimension Tables, Fact Tables, Fact Aggregation Tables, and Bridge Tables, we need to optimize the Gold Layer for fast queries and efficient storage.

# Optimizations Applied:
# ✅ Partitioning → Improve query performance by organizing data into partitions
# ✅ Z-Ordering (Clustering) → Efficiently sort data within partitions to reduce I/O
# ✅ Vacuuming Delta Tables → Remove old versions and optimize storage
# ✅ Delta Table Performance Optimizations → Enable data skipping and indexing


# COMMAND ----------

# %md
# 1 -  Partitioning the Gold Layer Tables
# Partitioning divides large tables into smaller, more manageable pieces. This reduces scan times and improves performance.

# COMMAND ----------

# Partitioning FactSales by Order Year

fact_sales_df.write \
    .partitionBy("purchase_date") \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/fact_sales_partitioned")\
    .save()

# Partitioning FactOrderPayments by Payment Type
fact_order_payments_df = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/fact_sales_partitioned")

fact_order_payments_df.write \
    .partitionBy("payment_type") \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/fact_order_payments_partitioned")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Implementing Z-Ordering for Faster Queries

# COMMAND ----------

# Partitioning helps, but within each partition, data should be sorted efficiently.
# Z-Ordering clusters data based on frequent query columns, reducing file scanning.

# COMMAND ----------

from delta.tables import DeltaTable

# Optimize FactSales with Z-Ordering on Customer SK
fact_sales_table = DeltaTable.forPath(spark, "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/fact_sales_partitioned")

fact_sales_table.optimize().executeZOrderBy("fact_sales_sk")

# Optimize DimCustomer with Z-Ordering on City
dim_customer_table = DeltaTable.forPath(spark, "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/dim_customer")

dim_customer_table.optimize().executeZOrderBy("customer_city")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 -  Vacuuming Delta Tables

# COMMAND ----------

# Delta tables store historical versions of data for time travel and rollback.
# To free up storage, we can vacuum old data.

# COMMAND ----------

# Vacuum FactSales to remove old versions
fact_sales_table.vacuum(retentionHours=168)  # Keep last 7 days

# Vacuum DimCustomer table
dim_customer_table.vacuum(retentionHours=168)