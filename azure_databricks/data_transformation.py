# Databricks notebook source
spark

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff
import pandas as pd
from pymongo import MongoClient

# COMMAND ----------

# Azure ADLS Gen2 Configuration (for accessing Data Lake Storage)
storage_account = "olistetlstga"
application_id = "0d394405-7465-4b4f-8179-b27eac5c6fb4"
directory_id = "5d25a949-9d59-4659-90df-82c00195d214"

# COMMAND ----------

# Set Spark configuration to authenticate with ADLS Gen2 using OAuth 2.0
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "p9u8Q~1T9a6AZo4uBrX0fgjyQy86qVMhtM3BMcIs")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# Define path to Bronze layer (raw CSV files)
base_path = "abfss://olistdata@olistetlstga.dfs.core.windows.net/bronze/"

# COMMAND ----------

# Load raw CSV datasets from Bronze Layer into DataFrames
customers_df = spark.read.csv(base_path + "olist_customers_dataset.csv", header=True, inferSchema=True)
geolocation_df = spark.read.csv(base_path + "olist_geolocation_dataset.csv", header=True, inferSchema=True)
order_items_df = spark.read.csv(base_path + "olist_order_items_dataset.csv", header=True, inferSchema=True)
order_payments_df = spark.read.csv(base_path + "olist_order_payments_dataset.csv", header=True, inferSchema=True)
order_reviews_df = spark.read.csv(base_path + "olist_order_reviews_dataset.csv", header=True, inferSchema=True)
orders_df = spark.read.csv(base_path + "olist_orders_dataset.csv", header=True, inferSchema=True)
products_df = spark.read.csv(base_path + "olist_products_dataset.csv", header=True, inferSchema=True)
sellers_df = spark.read.csv(base_path + "olist_sellers_dataset.csv", header=True, inferSchema=True)

# COMMAND ----------

# Connect to MongoDB to fetch product category translations
hostname = "lqjf0.h.filess.io"
database = "olistDataNoSQL_basestove"
port = "27018"
username = "olistDataNoSQL_basestove"
password = "921662682985a53bb9c910827602ae2113739b9a"

# Read from MongoDB and convert to Spark DataFrame
uri = f"mongodb://{username}:{password}@{hostname}:{port}/{database}"
client = MongoClient(uri)
mongo_data = pd.DataFrame(list(client[database]["product_categories"].find()))
mongo_data.drop('_id', axis=1, inplace=True)
mongo_sparf_df = spark.createDataFrame(mongo_data)

# COMMAND ----------

display(products_df)

# COMMAND ----------

display(mongo_data)

# COMMAND ----------

mongo_data

# COMMAND ----------

display(mongo_sparf_df)

# COMMAND ----------

# Data cleaning utility: remove duplicates and null rows
def clean_dataframe(df, name):
    print("Cleaning", name)
    return df.dropDuplicates().na.drop("all")

# COMMAND ----------

# Clean and cast `orders_df`, and add derived delivery time fields
orders_df = clean_dataframe(orders_df, "orders_df") \
    .withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp"))) \
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date"))) \
    .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date"))) \
    .withColumn("actual_delivery_time", datediff("order_delivered_customer_date", "order_purchase_timestamp")) \
    .withColumn("estimated_delivery_time", datediff("order_estimated_delivery_date", "order_purchase_timestamp")) \
    .withColumn("Delay Time", col("actual_delivery_time") - col("estimated_delivery_time"))

# COMMAND ----------

# Cast numeric columns in payments
order_payments_df = order_payments_df \
    .withColumn("payment_sequential", col("payment_sequential").cast("int")) \
    .withColumn("payment_installments", col("payment_installments").cast("int")) \
    .withColumn("payment_value", col("payment_value").cast("float"))

# COMMAND ----------

# Cast item ID to integer
order_items_df = order_items_df.withColumn("order_item_id", col("order_item_id").cast("int"))

# COMMAND ----------

# Cast product dimensions to long
products_df = products_df \
    .withColumn("product_name_lenght", col("product_name_lenght").cast("long")) \
    .withColumn("product_description_lenght", col("product_description_lenght").cast("long")) \
    .withColumn("product_photos_qty", col("product_photos_qty").cast("long")) \
    .withColumn("product_weight_g", col("product_weight_g").cast("long")) \
    .withColumn("product_length_cm", col("product_length_cm").cast("long")) \
    .withColumn("product_height_cm", col("product_height_cm").cast("long")) \
    .withColumn("product_width_cm", col("product_width_cm").cast("long"))

# COMMAND ----------

# Cast review score to int
order_reviews_df = order_reviews_df.withColumn("review_score", col("review_score").cast("int"))

# COMMAND ----------

# Cast zip code to int in customers
customers_df = customers_df.withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix").cast("int"))

# COMMAND ----------

# Cast zip code and coordinates in geolocation
geolocation_df = geolocation_df \
    .withColumn("geolocation_zip_code_prefix", col("geolocation_zip_code_prefix").cast("int")) \
    .withColumn("geolocation_lat", col("geolocation_lat").cast("long")) \
    .withColumn("geolocation_lng", col("geolocation_lng").cast("long"))

# COMMAND ----------

# Cast zip code in sellers
sellers_df = sellers_df.withColumn("seller_zip_code_prefix", col("seller_zip_code_prefix").cast("int"))

# COMMAND ----------

# Enrich product data with translated product category names
products_df = products_df.join(mongo_sparf_df, "product_category_name", "left")

# COMMAND ----------

# Join all datasets to build a wide fact table
orders_customers_df = orders_df.join(customers_df, "customer_id", "left")
orders_payments_df = orders_customers_df.join(order_payments_df, "order_id", "left")
orders_items_df = orders_payments_df.join(order_items_df, "order_id", "left")
orders_items_products_df = orders_items_df.join(products_df, "product_id", "left")
final_df = orders_items_products_df.join(sellers_df, "seller_id", "left")

# Remove duplicate column names caused by joins
def remove_duplicate_columns(df):
    seen = set()
    to_drop = []
    for col_name in df.columns:
        if col_name in seen:
            to_drop.append(col_name)
        else:
            seen.add(col_name)
    return df.drop(*to_drop)

final_df = remove_duplicate_columns(final_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# Write to Silver Layer
# Save Silver Layer to ADLS (cleaned but still raw structure, mainly for internal exploration)

silver_path = "abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/"

customers_df.write.mode("overwrite").parquet(silver_path + "dim_customers")
products_df.write.mode("overwrite").parquet(silver_path + "dim_products")
sellers_df.write.mode("overwrite").parquet(silver_path + "dim_sellers")
geolocation_df.write.mode("overwrite").parquet(silver_path + "dim_geolocation")
order_reviews_df.write.mode("overwrite").parquet(silver_path + "dim_reviews")
order_payments_df.write.mode("overwrite").parquet(silver_path + "dim_payments")
order_items_df.write.mode("overwrite").parquet(silver_path + "dim_items")
orders_df.write.mode("overwrite").parquet(silver_path + "dim_orders")
final_df.write.mode("overwrite").parquet(silver_path + "fact_orders")

# COMMAND ----------

# Write to Gold Layer (for Synapse)
# Save Gold Layer to ADLS (cleaned & curated structure for analytics tools like Synapse or Power BI)

gold_path = "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/"

customers_df.write.mode("overwrite").parquet(gold_path + "customers")
products_df.write.mode("overwrite").parquet(gold_path + "products")
sellers_df.write.mode("overwrite").parquet(gold_path + "sellers")
geolocation_df.write.mode("overwrite").parquet(gold_path + "geolocation")
order_reviews_df.write.mode("overwrite").parquet(gold_path + "reviews")
order_payments_df.write.mode("overwrite").parquet(gold_path + "payments")
order_items_df.write.mode("overwrite").parquet(gold_path + "items")

final_df.write.mode("overwrite").parquet(gold_path + "fact_orders")