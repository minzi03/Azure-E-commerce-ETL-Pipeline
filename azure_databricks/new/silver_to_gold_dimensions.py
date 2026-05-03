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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, monotonically_increasing_id

# COMMAND ----------

spark = SparkSession.builder.appName("SilverToGold_Dimensions").getOrCreate()

# COMMAND ----------

df_customers = spark.read.format("delta").load(f"{silver_base}/customers")
df_geolocation = spark.read.format("delta").load(f"{silver_base}/geolocation")
df_order_items = spark.read.format("delta").load(f"{silver_base}/order_items")
df_order_payments = spark.read.format("delta").load(f"{silver_base}/order_payments")
df_order_reviews = spark.read.format("delta").load(f"{silver_base}/order_reviews")
df_orders = spark.read.format("delta").load(f"{silver_base}/orders")
df_products = spark.read.format("delta").load(f"{silver_base}/products")
df_sellers = spark.read.format("delta").load(f"{silver_base}/sellers")
df_product_category_name_translation = spark.read.format("delta").load(f"{silver_base}/product_category_name_translation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIMENSIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Customer

# COMMAND ----------

# Generate surrogate key
df_customers_dim = df_customers.withColumn("customer_sk", monotonically_increasing_id())

# Handle nulls
df_customers_dim = df_customers_dim.fillna({
    "customer_id": "Unknown",
    "customer_unique_id": "Unknown",
    "customer_zip_code": 0,
    "customer_city": "unknown",
    "customer_state": "UNKNOWN",
    "customer_full_address": "unknown | UNKNOWN | 0"
})

# Select final columns
df_customers_dim = df_customers_dim.select(
    col("customer_sk"),
    col("customer_id").alias("natural_customer_key"),
    col("customer_unique_id"),
    col("customer_zip_code"),
    col("customer_city"),
    col("customer_state"),
    col("customer_full_address")
)

# COMMAND ----------

df_customers_dim.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/dim_customer") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Product

# COMMAND ----------

# Join products with category translation from Silver
df_products_enriched = df_products.join(
    df_product_category_name_translation.select(
        "product_category_name",
        "product_category_name_english"
    ),
    on="product_category_name",
    how="left"
)

# Generate surrogate key
df_dim_product = df_products_enriched.withColumn("product_sk", monotonically_increasing_id())

# Handle nulls
df_dim_product = df_dim_product.fillna({
    "product_id": "Unknown",
    "product_category_name": "unknown",
    "product_category_name_english": "unknown",
    "product_name_length": 0,
    "product_description_length": 0,
    "product_photos_quantity": 0,
    "product_weight_grams": 0,
    "product_length_centimeter": 0,
    "product_height_centimeter": 0,
    "product_width_centimeter": 0,
    "product_volume_cm3": 0,
    "has_category": False,
    "has_photos": False,
    "size_category": "unknown"
})

# Select final columns
df_dim_product = df_dim_product.select(
    col("product_sk"),
    col("product_id").alias("natural_product_key"),
    col("product_category_name").alias("category"),
    col("product_category_name_english").alias("category_english"),
    col("product_name_length"),
    col("product_description_length"),
    col("product_photos_quantity"),
    col("product_weight_grams"),
    col("product_length_centimeter"),
    col("product_height_centimeter"),
    col("product_width_centimeter"),
    col("product_volume_cm3"),
    col("has_category"),
    col("has_photos"),
    col("size_category")
)

# COMMAND ----------

df_dim_product.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/dim_product") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Seller

# COMMAND ----------

df_dim_seller = df_sellers.withColumn("seller_sk", monotonically_increasing_id())

df_dim_seller = df_dim_seller.fillna({
    "seller_id": "Unknown",
    "seller_zip_code": 0,
    "seller_city": "unknown",
    "seller_state": "UNKNOWN",
    "seller_full_address": "unknown | UNKNOWN | 0",
    "is_city_cleaned": False
})

df_dim_seller = df_dim_seller.select(
    col("seller_sk"),
    col("seller_id").alias("natural_seller_key"),
    col("seller_zip_code"),
    col("seller_city"),
    col("seller_state"),
    col("seller_full_address"),
    col("is_city_cleaned")
)

# COMMAND ----------

df_dim_seller.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/dim_seller") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Geolocation

# COMMAND ----------

# Keep unique location grain
df_geolocation_dim_base = df_geolocation.dropDuplicates([
    "geolocation_zip_code",
    "geolocation_latitude",
    "geolocation_longitude",
    "geolocation_city",
    "geolocation_state"
])

df_dim_geolocation = df_geolocation_dim_base.withColumn("geolocation_sk", monotonically_increasing_id())

df_dim_geolocation = df_dim_geolocation.fillna({
    "geolocation_zip_code": 0,
    "geolocation_latitude": 0.0,
    "geolocation_longitude": 0.0,
    "geolocation_city": "unknown",
    "geolocation_state": "UNKNOWN",
    "geolocation_full_address": "unknown | UNKNOWN | 0",
    "is_city_cleaned": False
})

df_dim_geolocation = df_dim_geolocation.select(
    col("geolocation_sk"),
    col("geolocation_zip_code").alias("zip_code"),
    col("geolocation_latitude").alias("latitude"),
    col("geolocation_longitude").alias("longitude"),
    col("geolocation_city").alias("city"),
    col("geolocation_state").alias("state"),
    col("geolocation_full_address"),
    col("is_city_cleaned")
)

# COMMAND ----------

df_dim_geolocation.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/dim_geolocation") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Order Items

# COMMAND ----------

df_dim_order_items = df_order_items.withColumn("order_item_sk", monotonically_increasing_id())

df_dim_order_items = df_dim_order_items.fillna({
    "order_id": "Unknown",
    "order_item_id": 0,
    "product_id": "Unknown",
    "seller_id": "Unknown",
    "price": 0.0,
    "freight_value": 0.0,
    "item_total_amount": 0.0,
    "shipping_limit_year": 0,
    "shipping_limit_month": 0,
    "shipping_limit_day": 0,
    "shipping_limit_weekday": 0,
    "shipping_limit_hour": 0,
    "is_shipping_limit_timestamp_valid": False
})

df_dim_order_items = df_dim_order_items.select(
    col("order_item_sk"),
    col("order_id"),
    col("order_item_id"),
    col("product_id"),
    col("seller_id"),
    col("shipping_limit_timestamp"),
    col("shipping_limit_date"),
    col("price"),
    col("freight_value"),
    col("item_total_amount"),
    col("shipping_limit_year"),
    col("shipping_limit_month"),
    col("shipping_limit_day"),
    col("shipping_limit_weekday"),
    col("shipping_limit_hour"),
    col("is_shipping_limit_timestamp_valid")
)

# COMMAND ----------

df_dim_order_items.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/dim_order_items") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Order Payments

# COMMAND ----------

df_dim_order_payments = df_order_payments.withColumn("payment_sk", monotonically_increasing_id())

df_dim_order_payments = df_dim_order_payments.fillna({
    "order_id": "Unknown",
    "payment_sequential": 0,
    "payment_type": "unknown",
    "payment_installments": 0,
    "payment_value": 0.0,
    "payment_group": "other",
    "is_installment_payment": False,
    "is_high_value_payment": False
})

df_dim_order_payments = df_dim_order_payments.select(
    col("payment_sk"),
    col("order_id"),
    col("payment_sequential"),
    col("payment_type"),
    col("payment_installments"),
    col("payment_value"),
    col("payment_group"),
    col("is_installment_payment"),
    col("is_high_value_payment")
)

# COMMAND ----------

df_dim_order_payments.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/dim_order_payments") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Order Reviews

# COMMAND ----------

df_dim_order_reviews = df_order_reviews.withColumn("review_sk", monotonically_increasing_id())

df_dim_order_reviews = df_dim_order_reviews.fillna({
    "review_id": "Unknown",
    "order_id": "Unknown",
    "review_score": 0,
    "review_comment_title": "No Title",
    "review_comment_message": "No Comment",
    "review_comment_message_raw": "No Comment",
    "response_time_days": 0,
    "review_year": 0,
    "review_month": 0,
    "review_day": 0,
    "review_sentiment": "negative",
    "has_comment": False
})

df_dim_order_reviews = df_dim_order_reviews.select(
    col("review_sk"),
    col("review_id"),
    col("order_id"),
    col("mongo_id"),
    col("review_score"),
    col("review_comment_title"),
    col("review_comment_message"),
    col("review_comment_message_raw"),
    col("review_creation_ts"),
    col("review_answer_ts"),
    col("review_year"),
    col("review_month"),
    col("review_day"),
    col("response_time_days"),
    col("review_sentiment"),
    col("has_comment")
)

# COMMAND ----------

df_dim_order_reviews.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/dim_order_reviews") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Orders

# COMMAND ----------

df_dim_orders = df_orders.withColumn("order_sk", monotonically_increasing_id())

df_dim_orders = df_dim_orders.fillna({
    "order_id": "Unknown",
    "customer_id": "Unknown",
    "order_status": "unknown",
    "delivery_days": 0,
    "approval_days": 0,
    "shipping_days": 0,
    "is_delayed": False,
    "order_status_group": "other",
    "purchase_year": 0,
    "purchase_month": 0,
    "purchase_day": 0
})

df_dim_orders = df_dim_orders.select(
    col("order_sk"),
    col("order_id").alias("natural_order_key"),
    col("customer_id"),
    col("order_status"),
    col("order_status_group"),
    col("order_purchase_ts"),
    col("order_approved_ts"),
    col("order_delivered_carrier_ts"),
    col("order_delivered_customer_ts"),
    col("order_estimated_delivery_ts"),
    col("order_purchase_date"),
    col("order_approved_date"),
    col("order_delivered_carrier_date"),
    col("order_delivered_customer_date"),
    col("order_estimated_delivery_date"),
    col("purchase_year"),
    col("purchase_month"),
    col("purchase_day"),
    col("delivery_days"),
    col("approval_days"),
    col("shipping_days"),
    col("is_delayed")
)

# COMMAND ----------

df_dim_orders.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{gold_base}/dim_orders") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

gold_tables = [
    "dim_customer",
    "dim_product",
    "dim_seller",
    "dim_geolocation",
    "dim_order_items",
    "dim_order_payments",
    "dim_order_reviews",
    "dim_orders"
]

for table_name in gold_tables:
    df_tmp = spark.read.format("delta").load(f"{gold_base}/{table_name}")
    print(f"{table_name}: {df_tmp.count()} rows")