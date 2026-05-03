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

dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, regexp_replace, concat_ws, when, lit,
    coalesce, expr, to_date, year, month, dayofmonth, dayofweek,
    hour, datediff, round, length
)
from pyspark.sql.types import IntegerType, DoubleType, StringType

# COMMAND ----------

spark = SparkSession.builder.appName("BronzeToSilver_Olist").getOrCreate()

# COMMAND ----------

df_customers = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"{bronze_base}/mysql/customers")

df_orders = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"{bronze_base}/mysql/orders")

df_order_items = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"{bronze_base}/mysql/order_items")

df_order_payments = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"{bronze_base}/mysql/order_payments")

df_sellers = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"{bronze_base}/mysql/sellers")

df_products = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"{bronze_base}/mysql/products")

# COMMAND ----------

df_order_reviews = spark.read.format("json") \
    .load(f"{bronze_base}/mongodb/order_reviews")

# COMMAND ----------

df_product_category_name_translation = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"{bronze_base}/api/product_category_name_translation")

# COMMAND ----------

df_geolocation = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"{bronze_base}/local_files/geolocation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers: Bronze → Silver

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

df_customers = df_customers.select(
    col("customer_id").cast("string").alias("customer_id"),
    col("customer_unique_id").cast("string").alias("customer_unique_id"),
    col("customer_zip_code_prefix").cast("int").alias("customer_zip_code"),
    col("customer_city").cast("string").alias("customer_city"),
    col("customer_state").cast("string").alias("customer_state")
)

# COMMAND ----------

df_customers = df_customers.withColumn(
    "customer_unique_id",
    regexp_replace(trim(col("customer_unique_id")), "[^a-zA-Z0-9]", "")
)

# COMMAND ----------

df_customers = df_customers.withColumn("customer_city", lower(trim(col("customer_city"))))
df_customers = df_customers.withColumn("customer_state", upper(trim(col("customer_state"))))

# COMMAND ----------

df_customers = df_customers.fillna({
    "customer_id": "Unknown",
    "customer_unique_id": "Unknown",
    "customer_zip_code": 0,
    "customer_city": "unknown",
    "customer_state": "UNKNOWN"
})

# COMMAND ----------

df_customers = df_customers.withColumn(
    "customer_full_address",
    concat_ws(" | ", col("customer_city"), col("customer_state"), col("customer_zip_code"))
)

# COMMAND ----------

df_customers = df_customers.dropDuplicates(["customer_id"])

# COMMAND ----------

df_products.printSchema()

# COMMAND ----------

df_customers.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{silver_base}/customers") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geolocation: Bronze → Silver

# COMMAND ----------

df_geolocation.printSchema()

# COMMAND ----------

df_geolocation = df_geolocation.select(
    col("geolocation_zip_code_prefix").cast("int").alias("geolocation_zip_code"),
    col("geolocation_lat").cast("double").alias("geolocation_latitude"),
    col("geolocation_lng").cast("double").alias("geolocation_longitude"),
    col("geolocation_city").cast("string").alias("geolocation_city"),
    col("geolocation_state").cast("string").alias("geolocation_state")
)

# COMMAND ----------

df_geolocation = df_geolocation.withColumn("geolocation_city_raw", col("geolocation_city"))

# COMMAND ----------

df_geolocation = df_geolocation.withColumn("geolocation_city", lower(trim(col("geolocation_city"))))
df_geolocation = df_geolocation.withColumn("geolocation_state", upper(trim(col("geolocation_state"))))

# COMMAND ----------

df_geolocation = df_geolocation.withColumn("geolocation_city", regexp_replace(col("geolocation_city"), "sĂ£o", "sao"))
df_geolocation = df_geolocation.withColumn("geolocation_city", regexp_replace(col("geolocation_city"), "uruaĂ§u", "uruacu"))
df_geolocation = df_geolocation.withColumn("geolocation_city", regexp_replace(col("geolocation_city"), "Ă§", "c"))
df_geolocation = df_geolocation.withColumn("geolocation_city", regexp_replace(col("geolocation_city"), "Ă£", "a"))
df_geolocation = df_geolocation.withColumn("geolocation_city", regexp_replace(col("geolocation_city"), "Ă¡", "a"))
df_geolocation = df_geolocation.withColumn("geolocation_city", regexp_replace(col("geolocation_city"), "Ă©", "e"))
df_geolocation = df_geolocation.withColumn("geolocation_city", regexp_replace(col("geolocation_city"), "Ă­", "i"))
df_geolocation = df_geolocation.withColumn("geolocation_city", regexp_replace(col("geolocation_city"), "Ă³", "o"))
df_geolocation = df_geolocation.withColumn("geolocation_city", regexp_replace(col("geolocation_city"), "Ăº", "u"))

# COMMAND ----------

df_geolocation = df_geolocation.withColumn(
    "is_city_cleaned",
    when(col("geolocation_city_raw") != col("geolocation_city"), lit(True)).otherwise(lit(False))
)

# COMMAND ----------

df_geolocation = df_geolocation.fillna({
    "geolocation_zip_code": 0,
    "geolocation_latitude": 0.0,
    "geolocation_longitude": 0.0,
    "geolocation_city": "unknown",
    "geolocation_city_raw": "unknown",
    "geolocation_state": "UNKNOWN"
})

# COMMAND ----------

df_geolocation = df_geolocation.withColumn(
    "geolocation_full_address",
    concat_ws(" | ", col("geolocation_city"), col("geolocation_state"), col("geolocation_zip_code"))
)

# COMMAND ----------

df_geolocation = df_geolocation.filter(
    (col("geolocation_latitude").between(-90, 90)) &
    (col("geolocation_longitude").between(-180, 180))
)

# COMMAND ----------

df_geolocation = df_geolocation.dropDuplicates([
    "geolocation_zip_code",
    "geolocation_latitude",
    "geolocation_longitude",
    "geolocation_city",
    "geolocation_state"
])

# COMMAND ----------

df_geolocation.printSchema()

# COMMAND ----------

df_geolocation.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{silver_base}/geolocation") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Items: Bronze → Silver

# COMMAND ----------

df_order_items.printSchema()

# COMMAND ----------

df_order_items = df_order_items.select(
    col("order_id").cast("string").alias("order_id"),
    col("order_item_id").cast("int").alias("order_item_id"),
    col("product_id").cast("string").alias("product_id"),
    col("seller_id").cast("string").alias("seller_id"),
    col("shipping_limit_date").cast("string").alias("shipping_limit_date_raw"),
    col("price").cast("double").alias("price"),
    col("freight_value").cast("double").alias("freight_value")
)

# COMMAND ----------

df_order_items = df_order_items.withColumn(
    "order_id",
    regexp_replace(trim(col("order_id")), "[^a-zA-Z0-9]", "")
)
df_order_items = df_order_items.withColumn(
    "product_id",
    regexp_replace(trim(col("product_id")), "[^a-zA-Z0-9]", "")
)
df_order_items = df_order_items.withColumn(
    "seller_id",
    regexp_replace(trim(col("seller_id")), "[^a-zA-Z0-9]", "")
)

# COMMAND ----------

df_order_items = df_order_items.withColumn(
    "shipping_limit_timestamp",
    coalesce(
        expr("try_to_timestamp(shipping_limit_date_raw, 'dd/MM/yyyy H:mm')"),
        expr("try_to_timestamp(shipping_limit_date_raw, 'dd/MM/yyyy HH:mm')"),
        expr("try_to_timestamp(shipping_limit_date_raw, 'yyyy-MM-dd HH:mm:ss')"),
        expr("try_to_timestamp(shipping_limit_date_raw)")
    )
)

# COMMAND ----------

df_order_items = df_order_items.fillna({
    "order_id": "Unknown",
    "order_item_id": 0,
    "product_id": "Unknown",
    "seller_id": "Unknown",
    "price": 0.0,
    "freight_value": 0.0
})

# COMMAND ----------

df_order_items = df_order_items.withColumn("shipping_limit_date", to_date(col("shipping_limit_timestamp")))
df_order_items = df_order_items.withColumn("shipping_limit_year", year(col("shipping_limit_timestamp")))
df_order_items = df_order_items.withColumn("shipping_limit_month", month(col("shipping_limit_timestamp")))
df_order_items = df_order_items.withColumn("shipping_limit_day", dayofmonth(col("shipping_limit_timestamp")))
df_order_items = df_order_items.withColumn("shipping_limit_weekday", dayofweek(col("shipping_limit_timestamp")))
df_order_items = df_order_items.withColumn("shipping_limit_hour", hour(col("shipping_limit_timestamp")))

# COMMAND ----------

df_order_items = df_order_items.withColumn(
    "item_total_amount",
    round(col("price") + col("freight_value"), 2)
)

# COMMAND ----------

df_order_items = df_order_items.withColumn(
    "is_shipping_limit_timestamp_valid",
    when(col("shipping_limit_timestamp").isNotNull(), lit(True)).otherwise(lit(False))
)

# COMMAND ----------

df_order_items = df_order_items.dropDuplicates([
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id"
])

# COMMAND ----------

df_order_items.printSchema()

# COMMAND ----------

df_order_items.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{silver_base}/order_items") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Payments: Bronze → Silver

# COMMAND ----------

df_order_payments.printSchema()

# COMMAND ----------

df_order_payments = df_order_payments.select(
    col("order_id").cast("string").alias("order_id"),
    col("payment_sequential").cast("int").alias("payment_sequential"),
    col("payment_type").cast("string").alias("payment_type"),
    col("payment_installments").cast("int").alias("payment_installments"),
    col("payment_value").cast("double").alias("payment_value")
)

# COMMAND ----------

df_order_payments = df_order_payments.withColumn(
    "order_id",
    regexp_replace(trim(col("order_id")), "[^a-zA-Z0-9]", "")
)
df_order_payments = df_order_payments.withColumn(
    "payment_type",
    lower(trim(col("payment_type")))
)

# COMMAND ----------

df_order_payments = df_order_payments.fillna({
    "order_id": "Unknown",
    "payment_sequential": 0,
    "payment_type": "unknown",
    "payment_installments": 0,
    "payment_value": 0.0
})

# COMMAND ----------

df_order_payments = df_order_payments.withColumn(
    "payment_group",
    when(col("payment_type") == "credit_card", "card")
    .when(col("payment_type") == "debit_card", "card")
    .when(col("payment_type") == "voucher", "voucher")
    .when(col("payment_type") == "boleto", "boleto")
    .otherwise("other")
)

# COMMAND ----------

df_order_payments = df_order_payments.withColumn(
    "is_installment_payment",
    when(col("payment_installments") > 1, lit(True)).otherwise(lit(False))
)

# COMMAND ----------

df_order_payments = df_order_payments.withColumn(
    "is_high_value_payment",
    when(col("payment_value") >= 500, lit(True)).otherwise(lit(False))
)

# COMMAND ----------

df_order_payments = df_order_payments.dropDuplicates([
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value"
])

# COMMAND ----------

df_order_payments.printSchema()

# COMMAND ----------

df_order_payments.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{silver_base}/order_payments") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Reviews: Bronze → Silver

# COMMAND ----------

df_order_reviews.printSchema()

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn(
    "mongo_id",
    col("_id").getField("$oid")
).drop("_id")

# COMMAND ----------

df_order_reviews = df_order_reviews.select(
    col("mongo_id"),
    col("review_id"),
    col("order_id"),
    col("review_score"),
    col("review_comment_title"),
    col("review_comment_message"),
    col("review_creation_date"),
    col("review_answer_timestamp")
)

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn(
    "review_comment_message_raw",
    col("review_comment_message")
)

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn(
    "review_comment_message",
    regexp_replace(trim(col("review_comment_message")), r"[\r\n\t]", " ")
)

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn(
    "review_comment_title",
    regexp_replace(trim(col("review_comment_title")), r"[\r\n\t]", " ")
)

# COMMAND ----------

df_order_reviews = df_order_reviews.fillna({
    "review_id": "Unknown",
    "order_id": "Unknown",
    "review_score": 0,
    "review_comment_title": "No Title",
    "review_comment_message": "No Comment",
    "review_comment_message_raw": "No Comment"
})

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn(
    "review_creation_ts",
    expr("try_to_timestamp(review_creation_date)")
)
df_order_reviews = df_order_reviews.withColumn(
    "review_answer_ts",
    expr("try_to_timestamp(review_answer_timestamp)")
)

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn("review_year", year(col("review_creation_ts")))
df_order_reviews = df_order_reviews.withColumn("review_month", month(col("review_creation_ts")))
df_order_reviews = df_order_reviews.withColumn("review_day", dayofmonth(col("review_creation_ts")))
df_order_reviews = df_order_reviews.withColumn(
    "response_time_days",
    datediff(col("review_answer_ts"), col("review_creation_ts"))
)

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn(
    "review_sentiment",
    when(col("review_score") >= 4, "positive")
    .when(col("review_score") == 3, "neutral")
    .otherwise("negative")
)

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn(
    "has_comment",
    when(col("review_comment_message") != "No Comment", True).otherwise(False)
)

# COMMAND ----------

df_order_reviews = df_order_reviews.dropDuplicates(["review_id"])

# COMMAND ----------

df_order_reviews.printSchema()

# COMMAND ----------

df_order_reviews.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{silver_base}/order_reviews") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders: Bronze → Silver

# COMMAND ----------

df_orders.printSchema()

# COMMAND ----------

df_orders = df_orders.select(
    col("order_id").cast("string").alias("order_id"),
    col("customer_id").cast("string").alias("customer_id"),
    col("order_status").cast("string").alias("order_status"),
    col("order_purchase_timestamp").cast("string").alias("order_purchase_timestamp_raw"),
    col("order_approved_at").cast("string").alias("order_approved_at_raw"),
    col("order_delivered_carrier_date").cast("string").alias("order_delivered_carrier_date_raw"),
    col("order_delivered_customer_date").cast("string").alias("order_delivered_customer_date_raw"),
    col("order_estimated_delivery_date").cast("string").alias("order_estimated_delivery_date_raw")
)

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_id",
    regexp_replace(trim(col("order_id")), "[^a-zA-Z0-9]", "")
)
df_orders = df_orders.withColumn(
    "customer_id",
    regexp_replace(trim(col("customer_id")), "[^a-zA-Z0-9]", "")
)
df_orders = df_orders.withColumn(
    "order_status",
    lower(trim(col("order_status")))
)

# COMMAND ----------

df_orders = df_orders.fillna({
    "order_id": "Unknown",
    "customer_id": "Unknown",
    "order_status": "unknown"
})

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_purchase_ts",
    coalesce(
        expr("try_to_timestamp(order_purchase_timestamp_raw, 'dd/MM/yyyy H:mm')"),
        expr("try_to_timestamp(order_purchase_timestamp_raw, 'dd/MM/yyyy HH:mm')"),
        expr("try_to_timestamp(order_purchase_timestamp_raw, 'yyyy-MM-dd HH:mm:ss')"),
        expr("try_to_timestamp(order_purchase_timestamp_raw)")
    )
)

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_approved_ts",
    coalesce(
        expr("try_to_timestamp(order_approved_at_raw, 'dd/MM/yyyy H:mm')"),
        expr("try_to_timestamp(order_approved_at_raw, 'dd/MM/yyyy HH:mm')"),
        expr("try_to_timestamp(order_approved_at_raw, 'yyyy-MM-dd HH:mm:ss')"),
        expr("try_to_timestamp(order_approved_at_raw)")
    )
)

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_delivered_carrier_ts",
    coalesce(
        expr("try_to_timestamp(order_delivered_carrier_date_raw, 'dd/MM/yyyy H:mm')"),
        expr("try_to_timestamp(order_delivered_carrier_date_raw, 'dd/MM/yyyy HH:mm')"),
        expr("try_to_timestamp(order_delivered_carrier_date_raw, 'yyyy-MM-dd HH:mm:ss')"),
        expr("try_to_timestamp(order_delivered_carrier_date_raw)")
    )
)

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_delivered_customer_ts",
    coalesce(
        expr("try_to_timestamp(order_delivered_customer_date_raw, 'dd/MM/yyyy H:mm')"),
        expr("try_to_timestamp(order_delivered_customer_date_raw, 'dd/MM/yyyy HH:mm')"),
        expr("try_to_timestamp(order_delivered_customer_date_raw, 'yyyy-MM-dd HH:mm:ss')"),
        expr("try_to_timestamp(order_delivered_customer_date_raw)")
    )
)

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_estimated_delivery_ts",
    coalesce(
        expr("try_to_timestamp(order_estimated_delivery_date_raw, 'dd/MM/yyyy H:mm')"),
        expr("try_to_timestamp(order_estimated_delivery_date_raw, 'dd/MM/yyyy HH:mm')"),
        expr("try_to_timestamp(order_estimated_delivery_date_raw, 'yyyy-MM-dd HH:mm:ss')"),
        expr("try_to_timestamp(order_estimated_delivery_date_raw)")
    )
)

# COMMAND ----------

df_orders = df_orders.withColumn("order_purchase_date", to_date(col("order_purchase_ts")))
df_orders = df_orders.withColumn("order_approved_date", to_date(col("order_approved_ts")))
df_orders = df_orders.withColumn("order_delivered_carrier_date", to_date(col("order_delivered_carrier_ts")))
df_orders = df_orders.withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_ts")))
df_orders = df_orders.withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_ts")))

# COMMAND ----------

df_orders = df_orders.withColumn("purchase_year", year(col("order_purchase_ts")))
df_orders = df_orders.withColumn("purchase_month", month(col("order_purchase_ts")))
df_orders = df_orders.withColumn("purchase_day", dayofmonth(col("order_purchase_ts")))

# COMMAND ----------

df_orders = df_orders.withColumn(
    "delivery_days",
    datediff(col("order_delivered_customer_date"), col("order_purchase_date"))
)
df_orders = df_orders.withColumn(
    "approval_days",
    datediff(col("order_approved_date"), col("order_purchase_date"))
)
df_orders = df_orders.withColumn(
    "shipping_days",
    datediff(col("order_delivered_carrier_date"), col("order_approved_date"))
)

# COMMAND ----------

df_orders = df_orders.withColumn(
    "is_delayed",
    when(
        col("order_delivered_customer_ts").isNotNull() &
        col("order_estimated_delivery_ts").isNotNull() &
        (col("order_delivered_customer_ts") > col("order_estimated_delivery_ts")),
        lit(True)
    ).otherwise(lit(False))
)

# COMMAND ----------

df_orders = df_orders.withColumn(
    "order_status_group",
    when(col("order_status") == "delivered", "completed")
    .when(col("order_status").isin("shipped", "processing", "approved", "invoiced"), "in_progress")
    .when(col("order_status").isin("canceled", "unavailable"), "failed")
    .otherwise("other")
)

# COMMAND ----------

df_orders = df_orders.dropDuplicates(["order_id"])

# COMMAND ----------

df_orders.printSchema()

# COMMAND ----------

df_orders.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{silver_base}/orders") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products: Bronze → Silver

# COMMAND ----------

df_products.printSchema()

# COMMAND ----------

df_products = df_products.select(
    col("product_id").cast("string").alias("product_id"),
    col("product_category_name").cast("string").alias("product_category_name"),
    col("product_name_lenght").cast("int").alias("product_name_length"),
    col("product_description_lenght").cast("int").alias("product_description_length"),
    col("product_photos_qty").cast("int").alias("product_photos_quantity"),
    col("product_weight_g").cast("int").alias("product_weight_grams"),
    col("product_length_cm").cast("int").alias("product_length_centimeter"),
    col("product_height_cm").cast("int").alias("product_height_centimeter"),
    col("product_width_cm").cast("int").alias("product_width_centimeter")
)

# COMMAND ----------

df_products = df_products.withColumn(
    "product_category_name_raw",
    col("product_category_name")
)

# COMMAND ----------

df_products = df_products.withColumn(
    "product_category_name",
    lower(trim(col("product_category_name")))
)

# COMMAND ----------

df_products = df_products.withColumn(
    "product_category_name",
    regexp_replace(col("product_category_name"), "[^a-zA-Z0-9_ ]", "")
)

# COMMAND ----------

df_products = df_products.fillna({
    "product_id": "Unknown",
    "product_category_name": "unknown",
    "product_category_name_raw": "unknown",
    "product_name_length": 0,
    "product_description_length": 0,
    "product_photos_quantity": 0,
    "product_weight_grams": 0,
    "product_length_centimeter": 0,
    "product_height_centimeter": 0,
    "product_width_centimeter": 0
})

# COMMAND ----------

# Filter chặt hơn để loại dòng malformed
df_products = df_products.filter(
    col("product_id").rlike("^[a-f0-9]{32}$")
)

# COMMAND ----------

df_products = df_products.withColumn(
    "product_volume_cm3",
    col("product_length_centimeter") *
    col("product_height_centimeter") *
    col("product_width_centimeter")
)

# COMMAND ----------

df_products = df_products.withColumn(
    "has_category",
    when(col("product_category_name") != "unknown", lit(True)).otherwise(lit(False))
)

# COMMAND ----------

df_products = df_products.withColumn(
    "has_photos",
    when(col("product_photos_quantity") > 0, lit(True)).otherwise(lit(False))
)

# COMMAND ----------

df_products = df_products.withColumn(
    "size_category",
    when(col("product_volume_cm3") < 1000, "small")
    .when(col("product_volume_cm3") < 5000, "medium")
    .otherwise("large")
)

# COMMAND ----------

df_products = df_products.dropDuplicates(["product_id"])

# COMMAND ----------

df_products.printSchema()

# COMMAND ----------

df_products.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{silver_base}/products") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sellers: Bronze → Silver

# COMMAND ----------

df_sellers.printSchema()

# COMMAND ----------

df_sellers = df_sellers.select(
    col("seller_id").cast("string").alias("seller_id"),
    col("seller_zip_code_prefix").cast("int").alias("seller_zip_code"),
    col("seller_city").cast("string").alias("seller_city"),
    col("seller_state").cast("string").alias("seller_state")
)

# COMMAND ----------

df_sellers = df_sellers.withColumn("seller_city_raw", col("seller_city"))

# COMMAND ----------

df_sellers = df_sellers.withColumn("seller_city", lower(trim(col("seller_city"))))
df_sellers = df_sellers.withColumn("seller_state", upper(trim(col("seller_state"))))
df_sellers = df_sellers.withColumn(
    "seller_city",
    regexp_replace(col("seller_city"), "[^a-zA-Z0-9' ]", "")
)

# COMMAND ----------

df_sellers = df_sellers.fillna({
    "seller_id": "Unknown",
    "seller_zip_code": 0,
    "seller_city": "unknown",
    "seller_city_raw": "unknown",
    "seller_state": "UNKNOWN"
})

# COMMAND ----------

df_sellers = df_sellers.withColumn(
    "seller_full_address",
    concat_ws(" | ", col("seller_city"), col("seller_state"), col("seller_zip_code"))
)

# COMMAND ----------

df_sellers = df_sellers.withColumn(
    "is_city_cleaned",
    when(col("seller_city_raw") != col("seller_city"), lit(True)).otherwise(lit(False))
)

# COMMAND ----------

df_sellers = df_sellers.dropDuplicates(["seller_id"])

# COMMAND ----------

df_sellers.printSchema()

# COMMAND ----------

df_sellers.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{silver_base}/sellers") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Category Translation: Bronze → Silver

# COMMAND ----------

df_product_category_name_translation.printSchema()

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.select(
    col("product_category_name").cast("string").alias("product_category_name"),
    col("product_category_name_english").cast("string").alias("product_category_name_english")
)

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation \
    .withColumn("product_category_name_raw", col("product_category_name")) \
    .withColumn("product_category_name_english_raw", col("product_category_name_english"))

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.withColumn(
    "product_category_name",
    lower(trim(col("product_category_name")))
)

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.withColumn(
    "product_category_name_english",
    lower(trim(col("product_category_name_english")))
)

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.withColumn(
    "product_category_name",
    regexp_replace(col("product_category_name"), "[^a-zA-Z0-9_ ]", "")
)

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.withColumn(
    "product_category_name_english",
    regexp_replace(col("product_category_name_english"), "[^a-zA-Z0-9_ ]", "")
)

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.fillna({
    "product_category_name": "unknown",
    "product_category_name_english": "unknown",
    "product_category_name_raw": "unknown",
    "product_category_name_english_raw": "unknown"
})

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.withColumn(
    "is_category_cleaned",
    when(col("product_category_name_raw") != col("product_category_name"), lit(True)).otherwise(lit(False))
)

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.withColumn(
    "is_category_english_cleaned",
    when(
        col("product_category_name_english_raw") != col("product_category_name_english"),
        lit(True)
    ).otherwise(lit(False))
)

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.dropDuplicates([
    "product_category_name"
])

# COMMAND ----------

df_product_category_name_translation.printSchema()

# COMMAND ----------

df_product_category_name_translation.write.format("delta") \
    .mode("overwrite") \
    .option("path", f"{silver_base}/product_category_name_translation") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Silver Layer

# COMMAND ----------

silver_tables = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "product_category_name_translation"
]

for table_name in silver_tables:
    df_tmp = spark.read.format("delta").load(f"{silver_base}/{table_name}")
    print(f"{table_name}: {df_tmp.count()} rows")