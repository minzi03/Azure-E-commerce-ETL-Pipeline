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

df_customers = spark.read.format("csv").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/bronze/olist_customers_dataset.csv")
df_geolocation = spark.read.format("csv").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/bronze/olist_geolocation_dataset.csv")
df_order_items = spark.read.format("csv").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/bronze/olist_order_items_dataset.csv")
df_order_payments = spark.read.format("csv").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/bronze/olist_order_payments_dataset.csv")
df_order_reviews = spark.read.format("csv").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/bronze/olist_order_reviews_dataset.csv")
df_products = spark.read.format("csv").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/bronze/olist_products_dataset.csv")
df_sellers = spark.read.format("csv").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/bronze/olist_sellers_dataset.csv")

# COMMAND ----------

df_orders = spark.read.format("csv").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/bronze//olist_orders_dataset.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DISPLAY TABLES FROM BRONZE LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 : Read Customers Table

# COMMAND ----------

display(df_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1 : Renaming Column Names for Better Readability

# COMMAND ----------

df_customers = df_customers.withColumnRenamed("_c0", "customer_unique_id")\
                            .withColumnRenamed("_c1", "customer_id")\
                            .withColumnRenamed("_c2", "customer_zip_code")\
                            .withColumnRenamed("_c3", "customer_city")\
                            .withColumnRenamed("_c4", "customer_state")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2 : Remove first row from all columns in one operation

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number, lit

# Remove first row from all columns in one operation
first_row = df_customers.limit(1)
df_customers = df_customers.exceptAll(first_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 3 : Data Type Casting

# COMMAND ----------

df_customers = df_customers.withColumn("customer_unique_id",col("customer_unique_id").cast(StringType()))\
                            .withColumn("customer_id",col("customer_id").cast(StringType()))\
                            .withColumn("customer_zip_code",col("customer_zip_code").cast(IntegerType()))            

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 4 : Handle Missing and Null Values

# COMMAND ----------

df_customers = df_customers.fillna({"customer_zip_code": 0, 
                                    "customer_city": "Unknown",
                                    "customer_state": "Unknown",
                                    "customer_unique_id": "Unknown",
                                    "customer_id": "Unknown"
                                    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 5 : Remove Special Characters from Text Fields

# COMMAND ----------

df_customers = df_customers.withColumn("customer_unique_id", regexp_replace(col("customer_unique_id"), "[^a-zA-Z0-9 ]", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 6 : Combining Fields for Business Understanding

# COMMAND ----------

# Merge customer_city, customer_state, customer_zip_code into a single column called customer_full_address

from pyspark.sql.functions import concat_ws, col

# Merge address columns with proper formatting
df_customers = df_customers.withColumn("customer_full_address",concat_ws(" ", 
        col("customer_city"),
        col("customer_state"), 
        col("customer_zip_code")
    )
)

display(df_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOAD : Store the Cleaned CUSTOMER Data in the Silver Layer (Delta Format)

# COMMAND ----------

df_customers.write.format("delta")\
               .mode("overwrite")\
                .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/customers")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 : Read Geolocation Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1 : Rename Columns for Better Readability

# COMMAND ----------

df_geolocation = df_geolocation.withColumnRenamed("_c0", "geolocation_zip_code")\
                               .withColumnRenamed("_c1", "geolocation_latitude")\
                               .withColumnRenamed("_c2", "geolocation_longitude")\
                               .withColumnRenamed("_c3", "geolocation_city")\
                               .withColumnRenamed("_c4", "geolocation_state")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2 : Remove first row from all columns in one operation

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number, lit

# Remove first row from all columns in one operation
first_row = df_geolocation.limit(1)
df_geolocation = df_geolocation.exceptAll(first_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 3 : Handle Missing and Null Values

# COMMAND ----------

df_geolocation = df_geolocation.fillna(
    {
        "geolocation_zip_code": 0,
        "geolocation_latitude": 0,
        "geolocation_longitude": 0,
        "geolocation_city": "Unknown",
        "geolocation_state": "Unknown"
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 4 : Data Type Casting

# COMMAND ----------

df_geolocation = df_geolocation.withColumn("geolocation_zip_code",col("geolocation_zip_code").cast(IntegerType()))\
                               .withColumn("geolocation_latitude",col("geolocation_latitude").cast(DoubleType()))\
                               .withColumn("geolocation_longitude",col("geolocation_longitude").cast(DoubleType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 5 :  Combining Fields for Business Understanding

# COMMAND ----------

df_geolocation = df_geolocation.withColumn("geolocation_full_address",concat_ws(" ", 
        col("geolocation_city"),
        col("geolocation_state"),
        col("geolocation_zip_code")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOAD : Store the Cleaned GEOLOCATION Data in the Silver Layer (Delta Format)

# COMMAND ----------

df_geolocation.write.format("delta")\
                    .mode("overwrite")\
                    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/geolocation")\
                    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 : Read Order Items Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1 : Rename Columns for Better Readability
# MAGIC

# COMMAND ----------

df_order_items = df_order_items.withColumnRenamed("_c0", "order_id")\
                                .withColumnRenamed("_c1", "order_item_id")\
                                .withColumnRenamed("_c2", "product_id")\
                                .withColumnRenamed("_c3", "seller_id")\
                                .withColumnRenamed("_c4", "shipping_limit_date")\
                                .withColumnRenamed("_c5", "price")\
                                .withColumnRenamed("_c6", "freight_value")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2 : Remove first row from all columns in one operation

# COMMAND ----------

# Remove first row from all columns in one operation
first_row = df_order_items.limit(1)
df_order_items = df_order_items.exceptAll(first_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 3 : Data Type Casting

# COMMAND ----------

df_order_items = df_order_items.withColumn("order_item_id",col("order_item_id").cast(IntegerType()))\
                                .withColumn("price",col("price").cast(DoubleType()))\
                                .withColumn("freight_value",col("freight_value").cast(DoubleType()))\
                                .withColumn("shipping_limit_date",col("shipping_limit_date").cast(DateType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 4 : Handle Missing and Null Values

# COMMAND ----------

df_order_items = df_order_items.fillna({
    "order_id": "Unknown",
    "order_item_id": 0,
    "product_id": "Unknown",
    "seller_id": "Unknown",
    "shipping_limit_date": "Unknown",
    "price": 0,
    "freight_value": 0
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 5 : yyyy-MM-DD Date Format

# COMMAND ----------

df_order_items = df_order_items.withColumn("shipping_limit_date",to_date("shipping_limit_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction 6 : Extracting Date Components

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, dayofweek

df_order_items = df_order_items.withColumn("order_year", year(col("shipping_limit_date"))) \
                     .withColumn("order_month", month(col("shipping_limit_date"))) \
                     .withColumn("order_day", dayofmonth(col("shipping_limit_date"))) \
                     .withColumn("order_weekday", dayofweek(col("shipping_limit_date")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 7 : Remove Special Characters from Text Fields

# COMMAND ----------

df_order_items = df_order_items.withColumn("order_id", regexp_replace(col("order_id"), "[^a-zA-Z0-9 ]", ""))\
                                .withColumn("product_id", regexp_replace(col("product_id"), "[^a-zA-Z0-9 ]", ""))\
                                .withColumn("seller_id", regexp_replace(col("seller_id"), "[^a-zA-Z0-9 ]", ""))


df_order_items = df_order_items.withColumn("order_id", regexp_replace(trim(col("order_id")),
                      r'[,\t\n>$\*]{2,}',  # Remove repeated special chars
                      ', '  # Replace with single comma+space
                  ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOAD  : Store the Cleaned ORDER ITEMS Data in the Silver Layer (Delta Format)

# COMMAND ----------

df_order_items.write.format("delta")\
                    .mode("overwrite")\
                    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/order_items")\
                    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 : Read Order Payments Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1 : Rename Columns for Better Readability

# COMMAND ----------

df_order_payments = df_order_payments.withColumnRenamed("_c0", "order_id")\
                                      .withColumnRenamed("_c1", "payment_sequential")\
                                      .withColumnRenamed("_c2", "payment_type")\
                                        .withColumnRenamed("_c3", "payment_installments")\
                                        .withColumnRenamed("_c4", "payment_value")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2 : Remove first row from all columns in one operation

# COMMAND ----------

# Remove first row from all columns in one operation
first_row = df_order_payments.limit(1)
df_order_payments = df_order_payments.exceptAll(first_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 3 :  Data Type Casting

# COMMAND ----------

df_order_payments = df_order_payments.withColumn("payment_sequential",col("payment_sequential").cast(IntegerType()))\
                                     .withColumn("payment_installments",col("payment_installments").cast(IntegerType()))\
                                      .withColumn("payment_value",col("payment_value").cast(DoubleType()))   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 4 :  Handle Missing and Null Values

# COMMAND ----------

df_order_payments = df_order_payments.fillna(
    {
        "order_id": "Unknown",
        "payment_sequential": 0,
        "payment_type": "Unknown",
        "payment_installments": 0,
        "payment_value": 0
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 5 : Remove Special Characters from Text Fields

# COMMAND ----------

df_order_payments = df_order_payments.withColumn("order_id", regexp_replace(col("order_id"), "[^a-zA-Z0-9 ]", ""))\
                            .withColumn("payment_type", regexp_replace(col("payment_type"), "[^a-zA-Z0-9 ]", ""))

display(df_order_payments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD : Store the Cleaned ORDER PAYMENTS Data in the Silver Layer (Delta Format)

# COMMAND ----------

df_order_payments.write.format("delta")\
                     .mode("overwrite")\
                     .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/order_payments")\
                     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 : Read Order Reviews Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1 :  Rename Columns for Better Readability

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumnRenamed("_c0", "review_id")\
                                     .withColumnRenamed("_c1", "order_id")\
                                     .withColumnRenamed("_c2", "review_score")\
                                     .withColumnRenamed("_c3", "review_comment_title")\
                                    .withColumnRenamed("_c4", "review_comment_message")\
                                    .withColumnRenamed("_c5", "review_creation_date")\
                                    .withColumnRenamed("_c6", "review_answer_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2 : Remove first row from all columns in one operation

# COMMAND ----------

# Remove first row from all columns in one operation
first_row = df_order_reviews.limit(1)
df_order_reviews = df_order_reviews.exceptAll(first_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 3 : Data Type Casting and Date Format

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn("review_score",col("review_score").cast(IntegerType()))\
                                    .withColumn("review_creation_date",to_date(col("review_creation_date")))\
                                     .withColumn("review_answer_timestamp",to_date(col("review_answer_timestamp")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 4 : Handle Missing and Null Values

# COMMAND ----------

df_order_reviews = df_order_reviews.fillna(
    {
        "review_id": "Unknown",
        "order_id": "Unknown",
        "review_score": 0,
        "review_comment_title": "Unknown",
        "review_comment_message": "Unknown",
        "review_creation_date": "Unknown",
        "review_answer_timestamp": "Unknown"
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 5 : Extracting Date Components
# MAGIC

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn("order_review_year", year(col("review_creation_date")))\
                                    .withColumn("order_review_month", month(col("review_creation_date")))\
                                    .withColumn("order_review_day", dayofmonth(col("review_creation_date")))\
                                    .withColumn("review_answer_year", year(col("review_answer_timestamp")))\
                                    .withColumn("review_answer_month", month(col("review_answer_timestamp")))\
                                    .withColumn("review_answer_day", dayofmonth(col("review_answer_timestamp")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 6 : Remove Special Characters from Text Fields

# COMMAND ----------

df_order_reviews = df_order_reviews.withColumn("review_id", regexp_replace(col("review_id"), "[^a-zA-Z0-9 ]", ""))\
                                    .withColumn("order_id", regexp_replace(col("order_id"), "[^a-zA-Z0-9 ]", ""))\
                                    .withColumn("review_comment_title", regexp_replace(col("review_comment_title"), "[^a-zA-Z0-9 ]", ""))\
                                    .withColumn("review_comment_message", regexp_replace(col("review_comment_message"), "[^a-zA-Z0-9 ]", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD : Store the Cleaned ORDER REVIEWS Data in the Silver Layer (Delta Format)

# COMMAND ----------

df_order_reviews.write.format("delta")\
                     .mode("overwrite")\
                     .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/order_reviews")\
                     .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Orders Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1 : Rename Columns for Better Readability

# COMMAND ----------

df_orders = df_orders.withColumnRenamed("_c0", "order_id")\
                     .withColumnRenamed("_c1", "customer_id")\
                     .withColumnRenamed("_c2", "order_status")\
                     .withColumnRenamed("_c3", "order_purchase_date")\
                     .withColumnRenamed("_c4", "order_approved_date")\
                     .withColumnRenamed("_c5", "order_delivered_carrier_date")\
                     .withColumnRenamed("_c6", "order_delivered_customer_date")\
                     .withColumnRenamed("_c7", "order_estimated_delivery_date")

# COMMAND ----------

from pyspark.sql.functions import col

df_orders = df_orders.withColumnRenamed("order_purchase_timestamp", "order_purchase_date")\
                        .withColumnRenamed("order_approved_at", "order_approved_date")\
                        .withColumnRenamed("order_delivered_carrier_date", "order_delivered_carrier_date")\
                        .withColumnRenamed("order_delivered_customer_date", "order_delivered_customer_date")\
                        .withColumnRenamed("order_estimated_delivery_date", "order_estimated_delivery_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2 : Remove first row from all columns in one operation
# MAGIC

# COMMAND ----------

# Remove first row from all columns in one operation
first_row = df_orders.limit(1)
df_orders = df_orders.exceptAll(first_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Transformation 3 : Data Type Casting

# COMMAND ----------

df_orders = df_orders.withColumn("order_purchase_date", to_date(col("order_purchase_date")))\
                        .withColumn("order_approved_date", to_date(col("order_approved_date")))\
                        .withColumn("order_delivered_carrier_date", to_date(col("order_delivered_carrier_date")))\
                        .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))\
                        .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 4 : Extracting Date Components

# COMMAND ----------

df_orders = df_orders.withColumn("order_purchase_year", year(col("order_purchase_date")))\
                        .withColumn("order_purchase_month", month(col("order_purchase_date")))\
                        .withColumn("order_purchase_day", dayofmonth(col("order_purchase_date")))\
                        .withColumn("order_approved_year", year(col("order_approved_date")))\
                        .withColumn("order_approved_month", month(col("order_approved_date")))\
                        .withColumn("order_approved_day", dayofmonth(col("order_approved_date")))\
                        .withColumn("order_delivered_carrier_year", year(col("order_delivered_carrier_date")))\
                        .withColumn("order_delivered_carrier_month", month(col("order_delivered_carrier_date")))\
                        .withColumn("order_delivered_carrier_day", dayofmonth(col("order_delivered_carrier_date")))\
                        .withColumn("order_delivered_customer_year", year(col("order_delivered_customer_date")))\
                        .withColumn("order_delivered_customer_month", month(col("order_delivered_customer_date")))\
                        .withColumn("order_delivered_customer_day", dayofmonth(col("order_delivered_customer_date")))\
                        .withColumn("order_estimated_delivery_year", year(col("order_estimated_delivery_date")))\
                        .withColumn("order_estimated_delivery_month", month(col("order_estimated_delivery_date")))\
                        .withColumn("order_estimated_delivery_day", dayofmonth(col("order_estimated_delivery_date")))\

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 5 : Handle Missing and Null Values

# COMMAND ----------

df_orders = df_orders.fillna(
    {
        "order_id": "Unknown",
        "customer_id": "Unknown",
        "order_status": "Unknown",
        "order_purchase_date": "Unknown",
        "order_approved_date": "Unknown",
        "order_delivered_carrier_date": "Unknown",
        "order_delivered_customer_date": "Unknown",
        "order_estimated_delivery_date": "Unknown",
        "order_purchase_year": "Unknown",
        "order_purchase_month": "Unknown",
        "order_purchase_day": "Unknown",
        "order_approved_year": "Unknown",
        "order_approved_month": "Unknown",
        "order_approved_day": "Unknown",
        "order_delivered_carrier_year": "Unknown",
        "order_delivered_carrier_month": "Unknown",
        "order_delivered_carrier_day": "Unknown",
        "order_delivered_customer_year": "Unknown",
        "order_delivered_customer_month": "Unknown",
        "order_delivered_customer_day": "Unknown",
        "order_estimated_delivery_year": "Unknown",
        "order_estimated_delivery_month": "Unknown",
        "order_estimated_delivery_day": "Unknown",

})

# COMMAND ----------

display(df_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD : Store the Cleaned ORDERS Data in the Silver Layer (Delta Format)

# COMMAND ----------

df_orders.write.format("delta")\
            .mode("overwrite")\
            .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/orders")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Products Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1 : Rename Columns for Better Readability

# COMMAND ----------

df_products = df_products.withColumnRenamed("_c0", "product_id")\
                            .withColumnRenamed("_c1", "product_category_name")\
                            .withColumnRenamed("_c2", "product_name_length")\
                            .withColumnRenamed("_c3", "product_description")\
                            .withColumnRenamed("_c4", "product_photos_quantity")\
                            .withColumnRenamed("_c5", "product_weight_grams")\
                            .withColumnRenamed("_c6", "product_length_centimeter")\
                            .withColumnRenamed("_c7", "product_height_centimeter")\
                            .withColumnRenamed("_c8", "product_width_centimeter")\
                            .withColumnRenamed("product_name", "product_name_length")\
                            .withColumnRenamed("product_description", "product_description_length")

# Remove first row from all columns in one operation
first_row = df_products.limit(1)
df_products = df_products.exceptAll(first_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2 : Data Type Casting

# COMMAND ----------

df_products = df_products.withColumn("product_name_length", col("product_name_length").cast(IntegerType()))\
                      .withColumn("product_description_length", col("product_description_length").cast(IntegerType()))\
                      .withColumn("product_photos_quantity", col("product_photos_quantity").cast(IntegerType()))\
                      .withColumn("product_weight_grams", col("product_weight_grams").cast(IntegerType()))\
                      .withColumn("product_length_centimeter", col("product_length_centimeter").cast(IntegerType()))\
                      .withColumn("product_height_centimeter", col("product_height_centimeter").cast(IntegerType()))\
                      .withColumn("product_width_centimeter", col("product_width_centimeter").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 3 : Handle Missing and Null Values

# COMMAND ----------

df_products = df_products.fillna(
{
    "product_id": "Unknown",
    "product_category_name": "Unknown",
    "product_name_length": 0,
    "product_description_length": 0,
    "product_photos_quantity": 0,
    "product_weight_grams": 0,
    "product_length_centimeter": 0,
    "product_height_centimeter": 0,
    "product_width_centimeter": 0

})

display(df_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD : Store the Cleaned Products Data in the Silver Layer (Delta Format)

# COMMAND ----------

df_products.write.format("delta")\
            .mode("overwrite")\
            .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Sellers Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1 :  Rename Columns for Better Readability
# MAGIC

# COMMAND ----------

df_sellers = df_sellers.withColumnRenamed("_c0", "seller_id")\
                        .withColumnRenamed("_c1", "seller_zip_code")\
                        .withColumnRenamed("_c2", "seller_city")\
                        .withColumnRenamed("_c3", "seller_state")

# Remove first row from all columns in one operation
first_row = df_sellers.limit(1)
df_sellers = df_sellers.exceptAll(first_row)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2 : Data Type Casting

# COMMAND ----------

df_sellers = df_sellers.withColumn("seller_zip_code", col("seller_zip_code").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 3 : Handle Missing and Null Values

# COMMAND ----------

df_sellers = df_sellers.fillna(
{
    "seller_id": "Unknown",
    "seller_zip_code": 0,
    "seller_city": "Unknown",
    "seller_state": "Unknown"
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD : Store the Cleaned SELLERS Data in the Silver Layer (Delta Format)

# COMMAND ----------

df_sellers.write.format("delta")\
                .mode("overwrite")\
                .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/sellers")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Product Category Name Translation Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 1 : Rename Columns for Better Readability

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.withColumnRenamed("_c0", "product_category_name")\
                                                                       .withColumnRenamed("_c1", "product_category_name_english")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 2 : Remove first row from all columns in one operation

# COMMAND ----------

# Remove first row from all columns in one operation
first_row = df_product_category_name_translation.limit(1)
df_product_category_name_translation = df_product_category_name_translation.exceptAll(first_row)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 3 : Handle Missing and Null Values

# COMMAND ----------

df_product_category_name_translation = df_product_category_name_translation.fillna(
 {
     "product_category_name": "Unknown",
     "product_category_name_english": "Unknown"
 })   


# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD : Store the Cleaned PRODUCT CATEGORY NAME TRANSLATION Data in the Silver Layer (Delta Format)

# COMMAND ----------

df_product_category_name_translation.write.format("delta")\
            .mode("overwrite")\
            .option("path", "abfss://silver@olistdatalake0.dfs.core.windows.net/product_category_name_translation")\
            .save()