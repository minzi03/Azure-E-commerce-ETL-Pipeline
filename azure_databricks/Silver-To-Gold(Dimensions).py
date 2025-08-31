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
# MAGIC # DIMENSIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimCustomers Table

# COMMAND ----------

# Objective:
# The DimCustomer table stores customer-related attributes. This table will be used for joining with fact tables like FactSales.

# Transformations Applied:
# ✅ Generating Surrogate Keys → Unique customer_sk for efficient joins
# ✅ Joining Multiple Tables → Enriching customer data
# ✅ Handling Missing Values → Replacing NULLs with 'Unknown'
# ✅ Formatting Date Columns → Converting to standard formats
# ✅ Optimizing Query Performance → Storing as Delta Table

# COMMAND ----------

from pyspark.sql.functions import col, lit, monotonically_increasing_id

# Generate Surrogate Key
df_customers = df_customers.withColumn("customer_sk", monotonically_increasing_id())

# Handle Missing Values
df_customers = df_customers.fillna(
    {
        'customer_unique_id': 'Unknown', 
        'customer_id': 'Unknown',
        'customer_zip_code': 'Unknown',
        'customer_city': 'Unknown', 
        'customer_state': 'Unknown',
        'customer_full_address': 'Unknown'
        
    })

# Select Required Columns
df_customers_dim = df_customers.select(
    col("customer_sk"),
    col("customer_id").alias("natural_customer_key"),  # Natural Key
    col("customer_unique_id"),
    col("customer_zip_code"),
    col("customer_city"),
    col("customer_state"),
    col("customer_full_address")
)

# Save as Delta Table in Gold Layer
df_customers_dim.write.format("delta")\
                .mode("overwrite")\
                .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/dim_customer")\
                .save()

# COMMAND ----------

# Explanation of Transformations
# 🔹 monotonically_increasing_id() → Generates a unique surrogate key (customer_sk).
# 🔹 fillna() → Replaces NULL values with 'Unknown' to prevent data loss.
# 🔹 Column Selection & Renaming → Keeps relevant columns and renames customer_id for clarity.
# 🔹 Saving as Delta Format → Ensures efficient query performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Read Products and Product Category Name Translation Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimProduct Table

# COMMAND ----------

# Objective:
# The DimProduct table stores product-related attributes such as product category, weight, dimensions, and name translation. It will be used for joining with fact tables like FactSales.

# Transformations Applied:
# ✅ Generating Surrogate Keys → Unique product_sk for better joins
# ✅ Joining Multiple Tables → Enriching product data with category names
# ✅ Handling Missing Values → Replacing NULLs with 'Unknown'
# ✅ Data Formatting & Standardization → Ensuring consistent formats
# ✅ Optimizing Query Performance → Storing as Delta Table

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id
from pymongo import MongoClient
import pandas as pd

# Step 1: Enrich Product Categories from MongoDB
hostname = "lqjf0.h.filess.io"
database = "olistDataNoSQL_basestove"
port = "27018"
username = "olistDataNoSQL_basestove"
password = "921662682985a53bb9c910827602ae2113739b9a"

uri = f"mongodb://{username}:{password}@{hostname}:{port}/{database}"

client = MongoClient(uri)
mongo_collection = client[database]['product_categories']

# Load MongoDB collection to Pandas and drop '_id'
mongo_data = pd.DataFrame(list(mongo_collection.find()))
mongo_data.drop('_id', axis=1, inplace=True)

# Convert to Spark DataFrame
df_product_category_translation = spark.createDataFrame(mongo_data)

# Step 2: Load Products from Silver Layer
df_products = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/products")

# Step 3: Generate Surrogate Key
df_products = df_products.withColumn("product_sk", monotonically_increasing_id())

# Step 4: Handle Missing Values
df_products = df_products.fillna({
    'product_id': 'Unknown',
    'product_category_name': 'Unknown',
    'product_name_length': 0,
    'product_description_length': 0,
    'product_photos_quantity': 0,
    'product_weight_grams': 0,
    'product_length_centimeter': 0,
    'product_height_centimeter': 0,
    'product_width_centimeter': 0      
})

# Step 5: Join with Translated Product Categories (from MongoDB)
df_dim_product = df_products.join(
    df_product_category_translation,
    on="product_category_name",
    how="left"
)

# Step 6: Select Final Columns for Dimension Table
df_dim_product = df_dim_product.select(
    col("product_sk"),
    col("product_id").alias("natural_product_key"),
    col("product_category_name").alias("category"),
    col("product_category_name_english").alias("category_english"),
    col("product_weight_grams"),
    col("product_length_centimeter"),
    col("product_height_centimeter"),
    col("product_width_centimeter")
)

# Step 7: Write to Gold Layer (Delta Format)
df_dim_product.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/dim_product")\
    .save()


# COMMAND ----------

# Explanation of Transformations
# 🔹 monotonically_increasing_id() → Creates a surrogate key (product_sk) for better joins.
# 🔹 fillna() → Replaces NULL values for missing weight, dimensions, and categories.
# 🔹 join() with product_category_df → Adds English product category names.
# 🔹 Column Selection & Renaming → Standardizes column names for clarity.
# 🔹 Saving as Delta Format → Ensures fast queries and optimization.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Read Sellers Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimSellers Table

# COMMAND ----------

# Objective:
# The DimSeller table will store seller-related attributes such as seller location (ZIP), seller unique ID, and additional information. It will be used for joining with fact tables like FactSales.

# Transformations Applied:
# ✅ Generating Surrogate Keys → Unique seller_sk for efficient joins
# ✅ Handling Missing Values → Filling NULLs with 'Unknown'
# ✅ Renaming & Standardizing Columns → Ensuring consistency
# ✅ Optimizing Query Performance → Storing as Delta Table

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

# Generate Surrogate Key
df_sellers = df_sellers.withColumn("seller_sk", monotonically_increasing_id())

# Handle Missing Values
df_sellers = df_sellers.fillna({
    'seller_zip_code': 'Unknown',
    'seller_city': 'Unknown',
    'seller_id': 'Unknown',
    'seller_state': 'Unknown'
})

# Select Required Columns & Rename for Clarity
df_dim_seller = df_sellers.select(
    col("seller_sk"),
    col("seller_id").alias("natural_seller_key"),  # Natural Key
    col("seller_zip_code").alias("seller_zip_code"),
    col("seller_city").alias("seller_city"),
    col("seller_state").alias("seller_state")
)

# Save as Delta Table in Gold Layer
df_dim_seller.write.format("delta")\
                    .mode("overwrite")\
                    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/dim_seller")\
                    .save()

# COMMAND ----------

# Explanation of Transformations
# 🔹 monotonically_increasing_id() → Generates a surrogate key (seller_sk).
# 🔹 fillna() → Fills NULL values for missing ZIP codes.
# 🔹 Renaming Columns → Standardizes naming for business clarity.
# 🔹 Saving as Delta Format → Enables efficient storage & querying.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 - Read GeoLocation Table

# COMMAND ----------

df_geolocation = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/geolocation")

display(df_geolocation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DimGeolocation Table

# COMMAND ----------

# Objective:
# The DimGeolocation table will store geographical details related to customer and seller locations. This dimension table helps in analyzing sales patterns region-wise.

# Transformations Applied:
# ✅ Generating Surrogate Keys → Unique geolocation_sk for efficient joins
# ✅ Removing Duplicates → Ensuring each location is unique
# ✅ Renaming & Standardizing Columns → Consistent and business-friendly names
# ✅ Handling Missing Values → Replacing NULL values
# ✅ Optimizing Query Performance → Saving as Delta format

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

# Remove Duplicates (Ensuring unique geolocation entries)
df_geolocation = df_geolocation.dropDuplicates(["geolocation_latitude", "geolocation_longitude"])

# Generate Surrogate Key
df_geolocation = df_geolocation.withColumn("geolocation_sk", monotonically_increasing_id())

# Handle Missing Values
df_geolocation = df_geolocation.fillna(
{
    'geolocation_city': 'Unknown',
    'geolocation_state': 'Unknown',
    'geolocation_zip_code': 'Unknown',
    'geolocation_latitude': 'Unknown',
    'geolocation_longitude': 'Unknown',
    'geolocation_full_address': 'Unknown'

})

# Select Required Columns & Rename for Clarity
df_dim_geolocation = df_geolocation.select(
    col("geolocation_sk"),
    col("geolocation_zip_code").alias("zip_code"),
    col("geolocation_latitude").alias("latitude"),
    col("geolocation_longitude").alias("longitude"),
    col("geolocation_city").alias("city"),
    col("geolocation_state").alias("state")
)

# Save as Delta Table in Gold Layer
df_dim_geolocation.write.format("delta")\
                     .mode("overwrite")\
                     .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/dim_geolocation")\
                     .save()


# COMMAND ----------

# Explanation of Transformations
# 🔹 Removing Duplicates → Ensures that each location is stored only once.
# 🔹 Generating Surrogate Keys → geolocation_sk replaces natural keys for faster lookups.
# 🔹 Handling NULL Values → If city or state is missing, we set it to "Unknown".
# 🔹 Renaming Columns → Making names clear and standardized.
# 🔹 Saving as Delta Table → Efficient for querying and analysis.

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id, year, month, dayofmonth, dayofweek, to_date
from pyspark.sql.types import IntegerType, DoubleType, DateType

# Step 1: Load from Silver
df_order_items = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/order_items")

# Step 2: Generate Surrogate Key
df_order_items = df_order_items.withColumn("order_item_sk", monotonically_increasing_id())

# Step 3: Handle Missing Values
df_order_items = df_order_items.fillna({
    "order_id": "Unknown",
    "order_item_id": 0,
    "product_id": "Unknown",
    "seller_id": "Unknown",
    "price": 0.0,
    "freight_value": 0.0
})

# Step 4: Select Required Columns for Dimension
df_dim_order_items = df_order_items.select(
    col("order_item_sk"),
    col("order_id"),
    col("order_item_id"),
    col("product_id"),
    col("seller_id"),
    col("shipping_limit_date"),
    col("price"),
    col("freight_value"),
    col("order_year"),
    col("order_month"),
    col("order_day"),
    col("order_weekday")
)

# Step 5: Write to Gold Layer as Delta
df_dim_order_items.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/dim_order_items")\
    .save()

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.types import IntegerType, DoubleType

# Step 1: Load cleaned order_payments from Silver
df_order_payments = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/order_payments")

# Step 2: Generate Surrogate Key
df_order_payments = df_order_payments.withColumn("payment_sk", monotonically_increasing_id())

# Step 3: Handle Missing Values (if needed)
df_order_payments = df_order_payments.fillna({
    "order_id": "Unknown",
    "payment_sequential": 0,
    "payment_type": "Unknown",
    "payment_installments": 0,
    "payment_value": 0.0
})

# Step 4: Select Required Columns for Dimension Table
df_dim_order_payments = df_order_payments.select(
    col("payment_sk"),
    col("order_id"),
    col("payment_sequential"),
    col("payment_type"),
    col("payment_installments"),
    col("payment_value")
)

# Step 5: Write to Gold Layer
df_dim_order_payments.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/dim_order_payments")\
    .save()


# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.types import IntegerType, DateType

# Step 1: Load from Silver Layer
df_order_reviews = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/order_reviews")

# Step 2: Generate Surrogate Key
df_order_reviews = df_order_reviews.withColumn("review_sk", monotonically_increasing_id())

# Step 3: Handle Missing Values
df_order_reviews = df_order_reviews.fillna({
    "review_id": "Unknown",
    "order_id": "Unknown",
    "review_score": 0,
    "review_comment_title": "Unknown",
    "review_comment_message": "Unknown"
})

# Step 4: Select Required Columns for Dimension Table
df_dim_order_reviews = df_order_reviews.select(
    col("review_sk"),
    col("review_id"),
    col("order_id"),
    col("review_score"),
    col("review_comment_title"),
    col("review_comment_message"),
    col("review_creation_date"),
    col("review_answer_timestamp"),
    col("order_review_year"),
    col("order_review_month"),
    col("order_review_day"),
    col("review_answer_year"),
    col("review_answer_month"),
    col("review_answer_day")
)

# Step 5: Write to Gold Layer
df_dim_order_reviews.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/dim_order_reviews")\
    .save()


# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id, to_date
from pyspark.sql.types import DateType

# Step 1: Load orders from Silver Layer
df_orders = spark.read.format("delta").load("abfss://olistdata@olistetlstga.dfs.core.windows.net/silver/orders")

# Step 2: Generate Surrogate Key
df_orders = df_orders.withColumn("order_sk", monotonically_increasing_id())

# Step 3: Handle NULL values
df_orders = df_orders.fillna({
    "order_id": "Unknown",
    "customer_id": "Unknown",
    "order_status": "Unknown"
})

# Step 4: Select relevant columns for dimension
df_dim_orders = df_orders.select(
    col("order_sk"),
    col("order_id").alias("natural_order_key"),
    col("customer_id"),
    col("order_status"),
    col("order_purchase_date"),
    col("order_approved_date"),
    col("order_delivered_carrier_date"),
    col("order_delivered_customer_date"),
    col("order_estimated_delivery_date"),
    col("order_purchase_year"),
    col("order_purchase_month"),
    col("order_purchase_day"),
    col("order_approved_year"),
    col("order_approved_month"),
    col("order_approved_day"),
    col("order_delivered_carrier_year"),
    col("order_delivered_carrier_month"),
    col("order_delivered_carrier_day"),
    col("order_delivered_customer_year"),
    col("order_delivered_customer_month"),
    col("order_delivered_customer_day"),
    col("order_estimated_delivery_year"),
    col("order_estimated_delivery_month"),
    col("order_estimated_delivery_day")
)

# Step 5: Write to Gold Layer
df_dim_orders.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://olistdata@olistetlstga.dfs.core.windows.net/gold/dim_orders")\
    .save()
