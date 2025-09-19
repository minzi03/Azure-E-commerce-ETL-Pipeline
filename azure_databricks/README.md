# 🔥 Azure Databricks - ETL & Data Transformation

## 📌 Overview
This directory contains the **ETL notebooks and scripts** used to transform the Olist E-Commerce dataset across the **Medallion Architecture** (Bronze → Silver → Gold).  

Azure Databricks with **PySpark** provides the scalable compute layer for cleansing, enrichment, and modeling data stored in **Azure Data Lake Gen2 (Delta Lake)**.

---

## 🗂 Directory Structure

```

azure\_databricks/
│── Bronze-To-Silver.py              # Cleansing & standardization
│── data\_transformation.py           # Reusable PySpark transformations
│── Silver-To-Gold(Dimensions).py    # Build Dimension tables
│── Silver-To-Gold(Facts).py         # Build Fact tables
│── Silver-To-Gold(Facts).ipynb      # Notebook version (interactive)
│── Silver-To-Gold(Facts).dbc        # Exported Databricks notebook (importable)

````

---

## 🏗 Transformation Flow

### 1. Bronze → Silver
- Enforce schema  
- Handle nulls and missing values  
- Deduplicate records  
- Standardize date/time & numeric formats  
- Store cleansed Delta tables in `silver/`  

---

### 2. Silver → Gold
- Create **Dimension Tables** (dim_customer, dim_product, dim_seller, etc.)  
- Create **Fact Tables** (fact_sales, fact_order_payments_partitioned, fact_sales_agg)  
- Generate **Bridge Tables** for many-to-many relationships  
- Apply **partitioning, Z-Ordering, and Delta vacuuming** for performance tuning  

---

## 🖥 Example: Bronze → Silver Transformation

```python
from pyspark.sql import functions as F

# Load Bronze data
bronze_df = spark.read.format("delta").load("/mnt/datalake/bronze/orders")

# Clean & deduplicate
silver_df = (
    bronze_df
    .dropDuplicates(["order_id"])
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_status", F.upper(F.col("order_status")))
)

# Write to Silver
silver_df.write.format("delta").mode("overwrite").save("/mnt/datalake/silver/orders")
````

---

## 🖥 Example: Silver → Gold Fact Table

```python
# Join orders, customers, and payments to build fact_sales
fact_sales = (
    silver_orders.alias("o")
    .join(silver_customers.alias("c"), "customer_id")
    .join(silver_payments.alias("p"), "order_id")
    .select(
        "o.order_id",
        "c.customer_id",
        "o.order_purchase_timestamp",
        "p.payment_type",
        "p.payment_value"
    )
)

# Partition by purchase_date
fact_sales = fact_sales.withColumn("purchase_date", F.to_date("order_purchase_timestamp"))

fact_sales.write.format("delta").mode("overwrite").partitionBy("purchase_date").save(
    "/mnt/datalake/gold/fact_sales_partitioned"
)
```

---

## ⚡ Key Features

* **PySpark-based transformations** for scalability
* **Delta Lake** for reliability (ACID transactions, schema evolution, time travel)
* **Partitioned facts** (by `year_month`, `payment_type`, `purchase_date`) for query optimization
* **Reusable scripts** and modular notebooks

---

## 📊 Usage

* Import `.dbc` or `.ipynb` files into **Databricks Workspace**.
* Attach to cluster with Delta support enabled.
* Run sequentially: **Bronze-To-Silver → Silver-To-Gold**.
* Monitor runs via **Databricks Jobs** for automation.

---

## 📚 References

* [Azure Databricks Documentation](https://learn.microsoft.com/en-us/azure/databricks/)
* [Delta Lake Guide](https://learn.microsoft.com/en-us/azure/databricks/delta/)
* [Medallion Architecture](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)
