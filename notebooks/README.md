# 📘 Notebooks — Data Ingestion Examples

The **`notebooks/`** directory contains examples of ingesting data from multiple sources (MongoDB, MySQL, HTTP/Filess.io) into **Azure Data Lake Storage Gen2 (ADLS Gen2)**.  
These notebooks serve as prototypes for connecting to source systems and validating data ingestion workflows before fully integrating them into **Azure Data Factory** and **Azure Databricks** pipelines.

---

## 📂 Notebook Files

### 1. `DataIngestion_MongoDB.ipynb`  
Ingest data from **MongoDB Atlas / On-premise** into ADLS Gen2.  

**Key Steps:**
1. Connect to MongoDB using PyMongo or Spark MongoDB Connector.  
2. Read collections such as `orders`, `customers`, and `products`.  
3. Write the raw data into the **Bronze Layer** in Parquet/Delta format.  

![MongoDB Ingestion](assets/olist_db/mongodb.png)

---

### 2. `DataIngestion_MySQL.ipynb`  
Ingest transactional data from a **MySQL database**.  

**Key Steps:**
1. Configure JDBC connection to MySQL.  
2. Extract data from tables like `orders`, `order_items`, and `order_payments`.  
3. Load the extracted data into ADLS **Bronze Layer** for downstream processing.  

![MySQL Ingestion](assets/olist_db/mysql.png)

---

### 3. `DataIngestionToDB.ipynb` / `dataingestiontodb.py`  
Ingest dataset from an **HTTP endpoint (Filess.io / GitHub)**.  

**Key Steps:**
1. Read CSV/JSON data from public endpoints (Filess.io, GitHub raw links).  
2. Create temporary staging tables or datasets.  
3. Store the ingested data into ADLS Gen2 for further pipeline processing.  

![Filess.io Overview](assets/olist_db/filess-io_overview.png)

---

## 🛠️ Technologies Used
- **PySpark** (Databricks / Local execution)  
- **JDBC Connectors** (MySQL, MongoDB)  
- **ADLS Gen2** (Delta/Parquet storage)  
- **Filess.io / GitHub** (Public dataset hosting)

---

## 🎯 Outcomes
These notebooks demonstrate how to ingest data from **heterogeneous sources**:
- **NoSQL (MongoDB)**  
- **Relational DB (MySQL)**  
- **HTTP-based datasets (Filess.io / GitHub)**  

👉 All datasets are stored in the **Bronze Layer**, enabling a **multi-source, extensible pipeline** that can be scaled and orchestrated within Azure.
