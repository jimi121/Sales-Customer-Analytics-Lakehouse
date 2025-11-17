# 📊 Sales & Customer Analytics Lakehouse

**End-to-end analytics pipeline built with Databricks, dbt, and Power BI**
*Using the Medallion Architecture and fully automated with Databricks Jobs*

---

# 1. 📘 Project Overview

This project simulates how a real company might bring together customer data from its CRM system and sales data from its ERP system.
Both generate important information, but because the data lives in separate places and comes in different formats, reporting becomes time-consuming and inconsistent.

To solve this, I built a modern analytics pipeline that:

* ingests raw CSV files with **Databricks Auto Loader**,
* applies transformations and modeling using **dbt**,
* structures the warehouse into **Bronze → Silver → Gold layers**,
* enforces data quality with **tests and macros**,
* and visualizes results in **Power BI dashboards**.

Everything runs automatically through a **Databricks Job Pipeline** so the data updates on its own.

---

# 2. 🧩 Problem Statement

The fictional business faced the following challenges:

### ❗ Disconnected systems

Customer data lived in the CRM while orders, products, and revenue lived in the ERP.
This made reporting slow and prone to errors.

### ❗ Manual data processing

Whenever new files arrived, analysts had to manually clean and merge the data before building dashboards.

### ❗ No single source of truth

Different teams worked from different versions of the data, leading to inconsistent KPIs.

---

# 3. 🎯 Project Objectives

I designed this project to:

1. Build a clean, automated analytics pipeline end-to-end
2. Ingest raw data continuously with Auto Loader
3. Land data in a structured **Medallion Architecture**
4. Use dbt for modeling, testing, macros, and documentation
5. Apply incremental models for scalable performance
6. Use Power BI to create actionable sales and customer dashboards
7. Orchestrate everything with a single scheduled Databricks job

---

# 4. 🏗️ Architecture

Below is the full workflow from ingestion to reporting.

👉 **Insert Architecture Diagram Here**
*(Recommended diagram: Source Folder → Auto Loader → raw_data Volumes → dbt Bronze → dbt Silver → dbt Gold → Power BI)*

---

# 5. 🛠️ Tools & Technologies

| Area                | Tools Used                                 |
| ------------------- | ------------------------------------------ |
| Ingestion           | Databricks Auto Loader                     |
| Storage             | Delta Lake, Volumes                        |
| Transformation      | dbt (models, tests, macros, documentation) |
| Pipeline Scheduling | Databricks Job Pipeline                    |
| Modeling            | Star Schema                                |
| Visualization       | Power BI                                   |
| Version Control     | GitHub                                     |

---

# 6. 🔁 Detailed Workflow

## 6.1 📥 Ingestion with Databricks Auto Loader

I uploaded CRM & ERP CSV files into a **source folder** inside Databricks Volumes.

Auto Loader was used because it handles:

* new files automatically
* schema inference
* schema evolution
* checkpointing
* incremental updates

Each dataset lands under:

```
/Volumes/<catalog>/<schema>/raw_data/<source>/<table>/data/
```

👉 **Add Screenshot: Raw Data Volume Structure**

---

## 6.2 🧱 Bronze Layer (dbt)

Bronze models load the raw Delta files directly into dbt.

Here I focused on:

* renaming columns into a consistent format
* adding ingestion metadata (`ingestion_timestamp`, `file_path`)
* keeping the data as close to the raw source as possible

Because some datasets can have new files added over time, **Bronze models use incremental logic** in dbt to avoid reprocessing the entire dataset.

👉 **Add Code Snippet: Bronze Incremental Model**

---

## 6.3 🧼 Silver Layer (dbt Cleaning)

Silver is where real cleaning happens.

Transformations included:

* fixing inconsistent date formats
* converting numeric columns
* trimming/standardizing strings
* removing duplicates
* handling nulls
* resolving mismatched country names
* unifying customer IDs from CRM & ERP

I implemented **dbt tests** here, including:

* `unique`
* `not_null`
* `relationships`
* custom tests using macros

👉 **Add Example: dbt Test YAML**
👉 **Add Example: Macro for Data Standardization**

Silver tables serve as the "single source of truth" for the business.

---

## 6.4 ⭐ Gold Layer (Star Schema)

The Gold layer focuses on reporting and analytics.
I designed a **Star Schema** with:

### 📌 Dimensions

* `dim_customer`
* `dim_product`
* `dim_location`
* `dim_date`

### 📌 Fact Tables

* `fact_sales`
* `fact_orders`

Facts contain numeric metrics; dimensions contain descriptive attributes.

👉 **Insert Diagram: Star Schema Model**

These Gold tables feed directly into Power BI.

---

## 6.5 ⚙️ Orchestration with Databricks Jobs

I created a multi-task job that automates the entire pipeline:

**Task 1:** Run Auto Loader notebook
**Task 2:** `dbt run` (Bronze)
**Task 3:** `dbt run` (Silver)
**Task 4:** `dbt run` (Gold)

This means once new CSV files land in the source folder, the entire pipeline runs from ingestion to dashboards.

👉 **Insert Screenshot: Databricks Pipeline Configuration**

---

# 7. 📊 Dashboards & Insights

I built two dashboards in Power BI using the Gold layer.
Here is an interpretation of each, written in a recruiter-friendly way.

---

## 7.1 **Sales Performance Dashboard**

👉 **Insert Sales Dashboard Image**

### Key findings:

#### 💰 Strong overall performance

* Total revenue: **$29.4M**
* Total profit: **$11.7M**
  The company is performing well financially with healthy margins.

#### 🌎 Clear geographic leaders

* **USA** and **Australia** consistently generate the most sales and profit
  These should remain priority markets for campaigns and inventory.

#### 🚲 Product insights

* Road Bikes and Mountain Bikes represent ~80% of revenue
  This concentration shows where customer interest is strongest and where stock planning matters most.

#### 📈 Sales trend

Monthly sales show a steady upward trend, indicating strong demand momentum.

---

## 7.2 **Customer Insights Dashboard**

👉 **Insert Customer Dashboard Image**

### Key findings:

#### 👥 Core demographic

The majority of customers fall between **31–50 years old**, suggesting this is the company’s strongest market segment.

#### ❤️ Marital status

Married customers tend to spend more.
Offers such as bundles, family promotions, or loyalty rewards could appeal to this group.

#### ⚠️ Data quality issue

Approximately **37% of customer gender data is missing**.
This shows where the CRM needs cleanup or better data collection processes.

#### 💎 High-value customers

A small group of VIP customers contributes disproportionately to revenue.
Focusing on retention and personalized offers could significantly increase lifetime value.

---

# 8. 🧠 What I Learned

This project helped me build confidence in:

* using Databricks Volumes + Auto Loader for ingestion
* writing dbt models across Bronze, Silver, and Gold layers
* applying dbt tests, incremental models, and macros
* documenting models using dbt docs
* implementing dimensional modeling
* orchestrating pipelines with Databricks Jobs
* creating meaningful dashboards in Power BI
* translating data into real business insights


