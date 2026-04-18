# Capstone Project: End-to-End Retail Data Pipeline

A production-grade data engineering pipeline implementing the **Medallion Architecture** on **Azure Databricks**, with **Snowflake** as the cloud data warehouse and **Apache Airflow** for orchestration.

---

## Architecture

**Medallion Architecture** (Bronze → Silver → Gold) on **Azure Databricks** with **Snowflake** and **Airflow**.

```
CSV Files → [Bronze: Raw Delta] → [Silver: Clean & Validated] → [Gold: Star Schema] → Snowflake
                                ↑ Orchestrated by Apache Airflow ↑
```

---

## Tech Stack

| Technology | Purpose |
|---|---|
| Azure Databricks (PySpark) | Distributed data processing + Delta Lake |
| Snowflake (Azure) | Cloud data warehouse |
| Apache Airflow (Docker) | Workflow orchestration |
| Python 3.11 | Pipeline logic |
| Git/GitHub | Version control |

---

## Project Structure

```
capstone-project/
├── data/raw/                   # Synthetic CSV datasets
├── src/
│   ├── ingestion.ipynb           # Bronze layer
│   ├── silver_transform.ipynb     # Silver layer
│   ├── gold_transform.ipynb       # Gold layer (star schema)
│   └── data_quality.ipynb          # DQ framework
│   └── snowflake_load.ipynb       # Load to Snowflake 
├── sql/
│   └── gold_tables.sql         # Snowflake DDL
├── airflow/
│   ├── retail_dag.py           # DAG definition
│   
├── config/config.yaml          # Centralised config
├── evidence/                   # Screenshots
└── README.md
```

---

## Setup Instructions

### 1. Clone Repo

```bash
git clone https://github.com/Anuragsarkar12/capstone-project.git
```

---

### 2. Azure Databricks Setup

1. Create an Azure Databricks workspace in the Azure Portal.
2. Create a Databricks Cluster.
3. Install Maven libraries on the cluster:
   - `net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5`
   - `net.snowflake:snowflake-jdbc:3.16.1`
4. Link GitHub account (**Settings → Linked accounts → paste PAT**).
5. Clone the repo into Workspace via Git folders.
6. Upload CSVs to Databricks Catalog.

---

### 3. Databricks Secrets (Snowflake Credentials)

```bash
pip install databricks-cli
databricks configure --token
# Host: https://adb-xxxxx.xx.azuredatabricks.net
# Token: your Databricks PAT

databricks secrets create-scope --scope snowflake
databricks secrets put --scope snowflake --key account    # e.g. orgname-accountname
databricks secrets put --scope snowflake --key user       # your username
databricks secrets put --scope snowflake --key password   # your password
```

**Usage in notebooks:**

```python
dbutils.secrets.get("snowflake", "account")
dbutils.secrets.get("snowflake", "user")
dbutils.secrets.get("snowflake", "password")
```

---

### 4. Finding Your Snowflake Account Identifier

1. Log into Snowflake → click the **profile icon** (bottom left).
2. Account identifier format: `ORGNAME-ACCOUNTNAME`
3. Full URL: `ORGNAME-ACCOUNTNAME.snowflakecomputing.com`

---

### 5. Snowflake Setup

Run `sql/gold_tables.sql` in a Snowflake worksheet to create all required tables.

---

### 6. Run Pipeline on Databricks

Execute notebooks in the following order:

| Step | Notebook | Layer |
|---|---|---|
| 1 | `src/ingestion.ipynb` | Bronze |
| 2 | `src/silver_transform.ipynb` | Silver |
| 3 | `src/data_quality.ipynb` → `run_silver_dq_checks()` | Silver DQ |
| 4 | `src/gold_transform.ipynb` | Gold |
| 5 | `src/data_quality.ipynb` → `run_gold_dq_checks()` | Gold DQ |
| 6 | `src/snowflake_load.ipynb` | Snowflake Load |

---

### 7. Airflow (Local Docker)

```bash
cd airflow
docker compose up -d
# UI: http://localhost:8080
# Default credentials: admin / admin
(Docker setup must be ready with Airflow image in order to run the above commands)
```

---

## License
MIT License
