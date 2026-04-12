🚀 **Customer Insights Pipeline: Airflow, dbt & DuckDB**

A modern, end-to-end data engineering pipeline for transforming raw transactional data into actionable business intelligence. This project automates the journey from raw CSVs to a tested analytics mart, answering the core question: "Who are our most valuable customers?"

🏗️ **The Architecture**

The pipeline demonstrates professional orchestration and analytics engineering:

**Apache Airflow**: Orchestrates the workflow and handles file movement.

**dbt (data build tool)**: Manages SQL transformations, data quality testing, and documentation.

**DuckDB**: Acts as a high-performance, in-process analytical database (OLAP).

**Python/Pandas**: Performs the final data validation and top-tier customer reporting.

📁 **Project Structure**

Plaintext
.
├── dags/

│   └── dbt_pipeline_dag.py        # Airflow DAG (TaskFlow API)

├── jaffle_shop_duckdb/            # Core dbt Project folder

│   ├── models/                    # Staging, Intermediate, and Mart SQL

│   ├── seeds/                     # Target directory for raw data ingestion

│   └── dbt_project.yml            # dbt configuration

├── raw_customers.csv              # Source Data

├── raw_orders.csv

├── raw_payments.csv

├── analysis/

│   └── top_customers_query.py     # Final DuckDB analysis script

├── Dockerfile                     # Containerization for Airflow/dbt

├── docker-compose.yaml            # Multi-container orchestration

└── requirements.txt               # Python dependencies

⚙️ **Pipeline Workflow**

1. *Automated Ingestion*

The Airflow BashOperator moves raw CSV files from the project root into the dbt seeds/ directory. This ensures the pipeline is self-contained and reproducible.

2. *Transformations with dbt*

The pipeline follows the Medallion Architecture logic:

+ Staging: Cleaning and renaming raw sources (stg_customers, etc.).

+ Intermediate: Combining orders and payments to calculate revenue.

+ Marts: Final business-ready tables (dim_customers, fct_orders).

3. *Data Quality & Integrity*

Every run includes automated dbt tests:

+ unique and not_null constraints on primary keys.

+ Referential integrity (Relationship tests) between orders and customers.

📊 **Final Output: Top 10 Most Valuable Customers**

The pipeline culminates in a Lifetime Value (LTV) report:

Rank	Customer	Total Lifetime Value

1	Howard R.	9900.0

2	Kathleen P.	6500.0

3	Billy L.	4700.0

🛠️ **Technologies Used**

+ Orchestration: Apache Airflow 2.10+

+ Transformations: dbt-duckdb

+ Database: DuckDB

+ Environment: Python 3.12, WSL2 / Docker

▶️ **How to Run**

Local Setup (WSL/Linux)

Clone & Install:

Bash

git clone https://github.com/Triet00/Customer-Insights-Data-Pipeline.git

cd Customer-Insights-Data-Pipeline

python -m venv venv && source venv/bin/activate

pip install -r requirements.txt

Initialize Airflow:

Bash

export AIRFLOW_HOME=$(pwd)

export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow db init

Trigger the Pipeline:

Bash

airflow tasks test dbt_jaffle_shop_pipeline dbt_build_pipeline 2026-04-01

📈 **Challenges Overcome**

+ Path Resolution: Configured dynamic workspace rooting to bridge the gap between Windows/WSL and dbt project-dirs.

+ Metadata Management: Resolved Airflow serialization conflicts by implementing a clean metadata reset and custom DAG bundling strategy.

🙌 **Acknowledgements**
The Jaffle Shop dataset provided by dbt Labs.

The community resources for DuckDB and Airflow integration.
