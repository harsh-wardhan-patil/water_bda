# AquaPulse: Water Consumption Management System
### Big Data Analytics with Apache PySpark, Apache Hive & Spark MLlib

---

## 📌 1. Project Overview

**AquaPulse** is an enterprise-grade Big Data Analytics (BDA) system engineered to solve two critical municipal water utility challenges:
1. **Physical Water Loss (Non-Revenue Water / NRW)**: Undetected pipe leaks, burst mains, and continuous domestic leakage.
2. **Inefficient Supply & Demand Planning**: Static pumping schedules that fail to anticipate diurnal morning/evening demand surges, weather-driven spikes, and consumer usage disparities.

Using **Apache PySpark 3.5.1** and **Apache Hive**, the system ingests and processes **648,000 hourly smart-meter readings** across 150 households over a 180-day monitoring period, categorizes the data into zone-partitioned Parquet storage, performs statistical and machine learning analytics, and presents operational intelligence through a modern, responsive web dashboard.

---

## 🏗️ 2. Architecture & Data Pipeline

```
[Smart Meter CSV Data] 
       │ (648,000 hourly readings across 150 households)
       ▼
[Apache PySpark Ingestion]
       │ (Schema inference, timestamp parsing, hour/date extraction)
       ▼
[Apache Hive Data Warehouse] (`water_db`)
       │ (Stored as Parquet, dynamically partitioned by `zone`)
       ▼
┌───────────────────────────────┴───────────────────────────────┐
│                                                               │
▼                                                               ▼
[Statistical Analytics & Rules]                 [Spark MLlib Machine Learning]
 • Descriptive Metrics (Zonal & Diurnal)         • Customer Segmentation (KMeans, k=3)
 • Leak Detection (Night Flow 2-4 AM)            • Demand Forecasting (Random Forest)
 • Anomaly Radar (PySpark Window Z-Score)
│                                                               │
└───────────────────────────────┬───────────────────────────────┘
                                ▼
         [Hive Output Tables + CSV Datasets]
          - `water_db.suspected_leaks`
          - `water_db.anomalies`
          - `water_db.household_clusters`
          - `water_db.forecast_next_14_days`
                                ▼
         [AquaPulse Interactive Web Dashboard]
          (`output/report.html` • Chart.js • Search/Filter/Export)
```

---

## 💻 3. Technology Stack

| Layer | Technology | Role in Project |
|---|---|---|
| **Distributed Compute** | **Apache PySpark 3.5.1** | In-memory distributed data processing, Window functions, DataFrame transformations. |
| **Data Warehousing** | **Apache Hive (with Derby)** | Metastore catalog, SQL query engine, partitioned table storage. |
| **Storage Format** | **Apache Parquet** | Columnar compressed storage for high-throughput analytical queries. |
| **Machine Learning** | **Spark MLlib** | Distributed KMeans clustering (customer segmentation) and Random Forest Regression (time-series forecasting). |
| **Platform Runtime** | **Java 21 (JDK) + Hadoop 3.3.5** | JVM execution layer and Windows native filesystem binaries (`winutils.exe`, `hadoop.dll`). |
| **Scripting & Analytics** | **Python 3.12 / Pandas / NumPy** | Pipeline orchestration, math operations, and data transformation. |
| **Visual Dashboard** | **HTML5, CSS3, JavaScript, Chart.js** | Responsive, dark/light mode dashboard with interactive charts and filterable tables. |

---

## ⚙️ 4. Prerequisites

Before running the project, ensure you have:
1. **Java**: Java 21 (or Java 11/17)  
   *Check in terminal:* `java -version`
2. **Python**: Python 3.10, 3.11, or 3.12  
   *Check in terminal:* `python --version`

---

## 🚀 5. How to Run the Project

### Option A: The Complete One-Click Runner (Easiest)
1. Open the project folder in Windows Explorer:
   ```
   c:\Users\harsh\OneDrive\Desktop\water_pyspark_hive
   ```
2. Double-click **`run.bat`**.
3. It will automatically install packages, generate data, execute the PySpark + Hive pipeline, and open the interactive dashboard in your browser.

---

### Option B: Step-by-Step Terminal Execution

Open **Command Prompt** or **PowerShell** in the project folder and run:

#### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

#### Step 2: Generate the Synthetic Smart Meter Dataset
Generates 648,000 hourly smart-meter readings with embedded leak patterns:
```bash
python generate_data.py
```
*Creates: `data/water_consumption.csv` and `data/true_leaks_for_checking.csv`.*

#### Step 3: Run the PySpark + Hive Analytical Pipeline
Executes partitioned Hive table ingestion, MLlib modeling, and generates the web report:
```bash
python spark_analysis.py
```
*Takes ~1 to 2 minutes. Saves outputs to `output/` and populates the Hive metastore `water_db`.*

#### Step 4: Open the Web Dashboard
```bash
python view_dashboard.py
```
*(Or double-click **`view_dashboard.bat`**, or double-click `output\report.html` directly).*

---

### Option C: Interactive HiveQL Terminal (Run Custom Queries)

To query the database directly using an interactive SQL terminal:
```bash
python query.py
```
*(Or double-click **`query.bat`**)*

You will get an interactive prompt where you can type queries ending with `;`:
```sql
HiveQL> SELECT zone, ROUND(SUM(litres)) total FROM readings GROUP BY zone ORDER BY total DESC;
```
Or run one-liners directly:
```bash
python query.py "SELECT * FROM suspected_leaks WHERE severity = 'Critical'"
```

---

### Option D: Automated Hive Demonstration (For Viva / Lab Exam)
To demonstrate Hive tables, partitions, and execution runtimes to an examiner:
```bash
python hive_demo.py
```
Runs 10 structured queries showcasing table metadata, partition pruning, leak detection, and ML forecasts.

---

## 🗄️ 6. Hive Database Architecture (`water_db`)

| Table Name | Partitioning | Storage | Description |
|---|---|---|---|
| `readings` | `PARTITIONED BY (zone STRING)` | Parquet | 648,000 smart meter readings partitioned across 5 operational zones (Central, East, North, South, West). |
| `suspected_leaks` | None | Managed | Households with night flow (2–4 AM) > 3× network median, with daily loss calculations and severity tags. |
| `anomalies` | None | Managed | Outlier household-day consumption events where \|z\| > 3.0 standard deviations from baseline. |
| `household_clusters` | None | Managed | Customer segmentation derived from Spark MLlib KMeans (`Low users`, `Medium users`, `High users`). |
| `forecast_next_14_days` | None | Managed | 14-day forward aggregate demand schedule generated by Spark MLlib Random Forest with 95% confidence intervals. |

---

## 📊 7. Dashboard Features (`output/report.html`)

- **8 Executive KPI Cards**: Total records (648k), gross water volume (1.59M L), daily mean per household (59 L), peak demand hour (07:00 & 19:00), highest-use zone (West), active leaks (15 meters), forecast MAPE (2.8%), and estimated daily leakage waste (~1,207 L/day).
- **8 Dedicated Navigation Tabs**:
  1. 📊 *Executive Overview*: High-level summary charts and operational alert pills.
  2. 🗺️ *Zonal & Temporal Profile*: Zone volume & share, property types (Apartment, Bungalow, Villa), and 24h diurnal curve.
  3. 💧 *Smart Leak Detection*: Night-flow radar chart with 3× threshold line and an interactive suspected leaks ledger.
  4. ⚡ *Anomaly Radar*: Z-score scatter plot vs. ambient temperature.
  5. 👥 *Customer Clusters*: 2D MLlib KMeans scatter plot (`avg_daily` vs. `per_person`) and tiered tariff policy recommendations.
  6. 📈 *14-Day Demand Forecast*: Validation test plot and 14-day forward schedule with 95% confidence intervals.
  7. 🐘 *Hive Metastore & SQL*: Interactive query console with pre-computed query outputs and execution timings.
  8. 🛡️ *Utility Action Plan & Policies*: 4 actionable operational pillars for municipal water authorities.
- **Interactive Controls**:
  - Live search bar and severity filter dropdown on all data tables.
  - Column sorting and 1-click **CSV Download** for every table.
  - Modal viewer for high-resolution Matplotlib PNG charts.
  - Dark Mode / Light Mode toggle with local persistence.

---

## 🎓 8. Viva & Evaluation Guide (Key Concepts)

### Why PySpark?
- **In-Memory Distributed Processing**: PySpark processes large-scale time-series sensor data (648,000+ rows) in memory, enabling sub-second window aggregations and feature engineering.
- **Spark SQL Catalyst Optimizer**: Optimizes SQL execution plans automatically.
- **Spark MLlib Integration**: Seamlessly runs distributed clustering (KMeans) and regression (Random Forest) directly on Spark DataFrames without moving data out of the cluster.

### Why Apache Hive & Zone Partitioning?
- **Schema Enforcement & Metastore**: Hive provides a centralized metastore catalog (`water_db`) over raw files, enabling standard HiveQL queries.
- **Partition Pruning**: By partitioning `readings` by `zone` (`PARTITIONED BY (zone STRING)`), any query filtering by zone (e.g., `WHERE zone = 'West'`) only scans the `zone=West/` directory on disk, skipping 80% of unneeded data.

### How Does Leak Detection Work?
- Normal domestic consumption between 2:00 AM and 4:00 AM drops close to 0 L/hr.
- The pipeline computes the network median night flow. Any meter consistently drawing **> 3× the median flow** during 2–4 AM is flagged as a physical leak.
- Validated against hidden ground truth: achieves **100% sensitivity** (detected 12 out of 12 real leaks).

### How Does Customer Segmentation Work?
- Features: `avg_daily` (household daily litres) and `per_person` (daily litres per resident).
- Scaled with `StandardScaler` and clustered with `KMeans(k=3)`.
- Dynamically mapped to **Low users** (lifeline subsidized tariff), **Medium users** (standard volumetric billing), and **High users** (progressive surcharge to discourage waste).

### How Does Demand Forecasting Work?
- Temporal features: 1-day lag (`lag1`), 7-day seasonal lag (`lag7`), Day of Week (`dow`), and ambient temperature (`temp`).
- Model: `RandomForestRegressor(numTrees=100)`.
- Evaluation: Achieves an exceptional **MAPE of 2.8%** and **R² of 0.732** on the validation test period.

---

## 📂 9. Project Directory Layout

```
water_pyspark_hive/
│
├── data/                                 # Datasets
│   ├── water_consumption.csv             # 648,000 smart meter readings
│   └── true_leaks_for_checking.csv       # Verification key for leak evaluation
│
├── output/                               # Generated Outputs
│   ├── report.html                       # AquaPulse Modern Interactive Dashboard
│   ├── suspected_leaks.csv               # Flagged leaking meters
│   ├── anomalies.csv                     # Statistical outlier events
│   ├── household_clusters.csv            # KMeans customer segments
│   ├── forecast_next_14_days.csv         # 14-day ahead demand schedule
│   └── *.png                             # High-resolution Matplotlib charts
│
├── spark-warehouse/                      # Hive Warehouse (Parquet partitioned data)
│   └── water_db.db/                      # Database storage
│
├── metastore_db/                         # Apache Derby Hive Metastore Catalog
│
├── env_setup.py                          # Windows helper (Java detection & short paths)
├── spark_session.py                      # SparkSession factory with Hive support
├── generate_data.py                      # Dataset generator
├── spark_analysis.py                     # Main 8-stage PySpark + Hive pipeline
├── report_template.py                    # Dashboard HTML/CSS/JS generator
├── query.py / query.bat                  # Interactive HiveQL database terminal
├── hive_demo.py                          # Automated HiveQL demonstration for viva
├── view_dashboard.py / view_dashboard.bat# 1-click dashboard launcher
├── run.bat                               # All-in-one execution script
├── requirements.txt                      # Python dependencies
└── README.md                             # Comprehensive project documentation
```
#   w a t e r _ b d a  
 