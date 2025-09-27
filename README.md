# ⚡ Renewable Energy Optimization Pipeline

A robust, scalable ETL pipeline built with **Apache Spark** and **Scala** to process real-time and historical renewable energy data. The goal of this project is to analyze hourly energy generation versus consumption to detect imbalances, providing a strong foundation for real-time grid monitoring and optimization.

## 🎯 Goal and Outcomes

| Goal | Outcomes |
| :--- | :--- |
| **Analyze** the hourly generation vs consumption of energy. | **Variability** in energy generation analyzed and visualized. |
| To **detect imbalance** in the energy supply/demand. | Identified **Peak vs. Off-Peak Demand** cycles. |
| | Detected **Supply Imbalance**, allowing for proactive grid management. |
| | [cite_start]Established a **Strong foundation for real-time monitoring**[cite: 5]. |

## 🛠️ Technology Stack

| Category | Tool / Technology | Purpose in Project |
| :--- | :--- | :--- |
| **Data Processing** | **Apache Spark (Scala)** | [cite_start]Implemented the high-performance ETL pipeline[cite: 3]. |
| **Data Storage** | **PostgreSQL** | [cite_start]Used to store the raw and hourly aggregated data for Business Intelligence (BI)[cite: 10]. |
| **Visualization** | **Power BI** | [cite_start]Used for creating dashboards to visualize energy trends and anomalies[cite: 11]. |
| **Programming** | **Scala** | [cite_start]Primary language for Spark ETL for better performance and type safety[cite: 3, 11]. |

## ⚙️ Architecture and Pipeline Flow

The pipeline is designed to transform complex raw data into actionable insights:

1.  [cite_start]**Extract:** Reads raw energy metrics (generated, consumed) and cybersecurity indicators (traffic, anomaly score) from a defined source[cite: 8, 9].
2.  **Transform (Spark/Scala):**
    * [cite_start]Validates and parses timestamps, dropping rows with invalid data[cite: 12].
    * **Aggregates** metrics (Sum, Average) to an hourly level.
    * [cite_start]**Computes derived fields** like `net_balance_kwh` (generated - consumed) and an `anomaly_flag`[cite: 13].
3.  [cite_start]**Load (JDBC):** Stores the curated hourly aggregated data into a **PostgreSQL** database table (`energy_cyber_hourly`)[cite: 10, 14].

## 📊 Key Data Schema (PostgreSQL)

The curated data is stored in the `energy_cyber_hourly` table:

| Field | Description | Derivation |
| :--- | :--- | :--- |
| `hour_utc` | Hourly timestamp | Truncated from the raw timestamp |
| `generated_kwh` | Sum of energy generated | Aggregation |
| `consumed_kwh` | Sum of energy consumed | Aggregation |
| **`net_balance_kwh`** | Net energy balance | [cite_start]`generated - consumed` [cite: 13] |
| **`anomaly_flag`** | Flag for high anomaly scores | [cite_start]`anomaly score > 0.5` [cite: 13] |

## 💡 Implementation Details

[cite_start]The ETL pipeline was implemented using **Scala** within Apache Spark to leverage its performance benefits and strong type system[cite: 11]. The core logic is defined in `EnergyETL.scala`.

**Challenges Overcome:**

* Handling data quality issues (missing values, inconsistent timestamps)[cite: 1, 2].
* [cite_start]Implementing complex time-series logic for timestamp alignment and hourly resampling[cite: 2].
* [cite_start]Successfully integrating Spark with PostgreSQL using the JDBC driver[cite: 3].
