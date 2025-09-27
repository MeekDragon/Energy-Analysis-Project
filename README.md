# ⚡ Renewable Energy Optimization Pipeline

A robust, scalable ETL pipeline built with **Apache Spark** and **Scala** to process large volumes of renewable energy data. This project demonstrates end-to-end data engineering capability, from raw data ingestion to visualization, focused on grid management and anomaly detection.

## 🎯 Goal and Outcomes

| Goal | Outcomes | 
| ----- | ----- | 
| **Analyze** the hourly generation vs consumption of energy. | Identified **Variability in energy generation** and clear **Peak vs. Off-Peak Demand** cycles. | 
| To **detect imbalance** in the energy supply/demand. | Detected **Supply Imbalance**, allowing for proactive grid management. | 
| | Established a **Strong foundation for real-time monitoring** and future ML integration. | 

## 🛠️ Technology Stack

| Category | Tool / Technology | Purpose in Project | 
| ----- | ----- | ----- | 
| **Data Processing** | **Apache Spark (Scala)** | Implemented the high-performance ETL pipeline for transformations and aggregations. | 
| **Data Storage** | **PostgreSQL** | Used to store the curated, hourly aggregated data for Business Intelligence (BI). | 
| **Visualization** | **Power BI** | Used for creating dynamic dashboards to visualize energy trends and anomalies. | 
| **Programming** | **Scala** | Primary language for Spark ETL, chosen for performance and type safety. | 

## ⚙️ ETL Pipeline and Data Transformation

The pipeline transforms complex raw data (including energy metrics and cybersecurity indicators) into actionable hourly insights.

### Transformation Logic (Spark/Scala)

1. **Validation:** Validated and parsed raw `ts_utc` timestamps; dropped rows with inconsistent or invalid data.

2. **Aggregation:** Resampled and aggregated metrics to an hourly level (`hour_utc`):

   * **Sum:** Energy generated, energy consumed, intrusion attempts.

   * **Average:** Carbon emissions, network traffic, anomaly score.

3. **Derived Fields:** Computed critical new features:

   * **`net_balance_kwh`**: Calculated as `generated - consumed` to quantify supply balance.

   * **`anomaly_flag`**: Boolean flag set when `anomaly` score > 0.5 for immediate operational alerting.

### Database Schema (PostgreSQL)

The curated data is loaded into the `energy_cyber_hourly` table:

| Field | Description | Type | 
| ----- | ----- | ----- | 
| `hour_utc` | Hourly timestamp (Primary Key) | Timestamp | 
| `generated_kwh` | Sum of energy generated | Numeric | 
| `consumed_kwh` | Sum of energy consumed | Numeric | 
| **`net_balance_kwh`** | Net energy balance (Derived) | Numeric | 
| **`anomaly_flag`** | High-risk anomaly indicator (Derived) | Boolean | 

## 📊 Analysis and Visualization (Power BI)

* **BI Integration:** The PostgreSQL database was directly connected to **Power BI**.

* **Dashboard Creation:** Created **dynamic dashboards** in Power BI to visually represent the hourly energy trends, consumption patterns, and imbalance events.

* **Actionable Insights:** This visualization was crucial for demonstrating the project's outcomes, allowing stakeholders to easily track **Peak** vs. Off-Peak **Demand** cycles and identify system anomalies for proactive grid management.

![Uploading image (1).png…]()



## 💡 Challenges and Solutions

| Challenge | Solution Implemented | 
| ----- | ----- | 
| **Data Quality Issues** | Used Spark transformations to handle missing or negative values, and standardized inconsistent timestamp formats. | 
| **Timestamp Alignment** | Implemented precise resampling logic in Spark to aggregate all metrics accurately to the required hourly frequency. | 
| **PostgreSQL Integration** | Successfully managed the **JDBC** driver setup **and configuration** within the Spark environment for robust data loading. | 

## 🚀 Future Enhancements

The architecture is scalable and designed for future expansion, including:

* Real-time streaming using **Kafka + Spark Structured Streaming**.

* Predictive analytics using **MLlib (Scala)** for forecasting demand and anomaly prediction.

* Integration with IoT sensors for live energy data feeds.

## 🔗 Repository Links

* **GitHub Repository:**
