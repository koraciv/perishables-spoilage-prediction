# Supply Chain Spoilage Prediction (PySpark & LightGBM)

## 📌 Project Overview
In large-scale grocery retail, perishable spoilage ("shrink") represents a massive degradation of gross margin. Environmental anomalies during transit, unexpected logistical delays, and inefficient routing directly impact the remaining shelf life of fresh produce and meats.

This project implements a **Predictive Supply Chain Model** to identify high-risk shipments before they arrive at distribution centers. By combining distributed logistical logs with simulated IoT temperature telemetry using **PySpark**, the pipeline trains a **LightGBM** classifier to predict the probability of a pallet spoiling.

**Business Value:** Enables proactive logistics management (e.g., dynamic rerouting of high-risk pallets to closer stores) and automated dynamic markdown pricing, significantly reducing retail shrink and waste.

## 🛠️ Tech Stack
* **Big Data Processing:** Apache Spark (PySpark SQL, Distributed Joins)
* **Machine Learning:** LightGBM (Gradient Boosting for categorical tabular data), scikit-learn
* **Data Engineering:** Pandas, NumPy
* **Language:** Python 3.x

## 🏗️ Architecture & Workflow
1. **Distributed Data Ingestion:** Utilizes PySpark to ingest and join massive datasets, simulating the combination of enterprise ERP shipment logs with high-velocity IoT truck sensor data.
2. **Feature Engineering:** Computes temporal transit metrics and environmental variances (e.g., temperature fluctuations, remaining shelf-life buffers) directly within the Spark cluster.
3. **Imbalanced Classification:** Transitions the aggregated, feature-rich dataset to Pandas to train an `LGBMClassifier`. Utilizes class-weight balancing to accurately detect the minority class (spoilage events).
4. **Risk Scoring:** Evaluates the model using ROC-AUC to ensure the algorithm successfully ranks shipments by their true risk of spoilage.

## 📂 Project Structure
```text
├── data/                   # Data directory (Dataset excluded via .gitignore)
├── src/                    
│   ├── data_ingestion.py   # PySpark joins for ERP and IoT data
│   ├── feature_engineering.py # PySpark temporal and environmental features
│   ├── model_training.py   # LightGBM training and ROC-AUC evaluation
│   └── main.py             # Pipeline orchestrator
├── requirements.txt        # Python dependencies
├── .gitignore              # Ignored files and directories
└── README.md               # Project documentation
