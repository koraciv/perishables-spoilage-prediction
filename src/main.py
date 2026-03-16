import os
from pyspark.sql import SparkSession
from data_ingestion import load_and_join_data
from feature_engineering import engineer_features
from model_training import train_spoilage_model

def run_supply_chain_pipeline():
    # File paths for portfolio simulation
    logistics_data = "../data/shipment_logs.csv"
    sensor_data = "../data/telemetry_logs.csv"
    
    if not os.path.exists(logistics_data) or not os.path.exists(sensor_data):
        print("Error: Please place simulation data in the '../data/' directory.")
        return

    spark = SparkSession.builder \
        .appName("Perishables_Spoilage_Prediction") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    try:
        # Step 1: Big Data Ingestion
        combined_data = load_and_join_data(spark, logistics_data, sensor_data)
        
        # Step 2: Feature Engineering
        ml_ready_data = engineer_features(combined_data)
        
        # Step 3: Model Training
        model = train_spoilage_model(ml_ready_data)
        
        print("\nSupply Chain Pipeline executed successfully.")

    except Exception as e:
        print(f"\nPipeline failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_supply_chain_pipeline()
