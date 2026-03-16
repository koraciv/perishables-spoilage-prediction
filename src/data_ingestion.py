from pyspark.sql import SparkSession

def load_and_join_data(spark, logistics_path, sensor_path):
    print("Ingesting logistics and telemetry data...")
    
    # Load shipment logs (e.g., origin, destination, product_type)
    logistics_df = spark.read.csv(logistics_path, header=True, inferSchema=True)
    
    # Load IoT temperature sensor data from trucks
    sensor_df = spark.read.csv(sensor_path, header=True, inferSchema=True)
    
    print("Joining datasets on shipment_id...")
    # Inner join to combine shipment details with their temperature logs
    combined_df = logistics_df.join(sensor_df, on="shipment_id", how="inner")
    
    return combined_df
