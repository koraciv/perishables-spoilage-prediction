from pyspark.sql.functions import col, datediff, stddev, avg

def engineer_features(df):
    print("Engineering temporal and environmental features...")
    
    # Calculate days in transit
    df = df.withColumn("transit_duration_days", datediff(col("arrival_date"), col("dispatch_date")))
    
    # Calculate how close the item is to its expiration date upon arrival
    df = df.withColumn("remaining_shelf_life", datediff(col("expiration_date"), col("arrival_date")))
    
    # Simulate aggregating temperature logs (In reality, this would be a groupBy on shipment_id)
    # Here we assume the data already has max_temp and min_temp columns from the sensors
    df = df.withColumn("temp_variance", col("max_temp") - col("min_temp"))
    
    # Drop dates as LightGBM needs numeric/categorical features, not raw date strings
    features_df = df.drop("dispatch_date", "arrival_date", "expiration_date", "shipment_id")
    
    print("Converting PySpark DataFrame to Pandas for LightGBM...")
    return features_df.toPandas()
