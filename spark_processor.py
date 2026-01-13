"""
OPTIONAL: Advanced PySpark processing if you want to scale up
Run only if you have Spark installed, otherwise skip this file
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum
import warnings

warnings.filterwarnings('ignore')


class SparkAdvancedProcessor:
    def __init__(self):
        try:
            self.spark = SparkSession.builder \
                .appName("AirTrafficAdvanced") \
                .config("spark.sql.parquet.compression.codec", "snappy") \
                .getOrCreate()
            self.spark_available = True
            print("Spark session created successfully")
        except:
            self.spark_available = False
            print("Spark not available, using pandas only")

    def process_with_spark(self, pandas_df):
        """Advanced processing with PySpark (optional)"""
        if not self.spark_available:
            print("Spark not available, skipping advanced processing")
            return pandas_df

        try:
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(pandas_df)

            print("Running advanced Spark analytics...")

            # Complex aggregations
            complex_agg = spark_df.groupBy("source_region", "target_region", "year") \
                .agg(
                spark_sum("estimated_trips").alias("sum_trips"),
                avg("estimated_trips").alias("avg_trips"),
                count("*").alias("count_routes"),
                avg("dist").alias("avg_distance")
            ) \
                .withColumn("trips_per_route", col("sum_trips") / col("count_routes")) \
                .withColumn("density_score", col("sum_trips") / col("avg_distance"))

            # Show results
            print("Sample results from Spark processing:")
            complex_agg.show(5)

            # Convert back to pandas for compatibility
            return complex_agg.toPandas()

        except Exception as e:
            print(f"Spark processing error: {e}")
            return pandas_df

    def stop_spark(self):
        """Clean up Spark session"""
        if self.spark_available:
            self.spark.stop()
            print("Spark session stopped")