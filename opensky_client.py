import requests
import pandas as pd
import numpy as np
import time
import random
from datetime import datetime, timedelta
import json
from pathlib import Path
from config import OPENSKY_CONFIG, DATA_PATHS, PROCESSING_CONFIG
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType


class AirTrafficDataPipeline:
    def __init__(self):
        self.base_url = OPENSKY_CONFIG["BASE_URL"]
        self.europe_bounds = OPENSKY_CONFIG["EUROPE_BOUNDS"]
        self.bearer_token = OPENSKY_CONFIG.get("BEARER_TOKEN")
        # Create directories
        for path in DATA_PATHS.values():
            if isinstance(path, Path):
                path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("AirTrafficBackend") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .master("local[*]") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    # ========== REAL‑TIME DATA ==========
    def get_realtime_flights(self):
        """Fetch current flights over Europe from OpenSky (respects 10‑second limit)."""
        try:
            url = f"{self.base_url}/states/all"
            params = {
                "lamin": self.europe_bounds["lamin"],
                "lamax": self.europe_bounds["lamax"],
                "lomin": self.europe_bounds["lomin"],
                "lomax": self.europe_bounds["lomax"]
            }

            headers = {}
            if self.bearer_token:
                headers["Authorization"] = f"Bearer {self.bearer_token}"

            # Add a small random delay to avoid hitting rate limits
            time.sleep(random.uniform(0.1, 0.5))

            response = requests.get(url, params=params,headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()

            return self._parse_realtime_data(data.get("states", []))

        except Exception as e:
            print(f"⚠ OpenSky API error: {e}")
            return pd.DataFrame()

    def _parse_realtime_data(self, states):
        """Parse OpenSky API response."""
        if not states:
            return pd.DataFrame()

        columns = [
            "icao24", "callsign", "origin_country", "time_position",
            "last_contact", "longitude", "latitude", "baro_altitude",
            "on_ground", "velocity", "true_track", "vertical_rate",
            "sensors", "geo_altitude", "squawk", "spi", "position_source"
        ]

        valid_states = [s for s in states if s is not None]
        df = pd.DataFrame(valid_states, columns=columns[:len(valid_states[0])])

        # Add metadata
        df["timestamp"] = datetime.utcnow()
        df["date"] = pd.to_datetime(df["timestamp"]).dt.date

        # Convert numeric columns
        numeric_cols = ["longitude", "latitude", "baro_altitude", "velocity",
                        "true_track", "vertical_rate", "geo_altitude"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Add simulated region data (in a real project, use actual mapping)
        np.random.seed(42)  # Keep consistent for demo
        regions = ["Western Europe", "Eastern Europe", "Southern Europe", "Northern Europe"]
        subregions = ["North", "South", "East", "West"]

        df["source_region"] = np.random.choice(regions, len(df))
        df["target_region"] = np.random.choice(regions, len(df))
        df["source_subregion"] = np.random.choice(subregions, len(df))
        df["target_subregion"] = np.random.choice(subregions, len(df))
        df["estimated_trips"] = 1  # Each flight counts as one trip
        df["dist"] = np.random.uniform(100, 3000, len(df))  # Simulated distance

        return df

    # ========== HISTORICAL DATA (Spark) ==========
    def load_historical_data(self):
        """Load CSV with historical flight data using Spark (with Pandas fallback)."""
        csv_path = str(DATA_PATHS["RAW_HISTORICAL"])
        
        # Helper to load via Pandas
        def load_via_pandas():
            print("⚠ Falling back to Pandas for CSV loading...")
            pdf = pd.read_csv(csv_path)
            # Ensure types for Spark compatibility
            for col in pdf.columns:
                if pdf[col].dtype == 'object':
                    pdf[col] = pdf[col].astype(str)
                elif pdf[col].dtype == 'float64':
                    pdf[col] = pdf[col].fillna(0.0)
                elif pdf[col].dtype == 'int64':
                    pdf[col] = pdf[col].fillna(0)
            return self.spark.createDataFrame(pdf)

        try:
            # Check if file exists first
            if not Path(csv_path).exists():
                 print("Historical file not found, creating sample data...")
                 self._create_sample_historical_data()
            
            try:
                # Try standard Spark load first
                df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
                # Trigger an action to verify it actually works (lazy evaluation might hide errors)
                if hasattr(df, "count"):
                    count = df.count()
                else:
                    count = len(df)
                print(f"Loaded historical data: {count} rows")
                return df
            except Exception as e:
                print(f"Spark read failed ({e}), retrying via Pandas...")
                return load_via_pandas()

        except Exception as e:
            print(f"Error loading historical data: {e}")
            # Fallback to creating sample data and trying again
            self._create_sample_historical_data()
            return load_via_pandas()

    def _create_sample_historical_data(self):
        """Create sample historical data if CSV doesn't exist."""
        countries = ["DEU", "FRA", "ITA", "ESP", "NLD", "BEL", "AUT", "CHE", "POL"]
        regions = ["Western Europe", "Eastern Europe", "Southern Europe", "Northern Europe"]
        subregions = ["North", "South", "East", "West"]

        np.random.seed(42)
        n_rows = 1000

        data = {
            "source_name": np.random.choice(countries, n_rows),
            "target_name": np.random.choice(countries, n_rows),
            "source_iso3": np.random.choice(countries, n_rows),
            "target_iso3": np.random.choice(countries, n_rows),
            "year": np.random.choice([2022, 2023, 2024], n_rows),
            "estimated_trips": np.random.randint(100, 10000, n_rows),
            "dist": np.random.uniform(100, 3000, n_rows),
            "source_region": np.random.choice(regions, n_rows),
            "target_region": np.random.choice(regions, n_rows),
            "source_subregion": np.random.choice(subregions, n_rows),
            "target_subregion": np.random.choice(subregions, n_rows)
        }

        df = pd.DataFrame(data)
        df = df[df["source_name"] != df["target_name"]]

        # Save it
        df.to_csv(DATA_PATHS["RAW_HISTORICAL"], index=False)
        print(f"Saved sample data to {DATA_PATHS['RAW_HISTORICAL']}")
        return df

    # ========== BATCH PROCESSING (Spark) ==========
    def process_batch_data(self, historical_df):
        """Process historical data with batch analytics using Spark."""
        print("Processing batch data with Spark...")

        # Determine grouping columns (handle mismatched CSV format)
        src_col = "source_region"
        tgt_col = "target_region"
        
        # Check columns existence in Spark DF
        columns = historical_df.columns
        if "source_subregion" in columns and "target_subregion" in columns:
            # Heuristic: if source_region is generic "Europe", use subregions
            first_row = historical_df.limit(1).collect()
            if first_row and first_row[0]["source_region"] == "Europe":
                src_col = "source_subregion"
                tgt_col = "target_subregion"
                print(f"⚠ Detected generic 'Europe' regions. Using subregions '{src_col}'/'{tgt_col}' for alignment.")

        # 1. Region‑level aggregations
        region_agg = historical_df.groupBy(src_col, tgt_col, "year").agg(
            F.sum("estimated_trips").alias("total_trips"),
            F.mean("estimated_trips").alias("avg_trips"),
            F.count("estimated_trips").alias("route_count"),
            F.mean("dist").alias("avg_distance"),
            F.stddev("dist").alias("std_distance")
        ).withColumnRenamed(src_col, "source_region") \
         .withColumnRenamed(tgt_col, "target_region")

        # 2. Country‑level aggregations
        country_agg = historical_df.groupBy("source_iso3", "target_iso3", "year").agg(
            F.sum("estimated_trips").alias("total_trips"),
            F.mean("dist").alias("avg_distance")
        )

        # 3. Mobility density (trips per km)
        mobility_density = historical_df.withColumn(
            "trips_per_km", 
            F.col("estimated_trips") / F.col("dist")
        )

        # Save results
        region_agg.write.mode("overwrite").parquet(str(DATA_PATHS["PROCESSED_BATCH"] / "region_aggregations.parquet"))
        country_agg.write.mode("overwrite").parquet(str(DATA_PATHS["PROCESSED_BATCH"] / "country_aggregations.parquet"))
        mobility_density.write.mode("overwrite").parquet(str(DATA_PATHS["PROCESSED_BATCH"] / "mobility_density.parquet"))

        print(f"Batch processing complete. Saved to {DATA_PATHS['PROCESSED_BATCH']}")
        
        # Return summary
        return {
            "region_aggregations_count": region_agg.count(),
            "country_aggregations_count": country_agg.count(),
            "total_records": historical_df.count()
        }

    def _process_streaming_batch(self, batch_df):
        """Process a single streaming batch (Pure Pandas is fine for individual small batches)."""
        if batch_df.empty:
            return pd.DataFrame()

        # Add processing timestamp
        batch_df["processed_at"] = datetime.now()

        # Calculate simple metrics
        if "velocity" in batch_df.columns:
            batch_df["speed_kmh"] = batch_df["velocity"] * 3.6
            batch_df["is_moving"] = batch_df["velocity"] > 50
        else:
            batch_df["speed_kmh"] = 0
            batch_df["is_moving"] = False

        return batch_df

    def _create_streaming_aggregations(self, streaming_df):
        """Create 5‑minute window aggregations from streaming data using Spark."""
        if streaming_df.empty:
            return pd.DataFrame()

        # Convert Pandas DF to Spark DF
        # Ensure timestamp is datetime type for Spark to handle it correctly
        streaming_df["timestamp"] = pd.to_datetime(streaming_df["timestamp"])
        
        # Define schema explicitly to avoid inference errors
        schema = StructType([
            StructField("icao24", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("source_region", StringType(), True),
            StructField("target_region", StringType(), True),
            StructField("estimated_trips", IntegerType(), True),
            StructField("speed_kmh", DoubleType(), True)
        ])
        
        # Select and clean columns matching schema
        cols_to_keep = ["icao24", "timestamp", "source_region", "target_region", "estimated_trips", "speed_kmh"]
        
        # Ensure columns exist and fill NaNs
        for col in cols_to_keep:
            if col not in streaming_df.columns:
                if col == "estimated_trips":
                    streaming_df[col] = 1
                elif col == "speed_kmh":
                    streaming_df[col] = 0.0
                else:
                    streaming_df[col] = None
        
        # Handle types before creation
        streaming_df["estimated_trips"] = streaming_df["estimated_trips"].fillna(1).astype(int)
        streaming_df["speed_kmh"] = streaming_df["speed_kmh"].fillna(0.0).astype(float)
        
        streaming_df_clean = streaming_df[cols_to_keep]
        
        try:
            spark_df = self.spark.createDataFrame(streaming_df_clean, schema=schema)
        except Exception as e:
            print(f"Error creating Spark DataFrame from streaming data: {e}")
            return pd.DataFrame()

        aggregations = spark_df.groupBy(
            F.window(F.col("timestamp"), "5 minutes"), 
            "source_region", 
            "target_region"
        ).agg(
            F.sum("estimated_trips").alias("total_trips"),
            F.mean("speed_kmh").alias("avg_speed_kmh"),
            F.count("icao24").alias("flight_count")
        )
        
        # Flatten the window column
        aggregations = aggregations.withColumn("time_window", F.col("window.start")) \
                                   .drop("window")
                                   
        # Convert back to Pandas for the API/Backend consumption
        result_df = aggregations.toPandas()
        
        # Ensure column order/renaming matches expected output
        cols = ["time_window", "source_region", "target_region", "total_trips", "avg_speed_kmh", "flight_count"]
        # Filter for existing columns only just in case
        existing_cols = [c for c in cols if c in result_df.columns]
        result_df = result_df[existing_cols]

        return result_df

    # ========== COMBINE BATCH & STREAMING (unchanged logic, mostly) ==========
    def combine_insights(self, batch_results, streaming_aggregations):
        """Combine batch historical data with real‑time streaming insights."""
        print("Creating combined insights...")

        try:
            # Spark writes parquet as directories of files. Pandas can read these if pyarrow is installed.
            batch_regions = pd.read_parquet(DATA_PATHS["PROCESSED_BATCH"] / "region_aggregations.parquet")

            historical_baseline = batch_regions.groupby(["source_region", "target_region"]).agg({
                "total_trips": "mean",
                "avg_distance": "mean"
            }).reset_index()

            historical_baseline.columns = ["source_region", "target_region",
                                           "historical_avg_trips", "historical_avg_distance"]

            if not streaming_aggregations.empty:
                # Get the latest time window
                latest_streaming = streaming_aggregations[
                    streaming_aggregations["time_window"] == streaming_aggregations["time_window"].max()
                    ]

                # Convert Timestamp to string BEFORE merging
                latest_streaming["time_window"] = latest_streaming["time_window"].astype(str)

                # Merge with historical baseline
                comparison = pd.merge(latest_streaming, historical_baseline,
                                      on=["source_region", "target_region"], how="left")

                # Calculate deviation
                comparison["trip_deviation_pct"] = (
                        (comparison["total_trips"] - comparison["historical_avg_trips"]) /
                        comparison["historical_avg_trips"] * 100
                )

                # Replace NaN with 0
                comparison["trip_deviation_pct"] = comparison["trip_deviation_pct"].fillna(0)

                # Convert DataFrame to serializable dict (handle all non-serializable types)
                def make_serializable(obj):
                    """Convert non-serializable objects to strings"""
                    if pd.isna(obj):  # Handle NaN
                        return None
                    elif isinstance(obj, (pd.Timestamp, datetime)):
                        return obj.isoformat()
                    elif isinstance(obj, np.integer):
                        return int(obj)
                    elif isinstance(obj, np.floating):
                        return float(obj)
                    elif isinstance(obj, np.ndarray):
                        return obj.tolist()
                    else:
                        return obj

                # Convert each record
                serializable_records = []
                for _, row in comparison.iterrows():
                    record = {}
                    for col, val in row.items():
                        record[col] = make_serializable(val)
                    serializable_records.append(record)

                # Total flights processed: check if batch_results is dict or what
                total_flights = 0
                if isinstance(batch_results, dict) and "total_records" in batch_results:
                    total_flights += batch_results["total_records"]
                
                # Create insights
                insights = {
                    "timestamp": datetime.now().isoformat(),
                    "data_sources": ["OpenSky API (real‑time)", "Historical CSV (batch)"],
                    "region_pairs_analyzed": len(comparison),
                    "total_flights_processed": total_flights + len(streaming_aggregations),
                    "comparison_results": serializable_records
                }

                # Save to JSON
                insights_path = DATA_PATHS["PROCESSED_MODELS"] / "combined_insights.json"
                with open(insights_path, "w") as f:
                    json.dump(insights, f, indent=2, default=str)  # Use default=str as fallback

                print(f"✅ Insights saved to {insights_path}")
                return insights

        except Exception as e:
            print(f"⚠ Could not combine insights: {e}")
            import traceback
            traceback.print_exc()

        return {"status": "insights_unavailable"}