import requests
import pandas as pd
import numpy as np
import time
import random
from datetime import datetime, timedelta
import json
from pathlib import Path
from config import OPENSKY_CONFIG, DATA_PATHS, PROCESSING_CONFIG


class AirTrafficDataPipeline:
    def __init__(self):
        self.base_url = OPENSKY_CONFIG["BASE_URL"]
        self.europe_bounds = OPENSKY_CONFIG["EUROPE_BOUNDS"]
        self.bearer_token = OPENSKY_CONFIG.get("BEARER_TOKEN")
        # Create directories
        for path in DATA_PATHS.values():
            if isinstance(path, Path):
                path.parent.mkdir(parents=True, exist_ok=True)

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

    # ========== HISTORICAL DATA (unchanged) ==========
    def load_historical_data(self):
        """Load CSV with historical flight data."""
        try:
            df = pd.read_csv(DATA_PATHS["RAW_HISTORICAL"])
            print(f"Loaded historical data: {len(df)} rows")
            return df
        except:
            print("Historical file not found, creating sample data...")
            return self._create_sample_historical_data()

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

    # ========== BATCH PROCESSING (pure pandas) ==========
    def process_batch_data(self, historical_df):
        """Process historical data with batch analytics (no Spark)."""
        print("Processing batch data...")

        # 1. Region‑level aggregations
        region_agg = historical_df.groupby(["source_region", "target_region", "year"]).agg({
            "estimated_trips": ["sum", "mean", "count"],
            "dist": ["mean", "std"]
        }).reset_index()

        region_agg.columns = ["source_region", "target_region", "year",
                              "total_trips", "avg_trips", "route_count",
                              "avg_distance", "std_distance"]

        # 2. Country‑level aggregations
        country_agg = historical_df.groupby(["source_iso3", "target_iso3", "year"]).agg({
            "estimated_trips": "sum",
            "dist": "mean"
        }).reset_index()

        country_agg.columns = ["source_iso3", "target_iso3", "year",
                               "total_trips", "avg_distance"]

        # 3. Mobility density (trips per km)
        historical_df["trips_per_km"] = historical_df["estimated_trips"] / historical_df["dist"]

        # Save results
        region_agg.to_parquet(DATA_PATHS["PROCESSED_BATCH"] / "region_aggregations.parquet")
        country_agg.to_parquet(DATA_PATHS["PROCESSED_BATCH"] / "country_aggregations.parquet")
        historical_df.to_parquet(DATA_PATHS["PROCESSED_BATCH"] / "mobility_density.parquet")

        print(f"Batch processing complete. Saved to {DATA_PATHS['PROCESSED_BATCH']}")

        return {
            "region_aggregations": region_agg,
            "country_aggregations": country_agg,
            "mobility_density": historical_df,
            "total_records": len(historical_df)
        }

    # ========== STREAMING PROCESSING (FAST REFRESH) ==========
    def run_streaming(self, duration_minutes=None):
        """
        Real‑time data collection and processing.
        If duration_minutes is None or 0, runs indefinitely until KeyboardInterrupt.
        """
        if duration_minutes is None:
            duration_minutes = PROCESSING_CONFIG["STREAMING_TEST_DURATION_MINUTES"]

        print(f"Starting streaming pipeline (refresh every {PROCESSING_CONFIG['STREAMING_INTERVAL_SECONDS']}s)...")

        if duration_minutes > 0:
            print(f"Will run for {duration_minutes} minute(s). Press Ctrl+C to stop early.")
            end_time = datetime.now() + timedelta(minutes=duration_minutes)
        else:
            print("Running indefinitely. Press Ctrl+C to stop.")
            end_time = None

        all_streaming_data = []
        batch_counter = 0

        try:
            while end_time is None or datetime.now() < end_time:
                batch_counter += 1
                print(f"\n📡 Streaming batch #{batch_counter}...")

                # Fetch real‑time data
                realtime_data = self.get_realtime_flights()

                if not realtime_data.empty:
                    # Process this batch
                    processed = self._process_streaming_batch(realtime_data)

                    # Save with timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    file_name = DATA_PATHS["PROCESSED_STREAMING"] / f"batch_{timestamp}.parquet"
                    processed.to_parquet(file_name, index=False)

                    all_streaming_data.append(processed)
                    print(f"   ✅ {len(processed)} flights saved to {file_name.name}")

                    # Create rolling aggregation (last 5 minutes)
                    if len(all_streaming_data) >= 3:
                        latest_aggregations = self._create_streaming_aggregations(
                            pd.concat(all_streaming_data[-3:], ignore_index=True)
                        )
                        latest_aggregations.to_parquet(
                            DATA_PATHS["PROCESSED_STREAMING"] / "latest_aggregations.parquet"
                        )
                        print(f"   📊 Updated rolling aggregations ({len(latest_aggregations)} region pairs)")
                else:
                    print("   ⚠ No flights in European airspace right now.")

                # Wait for next interval
                time.sleep(PROCESSING_CONFIG["STREAMING_INTERVAL_SECONDS"])

        except KeyboardInterrupt:
            print("\n🛑 Streaming stopped by user.")

        # Combine all streaming data if any
        if all_streaming_data:
            full_stream = pd.concat(all_streaming_data, ignore_index=True)
            full_stream.to_parquet(DATA_PATHS["PROCESSED_STREAMING"] / "full_streaming_data.parquet")

            # Final aggregations
            aggregations = self._create_streaming_aggregations(full_stream)
            aggregations.to_parquet(DATA_PATHS["PROCESSED_STREAMING"] / "streaming_aggregations.parquet")

            print(f"\n📦 Streaming summary: {len(full_stream)} total flights, {len(aggregations)} aggregated windows")
            return aggregations

        return pd.DataFrame()

    def _process_streaming_batch(self, batch_df):
        """Process a single streaming batch."""
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
        """Create 5‑minute window aggregations from streaming data."""
        if streaming_df.empty:
            return pd.DataFrame()

        streaming_df["timestamp"] = pd.to_datetime(streaming_df["timestamp"])
        streaming_df["time_window"] = streaming_df["timestamp"].dt.floor('5min')

        aggregations = streaming_df.groupby(["time_window", "source_region", "target_region"]).agg({
            "estimated_trips": "sum",
            "speed_kmh": "mean",
            "icao24": "count"
        }).reset_index()

        aggregations.columns = ["time_window", "source_region", "target_region",
                                "total_trips", "avg_speed_kmh", "flight_count"]

        return aggregations

    # ========== COMBINE BATCH & STREAMING ==========
    def combine_insights(self, batch_results, streaming_aggregations):
        """Combine batch historical data with real‑time streaming insights."""
        print("Creating combined insights...")

        try:
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

                # Create insights
                insights = {
                    "timestamp": datetime.now().isoformat(),
                    "data_sources": ["OpenSky API (real‑time)", "Historical CSV (batch)"],
                    "region_pairs_analyzed": len(comparison),
                    "total_flights_processed": batch_results["total_records"] + len(streaming_aggregations),
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