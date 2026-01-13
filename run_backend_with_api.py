"""
Runs the complete backend pipeline AND API server simultaneously.
Frontend can connect to http://localhost:8000 immediately.
"""
import threading
import time
from datetime import datetime
import uvicorn
import signal
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import json

# Import our modules
from opensky_client import AirTrafficDataPipeline
from config import DATA_PATHS

# We'll create a simple API server inline to avoid file dependencies
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ==================== HELPER FUNCTIONS ====================
def clean_dataframe_for_json(df, max_rows=100):
    """
    Clean DataFrame to make it JSON serializable:
    1. Replace NaN/Inf with None or 0
    2. Convert timestamps to strings
    3. Limit number of rows
    """
    if df.empty:
        return []

    # Make a copy to avoid modifying original
    df_clean = df.copy()

    # Limit rows
    if len(df_clean) > max_rows:
        df_clean = df_clean.head(max_rows)

    # Replace NaN and infinite values
    df_clean = df_clean.replace([np.nan, np.inf, -np.inf], None)

    # Convert datetime columns to string
    for col in df_clean.columns:
        # Check if column contains datetime objects
        if df_clean[col].dtype == 'object':
            try:
                # Try to convert to string
                df_clean[col] = df_clean[col].astype(str)
            except:
                pass
        # Also check for datetime dtype
        elif pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].astype(str)

    # Convert to list of dictionaries
    result = df_clean.to_dict(orient='records')

    # Final pass: replace any remaining NaN/None in the dict
    for record in result:
        for key, value in record.items():
            if pd.isna(value) or value is None:
                record[key] = None
            elif isinstance(value, (np.integer, np.int64)):
                record[key] = int(value)
            elif isinstance(value, (np.float64, np.float32)):
                # Check if it's a special float
                if np.isnan(value) or np.isinf(value):
                    record[key] = None
                else:
                    record[key] = float(value)

    return result

def safe_json_response(data, status_code=200):
    """Create a JSON response that handles NaN and other non-serializable values"""
    def default_serializer(obj):
        """Custom JSON serializer for non-serializable objects"""
        if pd.isna(obj):  # Handle pandas NaN
            return None
        elif isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Series):
            return obj.tolist()
        else:
            return str(obj)  # Convert anything else to string

    return JSONResponse(
        content=data,
        status_code=status_code,
        media_type="application/json"
    )

# ==================== GLOBAL STATE ====================
pipeline = None
streaming_active = False
latest_streaming_data = []
latest_realtime_flights = []
batch_data_loaded = False
insights_data = {}

# ==================== API SERVER ====================
app = FastAPI(title="Air Traffic Backend API", version="1.0")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "status": "running",
        "service": "Air Traffic Analysis Backend",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "status": "/api/status",
            "realtime": "/api/realtime",
            "streaming": "/api/streaming",
            "batch": "/api/batch/regions",
            "insights": "/api/insights",
            "docs": "/docs"
        }
    }

@app.get("/api/status")
async def get_status():
    """Get system status"""
    return {
        "backend": "running",
        "streaming_active": streaming_active,
        "batch_data_loaded": batch_data_loaded,
        "latest_streaming_count": len(latest_streaming_data),
        "latest_realtime_count": len(latest_realtime_flights),
        "timestamp": datetime.now().isoformat(),
        "refresh_rate_seconds": 10
    }

@app.get("/api/realtime")
async def get_realtime():
    """Get latest real-time flights"""
    try:
        # Ensure data is clean
        clean_data = []
        for flight in latest_realtime_flights:
            clean_flight = {}
            for key, value in flight.items():
                if pd.isna(value):
                    clean_flight[key] = None
                elif isinstance(value, (np.float64, np.float32, np.int64, np.integer)):
                    if np.isnan(value):
                        clean_flight[key] = None
                    else:
                        clean_flight[key] = float(value) if isinstance(value, (np.float64, np.float32)) else int(value)
                elif isinstance(value, (datetime, pd.Timestamp)):
                    clean_flight[key] = value.isoformat()
                else:
                    clean_flight[key] = value
            clean_data.append(clean_flight)

        return {
            "data": clean_data,
            "count": len(clean_data),
            "timestamp": datetime.now().isoformat(),
            "source": "OpenSky API"
        }
    except Exception as e:
        return {
            "data": [],
            "count": 0,
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
            "source": "OpenSky API"
        }

@app.get("/api/streaming")
async def get_streaming():
    """Get streaming aggregations"""
    try:
        clean_data = []
        for item in latest_streaming_data:
            clean_item = {}
            for key, value in item.items():
                if pd.isna(value):
                    clean_item[key] = None
                elif isinstance(value, (np.float64, np.float32, np.int64, np.integer)):
                    if np.isnan(value):
                        clean_item[key] = None
                    else:
                        clean_item[key] = float(value) if isinstance(value, (np.float64, np.float32)) else int(value)
                elif isinstance(value, (datetime, pd.Timestamp)):
                    clean_item[key] = value.isoformat()
                else:
                    clean_item[key] = value
            clean_data.append(clean_item)

        return {
            "data": clean_data,
            "count": len(clean_data),
            "timestamp": datetime.now().isoformat(),
            "window": "5-minute"
        }
    except Exception as e:
        return {
            "data": [],
            "count": 0,
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

@app.get("/api/batch/regions")
async def get_batch_regions():
    """Get batch processed data"""
    try:
        file_path = DATA_PATHS["PROCESSED_BATCH"] / "region_aggregations.parquet"
        if file_path.exists():
            df = pd.read_parquet(file_path)

            # Clean the DataFrame
            df = df.replace([np.nan, np.inf, -np.inf], None)
            df["year"] = df["year"].astype(str)

            # Convert to clean records
            records = []
            for _, row in df.iterrows():
                record = {}
                for col in df.columns:
                    val = row[col]
                    if pd.isna(val):
                        record[col] = None
                    elif isinstance(val, (np.float64, np.float32)):
                        if np.isnan(val):
                            record[col] = None
                        else:
                            record[col] = float(val)
                    elif isinstance(val, (np.int64, np.integer)):
                        record[col] = int(val)
                    else:
                        record[col] = val
                records.append(record)

            return {
                "data": records,
                "count": len(records),
                "columns": list(df.columns)
            }
        else:
            return {"data": [], "count": 0, "message": "Batch data not yet processed"}
    except Exception as e:
        return {"data": [], "count": 0, "error": str(e)}

@app.get("/api/insights")
async def get_insights():
    """Get combined insights"""
    if insights_data:
        # Ensure insights data is clean
        clean_insights = {}
        for key, value in insights_data.items():
            if isinstance(value, (datetime, pd.Timestamp)):
                clean_insights[key] = value.isoformat()
            elif isinstance(value, (list, np.ndarray, pd.Series)):
                clean_insights[key] = [
                    (None if pd.isna(x) else float(x) if isinstance(x, (np.float64, np.float32)) else x) 
                    for x in (value.tolist() if hasattr(value, 'tolist') else value)
                ]
            elif pd.isna(value):
                clean_insights[key] = None
            elif isinstance(value, dict):
                clean_dict = {}
                for k, v in value.items():
                    if pd.isna(v):
                        clean_dict[k] = None
                    elif isinstance(v, (np.float64, np.float32)):
                        if np.isnan(v):
                            clean_dict[k] = None
                        else:
                            clean_dict[k] = float(v)
                    else:
                        clean_dict[k] = v
                clean_insights[key] = clean_dict
            else:
                clean_insights[key] = value

        return clean_insights
    else:
        return {"status": "insights_not_generated_yet"}

# ==================== BACKEND FUNCTIONS ====================
def load_and_process_batch():
    """Load and process historical data"""
    global pipeline, batch_data_loaded

    print("📦 Loading and processing batch data...")
    try:
        historical_data = pipeline.load_historical_data()
        batch_results = pipeline.process_batch_data(historical_data)
        batch_data_loaded = True
        print(f"✅ Batch processing complete: {batch_results['total_records']} records")
        return batch_results
    except Exception as e:
        print(f"❌ Batch processing failed: {e}")
        return None

def update_realtime_data():
    """Fetch and update real-time flights with NaN cleaning"""
    global latest_realtime_flights, pipeline

    try:
        flights = pipeline.get_realtime_flights()
        if not flights.empty:
            # Clean NaN values immediately
            flights = flights.replace({np.nan: None})

            # Convert timestamp to string
            if 'timestamp' in flights.columns:
                flights['timestamp'] = flights['timestamp'].astype(str)

            # Limit to 50 flights and clean each record
            clean_flights = []
            for _, row in flights.head(50).iterrows():
                clean_row = {}
                for col, val in row.items():
                    if pd.isna(val):
                        clean_row[col] = None
                    elif isinstance(val, (np.float64, np.float32)):
                        if np.isnan(val):
                            clean_row[col] = None
                        else:
                            clean_row[col] = float(val)
                    elif isinstance(val, (np.int64, np.integer)):
                        clean_row[col] = int(val)
                    else:
                        clean_row[col] = val
                clean_flights.append(clean_row)

            latest_realtime_flights = clean_flights
            print(f"🔄 Updated real-time data: {len(latest_realtime_flights)} flights")
    except Exception as e:
        print(f"⚠ Real-time update failed: {e}")

def streaming_worker():
    """Background worker for continuous streaming"""
    global streaming_active, latest_streaming_data, pipeline, insights_data

    print("🚀 Starting streaming worker (10-second intervals)...")
    streaming_active = True

    batch_counter = 0
    streaming_history = []

    while streaming_active:
        try:
            batch_counter += 1

            # Fetch real-time data
            realtime_data = pipeline.get_realtime_flights()

            if not realtime_data.empty:
                # Process batch
                processed = pipeline._process_streaming_batch(realtime_data)

                # Add to history (keep last 6 batches = 1 minute)
                streaming_history.append(processed)
                if len(streaming_history) > 6:
                    streaming_history.pop(0)

                # Every 3 batches (30 seconds), update aggregations
                if batch_counter % 3 == 0 and len(streaming_history) >= 3:
                    combined = pd.concat(streaming_history[-3:], ignore_index=True)
                    aggregations = pipeline._create_streaming_aggregations(combined)

                    # Clean aggregations before storing
                    aggregations = aggregations.replace({np.nan: None})
                    if "time_window" in aggregations.columns:
                        aggregations["time_window"] = aggregations["time_window"].astype(str)

                    # Convert to clean records
                    clean_aggregations = []
                    for _, row in aggregations.iterrows():
                        clean_row = {}
                        for col, val in row.items():
                            if pd.isna(val):
                                clean_row[col] = None
                            elif isinstance(val, (np.float64, np.float32)):
                                if np.isnan(val):
                                    clean_row[col] = None
                                else:
                                    clean_row[col] = float(val)
                            elif isinstance(val, (np.int64, np.integer)):
                                clean_row[col] = int(val)
                            else:
                                clean_row[col] = val
                        clean_aggregations.append(clean_row)

                    latest_streaming_data = clean_aggregations
                    print(f"📊 Updated streaming aggregations: {len(latest_streaming_data)} region pairs")

                    # Update combined insights occasionally
                    if batch_counter % 6 == 0:  # Every minute
                        try:
                            batch_df = pd.read_parquet(DATA_PATHS["PROCESSED_BATCH"] / "region_aggregations.parquet")
                            batch_df = batch_df.replace({np.nan: None})

                            # Create simple insights without pipeline.combine_insights
                            simple_insights = {
                                "timestamp": datetime.now().isoformat(),
                                "data_sources": ["OpenSky API", "Historical CSV"],
                                "streaming_batches": batch_counter,
                                "latest_flight_count": len(realtime_data),
                                "historical_records": len(batch_df)
                            }
                            insights_data = simple_insights
                            print("💡 Updated simple insights")
                        except Exception as e:
                            print(f"⚠ Insights update failed: {e}")

                print(f"📡 Streaming batch #{batch_counter}: {len(realtime_data)} flights")

            # Wait for next interval
            time.sleep(10)  # 10-second intervals

        except Exception as e:
            print(f"⚠ Streaming error: {e}")
            time.sleep(30)

# ==================== MAIN EXECUTION ====================
def main():
    global pipeline, streaming_active

    print("=" * 60)
    print("🚀 AIR TRAFFIC BACKEND + API SERVER")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Initialize pipeline
    print("1. Initializing pipeline...")
    pipeline = AirTrafficDataPipeline()

    # Load and process batch data
    print("\n2. Processing batch data...")
    load_and_process_batch()

    # Start streaming in background thread
    print("\n3. Starting real-time streaming worker...")
    streaming_thread = threading.Thread(target=streaming_worker, daemon=True)
    streaming_thread.start()

    # Start real-time data updates (separate from streaming aggregations)
    print("4. Starting real-time data updates...")
    def update_realtime_worker():
        while True:
            update_realtime_data()
            time.sleep(30)  # Update every 30 seconds

    realtime_thread = threading.Thread(target=update_realtime_worker, daemon=True)
    realtime_thread.start()

    # Start API server
    print("\n5. Starting API server...")
    print("   🌐 API Documentation: http://localhost:8000/docs")
    print("   📡 API Base URL: http://localhost:8000")
    print("   📊 Endpoints available immediately:")
    print("      • GET /api/status")
    print("      • GET /api/realtime")
    print("      • GET /api/streaming")
    print("      • GET /api/batch/regions")
    print("      • GET /api/insights")
    print("\n" + "=" * 60)
    print("✅ Backend is running! Frontend can connect now.")
    print("   Press Ctrl+C to stop the server.")
    print("=" * 60)

    # Handle graceful shutdown
    def signal_handler(sig, frame):
        print("\n🛑 Shutting down backend...")
        streaming_active = False
        time.sleep(2)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Run API server
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")  # Changed to warning to reduce noise

if __name__ == "__main__":
    main()