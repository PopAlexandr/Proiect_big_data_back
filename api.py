# api.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import json
from datetime import datetime
import uvicorn
from pathlib import Path
from opensky_client import AirTrafficDataPipeline
from config import DATA_PATHS
import threading
import time

app = FastAPI(title="Air Traffic Analysis API")

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables for streaming state
streaming_active = False
latest_streaming_data = None
pipeline = AirTrafficDataPipeline()


@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    # Load batch data on startup
    if (DATA_PATHS["PROCESSED_BATCH"] / "region_aggregations.parquet").exists():
        print("✅ API: Batch data loaded")
    else:
        print("⚠ API: Batch data not found, will process on demand")


@app.get("/")
async def root():
    return {
        "name": "Air Traffic Mobility API",
        "version": "1.0",
        "endpoints": {
            "batch": "/api/batch/regions",
            "streaming": "/api/streaming/latest",
            "insights": "/api/insights",
            "realtime": "/api/realtime/current",
            "start_streaming": "/api/streaming/start",
            "stop_streaming": "/api/streaming/stop"
        }
    }


@app.get("/api/batch/regions")
async def get_batch_regions(limit: int = 100):
    """Get batch processed region aggregations"""
    try:
        file_path = DATA_PATHS["PROCESSED_BATCH"] / "region_aggregations.parquet"
        if not file_path.exists():
            raise HTTPException(404, "Batch data not found")

        df = pd.read_parquet(file_path)

        # Convert to JSON-serializable format
        df["year"] = df["year"].astype(str)
        data = df.head(limit).to_dict(orient="records")

        return {
            "data": data,
            "total": len(df),
            "columns": list(df.columns),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(500, f"Error loading batch data: {str(e)}")


#do not use
# @app.get("/api/batch/countries")
# async def get_batch_countries():
#     """Get batch processed country aggregations"""
#     try:
#         file_path = DATA_PATHS["PROCESSED_BATCH"] / "country_aggregations.parquet"
#         if not file_path.exists():
#             raise HTTPException(404, "Country data not found")

#         df = pd.read_parquet(file_path)
#         df["year"] = df["year"].astype(str)

#         return {
#             "data": df.to_dict(orient="records"),
#             "total": len(df)
#         }
#     except Exception as e:
#         raise HTTPException(500, f"Error loading country data: {str(e)}")


@app.get("/api/streaming/latest")
async def get_latest_streaming():
    """Get latest streaming aggregations (updated every 10-30s)"""
    global latest_streaming_data

    try:
        file_path = DATA_PATHS["PROCESSED_STREAMING"] / "latest_aggregations.parquet"

        if file_path.exists():
            df = pd.read_parquet(file_path)
            # Convert Timestamp to string
            if "time_window" in df.columns:
                df["time_window"] = df["time_window"].astype(str)

            latest_streaming_data = df.to_dict(orient="records")

        return {
            "data": latest_streaming_data or [],
            "streaming_active": streaming_active,
            "timestamp": datetime.now().isoformat(),
            "refresh_interval": 10  # seconds
        }
    except Exception as e:
        raise HTTPException(500, f"Error loading streaming data: {str(e)}")


@app.get("/api/realtime/current")
async def get_current_realtime():
    """Get current real-time flights from OpenSky API (live call)"""
    try:
        # Fetch fresh data from OpenSky
        current_data = pipeline.get_realtime_flights()

        if current_data.empty:
            return {
                "data": [],
                "total_flights": 0,
                "timestamp": datetime.now().isoformat()
            }

        # Convert to JSON-serializable
        current_data["timestamp"] = current_data["timestamp"].astype(str)
        flights = current_data.head(100).to_dict(orient="records")  # Limit to 100 flights

        return {
            "data": flights,
            "total_flights": len(current_data),
            "timestamp": datetime.now().isoformat(),
            "regions_present": current_data["source_region"].unique().tolist()
        }
    except Exception as e:
        raise HTTPException(500, f"Error fetching real-time data: {str(e)}")


@app.get("/api/insights")
async def get_insights():
    """Get combined insights (from JSON file)"""
    try:
        file_path = DATA_PATHS["PROCESSED_MODELS"] / "combined_insights.json"

        if not file_path.exists():
            raise HTTPException(404, "Insights not generated yet")

        with open(file_path, 'r') as f:
            insights = json.load(f)

        return insights
    except Exception as e:
        raise HTTPException(500, f"Error loading insights: {str(e)}")


@app.post("/api/streaming/start")
async def start_streaming():
    """Start the real-time streaming pipeline"""
    global streaming_active

    if streaming_active:
        return {"status": "already_running", "message": "Streaming is already active"}

    def run_streaming_background():
        global streaming_active
        streaming_active = True

        # Run streaming indefinitely (until stopped)
        # Pass a callback to check if streaming should stop (when streaming_active becomes False)
        pipeline.run_streaming(
            duration_minutes=0, 
            stop_check_callback=lambda: not streaming_active
        ) 

        streaming_active = False

    # Start streaming in background thread
    thread = threading.Thread(target=run_streaming_background, daemon=True)
    thread.start()

    return {
        "status": "started",
        "message": "Streaming pipeline started",
        "refresh_rate": "10 seconds",
        "start_time": datetime.now().isoformat()
    }


@app.post("/api/streaming/stop")
async def stop_streaming():
    """Stop the real-time streaming pipeline"""
    global streaming_active

    # Note: In current implementation, you need to stop via Ctrl+C
    # This would require a more sophisticated stop mechanism
    streaming_active = False

    return {
        "status": "stopping",
        "message": "Streaming will stop after current iteration",
        "stop_time": datetime.now().isoformat()
    }


@app.get("/api/export/batch/csv")
async def export_batch_csv():
    """Export batch data as CSV for frontend download"""
    try:
        file_path = DATA_PATHS["PROCESSED_BATCH"] / "region_aggregations.parquet"
        if not file_path.exists():
            raise HTTPException(404, "Data not found")

        df = pd.read_parquet(file_path)
        csv_path = DATA_PATHS["PROCESSED_BATCH"] / "export.csv"
        df.to_csv(csv_path, index=False)

        return FileResponse(
            path=csv_path,
            filename="air_traffic_batch_data.csv",
            media_type="text/csv"
        )
    except Exception as e:
        raise HTTPException(500, f"Error exporting CSV: {str(e)}")


@app.get("/api/status")
async def get_status():
    """Get system status"""
    files_exist = {
        "batch_regions": (DATA_PATHS["PROCESSED_BATCH"] / "region_aggregations.parquet").exists(),
        "batch_countries": (DATA_PATHS["PROCESSED_BATCH"] / "country_aggregations.parquet").exists(),
        "streaming_data": (DATA_PATHS["PROCESSED_STREAMING"] / "latest_aggregations.parquet").exists(),
        "insights": (DATA_PATHS["PROCESSED_MODELS"] / "combined_insights.json").exists(),
    }

    return {
        "status": "operational",
        "timestamp": datetime.now().isoformat(),
        "streaming_active": streaming_active,
        "files_available": files_exist,
        "data_sources": ["OpenSky API", "Historical CSV"],
        "api_version": "1.0"
    }


if __name__ == "__main__":
    print("🚀 Starting Air Traffic API Server...")
    print(f"📊 API available at: http://localhost:8000")
    print(f"📚 Documentation: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)