import os
from pathlib import Path

BASE_DIR = Path(__file__).parent

# OpenSky API
OPENSKY_CONFIG = {
    "BASE_URL": "https://opensky-network.org/api",
    "USERNAME": "",  # Optional: register at https://opensky-network.org/register
    "PASSWORD": "",
    "EUROPE_BOUNDS": {
        "lamin": 35.0,   # Southern Europe
        "lamax": 70.0,   # Northern Europe
        "lomin": -25.0,  # Western Europe
        "lomax": 40.0    # Eastern Europe
    }
}

# Data Paths
DATA_PATHS = {
    "RAW_HISTORICAL": BASE_DIR / "data" / "raw" / "historical_flights.csv",
    "PROCESSED_BATCH": BASE_DIR / "data" / "processed" / "batch",
    "PROCESSED_STREAMING": BASE_DIR / "data" / "processed" / "streaming",
    "PROCESSED_MODELS": BASE_DIR / "data" / "processed" / "models"
}

# Processing Settings – UPDATED FOR FAST STREAMING
PROCESSING_CONFIG = {
    "BATCH_INTERVAL_HOURS": 24,
    "STREAMING_INTERVAL_SECONDS": 10,          # ← 10‑second refresh (OpenSky minimum)
    "STREAMING_TEST_DURATION_MINUTES": 0,      # ← 1‑minute test (set to 0 for infinite)
    "EUROPE_COUNTRIES": [
        "Germany", "France", "Italy", "Spain", "Poland",
        "Netherlands", "Belgium", "Austria", "Switzerland"
    ]
}