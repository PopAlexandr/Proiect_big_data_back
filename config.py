import os
from pathlib import Path

BASE_DIR = Path(__file__).parent

# OpenSky API
OPENSKY_CONFIG = {
    "BASE_URL": "https://opensky-network.org/api",
    "BEARER_TOKEN": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ0SVIwSDB0bmNEZTlKYmp4dFctWEtqZ0RYSWExNnR5eU5DWHJxUzJQNkRjIn0.eyJleHAiOjE3NjgzNDE2MjEsImlhdCI6MTc2ODMzOTgyMSwianRpIjoiYWZkODE0MzktYjA4MS00MzA2LThjNTEtZTJhYjk5NWVlN2FjIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLm9wZW5za3ktbmV0d29yay5vcmcvYXV0aC9yZWFsbXMvb3BlbnNreS1uZXR3b3JrIiwiYXVkIjpbIndlYnNpdGUtdWkiLCJhY2NvdW50Il0sInN1YiI6ImQ0MzYyMjRjLWFkMzgtNDJkZi04OTQ0LWM1ZjJkNDVlZGY1ZSIsInR5cCI6IkJlYXJlciIsImF6cCI6ImRlbmlzOTM2NS1hcGktY2xpZW50IiwiYWNyIjoiMSIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsIk9QRU5TS1lfQVBJX0RFRkFVTFQiLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtb3BlbnNreS1uZXR3b3JrIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsid2Vic2l0ZS11aSI6eyJyb2xlcyI6WyJvcGVuc2t5X3dlYnNpdGVfdXNlciJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwiY2xpZW50SWQiOiJkZW5pczkzNjUtYXBpLWNsaWVudCIsImNsaWVudEhvc3QiOiIxMDkuMTAwLjEzNi4yMDAiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsInByZWZlcnJlZF91c2VybmFtZSI6InNlcnZpY2UtYWNjb3VudC1kZW5pczkzNjUtYXBpLWNsaWVudCIsImNsaWVudEFkZHJlc3MiOiIxMDkuMTAwLjEzNi4yMDAifQ.DFyx5X61PmQW4vvXERu8JihD3jLsSqs7dlHrfQnvbTY1xj7vM3ghIO7Ur0IKPOeoffa9yDItvct_DoXGc3AZ3Q0jecl-WipRyO-rja2DVcehpheINB3UyafLSGPmL2mZLfp_I2WuUANjl5it737HSnR91WVnhERTaL2UMQG9Gv2HLe-MfqEMomiE79CyrwhyqwjuQQAsN2pGpFeDbWCLsJy3VGbrSevrluta_gSwv9qN3C5Lrk_Hlt4OepFW2pgziv_zf6729pmONgLM4GbcMV1s2EKOVsfFvZR4ERbjwdVhZT4-li0X2C_Yc-mULC9jAnE24Qjv0iwDbMdeNX837Q",
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
    "STREAMING_INTERVAL_SECONDS": 90,          # ← 10‑second refresh (OpenSky minimum)
    "STREAMING_TEST_DURATION_MINUTES": 0,      # ← 1‑minute test (set to 0 for infinite)
    "EUROPE_COUNTRIES": [
        "Germany", "France", "Italy", "Spain", "Poland",
        "Netherlands", "Belgium", "Austria", "Switzerland"
    ]
}