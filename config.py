import os
from pathlib import Path

BASE_DIR = Path(__file__).parent

# OpenSky API
OPENSKY_CONFIG = {
    "BASE_URL": "https://opensky-network.org/api",
    "BEARER_TOKEN": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ0SVIwSDB0bmNEZTlKYmp4dFctWEtqZ0RYSWExNnR5eU5DWHJxUzJQNkRjIn0.eyJleHAiOjE3Njg1MDQ5MTAsImlhdCI6MTc2ODUwMzExMCwianRpIjoiNjA2NWI5ZjAtM2E4NS00MWE0LTkxZTItOTczNTU1N2M1ZjJjIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLm9wZW5za3ktbmV0d29yay5vcmcvYXV0aC9yZWFsbXMvb3BlbnNreS1uZXR3b3JrIiwiYXVkIjpbIndlYnNpdGUtdWkiLCJhY2NvdW50Il0sInN1YiI6ImQ0MzYyMjRjLWFkMzgtNDJkZi04OTQ0LWM1ZjJkNDVlZGY1ZSIsInR5cCI6IkJlYXJlciIsImF6cCI6ImRlbmlzOTM2NS1hcGktY2xpZW50IiwiYWNyIjoiMSIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsIk9QRU5TS1lfQVBJX0RFRkFVTFQiLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtb3BlbnNreS1uZXR3b3JrIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsid2Vic2l0ZS11aSI6eyJyb2xlcyI6WyJvcGVuc2t5X3dlYnNpdGVfdXNlciJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwiY2xpZW50SWQiOiJkZW5pczkzNjUtYXBpLWNsaWVudCIsImNsaWVudEhvc3QiOiIxODguMjcuMTMwLjE1MyIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwicHJlZmVycmVkX3VzZXJuYW1lIjoic2VydmljZS1hY2NvdW50LWRlbmlzOTM2NS1hcGktY2xpZW50IiwiY2xpZW50QWRkcmVzcyI6IjE4OC4yNy4xMzAuMTUzIn0.SJVroCQ9oeVQHlekeijqAybNbFVMRPPviDBjvdEbdsemJ3ij4SYEpGuQoydJjhha5DsNHBSnoo6qtbrFHenc9R6uzNl3DKRVl01dr2vAe019IXB_40R5lqtZ5s5GsCK3IrOJV7GQboDbgoh98lswH60eoAHu9sTeqAVCKR2-lZJ9ic_Z2r8zbbKLvJSrMhmR4lpgEbvcMRJOiaAujucA4nPgY1Pr7BqroAashIJ7LNFwPiVRT9ymTai68xslOYtCBddc1So_KqYPDL1R0wplsYxW_a2uTa9nZYjux1o_MvD-p9wTGacEPLlnCjvdK9DWnj_Rbmc2ku-2g91K5twM5A",
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