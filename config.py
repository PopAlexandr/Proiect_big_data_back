from pathlib import Path

BASE_DIR = Path(__file__).parent

# OpenSky API
OPENSKY_CONFIG = {
    "BASE_URL": "https://opensky-network.org/api",
    "BEARER_TOKEN": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ0SVIwSDB0bmNEZTlKYmp4dFctWEtqZ0RYSWExNnR5eU5DWHJxUzJQNkRjIn0.eyJleHAiOjE3Njg1MDc5NzAsImlhdCI6MTc2ODUwNjE3MCwianRpIjoiMDM0N2Y2MDgtMDQyNy00Y2I1LWE0YWUtZjM3YWQxMGE4OTIwIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLm9wZW5za3ktbmV0d29yay5vcmcvYXV0aC9yZWFsbXMvb3BlbnNreS1uZXR3b3JrIiwiYXVkIjpbIndlYnNpdGUtdWkiLCJhY2NvdW50Il0sInN1YiI6ImQ0MzYyMjRjLWFkMzgtNDJkZi04OTQ0LWM1ZjJkNDVlZGY1ZSIsInR5cCI6IkJlYXJlciIsImF6cCI6ImRlbmlzOTM2NS1hcGktY2xpZW50IiwiYWNyIjoiMSIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsIk9QRU5TS1lfQVBJX0RFRkFVTFQiLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtb3BlbnNreS1uZXR3b3JrIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsid2Vic2l0ZS11aSI6eyJyb2xlcyI6WyJvcGVuc2t5X3dlYnNpdGVfdXNlciJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwiY2xpZW50SWQiOiJkZW5pczkzNjUtYXBpLWNsaWVudCIsImNsaWVudEhvc3QiOiIxODguMjcuMTMwLjE1MyIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwicHJlZmVycmVkX3VzZXJuYW1lIjoic2VydmljZS1hY2NvdW50LWRlbmlzOTM2NS1hcGktY2xpZW50IiwiY2xpZW50QWRkcmVzcyI6IjE4OC4yNy4xMzAuMTUzIn0.dSJW54v6dSdN3Oo3tekg7c89p6SNDo0xJfKGsnCt8NFS2IcI8W29oTD5jObrduOgM1sh1m8SPF78SaoG2OltDoYt6MJW_4nzV4k4AVQhnLN4Sc2dE8At2qXo7mTg0MXrfS7gW5jqP_Bty5J_10mnnYP7tTblOqDS0QN64Qu779MtLO0dF-Fj90itRU30pW0P0Iy7rb-blh0nKvduZqh5qrG_lHbYGuHWrZ-xc27780NCcd8a9DvjIMZeckPVWgG-d1t2yNJOnUchKosai1fKIK4zm0ki9aYZ6ngqqQJwF3vqpSBCySab9nTxAbykfZ1oah4C_xNr2UyBmL8jHbYHxA",
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

# Processing Settings
PROCESSING_CONFIG = {
    "BATCH_INTERVAL_HOURS": 24,
    "STREAMING_INTERVAL_SECONDS": 90,
    "STREAMING_TEST_DURATION_MINUTES": 0,      # Set to 0 for infinite
    "EUROPE_COUNTRIES": [
        "Germany", "France", "Italy", "Spain", "Poland",
        "Netherlands", "Belgium", "Austria", "Switzerland"
    ]
}
