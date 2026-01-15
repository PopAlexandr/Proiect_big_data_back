from pathlib import Path

BASE_DIR = Path(__file__).parent

# OpenSky API
OPENSKY_CONFIG = {
    "BASE_URL": "https://opensky-network.org/api",
    "BEARER_TOKEN": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ0SVIwSDB0bmNEZTlKYmp4dFctWEtqZ0RYSWExNnR5eU5DWHJxUzJQNkRjIn0.eyJleHAiOjE3Njg1MTA5MDksImlhdCI6MTc2ODUwOTEwOSwianRpIjoiOGYxNzI1OGItODkwNi00ZGVjLWE2ODItZTczNzdhMDQyNTQ3IiwiaXNzIjoiaHR0cHM6Ly9hdXRoLm9wZW5za3ktbmV0d29yay5vcmcvYXV0aC9yZWFsbXMvb3BlbnNreS1uZXR3b3JrIiwiYXVkIjpbIndlYnNpdGUtdWkiLCJhY2NvdW50Il0sInN1YiI6IjI1Nzg2YTc2LWQ2ZGItNGRhNy04MDZmLTcxNDI4YmQyMDlkYyIsInR5cCI6IkJlYXJlciIsImF6cCI6ImRfMV9zZW5zb3ItYXBpLWNsaWVudCIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJPUEVOU0tZX0FQSV9ERUZBVUxUIiwidW1hX2F1dGhvcml6YXRpb24iLCJkZWZhdWx0LXJvbGVzLW9wZW5za3ktbmV0d29yayJdfSwicmVzb3VyY2VfYWNjZXNzIjp7IndlYnNpdGUtdWkiOnsicm9sZXMiOlsib3BlbnNreV93ZWJzaXRlX3VzZXIiXX0sImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoicHJvZmlsZSBlbWFpbCIsImNsaWVudElkIjoiZF8xX3NlbnNvci1hcGktY2xpZW50IiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJjbGllbnRIb3N0IjoiMTg4LjI3LjEzMC4xNTMiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtZF8xX3NlbnNvci1hcGktY2xpZW50IiwiY2xpZW50QWRkcmVzcyI6IjE4OC4yNy4xMzAuMTUzIn0.QqmKd9InbcXAT5fofntq8e77nbpjNP1UwhcFPzY9loShchdFUgmQOQ2jYYUvUYWMvhQkXhF2OXmCWCMGY7TuMJGoj5i0WapBzRQ_S1cLkyjp2cqKVzhEJDniSczm9WASL2q0ZfyPZJYCR2iMX2muHtSvDds3M-HmbIT3uWV0If0fVEjrtQWfody8Tq6XkqcMtnfTOuFHne16SY-odXInu3vlZVCQ523u7MeynBZF-1dp50dgsWhuBIbe3Wzr9fHzryQdXgci9oaeOdYyvWfZpDOxzbauh236sjbvXdNQ11zdHAywoSk6t2FdO3ZLOG92crL8JVo_jdZZzoh-LZ0Juw",
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
