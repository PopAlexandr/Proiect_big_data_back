# validate_output.py
import pandas as pd
import json
from pathlib import Path
import sys


def validate_quick_run():
    print("🔍 VALIDATING QUICK RUN OUTPUT")
    print("=" * 50)

    base_path = Path("data")

    # Check 1: Raw data exists
    print("\n1. Checking raw data...")
    csv_path = base_path / "raw" / "historical_flights.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        print(f"   ✅ CSV exists: {len(df)} rows")
        print(f"   📊 Columns: {list(df.columns)}")
        print(f"   📈 Sample:")
        print(df.head(3).to_string())
    else:
        print("   ❌ CSV file missing")

    # Check 2: Batch processed files
    print("\n2. Checking batch processed files...")
    batch_path = base_path / "processed" / "batch"

    if batch_path.exists():
        parquet_files = list(batch_path.glob("*.parquet"))
        print(f"   ✅ Found {len(parquet_files)} Parquet files")

        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                print(f"\n   📁 {file.name}:")
                print(f"      Rows: {len(df):,}")
                print(f"      Columns: {len(df.columns)}")
                print(f"      Sample columns: {list(df.columns)[:5]}")
                if len(df) > 0:
                    print(f"      First row sample: {df.iloc[0].to_dict()}")
            except Exception as e:
                print(f"   ❌ Error reading {file}: {e}")
    else:
        print("   ❌ Batch directory missing")

    # Check 3: Test OpenSky API connection
    print("\n3. Testing OpenSky API connection...")
    try:
        from opensky_client import AirTrafficDataPipeline
        pipeline = AirTrafficDataPipeline()
        realtime_data = pipeline.get_realtime_flights()

        if not realtime_data.empty:
            print(f"   ✅ API connected: {len(realtime_data)} current flights")
            print(f"   📍 Sample origin countries: {realtime_data['origin_country'].unique()[:5]}")
        else:
            print("   ⚠ API returned empty (normal if no flights in range)")
    except Exception as e:
        print(f"   ❌ API test failed: {e}")

    # Summary
    print("\n" + "=" * 50)
    print("VALIDATION COMPLETE")

    # Count total records
    total_records = 0
    if batch_path.exists():
        for file in batch_path.glob("*.parquet"):
            try:
                df = pd.read_parquet(file)
                total_records += len(df)
            except:
                pass

    print(f"\n📊 SUMMARY:")
    print(f"   • Historical CSV rows: {len(df) if 'df' in locals() else 0}")
    print(f"   • Processed Parquet files: {len(parquet_files) if 'parquet_files' in locals() else 0}")
    print(f"   • Total processed records: {total_records:,}")
    print(
        f"   • Real-time data: {'✅ Available' if 'realtime_data' in locals() and not realtime_data.empty else '⚠ Limited/None'}")

    return total_records > 0


if __name__ == "__main__":
    success = validate_quick_run()

    if success:
        print("\n🎉 QUICK RUN VALIDATION: SUCCESS!")
        print("   Your backend is working correctly.")
        print("\n   Next steps:")
        print("   1. Check the 'data/processed/batch/' folder")
        print("   2. Run full pipeline: python main.py → choose 'full'")
        print("   3. Add your real CSV data to 'data/raw/historical_flights.csv'")
    else:
        print("\n❌ QUICK RUN VALIDATION: FAILED")
        print("   Check the errors above.")

    sys.exit(0 if success else 1)