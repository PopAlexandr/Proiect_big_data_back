"""
MAIN EXECUTION FILE – Updated to skip Spark and use fast streaming.
"""
from datetime import datetime
import sys
from opensky_client import AirTrafficDataPipeline

def main():
    print("=" * 60)
    print("AIR TRAFFIC & MOBILITY ANALYSIS PIPELINE (FAST STREAMING)")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Step 1: Initialize pipeline
    print("1. Initializing pipeline...")
    pipeline = AirTrafficDataPipeline()

    # Step 2: Load and process historical data (BATCH)
    print("\n2. Processing historical data (BATCH)...")
    historical_data = pipeline.load_historical_data()
    batch_results = pipeline.process_batch_data(historical_data)
    print(f"   ✅ Processed {batch_results['total_records']} historical records")

    # Step 3: Skip Spark (Python 3.12 compatibility)
    print("\n3. Skipping Spark (Python 3.12 compatibility)...")
    print("   ⚠ Spark disabled to avoid 'distutils' error. Using pure pandas.")

    # Step 4: Run real‑time streaming (FAST REFRESH)
    print("\n4. Starting real‑time streaming (10‑second refresh)...")
    print("   This will run for 1 minute (adjust in config.py).")
    print("   Press Ctrl+C to stop early.\n")

    streaming_results = pipeline.run_streaming(duration_minutes=1)  # 1‑minute test

    # Step 5: Combine insights
    print("\n5. Combining batch and streaming insights...")
    combined_insights = pipeline.combine_insights(batch_results, streaming_results)

    # Step 6: Final summary
    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION COMPLETE")
    print("=" * 60)
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if isinstance(combined_insights, dict) and "region_pairs_analyzed" in combined_insights:
        print(f"\n📊 RESULTS:")
        print(f"  • Data sources integrated: {len(combined_insights['data_sources'])}")
        print(f"  • Region pairs analyzed: {combined_insights['region_pairs_analyzed']}")
        print(f"  • Total flights processed: {combined_insights['total_flights_processed']}")
        print(f"  • Processing types: Batch + Real‑time streaming (10‑second refresh)")

    print("\n✅ Pipeline executed successfully!")
    print("\n📁 Output files:")
    print("   data/processed/batch/        – historical aggregations")
    print("   data/processed/streaming/    – real‑time flights & aggregations")
    print("   data/processed/models/       – combined insights JSON")

def quick_test():
    """Quick test without streaming wait."""
    print("Running quick test (batch only)...")
    pipeline = AirTrafficDataPipeline()
    historical_data = pipeline.load_historical_data()
    batch_results = pipeline.process_batch_data(historical_data)
    print(f"\n✅ Quick test passed: {batch_results['total_records']} records processed.")
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        quick_test()
    else:
        main()