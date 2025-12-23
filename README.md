# Alpha Police Events — Automated Dual-Cloud Data Pipeline
This project demonstrates a production-style data ingestion pipeline that collects police event data from a public ArcGIS Feature Service that exposes only a 28-day rolling window of records. To prevent data loss and enable longer-term analysis, the pipeline preserves dated snapshots and incrementally builds a deduplicated historical master dataset.

The system runs automatically on a weekly schedule and stores outputs independently in Amazon S3 and Google Cloud Storage, using Parquet for analytics and CSV for accessibility. The pipeline is fully automated with GitHub Actions, uses cloud-native object storage, and applies deterministic deduplication to handle overlapping source data.

The design emphasizes reproducibility, durability, and analytics-ready data engineering practices across cloud platforms.
