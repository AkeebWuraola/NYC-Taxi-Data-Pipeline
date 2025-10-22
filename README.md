# NYC-Taxi-Data-Pipeline
flowchart TD
  subgraph SOURCE
    A[NYC TLC trip parquet files (monthly) - local after download]
  end

  subgraph RAW["Raw (Bronze) - raw_taxi_raw"]
    B[raw_taxi_raw]
  end

  subgraph METADATA["Metadata / Control"]
    M[load_metadata]
  end

  A -->|CSV copy / COPY FROM| B
  B -->|Clean, standardize, dedupe| S[Silver: taxi_silver]
  S -->|Aggregate monthly / incremental| G[Gold: taxi_monthly_agg]
  M -- tracks --> B
  M -- last_loaded_month used by --> A
