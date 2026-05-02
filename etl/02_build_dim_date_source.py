import pandas as pd
import os

# Crear carpetas si no existen
os.makedirs("warehouse/dim_date",   exist_ok=True)
os.makedirs("warehouse/dim_source", exist_ok=True)


df = pd.read_parquet("data/clean.parquet")

# dim date
dates = df["publish_date"].dt.date.unique()
dates = pd.to_datetime(dates).sort_values()

dim_date = pd.DataFrame({
    "date_id":    range(1, len(dates) + 1),
    "date":       dates.date,
    "year":       dates.year,
    "month":      dates.month,
    "day":        dates.day,
    "day_of_week": dates.day_name(),
    "quarter":    dates.quarter,
    "is_weekend": dates.day_of_week >= 5,
})

dim_date.to_parquet("warehouse/dim_date/dim_date.parquet", index=False)

# dim source
sources = df["source"].dropna().unique()
sources = sorted(sources)

dim_source = pd.DataFrame({
    "source_id": range(1, len(sources) + 1),
    "source":    sources,
})

dim_source.to_parquet("warehouse/dim_source/dim_source.parquet", index=False)

