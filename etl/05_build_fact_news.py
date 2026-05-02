import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

dim_date   = pd.read_parquet("warehouse/dim_date/dim_date.parquet")
dim_source = pd.read_parquet("warehouse/dim_source/dim_source.parquet")
dim_region = pd.read_parquet("warehouse/dim_region/dim_region.parquet")

dim_date["date"] = pd.to_datetime(dim_date["date"]).dt.date

writers = {}

# Leer clean.parquet en chunks
parquet_file = pq.ParquetFile("data/clean_with_region.parquet")

for i, batch in enumerate(parquet_file.iter_batches(batch_size=50_000)):
    print(f"Procesando batch {i}...")
    chunk = batch.to_pandas()

    # Campos derivados
    chunk["body_word_count"]  = chunk["body"].str.split().str.len()
    chunk["title_word_count"] = chunk["title"].str.split().str.len()

    # Joins con dimensiones
    chunk["date"] = pd.to_datetime(chunk["publish_date"]).dt.date
    chunk = chunk.merge(dim_date[["date_id", "date"]],     on="date",   how="left")
    chunk = chunk.merge(dim_source[["source_id", "source"]], on="source", how="left")
    chunk = chunk.merge(dim_region[["region_id", "region"]], on="region", how="left")

    fact_chunk = chunk[[
        "article_id", "date_id", "source_id", "region_id",
        "title", "body", "body_word_count", "title_word_count",
        "publish_date",
    ]]

    # Escribir por partición
    for (year, month), group in fact_chunk.groupby([
        fact_chunk["publish_date"].dt.year,
        fact_chunk["publish_date"].dt.month,
    ]):
        path = f"warehouse/fact_news/year={int(year)}/month={int(month):02d}"
        os.makedirs(path, exist_ok=True)
        filepath = f"{path}/data.parquet"

        table = pa.Table.from_pandas(
            group.drop(columns=["publish_date"]),
            preserve_index=False
        )

        key = (int(year), int(month))
        if key not in writers:
            writers[key] = pq.ParquetWriter(filepath, table.schema)
        writers[key].write_table(table)

for writer in writers.values():
    writer.close()

print("✅ fact_news particionado guardado")