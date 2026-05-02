import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

CHUNK_SIZE = 50_000
output_path = "data/clean.parquet"
writer = None  # se inicializa con el primer chunk

reader = pd.read_csv(
    "data/noticias_chile_2023_2025.csv",
    chunksize=CHUNK_SIZE,
    dtype=str
)

for i, chunk in enumerate(reader):
    print(f"Procesando chunk {i}...")

    chunk = chunk.dropna(subset=["article_id", "title", "body", "publish_date"])
    chunk = chunk.drop_duplicates(subset=["article_id"])
    chunk["publish_date"] = pd.to_datetime(chunk["publish_date"], errors="coerce")
    chunk = chunk.dropna(subset=["publish_date"])
    chunk["source"] = chunk["source"].str.strip().str.lower()

    # Convertir a tabla Arrow y escribir
    table = pa.Table.from_pandas(chunk, preserve_index=False)
    if writer is None:
        writer = pq.ParquetWriter(output_path, table.schema)
    writer.write_table(table)

if writer:
    writer.close()

print("✅ clean.parquet guardado")