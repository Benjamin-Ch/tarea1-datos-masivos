import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CHUNK_SIZE = 50_000
output_path = "data/clean.parquet"

# PASADA 1: recolectar IDs duplicados
print("Pasada 1: detectando duplicados cross-chunk...")
from collections import Counter
id_counts = Counter()

reader = pd.read_csv(
    "data/noticias_chile_2023_2025.csv",
    chunksize=CHUNK_SIZE,
    dtype=str,
    usecols=["article_id"]  # solo leer la columna necesaria, más rápido
)
for chunk in reader:
    id_counts.update(chunk["article_id"].dropna().tolist())

duplicated_ids = {id_ for id_, count in id_counts.items() if count > 1}
print(f"  IDs duplicados encontrados: {len(duplicated_ids)}")

# PASADA 2: limpiar y escribir sin duplicados
print("Pasada 2: limpiando y escribiendo...")
seen_ids = set()
writer   = None

reader = pd.read_csv(
    "data/noticias_chile_2023_2025.csv",
    chunksize=CHUNK_SIZE,
    dtype=str
)

for i, chunk in enumerate(reader):
    print(f"  Chunk {i}...")

    chunk = chunk.dropna(subset=["article_id", "title", "body", "publish_date"])
    chunk = chunk.drop_duplicates(subset=["article_id"])
    chunk["publish_date"] = pd.to_datetime(chunk["publish_date"], errors="coerce")
    chunk = chunk.dropna(subset=["publish_date"])
    chunk["source"] = chunk["source"].str.strip().str.lower()

    # Filtrar IDs ya vistos en chunks anteriores
    chunk = chunk[~chunk["article_id"].isin(seen_ids)]
    seen_ids.update(chunk["article_id"].tolist())

    table = pa.Table.from_pandas(chunk, preserve_index=False)
    if writer is None:
        writer = pq.ParquetWriter(output_path, table.schema)
    writer.write_table(table)

if writer:
    writer.close()

print(f"✅ clean.parquet guardado. Total artículos únicos: {len(seen_ids)}")