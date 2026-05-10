import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
from unidecode import unidecode
from multiprocessing import Pool, cpu_count
from region_aliases import REGION_ALIASES

# Precompilar patrones (debe estar en el módulo, no en función,
# para que multiprocessing pueda serializarlo)
# Referencias: https://superfastpython.com/multiprocessing-pool-map-chunksize/
COMPILED = {
    region: re.compile("|".join(aliases), re.IGNORECASE)
    for region, aliases in REGION_ALIASES.items()
    if aliases
}

def normalize(text):
    return unidecode(str(text)).lower()

def detect_region(args):
    """Map: (title, body) → region"""
    title, body = args
    title_norm = normalize(title)
    body_norm  = normalize(body)
    scores = {}
    for region, pattern in COMPILED.items():
        title_hits = len(pattern.findall(title_norm))
        body_hits  = len(pattern.findall(body_norm))
        total = title_hits * 2 + body_hits
        if total > 0:
            scores[region] = total
    if not scores:
        return "Desconocida"
    return max(scores, key=scores.get)

def process_batch(batch_df):
    """
    MAP:    lista de (title, body) → lista de regiones (en paralelo)
    REDUCE: agregar columna 'region' al DataFrame
    """
    pairs = list(zip(batch_df["title"], batch_df["body"]))

    # Map paralelo: cada worker procesa un subconjunto de filas
    with Pool(processes=cpu_count()) as pool:
        regions = pool.map(detect_region, pairs, chunksize=500)

    # Reduce: adjuntar resultado al DataFrame
    batch_df["region"] = regions
    return batch_df

if __name__ == "__main__":
    BATCH_SIZE = 50_000
    writer = None
    parquet_file = pq.ParquetFile("data/clean.parquet")

    for i, batch in enumerate(parquet_file.iter_batches(batch_size=BATCH_SIZE)):
        print(f"Batch {i} ({BATCH_SIZE * i}–{BATCH_SIZE * (i+1)})...")
        chunk = batch.to_pandas()

        # Map + Reduce
        chunk = process_batch(chunk)

        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter("data/clean_with_region.parquet", table.schema)
        writer.write_table(table)

    if writer:
        writer.close()

    print("✅ clean_with_region.parquet guardado")