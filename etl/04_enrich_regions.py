import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
from unidecode import unidecode
from region_aliases import REGION_ALIASES


# Precompilar patrones para velocidad
COMPILED = {
    region: re.compile("|".join(aliases), re.IGNORECASE)
    for region, aliases in REGION_ALIASES.items()
    if aliases
}

def normalize(text):
    return unidecode(str(text)).lower()

def detect_region(title, body):
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

# Leer y escribir en chunks, agregando columna region
writer = None
parquet_file = pq.ParquetFile("data/clean.parquet")

for i, batch in enumerate(parquet_file.iter_batches(batch_size=50_000)):
    print(f"Batch {i}...")
    chunk = batch.to_pandas()

    chunk["region"] = [
        detect_region(t, b)
        for t, b in zip(chunk["title"], chunk["body"])
    ]

    table = pa.Table.from_pandas(chunk, preserve_index=False)
    if writer is None:
        writer = pq.ParquetWriter("data/clean_with_region.parquet", table.schema)
    writer.write_table(table)

if writer:
    writer.close()

print("✅ clean_with_region.parquet guardado")