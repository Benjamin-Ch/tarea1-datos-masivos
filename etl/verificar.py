import pyarrow.parquet as pq
from collections import Counter

parquet_file = pq.ParquetFile("data/clean_with_region.parquet")
id_counts = Counter()

for batch in parquet_file.iter_batches(batch_size=50_000):
    ids = batch.column("article_id").to_pylist()
    id_counts.update(ids)

duplicated = {id_: count for id_, count in id_counts.items() if count > 1}
print(f"IDs duplicados: {len(duplicated)}")
for id_, count in duplicated.items():
    print(f"  {id_}: {count} veces")