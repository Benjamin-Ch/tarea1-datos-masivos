import pandas as pd
import pyarrow.parquet as pq
import os
import glob

print("=== Validaciones del Data Warehouse ===\n")

dim_date   = pd.read_parquet("warehouse/dim_date/dim_date.parquet")
dim_source = pd.read_parquet("warehouse/dim_source/dim_source.parquet")
dim_region = pd.read_parquet("warehouse/dim_region/dim_region.parquet")

fact_files = glob.glob("warehouse/fact_news/**/*.parquet", recursive=True)

valid_date_ids   = set(dim_date["date_id"])
valid_source_ids = set(dim_source["source_id"])
valid_region_ids = set(dim_region["region_id"])

orphan_dates   = 0
orphan_sources = 0
orphan_regions = 0
fact_row_count = 0
all_article_ids = set()
dupe_count = 0
v4_errors = []

for f in fact_files:
    chunk = pd.read_parquet(f, columns=["article_id", "date_id", "source_id", "region_id"])

    # V1 - Consistencia referencial
    orphan_dates   += (~chunk["date_id"].isin(valid_date_ids)).sum()
    orphan_sources += (~chunk["source_id"].isin(valid_source_ids)).sum()
    orphan_regions += (~chunk["region_id"].isin(valid_region_ids)).sum()

    # V2 - Conteo de filas
    fact_row_count += len(chunk)

    # V4 - Particiones correctas
    parts = f.split(os.sep)
    expected_year  = int([p for p in parts if p.startswith("year=")][0].split("=")[1])
    expected_month = int([p for p in parts if p.startswith("month=")][0].split("=")[1])
    chunk_with_date = chunk.merge(dim_date[["date_id", "year", "month"]], on="date_id")
    bad = chunk_with_date[
        (chunk_with_date["year"] != expected_year) |
        (chunk_with_date["month"] != expected_month)
    ]
    if len(bad) > 0:
        v4_errors.append(f)

    # V5 - Duplicados de article_id
    ids = set(chunk["article_id"])
    dupes_in_chunk = chunk["article_id"].duplicated().sum()
    cross_dupes = len(ids & all_article_ids)  # IDs que ya vimos en chunks anteriores
    dupe_count += dupes_in_chunk + cross_dupes
    all_article_ids.update(ids)

raw_file = pq.ParquetFile("data/clean_with_region.parquet")
raw_row_count = raw_file.metadata.num_rows

# Reportes 
print(f"V1 - FK huérfanas (date):   {orphan_dates}")
print(f"V1 - FK huérfanas (source): {orphan_sources}")
print(f"V1 - FK huérfanas (region): {orphan_regions}")
assert orphan_dates == 0 and orphan_sources == 0 and orphan_regions == 0, "V1 falló"

print(f"\nV2 - Filas raw:  {raw_row_count}")
print(f"V2 - Filas fact: {fact_row_count}")
assert raw_row_count == fact_row_count, "V2 falló: diferencia en conteo de filas"

print("\nV3 - Sin duplicados en dimensiones:")
assert dim_date["date_id"].nunique()     == len(dim_date),   "V3 falló: duplicado en dim_date"
assert dim_source["source_id"].nunique() == len(dim_source), "V3 falló: duplicado en dim_source"
assert dim_region["region_id"].nunique() == len(dim_region), "V3 falló: duplicado en dim_region"
print("V3 - OK")

print(f"\nV4 - Particiones incorrectas: {len(v4_errors)}")
assert len(v4_errors) == 0, f"V4 falló en: {v4_errors}"
print("V4 - OK")

print(f"\nV5 - article_id duplicados: {dupe_count}")
assert dupe_count == 0, "V5 falló: hay article_ids duplicados"

print("\n✅ Todas las validaciones pasaron.")