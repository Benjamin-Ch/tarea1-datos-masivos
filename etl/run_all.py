import subprocess, sys

scripts = [
    "etl/01_clean.py",
    "etl/02_build_dim_date_source.py",
    "etl/03_build_dim_region.py",
    "etl/04_enrich_regions.py",
    "etl/05_build_fact_news.py",
    "etl/06_validate_warehouse.py",
]

for script in scripts:
    print(f"Ejecutando {script}...")
    result = subprocess.run([sys.executable, script], check=True)

print("\n✅ Pipeline ETL completo.")