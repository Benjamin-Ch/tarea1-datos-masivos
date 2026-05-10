Como ejecutar el codigo

1. Descargar noticias_chile_2023_2025.csv o clean_with_region.parquet y posicinarlo en `/data`.

2. Para generar warehouse, ejecutar:

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python etl/run_all.py
# o python 05_build_fact_news.py si tienes el clean_with_region.parquet
```

3. Para analisis Map Reduce, ejecutar:

```
python mapreduce/run_all_map_reduce.py
# o analisis especifico ubicado en carpeta /mapreduce
```

Nota:
Se usa Pandas exclusivamente en el ETL, no en el analisis con mapreduce
