Como ejecutar el codigo

1. Descargar noticias_chile_2023_2025.csv o clean_with_region.parquet y posicinarlo en `/data`.

2. Ejecutar:
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python etl/run_all.py
# o python 05_build_fact_news.py si tienes el clean_with_region.parquet
```


