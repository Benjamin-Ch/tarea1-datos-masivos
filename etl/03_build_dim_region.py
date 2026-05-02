import pandas as pd
import os
import re
from unidecode import unidecode
from region_aliases import REGION_ALIASES

os.makedirs("warehouse/dim_region", exist_ok=True)
df = pd.read_parquet("data/clean.parquet")


def normalize(text):
    """Lowercase + quitar tildes."""
    return unidecode(str(text)).lower()

def detect_region(title, body):
    """
    Regla de desambiguación:
    1. Contar menciones en title (peso doble) y body (peso simple).
    2. La región con mayor puntaje total gana.
    3. Si empate, gana la que aparezca primero en el título.
    4. Si no hay ninguna mención, retornar 'Desconocida'.
    """
    title_norm = normalize(title)
    body_norm  = normalize(body)

    scores = {}
    for region, aliases in REGION_ALIASES.items():
        pattern = "|".join(aliases)
        title_hits = len(re.findall(pattern, title_norm))
        body_hits  = len(re.findall(pattern, body_norm))
        total = title_hits * 2 + body_hits  # título tiene peso doble
        if total > 0:
            scores[region] = total

    if not scores:
        return "Desconocida"
    return max(scores, key=scores.get)

df["region"] = df.apply(
    lambda row: detect_region(row["title"], row["body"]), axis=1
)

all_regions = list(REGION_ALIASES.keys()) + ["Desconocida"]

dim_region = pd.DataFrame({
    "region_id": range(1, len(all_regions) + 1),
    "region":    all_regions,
})

dim_region.to_parquet("warehouse/dim_region/dim_region.parquet", index=False)