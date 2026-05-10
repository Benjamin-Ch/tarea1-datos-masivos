import pandas as pd
import os
from region_aliases import REGION_ALIASES

os.makedirs("warehouse/dim_region", exist_ok=True)

# La tabla dim_region es solo la lista de regiones conocidas
# No necesita leer ningún artículo
all_regions = list(REGION_ALIASES.keys()) + ["Desconocida"]

dim_region = pd.DataFrame({
    "region_id": range(1, len(all_regions) + 1),
    "region":    all_regions,
})

dim_region.to_parquet("warehouse/dim_region/dim_region.parquet", index=False)
print(f"dim_region guardado ({len(dim_region)} regiones)")