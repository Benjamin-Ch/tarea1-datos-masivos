import os
from collections import defaultdict
import pyarrow.parquet as pq
import re 

#### VAMOS A REUTILIZAR MUCHAS FUNCIONES DEL CODIGO ANTERIOR
def obtener_parquet_path(path_base):
    paths = []
    for year in os.listdir(path_base):
        year_path = os.path.join(path_base, year)
        
        if not os.path.isdir(year_path):
            continue
        
        for month_folder in os.listdir(year_path):
            month_path = os.path.join(year_path, month_folder)
            
            if not os.path.isdir(month_path):
                continue
            
            parquet_file = os.path.join(month_path, "data.parquet")
            
            if os.path.exists(parquet_file):
                # extraemos mes como número ya que este esta en la caperta
                month = month_folder.split("=")[1]
                # zfill asegura que los meses tengan dos digitos
                paths.append((year, month.zfill(2), parquet_file))
    
    return paths
## FUncion que lee los archivos parquet
def read_data_parquet(path):
    tabla = pq.read_table(path)
    return tabla.to_pylist()

## Ahora hacemos un procesamiento de texto a tokens y luego usamos el STOPWORDS
## Paginas de referencia para entender mejor este proceso de nuestra parte:
# 1) https://cr0wg4n.medium.com/palabras-vac%C3%ADas-en-espa%C3%B1ol-stop-words-ft-python-3117e52d2bff
# 2) https://medium.com/@abhishekjainindore24/all-about-tokenization-stop-words-stemming-and-lemmatization-in-nlp-1620ffaf0f87

# EL diccionario de stop words, este tomo un tiempo de buscar en paginas para crearlo y que fuera suficiente o aceptable
stop_words = {"a", "ante", "bajo", 'cabe', 'con', 'contra', 'de', 'desde', 'durante', 'en', 'entre', 
'hacia', 'hasta', 'mediante', 'para', 'por', 'según', 'sin', 'sobre', 'tras', 'vía', 'el', 'la', 'los', 'las', 
'un', 'una', 'unos', 'unas', "que", "qué", "quien", "quienes", "cual", "cuales", "yo", "tú", "él", "ella", 
"nosotros", "vosotros", "ellos", "mi", "mis", "su", "sus", "y", "e", "ni", "o", "u", "pero", "aunque", "sino",
"ser", "estar", "haber", "tener", "fue", "era", "son", "es", "está", "había", "habría", "tiene", "tienen", "sea", 
"siendo", "aquí", "allí", "allá", "tan", "muy", "mucho", "poco", 'no', 'sí', 'más', 'ya', 'todo', 'todos', 'toda', 
'todas', 'donde', 'cuando', 'este', 'esta', 'estos', 'estas', 'ese', 'esa', 'esos', 'esas', 'aquel', 'aquella', 
'aquellos', 'aquellas', 'mío', 'tuyo', 'suyo', 'nuestro', 'vuestro', 'como', 'cómo', 'al', 'del', 'lo', 'le', 'les', 
'me', 'te', 'se', 'nos', 'os', 'estamos', 'están', 'he', 'has', 'ha', 'hemos', 'habéis', 'han', 'pero', 'porque', 'pues', 
'entonces', 'así', 'también', 'tampoco', 'si', 'esto', 'hay', 'eso', 'uno', 'km', 'gran', 'dijo', 'tanto', 'caso', 'dos',
'puede', 'además', 'hace', 'después', 'solo', 'vez', 'hacer', 'mientras', 'hoy', 'sido', 'cada', 'of',
'n', 'parte', 'años', 'año', 'the', 'c', 'luego', 'h', 'personas','tres', 'momento', 'día', 'bien', 'días', 'otros', 's',
'será', 'ahora', "horas"
} # Las ultimas filas son palabras que sientimos que no provee informacion o que eran poco representativas para analisis
# Algunas fueron elegidas como "ahora", por no tener una clara forma de dar informacion o similar asi como "dia" tambien,
# YA que estos pueden ser reportajes diarios o similar, otro ejemplo seria "horas"

# Funcion de transformar a tokens y luego stop words (Se podian importar pero no se si en español)
def separar_texto(text):
    text = text.lower()
    tokens = re.findall(r'[a-záéíóúñü]+', text)
    return tokens
def remove_stopwords(text):
     # Pensamos hacer el mapeo aqui mismo para no recorrer mucho los datos
     # Pero era mas ordenado no hacerlo 
    rest = []
    tokens = separar_texto(text)
    for item in tokens:
        if not item in stop_words:
            rest.append(item)
    return rest


###-------------------------------------------------------------------------------

## AQUI CREAMOS ALGUNAS NUEVAS
# ESTA PArte crea un diccionario con los id de la region y los nombres asociados
def load_dim_region():
    arch_region = os.path.join("warehouse/dim_region/dim_region.parquet")
    region_dict = {}

    table = pq.read_table(arch_region)
    filas = table.to_pylist()
    #print(f"{filas}")

    for f in filas:
        region_dict[f["region_id"]] = f["region"]
    return region_dict


# FUNCION DE MAP ALTERNATIVA A LA ANTERIOR que se divide en dos procesos, para la ocurrecia global y regional
# PRImero una funcion de map global la cual procesa los textos
def map_global(registro):
    # Esto puede ser un poco lento, pero en teoria hacerlo en una misma funcion seria lo mismo en complejidad
    text = (registro.get("title","") + " " + registro.get("body",""))
    words = remove_stopwords(text)

    retorno = []
    for item in words:
        retorno.append((item, 1))
    return retorno

# ESta tiene la  misma base
def map_region(registro, region_dict):
    region_id = registro.get("region_id")
    region = region_dict.get(region_id, "Desconocida")
    
    # decision del enunciado, debido al ETL si este es desconocido es mejor no atribuirlo a alguna region y generar errores
    # TAmbien el que existan como una region aparte tendria poco sentido ante su poca importancia para analisas de regiones
    if region == "Desconocida":
        return []
    # ESto lo hacemos en una linea 
    text = (registro.get("title","") + " " + registro.get("body",""))
    words = remove_stopwords(text)

    retorno = []
    for item in words:
        retorno.append(((region, item), 1))
    return retorno

######################################################################
# ====================================================================
#----------------------------
# FUNCIONES ANTERIORES  (NO se usan por logica alternativa, que es similar o igual) 
# (AUNQUE NO SE USAN, YA QUE SE REMPLAZA EL REDUCE POR POR LA LOGICA EN LAS FUNCIONES MAP y su calculo de conteo)
def shuffle(mapeado):
    # Con esto se evitan errores de Key
    grouped = defaultdict(list)
    
    for k, v in mapeado:
        grouped[k].append(v)
    # Agrega un 1 a la lista del value de la key, luego reduce lo suma todo de la lista
    return grouped
## Funcion Reduce (Esta es un caso general casi de la funcion en verdad)
def reduce_stand(key, values):
    return (key, sum(values))

#########################################################################
## ________________________________________
# LOgica general del conteo de la cantidad por region y global:
# (ESTA PARTE REMPLAZA EL TRABAJO DE LA FUNCION REDUCE)
### PRIMERO HACEMOS EL PROCESAMIENTO GLOBAL
def calculo_global_conteo(paths):
    conteo_global = defaultdict(int)
    # recoremos todos los archivos year - month
    for year, month, file_path in paths:
        registros = read_data_parquet(file_path)
        for reg in registros:
            # Esta parte remplaza el reduce
            for word, value in map_global(reg):
                conteo_global[word] += value
    return conteo_global
# SEGUNDO PROCESAMIENTO POR REGION
def calculo_region_conteo(paths, region_dict):
    region_counts = defaultdict(int)
    
    for year, month, file_path in paths:
        registros = read_data_parquet(file_path)
        for reg in registros:
            # Esta parte remplaza el reduce
            for key, value in map_region(reg, region_dict):
                region_counts[key] += value
    return region_counts

###########################
### AHORA EL USO GENERAL DE AMBAS FUNCIONES para tener lo que se nos pide
# COn un valor K igual a 7800, considerando que es similar al anterior
def proceso_data_conteo(region_counts, conteo_global, k=10000):
    resultados = []
    for (region, word), count_regional in region_counts.items():
        count_global_k = conteo_global.get(word,0)
        if count_global_k < k:
            continue

        frecuencia_relativa = (count_regional/count_global_k)
        resultados.append((region, word, count_regional, count_global_k, frecuencia_relativa))
    return resultados


# Por ultimo llamamos al main y ejecutamos las funciones anteriores en orden
def main():
    region_dict = load_dim_region()
    path_base = "warehouse/fact_news"
    paths = obtener_parquet_path(path_base)
    print(f"Empezo a procesar los datos")
    conteo_regional = calculo_region_conteo(paths, region_dict)
    print("Termino el conteo regional")
    conteo_global = calculo_global_conteo(paths)
    print("Empieza el procesamiento del conteo regional y global")
    resultados = proceso_data_conteo(conteo_regional, conteo_global, k=10500)

    os.makedirs("resultados", exist_ok=True)
    output_path = os.path.join("resultados", "2_2_word_region.txt")
    with open(output_path, "w", encoding="utf-8") as f:
        for i in range(0, len(resultados), 2):
            if len(resultados) != (i + 1):
                linea = f"{resultados[i]}  {resultados[i+1]}\n"
            else:
                linea = f"{resultados[i]}\n"
            print(linea, end="")
            f.write(linea)
    print(f"Termino de reducir los datos")
    print(f"Resultados guardados en {output_path}")


if __name__ == "__main__":
    main()


""" RESULTADOS
(SON DEMASIADOS PERO ESTOS ESTAN CORRECTOS en terminos de lo que se pide)
(Pueden tener un par de valores extra pero esta bien hecho)
"""
