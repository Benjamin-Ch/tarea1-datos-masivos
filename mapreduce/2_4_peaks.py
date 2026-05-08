import os
from collections import defaultdict
import pyarrow.parquet as pq
import re 
import matplotlib.pyplot as plt

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
# NUEVAS FUNCIONESs
def load_dim_date():
    arch_date = os.path.join("warehouse/dim_date/dim_date.parquet")
    date_dict = {}

    table = pq.read_table(arch_date)
    filas = table.to_pylist()
    #print(f"{filas}")

    for f in filas:
        date_dict[f["date_id"]] = f["date"]
    return date_dict


# MAPEO DE archivos diarios
def map_daily(registro, date_dict):
    date_id = registro.get("date_id")
    date = date_dict.get(date_id)

    # Si este no tiene fecha, retorna nada 
    if date is None:
        return []
    # En caso contrario retorna una tupla de la fecha y 1, para luego usar reduce en este caso
    return [(date.isoformat(),1)]

# AQUI SI usamos una funcion directa de reduce, porque es conveniente en este caso
def reduce_sum_date(key, values):
    return (key, sum(values))


##################################################
## ______________________________________________
# Logica de conteo de la cantidad diaria de archivos
def calculo_conteo_diario(paths, date_dict):
    conteo_diario = defaultdict(int)

    for year, month, file_path in paths:
        registros = read_data_parquet(file_path)
        
        for reg in registros:
            for key, value in map_daily(reg, date_dict):
                conteo_diario[key] += value
    return conteo_diario

#### AHORA se hace el Promedio movil semanal o de 7 dias
def promedio_movil(sorted_diario, rango=7):
    
    resultados = []

    for i in range(len(sorted_diario)):
        if i < rango:
            resultados.append((sorted_diario[i][0], None))
            continue
        
        window_values = [sorted_diario[j][1] for j in range(i-rango, i)]
        avg = sum(window_values) / rango
        resultados.append((sorted_diario[i][0], avg))
    
    return resultados

#########
# DETECCION de PEaks, vamos a usar un metodo simple que consiste en que si el valor esta un 
# 50% sobre el promedio movil, de la forma "valor > promedio*umbral" entonces es un peak
def detectar_peak(sorted_diario, promedio_movil, umbral=1.5):
    peaks = []
    for i in range(len(sorted_diario)):
        _, media = promedio_movil[i]
        # pueden existir valores no existentes, debido a falta de datos en fechas
        if media is None:
            continue
        date, value = sorted_diario[i]

        if value > (media*umbral):
            peaks.append((date, value, media))
    return peaks

# =================================================
# =================================================
# GRAFICA de analisis de los peaks detectados en el codigo 
def graficar_peaks(sorted_diario, promedio_movil, peaks):

    # DATOS PRINCIPALES y NESESARIOS

    fechas = [d for d, v in sorted_diario]
    cantidades = [v for d, v in sorted_diario]
    # promedio movil
    promedio_vals = []

    for _, avg in promedio_movil:
        promedio_vals.append(avg if avg is not None else 0)
    # CREAR FIGURA
    plt.figure(figsize=(16,7))

    # línea principal
    plt.plot(fechas, cantidades, label="Cantidad diaria artículos")

    # promedio movil
    plt.plot(fechas, promedio_vals, linestyle="--", label="Promedio móvil (7 días)")

    # GRAFICACION DE LOS PEAKS
    peak_fechas = [d for d, _, _ in peaks]
    peak_values = [v for _, v, _ in peaks]
    plt.scatter( peak_fechas, peak_values,
        s=80, label="Peaks detectados")

    # ANOTACIONES
    for fecha, valor, _ in peaks:
        plt.annotate(fecha, (fecha, valor),
            textcoords="offset points", xytext=(0,10),
            ha='center', fontsize=8)

    plt.title("Detección de peaks en volumen diario de artículos")
    plt.xlabel("Fecha")
    plt.ylabel("Cantidad de artículos")
    plt.xticks(rotation=45)

    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()

    # GUARDAMOS LA IMAGEN
    os.makedirs("graficos", exist_ok=True)

    plt.savefig("graficos/deteccion_peaks.png", dpi=300, bbox_inches='tight')
    plt.close()


# POR ULTIMO el MAIN
def main():
    path_base = "warehouse/fact_news"
    paths = obtener_parquet_path(path_base)
    date_dict = load_dim_date()
    print("Empezo el conteo diario de archivos")
    conteo_diarios = calculo_conteo_diario(paths, date_dict)
    print("Termino de calcular los archivos diarios")
    sorted_diario = sorted(conteo_diarios.items())
    p_movil = promedio_movil(sorted_diario, rango=7)
    
    peaks = detectar_peak(sorted_diario, p_movil, umbral=1.5)
    print("Termino la deteccion de peaks")
    for date, value, media in peaks:
        print(f"{date}: {value} articulos (promedio: {media:.2f})")

    graficar_peaks(sorted_diario, p_movil, peaks)

if __name__ == "__main__":
    main()
    



""" RESULTADOS

2023-09-20: 1365 articulos (promedio: 761.00)
2023-09-21: 1419 articulos (promedio: 762.57)
2023-09-22: 1388 articulos (promedio: 806.57)
2024-09-24: 1904 articulos (promedio: 1058.14)
2024-09-25: 1695 articulos (promedio: 1092.00)
2025-01-07: 1548 articulos (promedio: 1030.14)
2025-04-02: 1864 articulos (promedio: 1030.86)
2025-04-03: 1756 articulos (promedio: 1090.14)
2025-09-23: 1906 articulos (promedio: 1155.57)
2025-09-24: 1923 articulos (promedio: 1185.29)
2025-09-25: 1882 articulos (promedio: 1235.43)

"""