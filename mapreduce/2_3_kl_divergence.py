import os
from collections import defaultdict
import pyarrow.parquet as pq
import re 
import math

## FUNCIONES PASADAS
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

# USamos el sitema de stop words anterior 
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


#################################################################################
# NUEVAS FUNCIONESs
def load_dim_source():
    arch_source = os.path.join("warehouse/dim_source/dim_source.parquet")
    source_dict = {}

    table = pq.read_table(arch_source)
    filas = table.to_pylist()
    #print(f"{filas}")

    for f in filas:
        source_dict[f["source_id"]] = f["source"]
    return source_dict

# USAMOS EL MAPEO GLOBAL DE LA PARTE "2.2" 
def map_global(registro):
    # Esto puede ser un poco lento, pero en teoria hacerlo en una misma funcion seria lo mismo en complejidad
    text = (registro.get("title","") + " " + registro.get("body",""))
    words = remove_stopwords(text)

    retorno = []
    for item in words:
        retorno.append((item, 1))
    return retorno

########### 
## CREAMOS UN NUEVO MAPEO SIMILAR al de regiones en la anterior parte pero para las fuentes de medios de informacion
def map_source(registro, source_dict):
    source_id = registro.get("source_id")
    source = source_dict.get(source_id)

    # ESto lo hacemos en una linea 
    text = (registro.get("title","") + " " + registro.get("body",""))
    words = remove_stopwords(text)

    retorno = []
    for item in words:
        retorno.append(((source, item), 1))
    return retorno

##################################################
## ______________________________________________
# Logica de conteo de la cantidad global y por fuente 
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
# CALCULO DE CONTEO de fuentes
def calculo_source_conteo(paths, source_dict):
    source_counts = defaultdict(int)

    for year, month, file_path in paths:
        registro = read_data_parquet(file_path)
        
        for reg in registro:
            for key, value in map_source(reg, source_dict):
                source_counts[key] += value
    
    return source_counts

### PARA PODER CALCULAR las probabilidades con DIvergencia de KL es necesario utilizar
# El total por de cada uno tanto el source y el global
def calculo_total_source(source_counts):
    totals = defaultdict(int)
    
    for (source, word), count in source_counts.items():
        totals[source] += count
    return totals
def calculo_total_global(conteo_global):
    return sum(conteo_global.values())

###### INVESTIGANDO LA FORMULA DE LA DIVERGENCIA KL para codigo, pensamos usar la libreria "math"
# Asi calcular la formula seria mas facil
def compute_kl_divergence(source_counts, conteo_global, source_totals, global_total, k=50):
    
    epsilon = 1e-10
    results = {}
    
    # Filtrar un vocabulario estable, usamos un diccionario
    words_validas = set()
    for word, count in conteo_global.items():
        if count >= k:
            words_validas.add(word)
    
    for source in source_totals:
        kl = 0.0
        
        for (s, word), count in source_counts.items():
            if s != source:
                continue
            if word not in words_validas:
                continue
            
            p = count / source_totals[source]
            q = conteo_global.get(word, 0) / global_total
            
            q = max(q, epsilon)
            kl += p * math.log(p / q)
        
        results[source] = kl
    return results

####
# POR ULTIMO CREAMOS EL MAIN
def main():
    base_path = "warehouse/fact_news"
    paths = obtener_parquet_path(base_path)
    source_dict = load_dim_source()
    print("Empezo a calcular el conteo")
    global_counts = calculo_global_conteo(paths)
    source_counts = calculo_source_conteo(paths, source_dict)
    print("Termino el conteo de funtes y global")
    source_totals = calculo_total_source(source_counts)
    global_total = calculo_total_global(global_counts)
    
    kl_results = compute_kl_divergence(source_counts, global_counts, source_totals, global_total, k=50)
    # El Score es el valor KL de la divergencia
    for source, score in sorted(kl_results.items(), key=lambda x: x[1], reverse=True):
        print(f"{source}: {score:.6f}")
## SI el valor KL es ALTO, entonces es un vocabulario muy distinto al global
## Si el valor KL es BAJO, entonces es un vocabulario similar al promedio

if __name__ == "__main__":
    main()



""" RESULTADOS 

digital.elmercurio.com: 7.093810
digital.lasegunda.com: 6.722176
f4wonline.com: 5.840160
programas.cooperativa.cl: 5.250971
revo30.org: 4.787032
ilustrado.cl: 4.457670
sancarlosonline.cl: 4.325348
ecovisiones.cl: 3.996735
lasegunda.com: 3.820910
radiopolar.com: 3.468284
bnamericas.com: 3.217113
diarioantofagasta.cl: 3.106806
diarioelcentro.cl: 2.972004
portaldisc.com: 2.934707
canal9.cl: 2.926775
tribunalcalificador.cl: 2.858806
caras.cl: 2.833100
fotech.cl: 2.762270
itseller.cl: 2.761893
puntoticket.com: 2.678611
eltipografo.cl: 2.662190
santiagotimes.cl: 2.615666
chocale.cl: 2.482152
elpinguino.com: 2.456915
lahora.cl: 2.218347
iquiqueonline.cl: 2.105464
blog.clay.cl: 1.945169
pisapapeles.net: 1.817826
montenbaik.com: 1.814419
espn.cl: 1.695971
diarioconcepcion.cl: 1.671463
madboxpc.com: 1.626796
tarreo.com: 1.587723
servel.cl: 1.581752
kilometrocero.cl: 1.525205
hyperconectados.com: 1.500564
pousta.com: 1.473926
mega.cl: 1.430252
ellibero.cl: 1.409861
fmdos.cl: 1.351835
chilecologico.cl: 1.328508
diarioestrategia.cl: 1.252116
terra.cl: 1.190197
diarioeldia.cl: 1.187536
ohmygeek.net: 1.171241
pagina7.cl: 1.142154
encancha.cl: 1.130595
finde.latercera.com: 1.109362
diarioelheraldo.cl: 1.094387
redgol.cl: 1.034860
prensafutbol.cl: 0.917022
glamorama.latercera.com: 0.914712
portalportuario.cl: 0.904552
limalimon.cl: 0.867157
latendencia.cl: 0.863060
lared.cl: 0.822673
america-retail.com: 0.815134
alairelibre.cl: 0.694027
futuro.cl: 0.660949
itvpatagonia.com: 0.651535
pudahuel.cl: 0.639115
lanalhuenoticias.cl: 0.603164
meganoticias.cl: 0.523187
13.cl: 0.500018
puentealtoaldia.com: 0.485613
elinsular.cl: 0.475602
elperiscopio.cl: 0.462402
diariolongino.cl: 0.446181
miradiols.cl: 0.440784
tvn.cl: 0.404985
df.cl: 0.402140
ciperchile.cl: 0.397280
elrancaguino.cl: 0.363841
radio.uchile.cl: 0.353064
chvnoticias.cl: 0.341649
radionuevomundo.cl: 0.336195
duna.cl: 0.313638
elobservatodo.cl: 0.308494
cronicadigital.cl: 0.289008
elrancahuaso.cl: 0.276803
chilevision.cl: 0.276617
publimetro.cl: 0.274499
elamaule.cl: 0.274419
mercuriovalpo.cl: 0.269491
elvacanudo.cl: 0.268598
lacuarta.com: 0.266177
elmorrocotudo.cl: 0.259739
elnortero.cl: 0.258082
elciudadano.com: 0.252184
laopinon.cl: 0.246481
laprensaaustral.cl: 0.209561
infogate.cl: 0.194137
theclinic.cl: 0.151811
cnnchile.com: 0.151585
eldesconcierto.cl: 0.143649
elmostrador.cl: 0.127263
radioagricultura.cl: 0.122343
lanacion.cl: 0.120588
eldinamo.cl: 0.113984
emol.com: 0.105692
adnradio.cl: 0.098307
cooperativa.cl: 0.089815
t13.cl: 0.088563
24horas.cl: 0.086406
latercera.com: 0.084383
biobiochile.cl: 0.068779


"""