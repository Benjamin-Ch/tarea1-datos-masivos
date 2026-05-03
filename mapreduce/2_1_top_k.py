import os
from collections import defaultdict
import pyarrow.parquet as pq
import re # Para transformar a tokens o separar el texto

## Esta es una funcion que recorre la estructura de las carpetas del data warehouse
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


## Ahora hacemos la funcion del mapeo 
def mapeo(year, month, registro):
    retorno = []
    ## Por como se construyen los datos
    titulo = registro.get("title", "")
    cuerpo = registro.get("body", "")
    text = f"{titulo} {cuerpo}"

    words = remove_stopwords(text)
    year_mes = f"{year}-{month}"
    for item in words:
        retorno.append(((year_mes, item),1))
    return retorno

## Funcion para el Shuffle, pasa que sin el Shuffle la funcion Reduce recibiria datos desordenados y 
# no podria saber cuando termina de contar una palabra, o cuando empieza otra
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


# =================================================
# =================================================

# FUNCION que agrupa todas las funciones anteriores (o funciones que usan funciones anteiores)
# Esto crea el producto de la reduccion de los datos con map, shuffle y reduce
def process_data_reduce(path_base):
    ## Claro que existe una forma mejor optimizada sin acumular todo en "mapeado",
    # Pero esta es mas facil de ver y revisar nos parecio 
    # (Aunque consume harta memoria, por lo que liberamos memoria si no este entrega "Killed")
    paths = obtener_parquet_path(path_base)
    resultados_generales = []
    ## Recoremos los datos mes por mes
    for year, month, file_path in paths:
        print(f"- Procesando los datos de: {year}-{month}...")

        mapeado = []
        registros = read_data_parquet(file_path)
        for reg in registros:
            # Extiende porque mapeo devuelve una lista de tuplas
            mapeado.extend(mapeo(year, month, reg))

        agrupacion = shuffle(mapeado)
        reduce_data = []
        for key, values in agrupacion.items():
            reduce_data.append(reduce_stand(key, values))

        ## SE USA LA FUNCION DE ADELANTE QUE OBTIENE LOS TOP 20
        top_20_mes = calculo_top_k(reduce_data, k=20)
        # Guardamos los datos generales que son menos pesados 
        resultados_generales.extend(top_20_mes)

        ## POR ULTIMO, liberamos los datos de las estructuras por si acaso, apesar de que estas se iguales a [] despues
        del registros
        del mapeado
        del agrupacion
        del reduce_data


    print("- Termino de procesar el todos los mes")
    for year_mes, word, count in sorted(resultados_generales, key=lambda x: (x[0], -x[2])):
        print(f"{year_mes}, {word}, {count}")


########################################################################
# ======================================================================

## AHORA ES LA LOGICA DE ESTE PUNTO 2.1) TOP_K  
# (Lo tuvimos que usar antes para que liberar memoria en el procesamiento de datos)
def calculo_top_k(reduce_data, k=20): # Por defecto es 20, por lo de la pregunta que son los 20
    mensual = defaultdict(list)
    for (year_mes, word), count in reduce_data:
        mensual[year_mes].append((word, count))

    resultados = [] 
    for year_mes, words in mensual.items():
        top_k = sorted(words, key=lambda x: x[1], reverse=True)[:k]
        for word, count in top_k:
            resultados.append((year_mes, word, count))
    
    return resultados

# Por ultimo llamamos al main y ejecutamos las funciones anteriores en orden
def main():
    path_base = "warehouse/fact_news"
    print(f"Empezo a procesar los datos")
    process_data_reduce(path_base)
    print(f"Termino de reducir los datos {path_base}")

if __name__ == "__main__":
    main()


""" RESULTADOS DEL CODIGO

year=2023-01, chile, 23212
year=2023-01, gobierno, 16906
year=2023-01, país, 16267
year=2023-01, nacional, 15567
year=2023-01, presidente, 15026
year=2023-01, estado, 12718
year=2023-01, enero, 12607
year=2023-01, nuevo, 12349
year=2023-01, primera, 10599
year=2023-01, pasado, 10471
year=2023-01, millones, 10364
year=2023-01, vida, 10360
year=2023-01, tiempo, 10305
year=2023-01, cuenta, 10190
year=2023-01, hecho, 10178
year=2023-01, mundo, 10133
year=2023-01, lugar, 9985
year=2023-01, nueva, 9766
year=2023-01, trabajo, 9667
year=2023-01, acuerdo, 9513
year=2023-02, chile, 19526
year=2023-02, país, 14375
year=2023-02, incendios, 13450
year=2023-02, nacional, 12474
year=2023-02, viña, 11553
year=2023-02, región, 10937
year=2023-02, gobierno, 10775
year=2023-02, festival, 10487
year=2023-02, estado, 10476
year=2023-02, febrero, 10358
year=2023-02, millones, 8913
year=2023-02, primera, 8890
year=2023-02, tiempo, 8727
year=2023-02, pasado, 8660
year=2023-02, vida, 8618
year=2023-02, cuenta, 8547
year=2023-02, hecho, 8270
year=2023-02, lugar, 8186
year=2023-02, fueron, 8148
year=2023-02, partido, 7956
year=2023-03, chile, 25969
year=2023-03, gobierno, 16417
year=2023-03, país, 15440
year=2023-03, nacional, 14276
year=2023-03, presidente, 13489
year=2023-03, marzo, 12641
year=2023-03, estado, 12454
year=2023-03, vida, 10843
year=2023-03, millones, 10357
year=2023-03, trabajo, 10255
year=2023-03, tiempo, 10167
year=2023-03, cuenta, 10012
year=2023-03, mujeres, 10012
year=2023-03, primera, 9985
year=2023-03, mundo, 9921
year=2023-03, pasado, 9851
year=2023-03, hecho, 9824
year=2023-03, carabineros, 9686
year=2023-03, nueva, 9540
year=2023-03, nuevo, 9526
year=2023-04, chile, 22514
year=2023-04, país, 14350
year=2023-04, nacional, 13234
year=2023-04, estado, 12442
year=2023-04, gobierno, 12374
year=2023-04, carabineros, 10635
year=2023-04, seguridad, 10144
year=2023-04, presidente, 9772
year=2023-04, abril, 9447
year=2023-04, vida, 9437
year=2023-04, trabajo, 9130
year=2023-04, millones, 9058
year=2023-04, tiempo, 9047
year=2023-04, lugar, 9035
year=2023-04, ley, 8994
year=2023-04, región, 8913
year=2023-04, hecho, 8844
year=2023-04, cuenta, 8549
year=2023-04, fueron, 8290
year=2023-04, primera, 8279
year=2023-05, chile, 24443
year=2023-05, país, 15020
year=2023-05, gobierno, 13935
year=2023-05, nacional, 13489
year=2023-05, estado, 12959
year=2023-05, mayo, 12057
year=2023-05, partido, 11921
year=2023-05, vida, 10748
year=2023-05, millones, 10639
year=2023-05, presidente, 10462
year=2023-05, región, 9778
year=2023-05, cuenta, 9731
year=2023-05, nueva, 9639
year=2023-05, lugar, 9403
year=2023-05, pasado, 9362
year=2023-05, tiempo, 9342
year=2023-05, trabajo, 9266
year=2023-05, primera, 9244
year=2023-05, salud, 9096
year=2023-05, hecho, 9001
year=2023-06, chile, 22412
year=2023-06, país, 14806
year=2023-06, presidente, 13210
year=2023-06, estado, 12937
year=2023-06, nacional, 12251
year=2023-06, gobierno, 11583
year=2023-06, cuenta, 10588
year=2023-06, región, 10507
year=2023-06, junio, 10110
year=2023-06, salud, 9954
year=2023-06, vida, 9614
year=2023-06, millones, 9241
year=2023-06, lugar, 8954
year=2023-06, hecho, 8824
year=2023-06, tiempo, 8744
year=2023-06, trabajo, 8683
year=2023-06, medio, 8535
year=2023-06, mundo, 8500
year=2023-06, pasado, 8466
year=2023-06, primera, 8287
year=2023-07, chile, 19437
year=2023-07, estado, 12035
year=2023-07, gobierno, 11601
year=2023-07, país, 11437
year=2023-07, nacional, 10626
year=2023-07, presidente, 9004
year=2023-07, julio, 8971
year=2023-07, millones, 8952
year=2023-07, vida, 7845
year=2023-07, hecho, 7718
year=2023-07, región, 7615
year=2023-07, acuerdo, 7548
year=2023-07, tiempo, 7490
year=2023-07, cuenta, 7381
year=2023-07, respecto, 7372
year=2023-07, fueron, 7323
year=2023-07, lugar, 7302
year=2023-07, trabajo, 7079
year=2023-07, medio, 6941
year=2023-07, ministro, 6852
year=2023-08, chile, 21030
year=2023-08, gobierno, 12945
year=2023-08, país, 12937
year=2023-08, estado, 12793
year=2023-08, nacional, 12320
year=2023-08, presidente, 10975
year=2023-08, agosto, 10352
year=2023-08, millones, 9325
year=2023-08, región, 8750
year=2023-08, tiempo, 8330
year=2023-08, vida, 7969
year=2023-08, lugar, 7658
year=2023-08, pasado, 7408
year=2023-08, respecto, 7407
year=2023-08, cuenta, 7405
year=2023-08, hecho, 7333
year=2023-08, fueron, 7329
year=2023-08, partido, 7328
year=2023-08, acuerdo, 7132
year=2023-08, nuevo, 7092
year=2023-09, chile, 22875
year=2023-09, estado, 13898
year=2023-09, septiembre, 12637
year=2023-09, país, 12114
year=2023-09, nacional, 11636
year=2023-09, gobierno, 10124
year=2023-09, presidente, 9961
year=2023-09, vida, 8342
year=2023-09, lugar, 7849
year=2023-09, tiempo, 7623
year=2023-09, fueron, 7483
year=2023-09, región, 7412
year=2023-09, partido, 7287
year=2023-09, nueva, 7023
year=2023-09, hecho, 6893
year=2023-09, santiago, 6839
year=2023-09, trabajo, 6826
year=2023-09, primera, 6797
year=2023-09, pasado, 6690
year=2023-09, mismo, 6672
year=2023-10, chile, 30110
year=2023-10, país, 15744
year=2023-10, octubre, 15683
year=2023-10, santiago, 14678
year=2023-10, nacional, 14528
year=2023-10, israel, 12752
year=2023-10, estado, 12302
year=2023-10, gobierno, 11478
year=2023-10, tiempo, 10579
year=2023-10, presidente, 10440
year=2023-10, lugar, 10110
year=2023-10, primera, 9740
year=2023-10, región, 9482
year=2023-10, mundo, 9363
year=2023-10, vida, 9362
year=2023-10, nueva, 8907
year=2023-10, fueron, 8892
year=2023-10, salud, 8845
year=2023-10, cuenta, 8810
year=2023-10, millones, 8747
year=2023-11, chile, 29645
year=2023-11, país, 15943
year=2023-11, nacional, 15518
year=2023-11, noviembre, 14228
year=2023-11, santiago, 12641
year=2023-11, presidente, 12197
year=2023-11, estado, 12073
year=2023-11, gobierno, 12067
year=2023-11, tiempo, 10619
year=2023-11, millones, 10098
year=2023-11, vida, 9610
year=2023-11, región, 9486
year=2023-11, lugar, 9470
year=2023-11, nueva, 9392
year=2023-11, cuenta, 9243
year=2023-11, primera, 9221
year=2023-11, mundo, 9114
year=2023-11, hecho, 8763
year=2023-11, través, 8698
year=2023-11, fueron, 8601
year=2023-12, chile, 24966
year=2023-12, diciembre, 15097
year=2023-12, país, 13897
year=2023-12, nacional, 12152
year=2023-12, gobierno, 11608
year=2023-12, estado, 11466
year=2023-12, presidente, 10686
year=2023-12, nuevo, 10407
year=2023-12, tiempo, 10041
year=2023-12, región, 9708
year=2023-12, vida, 9359
year=2023-12, millones, 8844
year=2023-12, primera, 8719
year=2023-12, acuerdo, 8655
year=2023-12, lugar, 8582
year=2023-12, cuenta, 8509
year=2023-12, colo, 8508
year=2023-12, nueva, 8388
year=2023-12, hecho, 8283
year=2023-12, fueron, 8243
year=2024-01, chile, 23777
year=2024-01, país, 15386
year=2024-01, enero, 15149
year=2024-01, nacional, 13900
year=2024-01, gobierno, 13331
year=2024-01, estado, 13044
year=2024-01, millones, 11611
year=2024-01, tiempo, 11535
year=2024-01, región, 11044
year=2024-01, nuevo, 10954
year=2024-01, presidente, 10638
year=2024-01, vida, 10586
year=2024-01, colo, 10504
year=2024-01, lugar, 10441
year=2024-01, mundo, 10203
year=2024-01, cuenta, 10105
year=2024-01, trabajo, 9863
year=2024-01, pasado, 9827
year=2024-01, medio, 9710
year=2024-01, primera, 9706
year=2024-02, chile, 24960
year=2024-02, nacional, 16389
year=2024-02, región, 15938
year=2024-02, febrero, 14400
year=2024-02, presidente, 14335
year=2024-02, país, 14292
year=2024-02, piñera, 13802
year=2024-02, viña, 13055
year=2024-02, estado, 12865
year=2024-02, incendios, 11847
year=2024-02, gobierno, 11207
year=2024-02, vida, 10300
year=2024-02, tiempo, 10169
year=2024-02, millones, 10022
year=2024-02, colo, 10006
year=2024-02, primera, 9906
year=2024-02, cuenta, 9749
year=2024-02, seguridad, 9408
year=2024-02, pasado, 9403
year=2024-02, lugar, 9368
year=2024-03, chile, 26565
year=2024-03, país, 14378
year=2024-03, marzo, 14133
year=2024-03, gobierno, 14000
year=2024-03, nacional, 13883
year=2024-03, estado, 12217
year=2024-03, tiempo, 10841
year=2024-03, presidente, 10794
year=2024-03, vida, 10740
year=2024-03, colo, 10537
year=2024-03, partido, 10004
year=2024-03, región, 9816
year=2024-03, lugar, 9573
year=2024-03, cuenta, 9571
year=2024-03, pasado, 9387
year=2024-03, hecho, 9310
year=2024-03, mundo, 9298
year=2024-03, millones, 9194
year=2024-03, primera, 9086
year=2024-03, trabajo, 8992
year=2024-04, chile, 30069
year=2024-04, país, 17070
year=2024-04, nacional, 15487
year=2024-04, gobierno, 13083
year=2024-04, abril, 12762
year=2024-04, estado, 12358
year=2024-04, partido, 12025
year=2024-04, presidente, 11540
year=2024-04, región, 11099
year=2024-04, vida, 10888
year=2024-04, carabineros, 10616
year=2024-04, tiempo, 10552
year=2024-04, millones, 10499
year=2024-04, seguridad, 10419
year=2024-04, trabajo, 10410
year=2024-04, lugar, 10291
year=2024-04, cuenta, 10268
year=2024-04, hecho, 10029
year=2024-04, pasado, 9821
year=2024-04, medio, 9590
year=2024-05, chile, 26546
year=2024-05, país, 15328
year=2024-05, nacional, 14255
year=2024-05, mayo, 13906
year=2024-05, estado, 13809
year=2024-05, gobierno, 12926
year=2024-05, presidente, 12120
year=2024-05, región, 11866
year=2024-05, vida, 11526
year=2024-05, cuenta, 11234
year=2024-05, millones, 11068
year=2024-05, lugar, 10858
year=2024-05, salud, 10357
year=2024-05, mundo, 10337
year=2024-05, tiempo, 10006
year=2024-05, trabajo, 9994
year=2024-05, hecho, 9952
year=2024-05, pasado, 9671
year=2024-05, partido, 9529
year=2024-05, fueron, 9341
year=2024-06, chile, 31773
year=2024-06, país, 16416
year=2024-06, nacional, 13885
year=2024-06, presidente, 13231
year=2024-06, junio, 13101
year=2024-06, estado, 12540
year=2024-06, región, 12345
year=2024-06, partido, 11517
year=2024-06, gobierno, 11464
year=2024-06, cuenta, 11218
year=2024-06, sistema, 9897
year=2024-06, copa, 9692
year=2024-06, tiempo, 9562
year=2024-06, lugar, 9472
year=2024-06, vida, 9447
year=2024-06, mundo, 9071
year=2024-06, trabajo, 8918
year=2024-06, hecho, 8894
year=2024-06, millones, 8846
year=2024-06, primera, 8742
year=2024-07, chile, 31257
year=2024-07, país, 18293
year=2024-07, nacional, 16194
year=2024-07, presidente, 15460
year=2024-07, julio, 15315
year=2024-07, gobierno, 15243
year=2024-07, partido, 13819
year=2024-07, estado, 13481
year=2024-07, seguridad, 13052
year=2024-07, región, 11888
year=2024-07, millones, 11468
year=2024-07, vida, 11304
year=2024-07, lugar, 11190
year=2024-07, primera, 10659
year=2024-07, mundo, 10570
year=2024-07, tiempo, 10502
year=2024-07, hecho, 10172
year=2024-07, nuevo, 10127
year=2024-07, cuenta, 9961
year=2024-07, respecto, 9767
year=2024-08, chile, 27422
year=2024-08, país, 15733
year=2024-08, nacional, 14806
year=2024-08, región, 13182
year=2024-08, agosto, 12681
year=2024-08, presidente, 11511
year=2024-08, gobierno, 11403
year=2024-08, estado, 11184
year=2024-08, millones, 10261
year=2024-08, lugar, 10039
year=2024-08, vida, 9828
year=2024-08, tiempo, 9375
year=2024-08, mundo, 8884
year=2024-08, primera, 8758
year=2024-08, hecho, 8645
year=2024-08, través, 8637
year=2024-08, situación, 8614
year=2024-08, pasado, 8607
year=2024-08, partido, 8606
year=2024-08, trabajo, 8454
year=2024-09, chile, 27390
year=2024-09, septiembre, 15671
year=2024-09, país, 15321
year=2024-09, nacional, 13689
year=2024-09, estado, 12054
year=2024-09, gobierno, 11290
year=2024-09, millones, 10854
year=2024-09, región, 10426
year=2024-09, presidente, 10127
year=2024-09, vida, 10055
year=2024-09, lugar, 9631
year=2024-09, mundo, 9598
year=2024-09, tiempo, 9559
year=2024-09, seguridad, 9511
year=2024-09, partido, 8790
year=2024-09, nueva, 8781
year=2024-09, hecho, 8543
year=2024-09, nuevo, 8502
year=2024-09, través, 8297
year=2024-09, cuenta, 8198
year=2024-10, chile, 30972
year=2024-10, octubre, 17247
year=2024-10, país, 17233
year=2024-10, nacional, 16187
year=2024-10, gobierno, 12951
year=2024-10, estado, 12812
year=2024-10, millones, 12203
year=2024-10, región, 12076
year=2024-10, vida, 12035
year=2024-10, presidente, 11812
year=2024-10, lugar, 11394
year=2024-10, tiempo, 11119
year=2024-10, seguridad, 11083
year=2024-10, partido, 10912
year=2024-10, nuevo, 10481
year=2024-10, colo, 10455
year=2024-10, primera, 10427
year=2024-10, hecho, 10217
year=2024-10, mundo, 10187
year=2024-10, través, 10160
year=2024-11, chile, 27631
year=2024-11, nacional, 14276
year=2024-11, país, 13975
year=2024-11, presidente, 13337
year=2024-11, noviembre, 12385
year=2024-11, gobierno, 11278
year=2024-11, región, 10955
year=2024-11, millones, 10518
year=2024-11, estado, 10039
year=2024-11, colo, 9705
year=2024-11, lugar, 9369
year=2024-11, primera, 9202
year=2024-11, vida, 9191
year=2024-11, monsalve, 8739
year=2024-11, seguridad, 8515
year=2024-11, mundo, 8480
year=2024-11, nuevo, 8196
year=2024-11, público, 8107
year=2024-11, través, 8022
year=2024-11, nueva, 7992
year=2024-12, chile, 24572
year=2024-12, país, 14031
year=2024-12, diciembre, 12219
year=2024-12, nacional, 11472
year=2024-12, gobierno, 11227
year=2024-12, presidente, 11024
year=2024-12, millones, 11014
year=2024-12, nuevo, 10131
year=2024-12, estado, 9997
year=2024-12, vida, 9578
year=2024-12, región, 9109
year=2024-12, lugar, 8767
year=2024-12, proyecto, 8179
year=2024-12, acuerdo, 8127
year=2024-12, tiempo, 8122
year=2024-12, seguridad, 8082
year=2024-12, trabajo, 7998
year=2024-12, través, 7837
year=2024-12, primera, 7808
year=2024-12, mundo, 7776
year=2025-01, chile, 25176
year=2025-01, país, 13708
year=2025-01, enero, 13178
year=2025-01, nacional, 12574
year=2025-01, presidente, 12248
year=2025-01, gobierno, 11929
year=2025-01, millones, 11397
year=2025-01, acuerdo, 10565
year=2025-01, estado, 10557
year=2025-01, nuevo, 10110
year=2025-01, región, 9412
year=2025-01, trabajo, 9083
year=2025-01, vida, 8995
year=2025-01, colo, 8985
year=2025-01, lugar, 8565
year=2025-01, proyecto, 8458
year=2025-01, pasado, 8410
year=2025-01, primera, 8289
year=2025-01, tiempo, 8175
year=2025-01, seguridad, 8142
year=2025-02, chile, 22810
year=2025-02, país, 12098
year=2025-02, nacional, 11563
year=2025-02, estado, 10789
year=2025-02, febrero, 10611
year=2025-02, millones, 10235
year=2025-02, viña, 9985
year=2025-02, presidente, 9641
year=2025-02, gobierno, 9570
year=2025-02, primera, 8618
year=2025-02, región, 8385
year=2025-02, embargo, 8021
year=2025-02, lugar, 7802
year=2025-02, vida, 7744
year=2025-02, trump, 7581
year=2025-02, pasado, 7485
year=2025-02, tiempo, 7480
year=2025-02, público, 7411
year=2025-02, festival, 7404
year=2025-02, acuerdo, 7312
year=2025-03, chile, 29787
year=2025-03, país, 13839
year=2025-03, nacional, 13769
year=2025-03, marzo, 13052
year=2025-03, presidente, 11689
year=2025-03, gobierno, 11301
year=2025-03, estado, 10711
year=2025-03, partido, 10627
year=2025-03, seguridad, 9858
year=2025-03, millones, 9471
year=2025-03, vida, 9353
year=2025-03, primera, 8947
year=2025-03, lugar, 8707
year=2025-03, región, 8700
year=2025-03, embargo, 8697
year=2025-03, tiempo, 8612
year=2025-03, trabajo, 8553
year=2025-03, pasado, 8057
year=2025-03, público, 7965
year=2025-03, mundo, 7939
year=2025-04, chile, 34509
year=2025-04, país, 18174
year=2025-04, partido, 16185
year=2025-04, nacional, 15221
year=2025-04, colo, 15179
year=2025-04, presidente, 14497
year=2025-04, abril, 13860
year=2025-04, gobierno, 13750
year=2025-04, estado, 13272
year=2025-04, millones, 12518
year=2025-04, mundo, 12422
year=2025-04, seguridad, 12351
year=2025-04, vida, 11733
year=2025-04, estados, 11153
year=2025-04, tiempo, 11024
year=2025-04, nuevo, 10876
year=2025-04, unidos, 10678
year=2025-04, primera, 10641
year=2025-04, embargo, 10483
year=2025-04, trump, 10187
year=2025-05, chile, 30980
year=2025-05, país, 18291
year=2025-05, nacional, 16762
year=2025-05, mayo, 14886
year=2025-05, millones, 14785
year=2025-05, presidente, 14577
year=2025-05, estado, 14322
year=2025-05, gobierno, 13580
year=2025-05, vida, 12192
year=2025-05, partido, 11784
year=2025-05, colo, 11649
year=2025-05, tiempo, 11206
year=2025-05, nuevo, 11099
year=2025-05, mundo, 11080
year=2025-05, región, 10748
year=2025-05, salud, 10531
year=2025-05, lugar, 10512
year=2025-05, cuenta, 10307
year=2025-05, hecho, 10143
year=2025-05, primera, 10136
year=2025-06, chile, 30341
year=2025-06, país, 19153
year=2025-06, nacional, 14608
year=2025-06, presidente, 13744
year=2025-06, millones, 13380
year=2025-06, gobierno, 13355
year=2025-06, junio, 12978
year=2025-06, estado, 12811
year=2025-06, partido, 11395
year=2025-06, cuenta, 10794
year=2025-06, vida, 10678
year=2025-06, tiempo, 10566
year=2025-06, mundo, 10532
year=2025-06, nuevo, 10467
year=2025-06, región, 10358
year=2025-06, medio, 10004
year=2025-06, primera, 9902
year=2025-06, seguridad, 9896
year=2025-06, mayor, 9890
year=2025-06, embargo, 9822
year=2025-07, chile, 33637
year=2025-07, país, 18194
year=2025-07, nacional, 16262
year=2025-07, millones, 15137
year=2025-07, julio, 13980
year=2025-07, estado, 12913
year=2025-07, partido, 12862
year=2025-07, presidente, 12103
year=2025-07, gobierno, 11354
year=2025-07, nuevo, 10797
year=2025-07, primera, 10712
year=2025-07, forma, 10632
year=2025-07, tiempo, 10585
year=2025-07, mercado, 10543
year=2025-07, mundo, 10534
year=2025-07, mayor, 10530
year=2025-07, vida, 10490
year=2025-07, seguridad, 10255
year=2025-07, región, 10241
year=2025-07, nueva, 10222
year=2025-08, chile, 35203
year=2025-08, país, 17675
year=2025-08, nacional, 16455
year=2025-08, millones, 16094
year=2025-08, partido, 15055
year=2025-08, agosto, 13544
year=2025-08, gobierno, 12945
year=2025-08, presidente, 12319
year=2025-08, seguridad, 12083
year=2025-08, estado, 12070
year=2025-08, vida, 11254
year=2025-08, región, 11246
year=2025-08, primera, 10986
year=2025-08, nuevo, 10949
year=2025-08, tiempo, 10885
year=2025-08, lugar, 10615
year=2025-08, mayor, 10357
year=2025-08, trabajo, 10213
year=2025-08, primer, 10183
year=2025-08, colo, 10165
year=2025-09, chile, 38401
year=2025-09, país, 20073
year=2025-09, septiembre, 16993
year=2025-09, nacional, 16741
year=2025-09, millones, 15745
year=2025-09, estado, 14949
year=2025-09, gobierno, 14264
year=2025-09, presidente, 13378
year=2025-09, partido, 12397
year=2025-09, vida, 12242
year=2025-09, tiempo, 11901
year=2025-09, seguridad, 11675
year=2025-09, mundo, 11070
year=2025-09, mayor, 10878
year=2025-09, región, 10833
year=2025-09, primera, 10762
year=2025-09, primer, 10687
year=2025-09, lugar, 10653
year=2025-09, nueva, 10644
year=2025-09, mismo, 10099
year=2025-10, chile, 44176
year=2025-10, país, 23090
year=2025-10, nacional, 21102
year=2025-10, millones, 18366
year=2025-10, octubre, 17350
year=2025-10, gobierno, 16537
year=2025-10, presidente, 15614
year=2025-10, estado, 15334
year=2025-10, partido, 14708
year=2025-10, primera, 13984
year=2025-10, vida, 13969
year=2025-10, tiempo, 13304
year=2025-10, mundo, 12331
year=2025-10, acuerdo, 12076
year=2025-10, mundial, 12054
year=2025-10, seguridad, 11940
year=2025-10, región, 11860
year=2025-10, público, 11848
year=2025-10, nuevo, 11773
year=2025-10, nueva, 11669
year=2025-11, chile, 44203
year=2025-11, país, 21534
year=2025-11, nacional, 19255
year=2025-11, partido, 18433
year=2025-11, noviembre, 16340
year=2025-11, millones, 15598
year=2025-11, primera, 15545
year=2025-11, kast, 15284
year=2025-11, gobierno, 14613
year=2025-11, jara, 14043
year=2025-11, presidente, 13567
year=2025-11, estado, 13183
year=2025-11, lugar, 11964
year=2025-11, seguridad, 11916
year=2025-11, región, 11894
year=2025-11, vida, 11768
year=2025-11, tiempo, 11558
year=2025-11, trabajo, 11149
year=2025-11, mundo, 10825
year=2025-11, mismo, 10714
year=2025-12, chile, 39920
year=2025-12, kast, 22450
year=2025-12, país, 20621
year=2025-12, gobierno, 19012
year=2025-12, presidente, 18764
year=2025-12, nacional, 16702
year=2025-12, diciembre, 15164
year=2025-12, primera, 14185
year=2025-12, nuevo, 13809
year=2025-12, millones, 13783
year=2025-12, estado, 13378
year=2025-12, josé, 13101
year=2025-12, partido, 12330
year=2025-12, seguridad, 11777
year=2025-12, región, 11481
year=2025-12, antonio, 11223
year=2025-12, vida, 11199
year=2025-12, tiempo, 10985
year=2025-12, lugar, 10630
year=2025-12, acuerdo, 10434
"""
