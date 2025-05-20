# -*- coding: utf-8 -*-
"""
Created on Sun May 18 18:57:01 2025

@author: libso
"""

# -*- coding: utf-8 -*-
"""
Created on Thu May 15 13:40:25 2025

@author: libso
"""

# %%

# Materia: Laboratorio de Datos
# Fecha: 21/05/2025
# Nombre del Grupo: Andate Riquelme
# Integrantes: Pedro Raffo, Felipe Comas y Tomás Libson

# En el Codigo partimos desde 6 tablas: 4 de poblacion (distinguidas por grupo etario), Establecimientos Educativos (EE) y Bibliotecas Populares (BP).
# En el apartado Tablas de Poblacion por Grupo Etario manipulamos la informacion de las 4 tablas de poblacion para obtener nuestra tabla Poblacion descripta en el Modelo.
# Las tablas Establecimientos y Bibliotecas surgen a partir de la "limpieza" de las tablas EE y BP respectivamente. Proceso llevado a cabo segun los criterios ya mencionados en el infrome.
# Los apartados Ejercicios y Graficos estan destinados a la resolucion de los ejercios planteados en el enunciado.

#%%


import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os

carpeta = os.path.join(os.path.dirname(__file__), "TablasOriginales") + os.sep 

con = duckdb.connect()

###Tablas Poblacion por Grupo Etario

#GE_JARDIN
Jardin = pd.read_excel(carpeta+ "GE_Jardin.xlsX", header = 10)  #le saco el titulo de la tabla y demas informacion inutil


Jardin = Jardin.drop(Jardin.columns[0], axis =1) #Acomodamos para nombrar las columnas

Jardin.columns = Jardin.iloc[0]

Jardin = Jardin[1:] 

Jardin = Jardin.rename(columns = {'C1' : 'Poblacion'}) #Renombramos la columna con la informacion de la Poblacion



#Agregamos fila de CABA resumiendo la informacion de las filas de Comunas 

suma_Jardin = Jardin.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Jardin = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Jardin} ] ) )

Jardin = Jardin[15:].reset_index(drop=True)

Jardin = pd.concat([nueva_fila_Jardin, Jardin], ignore_index = True)

Jardin = Jardin.iloc[:-5,] #Le saco las ultimas filas, TOTAL y vacias.

#Repetimos lo mismo para las otras 3 tablas

#GE_PRIMARIA
Primaria = pd.read_excel(carpeta+ "GE_Primaria.xlsX", header = 10)

Primaria = Primaria.drop(Primaria.columns[0], axis =1)

Primaria.columns = Primaria.iloc[0]

Primaria = Primaria[1:]

Primaria = Primaria.rename(columns = {'C1' : 'Poblacion'})

suma_Primaria = Primaria.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Primaria = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Primaria} ] ) )

Primaria = Primaria[15:].reset_index(drop=True)

Primaria = pd.concat([nueva_fila_Primaria, Primaria], ignore_index = True)

Primaria = Primaria.iloc[:-5,] 

#GE_SECUNDARIA
Secundaria = pd.read_excel(carpeta+ "GE_Secundaria.xlsX", header = 10)

Secundaria = Secundaria.drop(Secundaria.columns[0], axis =1)

Secundaria.columns = Secundaria.iloc[0]

Secundaria = Secundaria[1:]

Secundaria = Secundaria.rename(columns = {'C1' : 'Poblacion'})


suma_Secundaria = Secundaria.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Secundaria = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Secundaria} ] ) )

Secundaria = Secundaria[15:].reset_index(drop=True)

Secundaria = pd.concat([nueva_fila_Secundaria, Secundaria], ignore_index = True)

Secundaria = Secundaria.iloc[:-5,] 

#GE_ADULTOS

Adultos = pd.read_excel(carpeta+ "GE_Adultos.xlsX", header = 10)

Adultos = Adultos.drop(Adultos.columns[0], axis =1)

Adultos.columns = Adultos.iloc[0]

Adultos = Adultos[1:]

Adultos = Adultos.rename(columns = {'C1' : 'Poblacion'})


suma_Adultos = Adultos.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Adultos = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Adultos} ] ) )

Adultos = Adultos[15:].reset_index(drop=True)

Adultos = pd.concat([nueva_fila_Adultos, Adultos], ignore_index = True)

Adultos = Adultos.iloc[:-5,] 


#Junto los Grupos Etarios

tabla_pob =  con.execute("""
               SELECT 
               Jardin.Código AS id_departamento,
               Jardin.Departamento,
               Jardin.Poblacion AS poblacion_Jardin,
               Primaria.Poblacion AS poblacion_Primaria,
               Secundaria.Poblacion AS poblacion_Secundaria,
               Adultos.Poblacion AS poblacion_Adultos
               FROM Jardin                                       --juntamos los 4 grupos con un join, dejando el id_departamento como key
               JOIN Primaria ON Jardin.Código = Primaria.Código
               JOIN Secundaria ON Jardin.Código = Secundaria.Código
               JOIN Adultos ON Jardin.Código = Adultos.Código


               """ ).fetchdf()
               
               
tabla_pob['poblacion_tot'] = tabla_pob.iloc[:, -4] + tabla_pob.iloc[:, -3] + tabla_pob.iloc[:, -2]  + tabla_pob.iloc[:, -1] #Armo el total de poblacion a partir de los 3 grupos

Poblacion = tabla_pob

con.register("Poblacion", Poblacion)



###TABLA BP####

tabla_BP = pd.read_csv(carpeta+"tabla_BP.csv")


Bibliotecas  =  con.execute ("""
                  
                  SELECT nro_conabip, id_departamento, mail, fecha_fundacion 
                  FROM tabla_BP
                  ORDER BY nro_conabip
                  
                  
    """).fetchdf()
   
con.register("Bibliotecas", Bibliotecas)

##Tabla EE###

tabla_EE = pd.read_csv(carpeta+"tabla_EE.csv", skiprows = 12)


    
Establecimientos = con.execute("""
               SELECT
               Cueanexo,
               Nombre,
               CASE 
               WHEN Departamento ILIKE 'Comuna %' THEN '02000'    --Juntamos las comunas debido a que la infromacion de BP esta dada en un solo departamento(Ciudad de Buenos Aires)
               ELSE "Código de departamento"
               END AS id_departamento,
               "Nivel inicial - Jardín maternal", "Nivel inicial - Jardín de infantes", Primario, Secundario
               FROM tabla_EE;
               """).fetchdf()


con.register("Establecimientos", Establecimientos)


###Tabla departamentos###
       
Departamentos = con.execute("""
               SELECT DISTINCT 
               CASE
               WHEN Departamento ILIKE 'Comuna %' THEN '02000'
               ELSE "Código de departamento"
               END AS id_departamento,
               Jurisdicción AS Provincia,
               CASE
               WHEN Departamento ILIKE 'Comuna %' THEN 'CIUDAD AUTÓNOMA DE BUENOS AIRES'
               ELSE Departamento
               END AS Departamento
               FROM tabla_EE
               GROUP BY "Código de departamento", Jurisdicción, Departamento
               ORDER BY "Código de departamento", Jurisdicción, Departamento
              
               """ ).fetchdf()

con.register("Departamentos", Departamentos)


#CONSULTAS SQL#

#EJERCICIO 1

consulta1_aux = con.execute("""
        SELECT 
        d.id_departamento,
        d.Provincia,
        d.Departamento,
        
        COUNT(CASE 
                WHEN e."Nivel inicial - Jardín maternal" = 1 
                  OR e."Nivel inicial - Jardín de infantes" = 1 
                THEN 1 END) AS cantidad_jardines,                
                    
        COUNT(CASE WHEN e.Primario = 1 THEN 1 END) AS cantidad_primarias,
        
        COUNT(CASE WHEN e.Secundario = 1 THEN 1 END) AS cantidad_secundarias    --obtenemos la cantidades de EE a partir de Establecimientos.
        
    FROM Establecimientos e
    JOIN Departamentos d                 --Unimos por Join a partir de la clave id_departamentos en ambas tablas. Esto lo hacemos para conectar las tablas de nuestro esquema.
      ON e.id_departamento = d.id_departamento            
    GROUP BY d.id_departamento, d.Provincia, d.Departamento
    ORDER BY d.Provincia , cantidad_primarias 
""").fetchdf()


consulta1 =  con.execute("""
                       SELECT 
                       j.Provincia,
                       j.Departamento,
                       j.cantidad_jardines AS Jardines,
                       p.poblacion_Jardin AS "Poblacion Jardines",
                       j.cantidad_primarias AS Primarias,
                       p.poblacion_Primaria AS "Poblacion Primarias",
                       j.cantidad_secundarias AS Secundarias,
                       p.poblacion_Secundaria AS "Poblacion Secundarias"
                       
                       FROM consulta1_aux j                                    
                       LEFT JOIN Poblacion p                            --intercalamos con un join la infromacion de la poblacion por cada grupo etario destinado a cada etapa educativa
                         ON j.id_departamento = p.id_departamento
                         
                       ORDER BY Provincia ASC , Primarias DESC  
                      
""").fetchdf()


##EJERCICIO 2



#Selcciono la suma cantidad de BPs por id_departamento
consulta2_aux = con.execute("""
            SELECT 
            id_departamento,
            COUNT(*) as "Cantidad de BP fundadas desde 1950"
            FROM Bibliotecas                                 --Seleccionamos las Bibliotecas con fecha de fundacion posterior a 1950
            WHERE SUBSTR(fecha_fundacion, 1, 4) >= '1950'
            GROUP BY id_departamento
            ORDER BY id_departamento
            """).fetchdf()



#Desde Departamentos selecciono a partir del id_departamento los nombres de la Provincia y el Departamento
consulta2 = con.execute("""
           SELECT 
               d.Provincia,
               d.Departamento,
               j."Cantidad de BP fundadas desde 1950",
               FROM consulta2_aux j
               JOIN Departamentos d
                 ON j.id_departamento = d.id_departamento
               ORDER BY d.Provincia ASC, j."Cantidad de BP fundadas desde 1950" DESC
           
           """).fetchdf()

##EJERCICIO 3


consulta3_aux = con.execute("""
                             
                SELECT
                D.id_departamento,
                D.Provincia,
                D.Departamento,
                (EE.cantidad_jardines + EE.cantidad_primarias + EE.cantidad_secundarias) AS Cant_EE,   -- del ejercicio 1 sumo cada columna de cantidad y consigo la cantidad total de EE en cada departamento
                COUNT(BP.nro_conabip) AS Cant_BP 
            FROM consulta1_aux AS EE
            JOIN Departamentos AS D
                ON LOWER(TRIM(EE.Provincia)) = LOWER(TRIM(D.Provincia))             --por las dudas paso todo a minusculas y elimino los espacios para que el join sea mas preciso
                AND LOWER(TRIM(EE.Departamento)) = LOWER(TRIM(D.Departamento))      --lo mismo para departamento
            LEFT JOIN Bibliotecas AS BP
                ON D.id_departamento = BP.id_departamento                           --hago un left join para que si hay un Departamento que tiene EE pero no tiene EE o viceversa, aparezca igual
            GROUP BY
                D.id_departamento,
                D.Provincia,
                D.Departamento,
                EE.cantidad_jardines,
                EE.cantidad_primarias,
                EE.cantidad_secundarias
            ORDER BY
                Cant_EE DESC, 
                Cant_BP DESC,
                D.Provincia ASC,
                D.Departamento ASC;

    """).fetchdf()
    
    
# le agregamos la poblacion total a cada departamento     
consulta3 =  con.execute("""
                       SELECT 
                       j.Provincia,
                       j.Departamento,
                       j.Cant_EE,
                       j.Cant_BP,
                       p.poblacion_tot AS Poblacion,
                       
                       FROM consulta3_aux j
                       LEFT JOIN Poblacion p
                         ON j.id_departamento = p.id_departamento
                         
                       ORDER BY j.Cant_EE DESC, 
                       j.Cant_BP DESC,
                       j.Provincia ASC,
                       j.Departamento ASC;
                      
""").fetchdf()

##EJERCICIO 4

consulta4_aux = con.execute("""
                       SELECT *
                       FROM (
                       SELECT
                       id_departamento,
                       SPLIT_PART( SPLIT_PART(mail, '@', 2), '.', 1) AS Dominio,   --Encontramos los dominios de mail y los contamos
                       COUNT(*) as cantidad,
                       ROW_NUMBER() OVER (PARTITION BY id_departamento ORDER BY cantidad DESC) AS ranking --a partir de la cantidad obtenida hacemos un ranking por departamento
                       FROM Bibliotecas
                       GROUP BY id_departamento, Dominio
                       ORDER BY ranking
                      ) sub
                       WHERE ranking = 1  --elegimos mas valuados por ranking en cada departamento
""").fetchdf()

# agregamos Provincia y Departamento por join en id_depatamento
consulta4 = con.execute("""
                       SELECT 
                       d.Provincia,
                       d.Departamento,
                       j.Dominio AS "Dominio más Frecuente en BP",
                       FROM consulta4_aux j
                       JOIN Departamentos d
                         ON d.id_departamento = j.id_departamento
                       GROUP BY d.Provincia , d.Departamento, j.Dominio  
                       ORDER BY d.Provincia ASC
""").fetchdf()


###GRAFICOS###

ruta_graficos = "C://Users//libso//OneDrive//Escritorio//ubaTarea//Labo_de_datos//TP1_labo_de_datos//ENTREGA//Graficos//"

#1 Cantidad de BP por Provincia

graf1 = con.execute("""
                      SELECT Provincia, COUNT(*) as "Cantidad BP por Provincia" --Armamos un COUNT por provincias. De manera que obtenemos la cantidad de filas donde existe esa provincia, es decir, la cantidad de EE por Provincia.
                      FROM
                      (
                       SELECT 
                       d.Provincia
                       FROM Departamentos d
                       JOIN Establecimientos e                   --hacemos una tabla donde cada fila representa un EE y dice su Provincia
                         ON d.id_departamento = e.id_departamento
                    )
                      GROUP BY Provincia
                      ORDER BY  "Cantidad BP por Provincia" DESC 
""").fetchdf()


# Armamamos un grafico de barras relacionando las Provincias con la cantidad de BP en cada una

plt.figure(figsize=(12,6))  
plt.bar(graf1['Provincia'], graf1['Cantidad BP por Provincia'])
plt.title('Cantidad BP por Provincia', fontsize=16)
plt.xlabel('Provincia', fontsize=14)
plt.ylabel('Cantidad BP', fontsize=14)
plt.xticks(rotation=45, ha='right', fontsize=12)  #Rotamos las etiquetas para que sean mas legibles
plt.yticks(fontsize=12)

plt.grid(axis='y', linestyle='--', alpha=0.7)  #Grilla horizontal


#2


#Usamos la tabla del ejercicio 1 de SQL:,
graf2 = consulta1

#Hacemos un DataFrame vacío para ir acumulando nuestros datos para el gráfico,
graf2_long = pd.DataFrame()
#Defino cuales son los niveles educativos para guardar su respectiva información.,
niveles = ['Jardines', 'Primarias', 'Secundarias']
#ahora buscamos la data para cada nivel en la tabla del ejercicio 1,
for nivel in niveles:
    graf2_nivel = pd.DataFrame({
        'Provincia': graf2['Provincia'],
        'Departamento': graf2['Departamento'],
        'Nivel': nivel,
        'Cantidad_EE': graf2[f'{nivel}'] , # Cantidad de EE del nivel educativo
        'Población': graf2[f"Poblacion {nivel}"] # Población correspondiente a ese nivel

    })
    graf2_long = pd.concat([graf2_long, graf2_nivel], ignore_index=True)

#Gráficamo,
plt.figure(figsize=(10, 6))
#Vamos a usar un gráfico de dispersión, ya que es el que mejor se amolda para representar las variables que nos piden:,
sns.scatterplot(
    data=graf2_long,
    x='Población', # Notemos que en el gráfico para que sea legible la poblacion se toma en millones 
    y='Cantidad_EE', 
    hue='Nivel', # Cada nivel educativo tiene su respectivo color
    palette='Set1', # color de los puntos 
    s=100 # tamaño de los puntos
)
plt.title('Cantidad de EE en función de la Población') # Título del gráfico
plt.xlabel('Población (millones)') # Nombre eje x
plt.ylabel('Cantidad de EE') # Nombre eje y 
plt.grid(True)
plt.tight_layout()




#3 

graf3_1 =  con.execute("""
                     SELECT
                         d.Provincia,
                         d.Departamento,
                         e.cant_depto AS "Cantidad EE por Departamento"
                     FROM (
                      SELECT 
                          id_departamento,
                          COUNT(*) AS cant_depto     
                     FROM Establecimientos      --Contamos las filas agrupadas por departamento para determinar la cantidad de EE por departamentos
                     GROUP BY id_departamento
                     ) e
                     JOIN Departamentos d
                       ON d.id_departamento = e.id_departamento
                     ORDER BY d.Provincia ASC, e.cant_depto DESC
""").fetchdf()


# Agarramos todas las provincias salvo Ciudad de Buenos Aires. 
# Debido a que como esta Provincia tiene un valor singular no nos beneficia hacer un analisis boxplot para ella. 
# Ademas hace mas ilegible el grafico. Ver como agregamos su valor con un *.  

provincias = graf3_1['Provincia'].unique()
provincias_filtradas = [prov for prov in provincias if prov != 'Ciudad de Buenos Aires'] 

# Agrupamos datos por provincia filtrada 
data_por_provincia = {
    prov: graf3_1.loc[graf3_1['Provincia'] == prov, 'Cantidad EE por Departamento'].dropna().values
    for prov in provincias_filtradas
}

# Ordenamos provincias segun su mediana
provincias_ordenadas = sorted(provincias_filtradas, key=lambda p: np.median(data_por_provincia[p]))

# Creamos una nueva lista ahora con el orde dado por las medianas
data_ordenada = [data_por_provincia[prov] for prov in provincias_ordenadas]

# Armamos el grafico
plt.figure(figsize=(14, len(provincias_ordenadas)*0.4))
plt.boxplot(data_ordenada, labels=provincias_ordenadas, vert=False)
plt.xlabel("Cantidad EE por Departamento")
plt.ylabel("Provincias*", fontsize = 14)
plt.title("Cantidad EE por Departamento por Provincia", fontsize = 14)
plt.grid(axis='x', linestyle='--', alpha=1)
plt.grid(axis='y', linestyle='--', alpha=1)
plt.figtext(0.05, 0.01, "*Ciudad de Buenos Aires: 2753", ha="left", fontsize=12, style="italic") # Aca el agregado del valor atipico de Ciudad de Buenos Aires
plt.tight_layout()



#4

#Hcemos tabla que me devuelva la provincia, el departamento y la cantidad de BP y EE cada mil habitantes usando la tabla del ejercicio 3.2

graf4 = con.execute("""
        SELECT Provincia, 
        Departamento, 
        ROUND((Cant_BP / Poblacion) * 1000.0, 2) AS bp_cada_mil, 
        ROUND((Cant_EE / Poblacion) * 1000.0, 2) AS ee_cada_mil
        FROM consulta3
        ORDER BY Provincia ASC, Departamento ASC;
        """).fetchdf()


#Hacemos un gráfico de disperción donde cada punto es un departamento representado con el color de su provincia. En el eje X figura la proporción de BP cada mil habitantes, y en el eje Y la proporción de EE cada mil habitantes.
plt.figure(figsize=(10, 6))
sns.scatterplot(data=graf4,
                x="bp_cada_mil",
                y="ee_cada_mil",
                hue="Provincia",
                s=25)

plt.title("Relación entre BP y EE cada mil habitantes")
plt.xlabel("BP cada mil habitantes")
plt.ylabel("EE cada mil habitantes")
plt.grid(True)
plt.tight_layout()
plt.legend(title="Provincia", bbox_to_anchor=(1.05, 1), loc='upper left')






