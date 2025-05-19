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

import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import numpy as np

###POBLACION####

carpeta = "C:\\Users\\libso\\OneDrive\\Escritorio\\ubaTarea\\Labo_de_datos\\TP1_labo_de_datos\\Tablas\\"

con = duckdb.connect()

#JARDIN
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

#PRIMARIA
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

#SECUNDARIA
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

#ADULTOS

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
###EJERCICIO 1#####

ejercicio1_1 = con.execute("""
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


ejercicio1_2 =  con.execute("""
                       SELECT 
                       j.Provincia,
                       j.Departamento,
                       j.cantidad_jardines AS Jardines,
                       p.poblacion_Jardin AS "Poblacion Jardines",
                       j.cantidad_primarias AS Primarias,
                       p.poblacion_Primaria AS "Poblacion Primaria",
                       j.cantidad_secundarias AS Secundarias,
                       p.poblacion_Secundaria AS "Poblacion Secundaria"
                       
                       FROM ejercicio1_1 j
                       LEFT JOIN Poblacion p   
                         ON j.id_departamento = p.id_departamento
                         
                       ORDER BY Provincia ASC , Primarias DESC  
                      
""").fetchdf()


##EJERCICIO 2



#Selcciono la suma cantidad de BPs por id_departamento
ejercicio2_1 = con.execute("""
            SELECT 
            id_departamento,
            COUNT(*) as "Cantidad de BP fundadas desde 1950"
            FROM Bibliotecas                                 --Seleccionamos las Bibliotecas con fecha de fundacion posterior a 1950
            WHERE SUBSTR(fecha_fundacion, 1, 4) >= '1950'
            GROUP BY id_departamento
            ORDER BY id_departamento
            """).fetchdf()



#Desde Departamentos selecciono a partir del id_departamento los nombres de la Provincia y el Departamento
ejercicio2_2 = con.execute("""
           SELECT 
               d.Provincia,
               d.Departamento,
               j."Cantidad de BP fundadas desde 1950",
               FROM ejercicio2_1 j
               JOIN Departamentos d
                 ON j.id_departamento = d.id_departamento
               ORDER BY d.Provincia ASC, j."Cantidad de BP fundadas desde 1950" DESC
           
           """).fetchdf()

##EJERCICIO 3


ejercicio3_1 = con.execute("""
                             
                SELECT
                D.id_departamento,
                D.Provincia,
                D.Departamento,
                (EE.cantidad_jardines + EE.cantidad_primarias + EE.cantidad_secundarias) AS Cant_EE, 
                -- del ejercicio 1 sumo cada columna de cantidad y consigo la cantidad total de EE en cada departamento
                COUNT(BP.nro_conabip) AS Cant_BP 
            FROM ejercicio1_1 AS EE
            JOIN Departamentos AS D
                ON LOWER(TRIM(EE.Provincia)) = LOWER(TRIM(D.Provincia)) --por las dudas paso todo a minusculas y elimino los espacios para que el join sea mas preciso
                AND LOWER(TRIM(EE.Departamento)) = LOWER(TRIM(D.Departamento)) --lo mismo para departamento
            LEFT JOIN Bibliotecas AS BP
                ON D.id_departamento = BP.id_departamento --hago un left join para que si hay un Departamento que tiene EE pero no tiene EE o viceversa, aparezca igual
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
    
    
ejercicio3_2 =  con.execute("""
                       SELECT 
                       j.Provincia,
                       j.Departamento,
                       j.Cant_EE,
                       j.Cant_BP,
                       p.poblacion_tot AS Poblacion,
                       
                       FROM ejercicio3_1 j
                       LEFT JOIN Poblacion p
                         ON j.id_departamento = p.id_departamento
                         
                       ORDER BY j.Cant_EE DESC, 
                       j.Cant_BP DESC,
                       j.Provincia ASC,
                       j.Departamento ASC;
                      
""").fetchdf()

##EJERCICIO 4

ejercicio4_1 = con.execute("""
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


ejercicio4_2 = con.execute("""
                       SELECT 
                       d.Provincia,
                       d.Departamento,
                       j.Dominio AS "Dominio más Frecuente en BP",
                       FROM ejercicio4_1 j
                       JOIN Departamentos d
                         ON d.id_departamento = j.id_departamento
                       GROUP BY d.Provincia , d.Departamento, j.Dominio  
                       ORDER BY d.Provincia ASC
""").fetchdf()


###GRAFICOS

#1 Cantidad de BP por Provincia

graf1_1 = con.execute("""
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


plt.figure(figsize=(12,6))  # tamaño ancho x alto

plt.bar(graf1_1['Provincia'], graf1_1['Cantidad BP por Provincia'])

plt.title('Cantidad BP por Provincia', fontsize=16)
plt.xlabel('Provincia', fontsize=14)
plt.ylabel('Cantidad BP', fontsize=14)

plt.xticks(rotation=45, ha='right', fontsize=12)  # rotar etiquetas 45° y alinearlas a la derecha

plt.yticks(fontsize=12)

plt.grid(axis='y', linestyle='--', alpha=0.7)  # líneas guía horizontales


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
                     FROM Establecimientos
                     GROUP BY id_departamento
                     ) e
                     JOIN Departamentos d
                       ON d.id_departamento = e.id_departamento
                     ORDER BY d.Provincia ASC, e.cant_depto DESC
""").fetchdf()


# Agarro todas las provincias salvo Ciudad de Buenos Aires
provincias = graf3_1['Provincia'].unique()
provincias_filtradas = [prov for prov in provincias if prov != 'Ciudad de Buenos Aires'] 

# Agrupar datos por provincia filtrada
data_por_provincia = {
    prov: graf3_1.loc[graf3_1['Provincia'] == prov, 'Cantidad EE por Departamento'].dropna().values
    for prov in provincias_filtradas
}

# Ordenar provincias por mediana
provincias_ordenadas = sorted(provincias_filtradas, key=lambda p: np.median(data_por_provincia[p]))

# Crear lista con los datos en orden correcto
data_ordenada = [data_por_provincia[prov] for prov in provincias_ordenadas]

# Graficar boxplot horizontal
plt.figure(figsize=(14, len(provincias_ordenadas)*0.4))
plt.boxplot(data_ordenada, labels=provincias_ordenadas, vert=False)
plt.xlabel("Cantidad EE por Departamento")
plt.ylabel("Provincias*", fontsize = 14)
plt.title("Boxplot horizontal de Cantidad EE por Departamento por Provincia", fontsize = 14)
plt.grid(axis='x', linestyle='--', alpha=1)
plt.grid(axis='y', linestyle='--', alpha=1)
plt.figtext(0.05, 0.01, "*Ciudad de Buenos Aires: 2753", ha="left", fontsize=12, style="italic")

plt.tight_layout()
plt.show()