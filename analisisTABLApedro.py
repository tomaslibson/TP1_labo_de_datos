# -*- coding: utf-8 -*-
"""
Created on Thu May  8 10:25:16 2025

@author: libso
"""

import pandas as pd
import duckdb
import seaborn as sns
import matplotlib.pyplot as plt

tabla_BP = pd.read_csv("C:/Users/pedro/OneDrive/Documentos/A LABODATOS/tabla_BP.csv", encoding="latin1")

tabla_EE = pd.read_csv("C:/Users/pedro/OneDrive/Documentos/A LABODATOS/tabla_EE.csv", skiprows= 12) 

con = duckdb.connect()

con.register('tabla_BP', tabla_BP)

con.register('tabla_EE', tabla_EE)

###POBLACION####



#JARDIN
Jardin = pd.read_excel("C:/Users/pedro/OneDrive/Documentos/A LABODATOS/GE_Jardin.xlsX", header = 10)  #le saco el titulo de la tabla y demas.


Jardin = Jardin.drop(Jardin.columns[0], axis =1) #acomodo para nombrar las columnas

Jardin.columns = Jardin.iloc[0]

Jardin = Jardin[1:] 

Jardin = Jardin.rename(columns = {'C1' : 'Poblacion'}) #Renombro la columna con la informacion de la Poblacion



#Agrego fila de CABA resumiendo la informacion de las filas de Comunas 

suma_Jardin = Jardin.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Jardin = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Jardin} ] ) )

Jardin = Jardin[15:].reset_index(drop=True)

Jardin = pd.concat([nueva_fila_Jardin, Jardin], ignore_index = True)

Jardin = Jardin.iloc[:-5,] #Le saco las ultimas filas, TOTAL y vacias.

#PRIMARIA
Primaria = pd.read_excel("C:/Users/pedro/OneDrive/Documentos/A LABODATOS/GE_Primaria.xlsX", header = 10)

Primaria = Primaria.drop(Primaria.columns[0], axis =1)

Primaria.columns = Primaria.iloc[0]

Primaria = Primaria[1:]

Primaria = Primaria.rename(columns = {'C1' : 'Poblacion'})

#Agrego fila de CABA resumiendo la informacion de las filas de Comunas 

suma_Primaria = Primaria.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Primaria = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Primaria} ] ) )

Primaria = Primaria[15:].reset_index(drop=True)

Primaria = pd.concat([nueva_fila_Primaria, Primaria], ignore_index = True)

Primaria = Primaria.iloc[:-5,] 

#SECUNDARIA
Secundaria = pd.read_excel("C:/Users/pedro/OneDrive/Documentos/A LABODATOS/GE_Secundaria.xlsX", header = 10)

Secundaria = Secundaria.drop(Secundaria.columns[0], axis =1)

Secundaria.columns = Secundaria.iloc[0]

Secundaria = Secundaria[1:]

Secundaria = Secundaria.rename(columns = {'C1' : 'Poblacion'})

#Agrego fila de CABA resumiendo la informacion de las filas de Comunas 

suma_Secundaria = Secundaria.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Secundaria = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Secundaria} ] ) )

Secundaria = Secundaria[15:].reset_index(drop=True)

Secundaria = pd.concat([nueva_fila_Secundaria, Secundaria], ignore_index = True)

Secundaria = Secundaria.iloc[:-5,] 

#ADULTOS

Adultos = pd.read_excel("C:/Users/pedro/OneDrive/Documentos/A LABODATOS/GE_Adultos.xlsX", header = 10)

Adultos = Adultos.drop(Adultos.columns[0], axis =1)

Adultos.columns = Adultos.iloc[0]

Adultos = Adultos[1:]

Adultos = Adultos.rename(columns = {'C1' : 'Poblacion'})

#Agrego fila de CABA resumiendo la informacion de las filas de Comunas 

suma_Adultos = Adultos.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Adultos = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Adultos} ] ) )

Adultos = Adultos[15:].reset_index(drop=True)

Adultos = pd.concat([nueva_fila_Adultos, Adultos], ignore_index = True)

Adultos = Adultos.iloc[:-5,] 

# 1 a 15 index son comunas, quiero ju ntar todas las filas,  


tabla_pob =  con.execute("""
               SELECT 
               Jardin.Código AS id_departamento,
               Jardin.Departamento,
               Jardin.Poblacion AS poblacion_Jardin,
               Primaria.Poblacion AS poblacion_Primaria,
               Secundaria.Poblacion AS poblacion_Secundaria,
               Adultos.Poblacion AS poblacion_Adultos
               FROM Jardin
               JOIN Primaria ON Jardin.Código = Primaria.Código
               JOIN Secundaria ON Jardin.Código = Secundaria.Código
               JOIN Adultos ON Jardin.Código = Adultos.Código


               """ ).fetchdf()
               
               
tabla_pob['poblacion_tot'] = tabla_pob.iloc[:, -4] + tabla_pob.iloc[:, -3] + tabla_pob.iloc[:, -2]  + tabla_pob.iloc[:, -1]

Poblacion = tabla_pob

con.register("Poblacion", Poblacion)




consulta = con.execute("""
               SELECT DISTINCT Sector
               FROM tabla_EE
              
               """ ).fetchdf()
               


nulos_mail = con.execute("""
               SELECT Mail 
               FROM tabla_EE
               WHERE Mail IS NULL
               """ ).fetchdf()
               
              

nulos_telefonofake = con.execute("""
               SELECT Teléfono 
               FROM tabla_EE
               WHERE Teléfono IS NULL
               """ ).fetchdf()
              
                

modalidad_comun = con.execute("""
               SELECT Común 
               FROM tabla_EE
               WHERE Común IS NOT NULL
               """ ).fetchdf()
              
cpnull = con.execute("""
               SELECT "C. P." 
               FROM tabla_EE
               WHERE "C. P." IS NULL
               """ ).fetchdf()
               
               
ambitonull = con.execute("""
               SELECT Ámbito 
               FROM tabla_EE
               WHERE Ámbito  IS NULL
               """ ).fetchdf()

        
#CHEQUEO LA COLUMNA TELEFONOS 

telefonos_validos = con.execute("""
               SELECT COUNT(*) AS cantidad_validos
               FROM tabla_EE
               WHERE LENGTH(REGEXP_REPLACE(Teléfono, '[^0-9]', '', 'g')) = 10
               """).fetchdf()
 
               
telefonos_Novalidos = con.execute("""
               SELECT COUNT(*) AS cantidad_validos
               FROM tabla_EE
               WHERE LENGTH(REGEXP_REPLACE(Teléfono, '[^0-9]', '', 'g')) != 10
               """).fetchdf()           

#CHEQUEO LA COLUMNA MAILS

testtomi = con.execute("""
               SELECT Mail 
               FROM tabla_EE
               WHERE Mail NOT LIKE '%@%' OR Mail IS NULL
               
               
               """ ).fetchdf()


# defino las tablas en las que voy a descomponer:

    
Establecimientos = con.execute("""
               SELECT
               Cueanexo,
               Nombre,
               CASE 
               WHEN Departamento ILIKE 'Comuna %' THEN '02000'
               ELSE "Código de departamento"
               END AS id_departamento,
               "Nivel inicial - Jardín maternal", "Nivel inicial - Jardín de infantes", Primario, Secundario
               FROM tabla_EE;
               """).fetchdf()
               
Bibliotecas  =  con.execute ("""
                  
                  SELECT nro_conabip, id_departamento, mail, fecha_fundacion
                  FROM tabla_BP
                  ORDER BY nro_conabip
                  
                  
    """).fetchdf()


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



con.register("Establecimientos", Establecimientos)

con.register("Bibliotecas", Bibliotecas)

con.register("Departamentos", Departamentos)    












               
               
               
ejercicio1 = con.execute("""
        SELECT 
        Jurisdicción,
        Departamento,
        COUNT(CASE WHEN "Nivel inicial - Jardín maternal" = 1 OR "Nivel inicial - Jardín de infantes" THEN 1 END) AS Cant_Jardines,
        COUNT(CASE WHEN Primario = 1 THEN 1 END) AS Cant_Primarias,
        COUNT(CASE WHEN Secundario = 1 THEN 1 END) AS Cant_Secundarias
        FROM tabla_EE
        GROUP BY Jurisdicción, Departamento
        ORDER BY Jurisdicción, Departamento;
               """ ).fetchdf()
               
#OJO este codigo saca la data de la tabla original, habria que ver como conseguir el mismo resultado pero a partir de las tablas que descompuse.
#OSEA habria que sacar la data de "Establecimientos" y de "nivel_educativo". Se hace con un join????

               

#prubo  hacer un join a partir del esquema que hice

#Join1 = con.execute("""
 #       SELECT 
    #    e.Departamento,
     #   COUNT(CASE WHEN n."Nivel inicial - Jardín maternal" = 1 
      #               OR n."Nivel inicial - Jardín de infantes" = 1 THEN 1 END) AS Cant_Jardines,
       # COUNT(CASE WHEN n.Primario = 1 THEN 1 END) AS Cant_Primarias,
        #COUNT(CASE WHEN n.Secundario = 1 THEN 1 END) AS Cant_Secundarias
        #FROM Establecimientos e
        #JOIN nivel_educativo n ON e.Cueanexo = n.Cueanexo
        #GROUP BY  e.Departamento
        # ORDER BY  e.Departamento;
         #      """).fetchdf()

#con.register("Join1" , Join1)


       
#Provinciapaso1 = con.execute("""
#               SELECT DISTINCT
 #              CASE
  #             WHEN Departamento ILIKE 'Comuna %' THEN '02000'
   #            ELSE "Código de departamento"
    #           END AS "Código de departamento",
     #          Jurisdicción,
       #        CASE
        #        WHEN Departamento ILIKE 'Comuna %' THEN 'CIUDAD AUTÓNOMA DE BUENOS AIRES'
         #      ELSE Departamento
          #     END AS Departamento
           #    FROM tabla_EE
            #   GROUP BY "Código de departamento", Jurisdicción, Departamento
             #  ORDER BY "Código de departamento", Jurisdicción, Departamento
              
  #              """ ).fetchdf()

#Provincias = con.execute("""
 #              SELECT "Código de departamento", 
  #             Jurisdicción
   #            FROM Provinciapaso1
    #          
     #          """ ).fetchdf()            
               

dale = con.execute("""
    SELECT 
        d.Provincia,
        d.Departamento,
        
        COUNT(CASE 
                WHEN e."Nivel inicial - Jardín maternal" = 1 
                  OR e."Nivel inicial - Jardín de infantes" = 1 
                THEN 1 END) AS cantidad_jardines,
        
        COUNT(CASE WHEN e.Primario = 1 THEN 1 END) AS cantidad_primarias,
        
        COUNT(CASE WHEN e.Secundario = 1 THEN 1 END) AS cantidad_secundarias
        
    FROM Establecimientos e
    JOIN Departamentos d
      ON e.id_departamento = d.id_departamento
      
    GROUP BY d.Provincia, d.Departamento
    ORDER BY d.Provincia, cantidad_primarias
""").fetchdf()


ejercicio3 = con.execute("""
    SELECT
    D.Provincia,
    D.Departamento,
    (EE.cantidad_jardines + EE.cantidad_primarias + EE.cantidad_secundarias) AS Cant_EE, 
    --del ejercicio 1 sumo cada columna de cantidad y consigo la cantidad total de EE en cada departamento
    COUNT(BP.nro_conabip) AS Cant_BP 
FROM dale AS EE
JOIN Departamentos AS D
    ON LOWER(TRIM(EE.Provincia)) = LOWER(TRIM(D.Provincia)) --por las dudas paso todo a minusculas y elimino los espacios para que el join sea mas preciso
    AND LOWER(TRIM(EE.Departamento)) = LOWER(TRIM(D.Departamento)) --lo mismo para departamento
LEFT JOIN Bibliotecas AS BP
    ON D.id_departamento = BP.id_departamento --hago un left join para que si hay un Departamento que tiene EE pero no tiene EE o viceversa, aparezca igual
GROUP BY
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
        
        COUNT(CASE WHEN e.Secundario = 1 THEN 1 END) AS cantidad_secundarias
        
    FROM Establecimientos e
    JOIN Departamentos d
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





# Paso 1: Transformar a formato "largo" (long format)
df_long = pd.ejercicio1_2()

niveles = ['Jardín', 'Primaria', 'Secundaria']
for nivel in niveles:
    df_nivel = pd.ejercicio1_2({
        'Provincia': ejercicio1_2['Provincia'],
        'Departamento': ejercicio1_2['Departamento'],
        'Nivel': nivel,
        'Cantidad_EE': ejercicio1_2[f'{nivel}es'] if nivel != 'Jardín' else ejercicio1_2['Jardines'],
        'Población': ejercicio1_2[f'Población {nivel}']
    })
    df_long = pd.concat([df_long, df_nivel], ignore_index=True)

# Paso 2: Gráfico
plt.figure(figsize=(10, 6))
sns.scatterplot(
    data=df_long,
    x='Población',
    y='Cantidad_EE',
    hue='Nivel',
    palette='Set1',
    s=100
)
plt.title('Cantidad de EE vs Población por Nivel Educativo')
plt.xlabel('Población')
plt.ylabel('Cantidad de EE')
plt.grid(True)
plt.tight_layout()
plt.show()

    
    


    
               
               
