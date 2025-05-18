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


###POBLACION####

carpeta = "C:\\Users\\libso\\OneDrive\\Escritorio\\ubaTarea\\Labo_de_datos\\TP1_labo_de_datos\\Tablas\\"

con = duckdb.connect()

#JARDIN
Jardin = pd.read_excel(carpeta+ "GE_Jardin.xlsX", header = 10)

Jardin = Jardin.drop(Jardin.columns[0], axis =1)

Jardin.columns = Jardin.iloc[0]

Jardin = Jardin[1:]

Jardin = Jardin.rename(columns = {'C1' : 'Poblacion'})



#Agrego fila de CABA resumiendo la informacion de las filas de Comunas 

suma_Jardin = Jardin.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Jardin = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Jardin} ] ) )

Jardin = Jardin[15:].reset_index(drop=True)

Jardin = pd.concat([nueva_fila_Jardin, Jardin], ignore_index = True)


#PRIMARIA
Primaria = pd.read_excel(carpeta+ "GE_Primaria.xlsX", header = 10)

Primaria = Primaria.drop(Primaria.columns[0], axis =1)

Primaria.columns = Primaria.iloc[0]

Primaria = Primaria[1:]

Primaria = Primaria.rename(columns = {'C1' : 'Poblacion'})

#Agrego fila de CABA resumiendo la informacion de las filas de Comunas 

suma_Primaria = Primaria.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Primaria = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Primaria} ] ) )

Primaria = Primaria[15:].reset_index(drop=True)

Primaria = pd.concat([nueva_fila_Primaria, Primaria], ignore_index = True)


#SECUNDARIA
Secundaria = pd.read_excel(carpeta+ "GE_Secundaria.xlsX", header = 10)

Secundaria = Secundaria.drop(Secundaria.columns[0], axis =1)

Secundaria.columns = Secundaria.iloc[0]

Secundaria = Secundaria[1:]

Secundaria = Secundaria.rename(columns = {'C1' : 'Poblacion'})

#Agrego fila de CABA resumiendo la informacion de las filas de Comunas 

suma_Secundaria = Secundaria.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Secundaria = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Secundaria} ] ) )

Secundaria = Secundaria[15:].reset_index(drop=True)

Secundaria = pd.concat([nueva_fila_Secundaria, Secundaria], ignore_index = True)


#ADULTOS

Adultos = pd.read_excel(carpeta+ "GE_Adultos.xlsX", header = 10)

Adultos = Adultos.drop(Adultos.columns[0], axis =1)

Adultos.columns = Adultos.iloc[0]

Adultos = Adultos[1:]

Adultos = Adultos.rename(columns = {'C1' : 'Poblacion'})

#Agrego fila de CABA resumiendo la informacion de las filas de Comunas 

suma_Adultos = Adultos.loc[0:15 , 'Poblacion'].sum()

nueva_fila_Adultos = pd.DataFrame( ( [ { 'Código': 2000, 'Departamento': 'Ciudad de Buenos Aires', 'Poblacion': suma_Adultos} ] ) )

Adultos = Adultos[15:].reset_index(drop=True)

Adultos = pd.concat([nueva_fila_Adultos, Adultos], ignore_index = True)


# 1 a 15 index son comunas, quiero ju ntar todas las filas,  


tabla_pob =  con.execute("""
               SELECT 
               Jardin.Código,
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
               WHEN Departamento ILIKE 'Comuna %' THEN '02000'
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
    JOIN tabla_departamentos d
      ON e.id_departamento = d.id_departamento
      
    GROUP BY d.Provincia, d.Departamento
    ORDER BY d.Provincia, cantidad_primarias
""").fetchdf()



    