# -*- coding: utf-8 -*-
"""
Created on Thu May  8 10:25:16 2025

@author: libso
"""

import pandas as pd
import duckdb

tabla_BP = pd.read_csv("C:/Users/pedro/OneDrive/Documentos/A LABODATOS/tabla_BP.csv", encoding="latin1")

tabla_EE = pd.read_csv("C:/Users/pedro/OneDrive/Documentos/A LABODATOS/tabla_EE.csv", skiprows= 12) 

con = duckdb.connect()

con.register('tabla_BP', tabla_BP)

con.register('tabla_EE', tabla_EE)



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
               END AS "Código de departamento",
               "Nivel inicial - Jardín maternal", "Nivel inicial - Jardín de infantes", Primario, Secundario
               FROM tabla_EE;
               """).fetchdf()

               
               
Departamentos = con.execute("""
            SELECT DISTINCT
            CASE 
            WHEN Departamento ILIKE 'Comuna %' THEN '02000'
            ELSE "Código de departamento"
            END AS "Código de departamento",
            CASE 
            WHEN Departamento ILIKE 'Comuna %' THEN 'CIUDAD AUTÓNOMA DE BUENOS AIRES'
            ELSE Departamento
            END AS Departamento
            
            FROM tabla_EE
            GROUP BY 1, 2
            ORDER BY 1;
            """).fetchdf()
       
Provinciapaso1 = con.execute("""
               SELECT DISTINCT
               CASE
               WHEN Departamento ILIKE 'Comuna %' THEN '02000'
               ELSE "Código de departamento"
               END AS "Código de departamento",
               Jurisdicción,
               CASE
               WHEN Departamento ILIKE 'Comuna %' THEN 'CIUDAD AUTÓNOMA DE BUENOS AIRES'
               ELSE Departamento
               END AS Departamento
               FROM tabla_EE
               GROUP BY "Código de departamento", Jurisdicción, Departamento
               ORDER BY "Código de departamento", Jurisdicción, Departamento
              
               """ ).fetchdf()

Provincias = con.execute("""
               SELECT "Código de departamento", 
               Jurisdicción
               FROM Provinciapaso1
              
               """ ).fetchdf()            
               

               


con.register("Establecimientos", Establecimientos)
               
               
               
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

Join1 = con.execute("""
       SELECT 
       e.Departamento,
        COUNT(CASE WHEN n."Nivel inicial - Jardín maternal" = 1 
                     OR n."Nivel inicial - Jardín de infantes" = 1 THEN 1 END) AS Cant_Jardines,
        COUNT(CASE WHEN n.Primario = 1 THEN 1 END) AS Cant_Primarias,
        COUNT(CASE WHEN n.Secundario = 1 THEN 1 END) AS Cant_Secundarias
        FROM Establecimientos e
        JOIN nivel_educativo n ON e.Cueanexo = n.Cueanexo
        GROUP BY  e.Departamento
        ORDER BY  e.Departamento;
               """).fetchdf()

#con.register("Join1" , Join1)





               
               
