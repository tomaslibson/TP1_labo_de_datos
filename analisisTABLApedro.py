# -*- coding: utf-8 -*-
"""
Created on Thu May  8 10:25:16 2025

@author: libso
"""

import pandas as pd
import duckdb

tabla_BP = pd.read_csv("/home/Estudiante/Descargas/tabla_BP.csv", encoding="latin1")

tabla_EE = pd.read_csv("/home/Estudiante/Descargas/tabla_EE.csv", skiprows= 12) 

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


Establecimientos = con.execute("""
               SELECT Cueanexo, Nombre, Jurisdicción, Departamento
               FROM tabla_EE
              
               """ ).fetchdf()
               
               
Deptos =  con.execute("""
               SELECT Departamento, "Código de Departamento"
               FROM tabla_EE
              
               """ ).fetchdf()          
               
               
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


