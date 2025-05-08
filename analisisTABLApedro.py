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
               
              

nulos_telefono = con.execute("""
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
     

               
