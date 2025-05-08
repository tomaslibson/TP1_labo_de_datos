# -*- coding: utf-8 -*-
"""
Created on Thu May  8 10:25:16 2025

@author: libso
"""

import pandas as pd
import duckdb

carpeta = "C:\\Users\\libso\\OneDrive\\Escritorio\\ubaTarea\\Labo_de_datos\\TP_1_labo_de_datos\\Tablas\\"

tabla_BP = pd.read_csv(carpeta+"tabla_BP.csv")

tabla_EE = pd.read_csv(carpeta+"tabla_EE.csv", skiprows= 11)

con = duckdb.connect()


consulta = con.execute("""
               SELECT DISTINCT anio_actualizacion
               FROM tabla_BP
              
               """ ).fetchdf()
               
problema = con.execute("""
               SELECT DISTINCT id_departamento, departamento
               FROM tabla_BP
              GROUP BY id_departamento, departamento
              HAVING COUNT(*) > 1
              ORDER BY id_departamento
               """ ).fetchdf()
               
problema2 =con.execute("""
               SELECT DISTINCT 
               FROM problema
              
               """ ).fetchdf()