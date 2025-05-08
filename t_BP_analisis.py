# -*- coding: utf-8 -*-
"""
Created on Thu May  8 10:25:16 2025

@author: libso
"""

import pandas as pd
import duckdb

carpeta = "C:\\Users\\libso\\OneDrive\\Escritorio\\ubaTarea\\Labo_de_datos\\TP1_labo_de_datos\\Tablas\\"

tabla_BP = pd.read_csv(carpeta+"tabla_BP.csv")

tabla_EE = pd.read_csv(carpeta+"tabla_EE.csv", skiprows= 11)

con = duckdb.connect()


consulta = con.execute("""
               SELECT DISTINCT anio_actualizacion
               FROM tabla_BP
              
               """ ).fetchdf()
               
               
 ############      
               
p_iddepto_depto = con.execute("""
               SELECT DISTINCT id_provincia, provincia, id_departamento, departamento
               FROM tabla_BP
               WHERE departamento = 'Capital'
               """ ).fetchdf()
               
p_localidad = con.execute("""
               SELECT DISTINCT id_provincia, provincia, id_departamento,departamento, localidad
               FROM tabla_BP
               WHERE departamento = 'Capital'
               AND (id_departamento = 14014 or id_departamento = 14091)
               """ ).fetchdf()
              
                
p_idproblematicos = con.execute("""
               SELECT id_provincia, provincia, id_departamento,departamento, localidad
               FROM tabla_BP
               WHERE id_departamento = 14014 or id_departamento = 14091
               """ ).fetchdf()
               
###########

p2 = con.execute("""
                 
               SELECT DISTINCT  departamento, localidad
               FROM tabla_BP
               WHERE localidad IS NOT NULL and departamento IS NOT NULL
               ORDER BY localidad
               """ ).fetchdf()
               
p_localidadesrepetidas = con.execute("""
                 
               SELECT DISTINCT localidad
               FROM p2
              
               """ ).fetchdf()
               
 ###########


p3aux = con.execute("""
                  
                SELECT nombre
                FROM tabla_BP
                
                
                """ ).fetchdf()            
               

p3 = con.execute("""
                  
                SELECT nombre, COUNT(*) AS cantidad
                FROM tabla_BP
                GROUP BY nombre
                ORDER BY cantidad
                """ ).fetchdf()            
               

biblioteca = 'Bib.Pop. D.F.Sarmiento'

p3_ejemplo = con.execute( """
              SELECT id_provincia, provincia, id_departamento,departamento, localidad, nombre
              FROM tabla_BP
              WHERE nombre = 'Bib.Pop. D.F.Sarmiento'
   """ ).fetchdf()




##################################

p4aux = con.execute("""
                  
                SELECT domicilio
                FROM tabla_BP
                
                
                """ ).fetchdf()            
               

p4 = con.execute("""
                  
                SELECT domicilio, COUNT(*) AS cantidad
                FROM tabla_BP
                GROUP BY domicilio
                ORDER BY cantidad
                """ ).fetchdf()            
               

domicilio = 'Bib.Pop. D.F.Sarmiento'

p4_ejemplo = con.execute( """
              SELECT id_provincia, provincia, id_departamento,departamento, localidad, domicilio
              FROM tabla_BP
   """ ).fetchdf()



##################

prob_cp = con.execute("""
                  
                SELECT cp, COUNT(*) AS cantidad
                FROM tabla_BP
                GROUP BY cp
                ORDER BY cantidad
                """ ).fetchdf()            
               

prob_codtel = con.execute("""
                  
                SELECT cod_tel, COUNT(*) AS cantidad
                FROM tabla_BP
                GROUP BY cod_tel
                ORDER BY cantidad
                """ ).fetchdf()            
               
#################


prob_tel = con.execute("""
                  
                SELECT cod_tel,telefono ,  COUNT(*) AS cantidad
                FROM tabla_BP
                GROUP BY cod_tel, telefono
                ORDER BY cantidad
                """ ).fetchdf()            
               



##################

prob_mail = con.execute("""
                  
                SELECT mail,  COUNT(*) AS cantidad
                FROM tabla_BP
                GROUP BY mail
                ORDER BY cantidad
                """ ).fetchdf() 

'nuevabibliomatienzo@hotmail.com'

prob_mailaux = con.execute("""
                  
                SELECT id_provincia, provincia, id_departamento,departamento, localidad, domicilio , mail
                FROM tabla_BP
                WHERE mail = 'nuevabibliomatienzo@hotmail.com'
                """ ).fetchdf() 


