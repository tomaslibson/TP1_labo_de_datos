# -*- coding: utf-8 -*-
"""
Created on Thu May  8 10:25:16 2025

@author: libso
"""

import pandas as pd
import duckdb

carpeta = "C:\\Users\\libso\\OneDrive\\Escritorio\\ubaTarea\\Labo_de_datos\\TP1_labo_de_datos\\ENTREGA\\TablasOriginales\\"

tabla_BP = pd.read_csv(carpeta+"tabla_BP.csv")

tabla_EE = pd.read_csv(carpeta+"tabla_EE.csv", skiprows= 11)



tabla_EE.columns = tabla_EE.iloc[0]

tabla_EE = tabla_EE[1:]

tabla_EE = tabla_EE.reset_index(drop=True)





con = duckdb.connect()

                
#Metricas de calidad##

#M1

#domicilios sin altura, sin calle o sin ambas
domicilios_incompletos = con.execute ("""
                  
                  SELECT domicilio
                  FROM tabla_BP
                  WHERE LOWER(domicilio)  LIKE '%sin %' 
                  OR LOWER(domicilio)  LIKE '% sin%'  
                  OR domicilio IS NULL
                  OR NOT REGEXP_MATCHES(domicilio, '[0-9]') 
                  OR NOT REGEXP_MATCHES(domicilio, '[A-Za-z]')  
                  OR REGEXP_MATCHES(LOWER(domicilio), 's\n')   

    """).fetchdf()
    
cant_dom_inc = len(domicilios_incompletos)    

cant_tot_domicilios = len(tabla_BP)

cant_dom_completos = cant_tot_domicilios - cant_dom_inc

M1 = (cant_dom_completos / cant_tot_domicilios) *100 # 10% de los datos de domicilio estan incompletos
# el 89 % de los datos estan completos, es decir, mas del 10% esta incompleto


#M2###########


domicilios_por_cant = con.execute ("""
                  
                  SELECT domicilio, COUNT(*) AS cantidad
                  FROM tabla_BP
                  GROUP BY domicilio
                  ORDER BY cantidad DESC
                  
    """).fetchdf()
    
    
#domicilios con repeticiones    
domicilios_repetidos = con.execute ("""
                  
                  SELECT SUM(cantidad) as TOTAL
                  FROM domicilios_por_cant
                  WHERE cantidad > 1
                  
    """).fetchdf()




M2 = (domicilios_repetidos.TOTAL / cant_tot_domicilios) * 100

#el 2% de los dato son inconcistentes

#Conclusion: el 12% de los datos de domicilio genera problemas de Calidad de Datos


#####CLAVE PPRINCIPAL

clave_prin =  con.execute ("""
                  
                  SELECT nro_conabip, COUNT(*) as cantidad
                  FROM tabla_BP
                  GROUP BY nro_conabip
                  ORDER BY cantidad 
                  
    """).fetchdf()
    
#conqbip es clave principa

# id departamento 

id_BP = con.execute ("""
                  
                  SELECT DISTINCT "Código de departamento"
                  FROM tabla_EE
                  
                  
    """).fetchdf()



#capital como departmento Repetido


capitla_BP = con.execute ("""
                  
                  SELECT DISTINCT provincia, departamento
                  FROM tabla_BP
                  WHERE LOWER(departamento) = 'capital'
                  
                  
    """).fetchdf()
    
capitla_EE = con.execute ("""
                  
                  SELECT DISTINCT Jurisdicción, Departamento
                  FROM tabla_EE
                  WHERE LOWER(Departamento) = 'capital'
                  
                  
    """).fetchdf()

#deptos repetidos

rep_dep = con.execute ("""
                  
                  SELECT id_departamento,departamento, COUNT(*) AS cantidad
                  FROM tabla_BP
                  GROUP BY id_departamento, departamento
                  ORDER BY cantidad
                  
                  
    """).fetchdf()
    
    

#Tablas Esquemas

tabla_BP_limpia  =  con.execute ("""
                  
                  SELECT nro_conabip, id_departamento, mail, fecha_fundacion
                  FROM tabla_BP
                  ORDER BY nro_conabip
                  
                  
    """).fetchdf()
    

tabla_departamento = con.execute ("""
                  
                  SELECT DISTINCT id_departamento, departamento
                  FROM tabla_BP
                  ORDER BY id_departamento
                  
    """).fetchdf()
    
tabla_provincia = con.execute ("""
                  
                  SELECT DISTINCT id_provincia, provincia
                  FROM tabla_BP
                  ORDER BY id_provincia
                  
    """).fetchdf()
    

## pruebas EE

