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



tabla_EE.columns = tabla_EE.iloc[0]

tabla_EE = tabla_EE[1:]

tabla_EE = tabla_EE.reset_index(drop=True)





con = duckdb.connect()


consulta = con.execute("""
               SELECT  cod_localidad
               FROM tabla_BP
               WHERE cod_localidad IS NULL
              
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
               
########################






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



########################333


prob_cod_localidad = con.execute("""
                  
                SELECT DISTINCT cod_localidad, localidad, COUNT(*) AS cantidad
                FROM tabla_BP
                GROUP BY cod_localidad, localidad
                ORDER BY cantidad
                """ ).fetchdf()            
               
########CLAVE PRIMARIA#############

clave_prim = con.execute("""
                  
                SELECT DISTINCT cod_localidad, id_provincia, id_departamento
                FROM tabla_BP
                """ ).fetchdf() 
                
prueba_clave = con.execute("""
                  
                SELECT  
        cod_localidad,
        SUBSTR(CAST(cod_localidad AS TEXT), 1, 1) AS id_provincia,
        SUBSTR(CAST(cod_localidad AS TEXT), 1, 4) AS id_departamento,
        FROM tabla_BP
                
                """ ).fetchdf()
                
tabla_prov = prueba_clave = con.execute("""
                  
                SELECT  id_departamento
                FROM tabla_BP
                
                """ ).fetchdf()
                
prueba_provincia = con.execute("""
                               
                SELECT id_departamento
                FROM prueba_clave
                INTERSECT ALL
                SELECT id_departamento
                FROM tabla_prov
        
                
                """ ).fetchdf()
         
            
#######################
         

fecha_fundacion_null= con.execute("""
                               
                SELECT fecha_fundacion
                FROM tabla_BP
                WHERE fecha_fundacion IS  NULL
        
                
                """ ).fetchdf()
                
                
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

#dommicilios sin el dato altura
domicilios_sin_altura = con.execute ("""
                  
                  SELECT domicilio, COUNT(*) AS cantidad
                  FROM tabla_BP
                  WHERE NOT REGEXP_MATCHES(domicilio, '[0-9]')  
                  GROUP BY domicilio
                  ORDER BY cantidad DESC
                  

    """).fetchdf()


domicilios_por_cant = con.execute ("""
                  
                  SELECT domicilio, COUNT(*) AS cantidad
                  FROM tabla_BP
                  GROUP BY domicilio
                  ORDER BY cantidad DESC
                  
    """).fetchdf()
    
    
#domicilios con repeticiones    
domicilios_cant_mayo_1 = con.execute ("""
                  
                  SELECT *
                  FROM domicilios_por_cant
                  WHERE cantidad > 1
                  
    """).fetchdf()

#domicilios repetidos y sin altura que no son incompletos

domicilios_repetidos_sinaltura = con.execute ("""
                  
                  SELECT domicilio
                  FROM tabla_BP
                  WHERE ( domicilio IN (SELECT domicilio FROM domicilios_cant_mayo_1)
                  OR domicilio IN (SELECT domicilio FROM domicilios_sin_altura) )
                  AND domicilio NOT IN (SELECT domicilio FROM domicilios_incompletos)
                  
    """).fetchdf()



cant_domicilios_repetidos_sinaltura = len(domicilios_repetidos_sinaltura)

M2 = (cant_domicilios_repetidos_sinaltura / cant_tot_domicilios) * 100

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

