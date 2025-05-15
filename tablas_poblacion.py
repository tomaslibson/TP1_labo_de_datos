# -*- coding: utf-8 -*-
"""
Created on Thu May 15 13:40:25 2025

@author: libso
"""

import pandas as pd
import duckdb

carpeta = "C:\\Users\\libso\\OneDrive\\Escritorio\\ubaTarea\\Labo_de_datos\\TP1_labo_de_datos\\Tablas\\"


#JARDIN
GE_Jardin = pd.read_excel(carpeta+ "GE_Jardin.xlsX", header = 10)

GE_Jardin = GE_Jardin.drop(GE_Jardin.columns[0], axis =1)



#PRIMARIA
GE_Primaria = pd.read_excel(carpeta+ "GE_Primaria.xlsX", header = 10)

GE_Primaria = GE_Primaria.drop(GE_Primaria.columns[0], axis =1)


#SECUNDARIA
GE_Secundaria = pd.read_excel(carpeta+ "GE_Secundaria.xlsX", header = 10)

GE_Secundaria = GE_Secundaria.drop(GE_Secundaria.columns[0], axis =1)

#ADULTOS

GE_Adultos = pd.read_excel(carpeta+ "GE_Adultos.xlsX", header = 10)

GE_Adultos = GE_Adultos.drop(GE_Adultos.columns[0], axis =1)


