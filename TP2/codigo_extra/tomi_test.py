# -*- coding: utf-8 -*-
"""
Created on Sat Jun  7 16:42:05 2025

@author: libso
"""

#!/usr/bin/env python
# coding: utf-8

# Visualizar imágenes


#%% Import

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn import metrics
import duckdb


#%% Load dataset 

carpeta = "C:\\Users\\libso\\OneDrive\\Escritorio\\ubaTarea\\Labo_de_datos\\"

data_df = pd.read_csv(carpeta + "Fashion-MNIST.csv", index_col=0)



#%% Ejercicio 1

###Heatmap de imágenes promedio por clase###

#Agrupamos por clase y calculamos promedio
heatmap_clases = data_df.groupby("label").mean()

fig, axes = plt.subplots(2, 5, figsize=(15, 6))
fig.suptitle("Imágenes Promedio por Clase", fontsize=16)

for i, ax in enumerate(axes.flat):
    #un heatmap por cada clase
    imagen_promedio = heatmap_clases.iloc[i].values.reshape(28, 28)
    sns.heatmap(imagen_promedio, ax=ax, cmap="magma", cbar= False)
    ax.set_title(f"Clase {i}")
  
    
###Ejemplos clase 5###
imagenes_clase_5 = data_df[data_df['label'] == 5].drop('label', axis=1).values

fig, axes = plt.subplots(4, 5, figsize=(7, 6))
imagenes = imagenes_clase_5.reshape(-1, 28, 28)

for i, ax in enumerate(axes.flat):
    ax.imshow(imagenes[i], cmap='gray')
    ax.axis('off')

plt.suptitle("Ejemplos de la Clase 5 (Sandalia)")
plt.tight_layout()
plt.show()

###Ejemplos clase 7###
imagenes_clase_7 = data_df[data_df['label'] == 7].drop('label', axis=1).values

fig, axes = plt.subplots(4, 5, figsize=(7, 6))
imagenes = imagenes_clase_7.reshape(-1, 28, 28)

for i, ax in enumerate(axes.flat):
    ax.imshow(imagenes[i], cmap='gray')
    ax.axis('off')

plt.suptitle("Ejemplos de la Clase 7 (Zapatilla)")
plt.tight_layout()
plt.show()

#%%Ejercicio 2


#### A) ###
con = duckdb.connect()

# Creamos un nuevo dataframe que incluya solo a las clases 0 y 8
dataset_filtrado = con.execute("""
    SELECT * 
    FROM data_df
    WHERE label = 0 OR label = 8
""").fetchdf()

# Nos fijamos cuantas muestras hay por clase 
cant_prendas_clase= dataset_filtrado['label'].value_counts()
print("Cantidad de muestras por clase:")
print(cant_prendas_clase)

# Verificamos si está balanceado
# Si obtenemos una diferencia absoluta menor o igual al 5% del total de la clase con mas muestras, 
# decimos que el conjunto esta aproximadamente balanceado
if abs(cant_prendas_clase[0] - cant_prendas_clase[8]) <= 0.05 * max(cant_prendas_clase[0], cant_prendas_clase[8]): 
    print("El subconjunto esta aproximadamente balanceado")
else:
    print("El subconjunto no esta balanceado")

#GEPETTO EXPLICATIONE
#¿Por qué 5%?
#Es un criterio común en aprendizaje automático para evaluar si el desequilibrio es lo suficientemente pequeño como para no requerir técnicas de re-muestreo

### B) ###

Atributos = dataset_filtrado.drop(columns=["label"])
#Pasamos atributos a numpy para poder usar reshape indexar pixeles deseados de manera mas simple
Atributos = Atributos.to_numpy() 
Atributos = Atributos.reshape(-1, 28, 28)

Clases = dataset_filtrado["label"]

#Armamos los train- y los test-set. Fijamos el random_state para que no se altere con cada ejecucion.
A_train, A_test, C_train, C_test = train_test_split(Atributos, Clases, test_size=0.2, random_state= 1) 


### C) ###

#Analisis del espacio en "negro" que deja la manga corta de la clase 0. Pixeles que se encuentran "ocupados" en la clase 8.

sub_a = A_train[:, 12:, 0:7]
#Para cada subconjuntos re indexamos su test
test_a = A_test[:, 12:, 0:7]

sub_b = A_train[:, 12:, 21:]
test_b = A_test[:, 12:, 21:]

sub_c = A_train[:, 14:21, np.r_[3:7, 21:25]]
test_c = A_test[:, 14:21, np.r_[3:7, 21:25]]

#Analisis del espacio en "ocupados" por los hombros de las remeras. Pixeles "desocupados" en la clase 8
sub_d = A_train[:, 0:7, 0:10]
test_d = A_test[:, 0:7, 0:10]

sub_e = A_train[:, 0:7, 18:]
test_e = A_test[:, 0:7, 18:]

sub_f = A_train[:, 0:7, 14:17]
test_f = A_test[:, 0:7, 14:17]

lista_subconjuntos = [sub_a, sub_b, sub_c, sub_d, sub_e, sub_f]
lista_test = [test_a, test_b, test_c, test_d, test_e, test_f]

#Aplanamos los subconjuntos para que el modelo pueda procesarlos.
lista_subconjuntos_flat = [sub.reshape(sub.shape[0], -1) for sub in lista_subconjuntos]
lista_test_flat = [test.reshape(test.shape[0], -1) for test in lista_test]    


nombres_subconjuntos = ['sub_a', 'sub_b', 'sub_c', 'sub_d', 'sub_e', 'sub_f']
#Seleccionamos valores de k con los uqe decidimos experimentar. 
valores_k = [1, 3, 5, 7, 9]

#Iniciamos tablas comparativas de Precision y Exactitud
tabla_precision = pd.DataFrame(index=nombres_subconjuntos, columns=valores_k)
tabla_exactitud = pd.DataFrame(index=nombres_subconjuntos, columns=valores_k)
#Armamos una lista con las tablas para agregarle los promedios de manera mas efectiva.
lista_tablas = [tabla_precision, tabla_exactitud]

#Funciones para obtener metricas de evaluacion. 
def matriz_confusion_binaria(y_test, y_pred):
    y_test = np.array(y_test)
    y_pred = np.array(y_pred)
    
    tp = 0
    fp = 0
    tn = 0
    fn = 0
    for i in range(len(y_test)):
        if y_test[i]:
            if y_pred[i]:
                tp += 1
            else:
                fn += 1
        else:
            if y_pred[i]:
                fp += 1
            else:
                tn += 1
    
    return tp, tn, fp, fn

def accuracy_score(tp, tn, fp, fn):
    acc = (tp+tn)/(tp+tn+fp+fn)
    return acc

def precision_score(tp, tn, fp, fn):
    prec = tp/(tp+fp)
    return prec

#Hacemos que para cada valor de k se analicen todos los subconjuntos. 
for k in valores_k:
    modelo = KNeighborsClassifier(k)
    for sub, test, fila in zip(lista_subconjuntos_flat, lista_test_flat, nombres_subconjuntos):
        modelo.fit(sub, C_train)
        Predicciones = modelo.predict(test)
        
        tp, tn, fp, fn = matriz_confusion_binaria(C_test, Predicciones)
        
        #Para cada iteracion guardamos los porcentajes de Exactitud y Precision en la tablas comparativas. 
        Exactitud = accuracy_score(tp, tn, fp, fn) * 100 
        tabla_exactitud.loc[fila, k] = Exactitud
        
        Precision = precision_score(tp, tn, fp, fn) * 100 
        tabla_precision.loc[fila, k] = Precision
        

#Agregamos promedios por valores de k y por subconjuntos.
for t in lista_tablas:
    t["prom_sub"] = t.mean(axis=1)
    t.loc["prom_k"] = t.mean(axis=0)
    t.loc["prom_k", "prom_sub"] = np.nan

#%% Grafico ejercicio 2 (Heatmaps clases 0 y 8)


heatmap_clases = dataset_filtrado.groupby("label").mean()

fig, axes = plt.subplots(1, 2, figsize=(10, 7)) 
fig.suptitle("Hetmap Clases 0 y 8", fontsize=14)

for i, ax in enumerate(axes.flat):
    imagen_promedio = heatmap_clases.iloc[i].values.reshape(28, 28)
    sns.heatmap(imagen_promedio, ax=ax, cmap="magma", cbar=False)
    ax.set_title(f"Clase {i}")


