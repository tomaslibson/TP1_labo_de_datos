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


#%% Load dataset 

carpeta = "C:\\Users\\libso\\OneDrive\\Escritorio\\ubaTarea\\Labo_de_datos\\"

data_df = pd.read_csv(carpeta + "Fashion-MNIST.csv", index_col=0)
print(data_df.head())


#%% Select single image and convert to 28x28 array

img_nbr = 1

# keep label out
img = np.array(data_df.iloc[img_nbr,:-1]).reshape(28,28)


#%% Plot image

plt.imshow(img, cmap = "gray")


#%% Heatmap de imágenes promedio por clase

import seaborn as sns

# Agrupar por clase (última columna) y calcular promedio
heatmap_clases = data_df.groupby("label").mean()

# Mostrar heatmaps para cada clase
fig, axes = plt.subplots(2, 5, figsize=(15, 6))
fig.suptitle("Imágenes Promedio por Clase", fontsize=16)

for i, ax in enumerate(axes.flat):
    imagen_promedio = heatmap_clases.iloc[i].values.reshape(28, 28)
    sns.heatmap(imagen_promedio, ax=ax, cmap="grey", cbar= False)
    ax.set_title(f"Clase {i}")

#%% Heatmap desviacion estandar
import numpy as np
import seaborn as sns

imagenes_clase_5 = data_df[data_df['label'] == 5].drop('label', axis=1).values
desviacion = np.std(imagenes_clase_5, axis=0).reshape(28, 28)

plt.figure(figsize=(6, 5))
sns.heatmap(desviacion, cmap="magma")
plt.title("Desviación estándar por píxel - Clase 5")
plt.xlabel("Columna")
plt.ylabel("Fila")
plt.show()


#%% Ejemplos  clase 5

fig, axes = plt.subplots(4, 5, figsize=(7, 6))
imagenes = imagenes_clase_5.reshape(-1, 28, 28)

for i, ax in enumerate(axes.flat):
    ax.imshow(imagenes[i], cmap='gray')
    ax.axis('off')

plt.suptitle("Ejemplos de la Clase 5 (Sandalias)")
plt.tight_layout()
plt.show()