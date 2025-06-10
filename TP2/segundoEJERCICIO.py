#!/usr/bin/env python
# coding: utf-8

# Visualizar imágenes


#%% Import

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


#%% Load dataset 

data_df = pd.read_csv("Fashion-MNIST.csv", index_col=0)
print(data_df.head())


#%% Select single image and convert to 28x28 array

img_nbr = 1

# keep label out
img = np.array(data_df.iloc[img_nbr,:-1]).reshape(28,28)


#%% Plot image

plt.imshow(img, cmap = "gray")


#%%

#%% Heatmap de imágenes promedio por clase

import seaborn as sns

# Agrupar por clase (última columna) y calcular promedio
average_images = data_df.groupby("label").mean()

# Mostrar heatmaps para cada clase
fig, axes = plt.subplots(2, 5, figsize=(15, 6))
fig.suptitle("Imágenes Promedio por Clase (Heatmaps)", fontsize=16)

for i, ax in enumerate(axes.flat):
    img_avg = average_images.loc[i].values.reshape(28, 28)
    sns.heatmap(img_avg, ax=ax, cmap="gray_r", cbar=False)
    ax.set_title(f"Clase {i}")
    ax.axis('off')

plt.tight_layout()
plt.show()

#%%
# ej 2 
# A)


# Creamos un nuevo dataframe que incluya solo a las clases 0 y 8
df_binario = duckdb.query("""
    SELECT * FROM data_df
    WHERE label = 0 OR label = 8
""").to_df()

# Nos fijamos cuantas muestras hay por clase 
counts = df_binario['label'].value_counts()
print("Cantidad de muestras por clase:")
print(counts)

# Verificamos si está balanceado
# Si obtenemos una diferencia absoluta menor o igual al 5% del total de la clase con mas muestras, 
# decimos que el conjunto esta aproximadamente balanceado
if abs(counts[0] - counts[8]) <= 0.05 * max(counts[0], counts[8]): 
    print("El subconjunto esta aproximadamente balanceado")
else:
    print("El subconjunto no esta balanceado")

#GEPETTO EXPLICATIONE
#¿Por qué 5%?
#Es un criterio común en aprendizaje automático para evaluar si el desequilibrio es lo suficientemente pequeño como para no requerir técnicas de re-muestreo


