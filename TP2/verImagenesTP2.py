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
