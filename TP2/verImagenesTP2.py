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

img_nbr = 3

# keep label out
img = np.array(data_df.iloc[img_nbr,:-1]).reshape(28,28)


#%% Plot image

plt.imshow(img, cmap = "gray")


#%%
