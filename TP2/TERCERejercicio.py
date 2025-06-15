# -*- coding: utf-8 -*-
"""
Created on Thu Jun 12 12:41:08 2025

@author: pedro
"""
#%% Import

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split, KFold
from sklearn import tree
import seaborn as sns


# EJERCICIO 3
# Utilizamos el codigo de la clase 17, pero lo modificamos para nuestro caso
#%% Función de matriz de confusión multiclase, tuvimos que cambiar la original porque ya no es binario
def matriz_confusion_multiclase(y_test, y_pred, clases):
    n = len(clases)
    conf = np.zeros((n, n), dtype=int)  # En las filas vemos las clses reales y en las columnas las predichas
    for i in range(len(y_test)):
        real = y_test[i]
        pred = y_pred[i]
        conf[real][pred] += 1
    return conf

#%% Accuracy general
def accuracy_score(conf):
    correcto = np.trace(conf)  # Sumamos de los elementos diagonales (predicciones correctas)
    total = np.sum(conf)
    return correcto / total

#%% Cargamos los datos

carpeta = "C:\\Users\\libso\\OneDrive\\Escritorio\\ubaTarea\\Labo_de_datos\\"

df = pd.read_csv(carpeta + "Fashion-MNIST.csv", index_col=0)


X = df.drop("label", axis=1)
y = df["label"]

#%% Separamos entre dev, eval y held-out
X_dev, X_eval, y_dev, y_eval = train_test_split(X, y, test_size=0.1, random_state=1)

#%% Hacemos Cross-validation con árboles de decisión y distintas profundidades maximas
alturas = list(range(1, 11))  # de 1 a 10
kf = KFold(n_splits=5)
clases = sorted(y.unique())

resultados = np.zeros((5, len(alturas)))  # 5 folds x 10 profundidades

for i, (train_index, test_index) in enumerate(kf.split(X_dev)):
    X_train, X_test = X_dev.iloc[train_index], X_dev.iloc[test_index]
    y_train, y_test = y_dev.iloc[train_index], y_dev.iloc[test_index]

    for j, hmax in enumerate(alturas):
        modelo = tree.DecisionTreeClassifier(max_depth=hmax)
        modelo.fit(X_train, y_train)
        y_pred = modelo.predict(X_test)

        conf = matriz_confusion_multiclase(y_test.values, y_pred, clases)
        score = accuracy_score(conf)
        resultados[i, j] = score

#%% Promediamos scores y seleccionamos el mejor modelo
scores_promedio = resultados.mean(axis=0)

for j, h in enumerate(alturas):
    print(f"Altura {h}: Accuracy promedio = {scores_promedio[j]:.4f}")

best_index = np.argmax(scores_promedio)
mejor_altura = alturas[best_index]
print(f"\nMejor altura: {mejor_altura} con accuracy promedio: {scores_promedio[best_index]:.4f}")

#%% entrenamos el modelo elegido en el conjunto dev entero
modelo_final = tree.DecisionTreeClassifier(max_depth=mejor_altura)
modelo_final.fit(X_dev, y_dev)
y_pred_dev = modelo_final.predict(X_dev)

conf_dev = matriz_confusion_multiclase(y_dev.values, y_pred_dev, clases)
acc_dev = accuracy_score(conf_dev)
print(f"Accuracy en conjunto dev: {acc_dev:.4f}")


#%% Evaluamos en el held-out
y_pred_eval = modelo_final.predict(X_eval)
conf_eval = matriz_confusion_multiclase(y_eval.values, y_pred_eval, clases)
acc_eval = accuracy_score(conf_eval)
print(f"Accuracy en conjunto held-out: {acc_eval:.4f}")

#%%
# Ahora hagamos unos graficos para visualizar los resultados a los que llegamos
# Primero grafiquemos la accuracy promedio vs la profundidad del arbol
# Para ver como varia el rendimiento del model en funcion del hiperparametro (max Depth)
plt.figure(figsize=(8, 5))
plt.plot(alturas, scores_promedio, marker='o', linestyle='-', color='navy')
# Agregamos el titulo
plt.title("Accuracy promedio por profundidad del árbol (cross-validation)", fontsize=12)
plt.xlabel("Profundidad máxima del árbol", fontsize=11) #definimos el nombre de la variable del eje x
plt.ylabel("Accuracy promedio", fontsize=11) #definimos el nombre de la variable del eje y
plt.xticks(alturas)
plt.grid(True)
plt.tight_layout()
plt.show()

#%%

# Veamos ahora como nos queda la matriz de confusion en el conjunto Held- Out
# Asi podemos ver como se distribuyen los errores del modelo en cada clase


plt.figure(figsize=(8, 6))
sns.heatmap(conf_eval, annot=True, fmt='d', cmap='Blues',
            xticklabels=clases, yticklabels=clases)
# Agregamos titulo
plt.title("Matriz de Confusión - Conjunto Held-out", fontsize=12)
plt.xlabel("Clase Predicha", fontsize=11) #definimos el nombre de la variable del eje x
plt.ylabel("Clase Real", fontsize=11) #definimos el nombre de la variable del eje y
plt.tight_layout()
plt.show()

