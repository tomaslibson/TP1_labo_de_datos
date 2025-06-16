# Nombres: Felipe Comas, Pedro Raffo y Tomas Libson

# Nombre y numero : "B.O.B" , 02 

# EL archivo contiene los experimentos realizados para los distintos 
# modelos descriptos por el enunciado y la construccion de
# graficos/tablas descriptos en el infrome.

#%% Imports
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split, KFold
from sklearn import metrics, tree
import duckdb


#%% Carga de datos 

carpeta = os.path.dirname(os.path.abspath(__file__))

ruta_archivo = os.path.join(carpeta, "Fashion-MNIST.csv")

data_df = pd.read_csv(ruta_archivo, index_col=0)

#%% Funciones 

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

# Funciones para el caso multiclase:

def matriz_confusion_multiclase(y_test, y_pred, clases):
    n = len(clases)
    conf = np.zeros((n, n), dtype=int)  # En las filas vemos las clases reales y en las columnas las predichas
    for i in range(len(y_test)):
        real = y_test[i]
        pred = y_pred[i]
        conf[real][pred] += 1
    return conf

# La ecuacion cambia porque es multiclase. (Ver clase 17)
def accuracy_score_multiclase(conf):
    correcto = np.trace(conf)  # Sumamos de los elementos diagonales (predicciones correctas)
    total = np.sum(conf)
    return correcto / total

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

### B) ###

Atributos = dataset_filtrado.drop(columns=["label"])
#Pasamos atributos a numpy para poder usar reshape e indexar pixeles deseados de manera mas simple
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



#Hacemos que para cada valor de k se analicen todos los subconjuntos. 
for k in valores_k:
    modelo = KNeighborsClassifier(k)
    for sub_train, sub_test, fila in zip(lista_subconjuntos_flat, lista_test_flat, nombres_subconjuntos):
        modelo.fit(sub_train, C_train)
        Predicciones = modelo.predict(sub_test)
        
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

#%% Grafico ejercicio 2


heatmap_clases = dataset_filtrado.groupby("label").mean()

fig, axes = plt.subplots(1, 2, figsize=(10, 7)) 
fig.suptitle("Hetmap Clases 0 y 8", fontsize=14)

for i, ax in enumerate(axes.flat):
    imagen_promedio = heatmap_clases.iloc[i].values.reshape(28, 28)
    sns.heatmap(imagen_promedio, ax=ax, cmap="magma", cbar=False)
    ax.set_title(f"Clase {int(i/8 * 64)}")

#%% Ejercicio 3 

# datos

X = data_df.drop("label", axis=1)
y = data_df["label"]

#Separamos entre dev, eval y held-out
X_dev, X_eval, y_dev, y_eval = train_test_split(X, y, test_size=0.1, random_state=1)

# Usamos solo el 30% del conjunto de desarrollo para la validación cruzada, para minimizar los tiempos (ver en informe)
X_dev_cv = X_dev.sample(frac=0.3, random_state=42)
y_dev_cv = y_dev.loc[X_dev_cv.index]


# Hacemos Cross-validation con árboles de decisión y distintas profundidades maximas
alturas = list(range(1, 11))  # de 1 a 10
kf = KFold(n_splits=5)
clases = sorted(y.unique())

resultados = np.zeros((5, len(alturas)))  # 5 folds x 10 profundidades

for i, (train_index, test_index) in enumerate(kf.split(X_dev_cv)):
    X_train, X_test = X_dev_cv.iloc[train_index], X_dev_cv.iloc[test_index]
    y_train, y_test = y_dev_cv.iloc[train_index], y_dev_cv.iloc[test_index]

    for j, hmax in enumerate(alturas):
        modelo = tree.DecisionTreeClassifier(max_depth=hmax, criterion='entropy') # probamos con entropy, gini y logloss. Entropy nos da los mejores resultados
        modelo.fit(X_train, y_train)
        y_pred = modelo.predict(X_test)

        conf = matriz_confusion_multiclase(y_test.values, y_pred, clases)
        score = accuracy_score_multiclase(conf)
        resultados[i, j] = score

# Promediamos scores y seleccionamos el mejor modelo
scores_promedio = resultados.mean(axis=0)

for j, h in enumerate(alturas):
    print(f"Altura {h}: Accuracy promedio = {scores_promedio[j]:.4f}")

mejor_indice = np.argmax(scores_promedio)
mejor_altura = alturas[mejor_indice]
print(f"\nMejor altura: {mejor_altura} con accuracy promedio: {scores_promedio[mejor_indice]:.4f}")

# Entrenamos el modelo elegido en el conjunto dev entero
modelo_final = tree.DecisionTreeClassifier(max_depth=mejor_altura, criterion='entropy')
modelo_final.fit(X_dev, y_dev)
y_pred_dev = modelo_final.predict(X_dev)

conf_dev = matriz_confusion_multiclase(y_dev.values, y_pred_dev, clases)
acc_dev = accuracy_score_multiclase(conf_dev)
print(f"Accuracy en conjunto dev: {acc_dev:.4f}")

# Evaluamos en el held-out
y_pred_eval = modelo_final.predict(X_eval)
conf_eval = matriz_confusion_multiclase(y_eval.values, y_pred_eval, clases)
acc_eval = accuracy_score_multiclase(conf_eval)
print(f"Accuracy en conjunto held-out: {acc_eval:.4f}")

#%% Graficos ejercicio 3 

# Primero grafiquemos la accuracy promedio vs la profundidad del arbol
# Para ver como varia el rendimiento del modelo en funcion del hiperparametro (max Depth)
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

