from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys
#import pandas as pd
import os 
from pyspark.sql import functions as F

import time

# Contamos el tiempo que dura la ejecucion del programa 
start_time = time.time()

# Ubicacion del conjunto de datos 
logFile = "./data.csv"  # CSV con los datos 
ipMaster = "spark://192.168.2.107:7077"

# SparkConf -> Establecer la configuracion inicial para que Ejecutar Spark Cluster 
# setAppName -> Nombre de la aplicacion 
# spark.shuffle.service.enabled -> Habilita el servicio de reproduccion aleatoria externa. ( false )
# spark.dynamicAllocation.enabled ->  Asignacion dinamica de recursos ( false )
# spark.cores.max -> Cantidad maxima de nucleos de CPU para solicitar la aplicacion de todo el cluster
# spark.executor.memory -> Cantidad de memoria a usar por proceso ejecutor,
conf = SparkConf().setAppName("Proyecto Final Big Data").set("spark.shuffle.service.enabled", "false").set("spark.dynamicAllocation.enabled", "false").set("spark.cores.max", "6").set("spark.executor.memory", "1g");

# SparkSession -> Punto de entrada principal para la funcionalidad DataFrame y SQL
spark = SparkSession.builder.master(ipMaster).config(conf=conf).getOrCreate()

# Lecturas de los datos 
df = spark.read.csv(logFile, header=True)

# dfConsulta1 -> Numero de retrasos de salida y de llegada (ArrDelay, DepDelay) por ruta
dfConsulta1 = df.select(df['Year'] , df['ArrDelay'] ,df['DepDelay'] , df['Origin'] , df['Dest'])

# Agregamos la columna retrasos, la cual sera la suma de ArrDelay y DepDelay
dfConsulta1 = dfConsulta1.withColumn("Retraso", sum(dfConsulta1[col] for col in ["ArrDelay","DepDelay"]))

# Filtra todas aquellas rutas que tengan retrasos
dfConsulta1 = dfConsulta1.filter(dfConsulta1['Retraso'] > 0)

#Cuenta cuantas rutas tienen retrasos
dfConsulta1 = dfConsulta1.groupBy( 'Year' , 'Origin' , 'Dest' ).count()

#Guarda la informacion en un archivo llamado consulta1_.csv
path_master = './consulta1_.csv/'
dfConsulta1.repartition(1).write.format('com.databricks.spark.csv').save(path_master,header = 'true')
csv = [x for x in os.listdir(path_master) if x.endswith(".csv")]
os.rename(path_master+csv[0], path_master+"resultado1.csv")

print("--- %s seconds ---" % (time.time() - start_time))
print()
spark.stop()
