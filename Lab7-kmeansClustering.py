#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("kmeansClustering").getOrCreate()

spark


# In[2]:


dfCC = spark.read.csv("C:/Users/koka_/Downloads/credit_card_data.csv",inferSchema=True,header=True)


# In[3]:


dfCC.limit(5).toPandas()


# In[4]:


dfCC.printSchema()


# In[5]:


from pyspark.sql.functions import *

def fill_with_mean(dfCC, include=set()):
    stats = dfCC.agg(*(avg(c).alias(c) for c in dfCC.columns if c in include))
    return dfCC.na.fill(stats.first().asDict())

columns = dfCC.columns
columns = columns[1:]
dfCC = fill_with_mean(dfCC,columns)
dfCC.limit(5).toPandas()


# In[6]:


from pyspark.ml.feature import VectorAssembler

input_columns = dfCC.columns
input_columns = input_columns[1:]
vecAssembler = VectorAssembler(inputCols=input_columns, outputCol="features")
df_kmeans = vecAssembler.transform(dfCC)
df_kmeans.limit(4).toPandas()


# In[8]:


from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import numpy as np

kmax = 30

kmcost = np.zeros(kmax)

for k in range(2,kmax):
    kmeans = KMeans().setK(k).setSeed(3).setFeaturesCol("features")
    model = kmeans.fit(df_kmeans)
    
    predictions = model.transform(df_kmeans)
    evaluator = ClusteringEvaluator()
    kmcost[k] = evaluator.evaluate(predictions)


# In[12]:


import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

fig, ax = plt.subplots(1,1,figsize =(8,6))
ax.plot(range(2,kmax),kmcost[2:kmax])

ax.set_xlabel('k')
ax.set_ylabel('cost')


# In[14]:


k = 11
kmeans = KMeans().setK(k).setSeed(3).setFeaturesCol("features")
model = kmeans.fit(df_kmeans)

predictions = model.transform(df_kmeans)

evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = "+str(silhouette))
print("")

centers = model.clusterCenters()
print("Cluster Center: ")
for center in centers:
    print(center)


# In[15]:


import pandas as pd
import numpy as np

center_pdf = pd.DataFrame(list(map(np.ravel,centers)))
center_pdf.columns = columns
center_pdf


# In[16]:


predictions.limit(5).toPandas()


# In[17]:


predictions.groupBy("prediction").agg(min(predictions.BALANCE),max(predictions.BALANCE)).toPandas()


# In[ ]:




