#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("LAB3CheckPoint").getOrCreate()
spark


# In[3]:


googlep = spark.read.csv("C:/Users/koka_/Desktop/UQU/10th semester/تحليلات البيانات الضخمة/CheckPoints/googleplaystore.csv", header =True)


# In[4]:


googlep


# In[5]:


googlep.createOrReplaceTempView("tempview")


# In[6]:


spark.sql("SELECT * FROM tempview WHERE Rating > 4.1").limit(3).toPandas()


# In[7]:


spark.sql("SELECT App, Category FROM tempview ORDER BY Category DESC").toPandas()


# In[8]:


spark.sql("SELECT Category, count(App) AS Total FROM tempview GROUP BY Category").limit(10).toPandas()


# In[9]:


from pyspark.ml.feature import SQLTransformer
sqlTrans = SQLTransformer(statement="SELECT count(*) FROM __THIS__ WHERE Type = 'Free' ") 
sqlTrans.transform(googlep).show()


# In[ ]:




