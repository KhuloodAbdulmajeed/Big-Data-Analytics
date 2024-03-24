#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("LAB4CheckPoint").getOrCreate()
spark


# In[3]:


weather = spark.read.csv("C:/Users/koka_/Downloads/Weather.csv",inferSchema=True,header=True)


# In[4]:


weather


# In[5]:


weather.limit(5).toPandas()


# In[6]:


dropped = weather.na.drop()
dropped.limit(3).toPandas()


# In[7]:


afterDrop_len = weather.na.drop(12).count()
print(afterDrop_len)


# In[8]:


afterDrop_len = weather.na.drop(thresh=12).count()
print(afterDrop_len)


# In[9]:


afterDrop_len = weather.na.drop(how='all').count()
print(afterDrop_len)


# In[ ]:




