#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Lab3").getOrCreate()
spark


# In[4]:


crimes = spark.read.csv ("C:/Users/koka_/Desktop/UQU/10th semester/تحليلات البيانات الضخمة/crimes.csv", header=True)
crimes


# In[5]:


crimes.toPandas()


# In[6]:


crimes.printSchema()


# In[8]:


newCrimes = crimes.withColumnRenamed('Rolling year total number of offences','count')
newCrimes


# In[10]:


newCrimes.createOrReplaceTempView("temview")


# In[12]:


spark.sql("select * from temview").limit(5).toPandas()


# In[13]:


spark.sql("select * from temview WHERE count>5000").limit(5).toPandas()


# In[14]:


sqlResults  =spark.sql("select * from temview WHERE count>5000")
sqlResults.limit(5).toPandas()


# In[16]:


spark.sql("select * from temview WHERE count>300000 ORDER BY count DESC").toPandas()


# In[18]:


spark.sql("select Region,sum(count) AS Total from temview GROUP BY Region").toPandas()


# In[19]:


spark.sql("select Offence, max(count) AS Highest from temview GROUP BY Offence").toPandas()


# In[20]:


from pyspark.ml.feature import SQLTransformer


# In[21]:


sqlTrans = SQLTransformer(statement="SELECT PFA,Region,Offence FROM __THIS__")


# In[22]:


sqlTrans.transform(newCrimes).show()


# In[23]:


sqlTrans = SQLTransformer(statement="SELECT Region,sum(count) AS Total FROM __THIS__ GROUP BY region")
sqlTrans.transform(newCrimes).show()


# In[25]:


sqlTrans = SQLTransformer(statement="SELECT Region,count FROM __THIS__ WHERE count>300000 ORDER BY count DESC")
sqlTrans.transform(newCrimes).show()


# In[26]:


from pyspark.sql.functions import *


# In[27]:


newCrimes.select('Region', 'PFA', 'count').show()


# In[28]:


newCrimes.select('Region', 'PFA', 'count').orderBy(newCrimes['count'].desc()).show()


# In[29]:


newCrimes.select('Region', 'count').where(newCrimes.Region.like("East")).show()


# In[ ]:




