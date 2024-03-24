#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FPM").getOrCreate()
spark


# In[2]:


CustomerDF = spark.read.csv("C:/Users/koka_/Downloads/Mall_Customers.csv",inferSchema=True,header=True)


# In[3]:


CustomerDF.limit(6).toPandas()


# In[4]:


CustomerDF.printSchema()


# In[7]:


CustomerDF = CustomerDF.withColumnRenamed("Annual Income (k$)","income")
CustomerDF = CustomerDF.withColumnRenamed("Spending Score (1-100)","spending_score")
CustomerDF.limit(6).toPandas()


# In[8]:


CustomerDF.count()


# In[32]:


from pyspark.sql.functions import *
groups = CustomerDF.withColumn('age_group',expr("CASE WHEN Age < 30 THEN 'Under 30' WHEN Age BETWEEN 30 AND 50 THEN '30 to 50'\
                                                    WHEN Age > 50 THEN 'Above 50' ELSE 'Other' END AS age_group"))
groups.groupBy('age_group').count().toPandas()


# In[33]:


groups = groups.withColumn('income_group',expr("CASE WHEN income < 40 THEN 'Under 40' WHEN income BETWEEN 40 AND 70 THEN '40 - 70'\
                                                    WHEN income > 70 THEN 'Above 70' ELSE 'Other' END AS income_group"))
groups.groupBy('income_group').count().toPandas()


# In[34]:


groups = groups.withColumn('spending_group',expr("CASE WHEN spending_score < 30 THEN 'Less than 30' WHEN spending_score BETWEEN 30 AND 60 THEN '30 to 60'\
                                                    WHEN spending_score > 60 THEN 'More than 60' ELSE 'Other' END AS spending_group"))
groups.groupBy('spending_group').count().toPandas()


# In[35]:


groups.groupBy('Gender').count().toPandas()


# In[37]:


groups = groups.withColumn('items',array('Gender','age_group','income_group','spending_group'))
groups.limit(4).toPandas()


# In[39]:


from pyspark.ml.fpm import FPGrowth
fpGrowth = FPGrowth(itemsCol="items", minSupport=0.2, minConfidence=0.1)
model = fpGrowth.fit(groups)


# In[41]:


itempopularity = model.freqItemsets
itempopularity.createOrReplaceTempView('itemPTempView')
itempopularityResults = spark.sql("SELECT * FROM itemPTempView ORDER BY freq DESC")
print("Top 20")
itempopularityResults.toPandas()


# In[42]:


assoc = model.associationRules
assoc.createOrReplaceTempView("assocTempView")
assocRulesResults = spark.sql("SELECT * FROM assocTempView ORDER BY confidence DESC")
print("Top 20")
assocRulesResults.toPandas()


# In[ ]:




