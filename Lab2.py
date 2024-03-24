#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Lab2").getOrCreate()
spark


# In[9]:


student = spark.read.csv("C:/Users/koka_/Desktop/UQU/10th semester/Lab2/students.csv", inferSchema=True, header=True)
student.show()


# In[10]:


student.toPandas()


# In[11]:


student.limit(3).toPandas()


# In[12]:


student.toPandas().tail(3)


# In[35]:


userParq = spark.read.parquet("C:/Users/koka_/Desktop/UQU/10th semester/Lab2/users1.parquet")
userParq.show()


# In[22]:


userParq.toPandas().tail(3)


# In[18]:


people = spark.read.json("C:/Users/koka_/Desktop/UQU/10th semester/Lab2/people.json")
people.show()


# In[19]:


people.limit(3).toPandas()


# In[23]:


student.printSchema()


# In[24]:


userParq.printSchema()


# In[26]:


people.printSchema()


# In[28]:


student.columns


# In[29]:


userParq.columns


# In[30]:


people.columns


# In[37]:


student.schema['writing score'].dataType


# In[40]:


userParq.select('salary').summary('count','min','max').show()


# In[44]:


from pyspark.sql.types import *
data_schema = [StructField("city",StringType(),True),
               StructField("creaditcard",StringType(),True),
               StructField("email",StringType(),True),
               StructField("mac",StringType(),True),
               StructField("name",StringType(),True),
               StructField("timestamp",DateType(),True)]
final_struct = StructType(fields = data_schema)


# In[45]:


people2 = spark.read.json("C:/Users/koka_/Desktop/UQU/10th semester/Lab2/people.json", schema = final_struct)


# In[47]:


people2.printSchema()


# In[ ]:




