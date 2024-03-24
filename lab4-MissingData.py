#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MissingData").getOrCreate()
spark


# In[3]:


zomato = spark.read.csv("C:/Users/koka_/Downloads/zomato/zomato.csv",inferSchema=True,header=True)


# In[4]:


zomato.limit(3).toPandas()


# In[5]:


zomato.printSchema()


# In[7]:


from pyspark.sql.types import *
newZomato = zomato.withColumn("approx_cost(for two people)", zomato["approx_cost(for two people)"].cast(IntegerType()))\
            .withColumn("votes",zomato["votes"].cast(IntegerType()))
newZomato.printSchema()


# In[9]:


from pyspark.sql.functions import *
newZomato.select(['name','cuisines']).filter(newZomato.cuisines.isNull()).show(5)


# In[11]:


newZomato.select(['name','cuisines']).filter(newZomato.cuisines.isNull()).limit(5).toPandas()


# In[12]:


cuisinNull=newZomato.filter(newZomato.cuisines.isNull()).count()
allRows=newZomato.count()
print("Number of null values in cuisines =",cuisinNull,", Percentage of Null values = ", cuisinNull/allRows*100)


# In[14]:


# Create a function to display the number and percentage of missing data in the dataframe
def null_value_calc(newZomato):
    null_columns_counts= []
    numRows = newZomato.count()
    
    for k in newZomato.columns:
        nullRows= newZomato.where(col(k).isNull()).count()
        if(nullRows > 0):
            temp = k,nullRows,(nullRows/numRows)*100
            null_columns_counts.append(temp)
    return(null_columns_counts)
# Call the function
null_columns_calc_list = null_value_calc(newZomato)

# Print results
for i in null_columns_calc_list:
    print(i)


# In[15]:


newZomato.na.drop().limit(4).toPandas()


# In[17]:


original_len = newZomato.count()
afterDrop_len = newZomato.na.drop().count()
print("Total Rows:",original_len)
print("Total Rows After Drop:",afterDrop_len)
print("Total Rows Dropped:",original_len - afterDrop_len)
print("Percentage of Rows Dropped:",(original_len - afterDrop_len)/original_len)


# In[18]:


original_len = newZomato.count()
#v Drop rows that have less than 8 NON-nul values
afterDrop_len = newZomato.na.drop(thresh=8).count()
print("Total Rows:",original_len)
print("Total Rows After Drop:",afterDrop_len)
print("Total Rows Dropped:",original_len - afterDrop_len)
print("Percentage of Rows Dropped:",(original_len - afterDrop_len)/original_len)


# In[21]:


# Only drop the rows whose values in the votes column are null
original_len = newZomato.count()
afterDrop_len = newZomato.na.drop(subset=['votes']).count()
print("Total Rows:",original_len)
print("Total Rows After Drop:",afterDrop_len)
print("Total Rows Dropped:",original_len - afterDrop_len)
print("Percentage of Rows Dropped:",(original_len - afterDrop_len)/original_len)


# In[25]:


# Only drop the rows whose values in the rate column are null
original_len = newZomato.count()
afterDrop_len = newZomato.filter(newZomato['rate'].isNotNull()).count()
print("Total Rows:",original_len)
print("Total Rows After Drop:",afterDrop_len)
print("Total Rows Dropped:",original_len - afterDrop_len)
print("Percentage of Rows Dropped:",(original_len - afterDrop_len)/original_len)


# In[26]:


newZomatoDrop=newZomato.filter(newZomato['rate'].isNotNull())
newZomatoDrop.count()


# In[27]:


# Drop a row only if ALL its values are null
original_len = newZomato.count()
afterDrop_len = newZomato.na.drop(how='all').count()
print("Total Rows:",original_len)
print("Total Rows After Drop:",afterDrop_len)
print("Total Rows Dropped:",original_len - afterDrop_len)
print("Percentage of Rows Dropped:",(original_len - afterDrop_len)/original_len)


# In[28]:


# Fill all nuls values with one common value (character value)
fill1=newZomato.na.fill('Missing')
fill1.limit(4).toPandas()


# In[29]:


# Fill all nulls values with one common value (numric value)
fill2=fill1.na.fill(999)
fill2.limit(4).toPandas()


# In[30]:


newZomato.filter(newZomato.name.isNull()).na.fill('No Name',subset=['name']).limit(5).toPandas()


# In[35]:


# Create a function to calculate the mean value in a column
def fill_with_mean(newZomato, include=set()):
    stats = newZomato.agg(*(avg(c).alias(c) for c in newZomato.columns if c in include))
    return newZomato.na.fill(stats.first().asDict())
# Call the function
updated_newZomato = fill_with_mean(newZomato, ["votes"])
updated_newZomato.limit(3).toPandas()


# In[ ]:




