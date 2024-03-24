#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ClassW_MLlib").getOrCreate()
spark


# In[5]:


from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import MinMaxScaler


# In[9]:


AutismDF = spark.read.csv( "C:/Users/koka_/Downloads/Toddler Autism dataset July 2018.csv",inferSchema=True,header=True)
AutismDF.limit(5).toPandas()


# In[7]:


AutismDF.printSchema()


# In[10]:


AutismDF.groupBy("Class/ASD Traits ").count().show()


# In[26]:


dependent_var= 'Class/ASD Traits '
#renamed=None
#indexer=None
#indexed=None
renamed = AutismDF.withColumn("label_str",AutismDF[dependent_var])
indexer = StringIndexer(inputCol="label_str", outputCol="label")
indexed = indexer.fit(renamed).transform(renamed)
indexed


# In[20]:


input_columns = AutismDF.columns
input_columns


# In[21]:


input_columns = input_columns[1:-1]
input_columns


# In[27]:


numeric_inputs = []
string_inputs = []
for column in input_columns:
    if str(indexed.schema[column].dataType) == 'StringType()':
        indexer = StringIndexer(inputCol=column, outputCol=column+"_num")
        indexed = indexer.fit(indexed).transform(indexed)
        new_col_name = column = column+"_num"
        string_inputs.append(new_col_name)
    else:
        numeric_inputs.append(column)


# In[28]:


indexed.printSchema()


# In[29]:


indexed.select('Ethnicity','Ethnicity_num','Jaundice','Jaundice_num').limit(12).toPandas()


# In[31]:


minimums = AutismDF.select([min(c).alias(c) for c in AutismDF.columns if c in numeric_inputs])
minimums.show()


# In[32]:


features_list = numeric_inputs + string_inputs
assembler = VectorAssembler(inputCols=features_list,outputCol='features')
output = assembler.transform(indexed).select('features','label')
output.limit(3).toPandas()


# In[34]:


scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures",min=0,max=1)
print("Features scaled to range: [%d, %d]" %(scaler.getMin(), scaler.getMax()))
scalerModel=scaler.fit(output)
scaled_data = scalerModel.transform(output)
final_data = scaled_data.select('label', 'scaledFeatures')
final_data = final_data.withColumnRenamed('scaledFeatures','features')
final_data.limit(3).toPandas()


# In[36]:


train_val = 0.7
test_val = 1-train_val
train,test = final_data.randomSplit([train_val,test_val],seed=40)


# In[37]:


train.count()


# In[38]:


test.count()


# In[39]:


from pyspark.ml.classification import *
from pyspark.ml.evaluation import *
from pyspark.sql.functions import *
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


# In[40]:


Bin_evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction')
MC_evaluator = MulticlassClassificationEvaluator(metricName="accuracy")


# In[42]:


classifier = LogisticRegression()
fitModel = classifier.fit(train)

predictionAndLables = fitModel.transform(test)
auc = Bin_evaluator.evaluate(predictionAndLables)
print("AUC:",auc)

predictions = fitModel.transform(test)
accuracy = (MC_evaluator.evaluate(predictions))*100

print("Accuracy: {0:0.2f}".format(accuracy,"%"))


# In[ ]:




