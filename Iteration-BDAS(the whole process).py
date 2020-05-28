
# coding: utf-8

# In[1]:


# Configure spark in jupyter
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("iteration4").config("spark.some.config.option", "some-value").getOrCreate()


# In[10]:


# data description
data1 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\1309.csv", header=True)
data2 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\Arctic_oscillation.csv", header=True)
data3 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\CO2_emission.csv", header=True)
data4 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\Pacific_decadal_oscillation.csv", header=True)
data5 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\Southern_Oscillation_Index.csv", header=True)
data6 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\surface_temp.csv", header=True)
print("main_data");data1.show(5); data1.printSchema(); print('Number of data1_Rows: ', data1.count())
print("arc_oscil");data2.show(5); data2.printSchema(); print('Number of data2_Rows: ', data2.count())
print("CO2");data3.show(5); data3.printSchema(); print('Number of data3_Rows: ', data3.count())
print("pacific");data4.show(5); data4.printSchema(); print('Number of data4_Rows: ', data4.count())
print("soutnhern");data5.show(5); data5.printSchema(); print('Number of data5_Rows: ', data5.count())
print("surface");data6.show(5); data6.printSchema(); print('Number of data6_Rows: ', data6.count())


# In[18]:


# data formation
from pyspark.sql.types import (StructField, StructType, StringType, IntegerType, FloatType)

data_schema = [StructField('Longitude (x)', FloatType(), True),
               StructField('Latitude (y)', FloatType(), True),
               StructField('Station Name', IntegerType(), True),
               StructField('Climate ID', IntegerType(), True),
               StructField('Date/Time', StringType(), True),
               StructField('Year', IntegerType(), True),
               StructField('Month', IntegerType(), True),
               StructField('Day', IntegerType(), True),
               StructField('Data Quality', StringType(), True),
               StructField('Max Temp (°C)', FloatType(), True),
               StructField('Max Temp Flag', StringType(), True),
               StructField('Min Temp (°C)', FloatType(), True),
               StructField('Min Temp Flag', StringType(), True),
               StructField('Mean Temp (°C)', FloatType(), True),
               StructField('Mean Temp Flag', StringType(), True),
               StructField('Heat Deg Days (°C)', FloatType(), True),
               StructField('Heat Deg Days Flag', StringType(), True),
               StructField('Cool Deg Days (°C)', FloatType(), True),
               StructField('Cool Deg Days Flag', StringType(), True),
               StructField('Total Rain (mm)', FloatType(), True),
               StructField('Total Rain Flag', StringType(), True),
               StructField('Total Snow (cm)', FloatType(), True),
               StructField('Total Snow Flag', StringType(), True),
               StructField('Total Precip (mm)', FloatType(), True),
               StructField('Total Precip Flag', StringType(), True),
               StructField('Snow on Grnd (cm)', FloatType(), True),
               StructField('Snow on Grnd Flag', StringType(), True),
               StructField('Dir of Max Gust (10s deg)', FloatType(), True),
               StructField('Dir of Max Gust Flag',StringType(), True),
               StructField('Spd of Max Gust (km/h)', FloatType(), True),
               StructField('Spd of Max Gust Flag',StringType(), True)]
final_struct = StructType(fields = data_schema)
data1 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\1309.csv', schema=final_struct, header=True)

data_schema = [StructField('Date', StringType(), True),
               StructField('Value', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data2 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\Arctic_oscillation.csv', schema=final_struct, header=True)

data_schema = [StructField('year', IntegerType(), True),
               StructField('month', IntegerType(), True),
               StructField('emission', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data3 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\CO2_emission.csv', schema=final_struct, header=True)

data_schema = [StructField('Date', StringType(), True),
               StructField('Value', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data4 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\Pacific_decadal_oscillation.csv', schema=final_struct, header=True)

data_schema = [StructField('Date', StringType(), True),
               StructField('Value', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data5 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\Southern_Oscillation_Index.csv', schema=final_struct, header=True)

data_schema = [StructField('year', IntegerType(), True),
               StructField('month', IntegerType(), True),
               StructField('temp', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data6 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\surface_temp.csv', schema=final_struct, header=True)


# In[19]:


# check data formation again
print("main_data");data1.show(5); data1.printSchema(); print('Number of data1_Rows: ', data1.count())
print("arc_oscil");data2.show(5); data2.printSchema(); print('Number of data2_Rows: ', data2.count())
print("CO2");data3.show(5); data3.printSchema(); print('Number of data3_Rows: ', data3.count())
print("pacific");data4.show(5); data4.printSchema(); print('Number of data4_Rows: ', data4.count())
print("soutnhern");data5.show(5); data5.printSchema(); print('Number of data5_Rows: ', data5.count())
print("surface");data6.show(5); data6.printSchema(); print('Number of data6_Rows: ', data6.count())


# In[6]:


data2 = data2.toPandas()
sn.pointplot(data2['Date'], data2['Value'])
plt.title('Arctic_oscillation')


# In[7]:


data3 = data3.toPandas()
sn.pointplot(data3['year'], data3['emission'])
plt.title('CO2_emission')


# In[8]:


data4 = data4.toPandas()
sn.pointplot(data4['Date'], data4['Value'])
plt.title('Pacific_decadal_oscillation')


# In[9]:


data5 = data5.toPandas()
sn.pointplot(data5['Date'], data5['Value'])
plt.title('Southern_Oscillation_Index')


# In[10]:


data6 = data6.toPandas()
sn.pointplot(data6['year'], data6['temp'])
plt.title('surface temperature')


# In[15]:


# Data Quality Audit

data2.describe('Date','Value').show(1)
data3.describe('year','month','emission').show(1)
data4.describe('Date','Value').show(1)
data5.describe('Date','Value').show(1)
data6.describe('year','month','temp').show(1)


# In[84]:


data1.describe('Date/Time','Year','Month','Max Temp (°C)', 'Mean Temp (°C)', 'Min Temp (°C)').show(1)


# In[85]:


# data selection
data1_selected = data1.drop("Climate ID","Data Quality","Max Temp Flag", "Min Temp Flag","Mean Temp Flag", "Heat Deg Days (°C)","Heat Deg Days Flag","Cool Deg Days (°C)","Cool Deg Days Flag","Total Rain (mm)","Total Rain Flag","Total Snow (cm)","Total Snow Flag","Total Precip (mm)","Total Precip Flag","Snow on Grnd (cm)","Snow on Grnd Flag","Dir of Max Gust (10s deg)","Dir of Max Gust Flag","Spd of Max Gust (km/h)","Spd of Max Gust Flag")
data1_selected = data1_selected.withColumnRenamed('Max Temp (°C)','max_temp')
data1_selected = data1_selected.withColumnRenamed('Min Temp (°C)','min_temp')
data1_selected = data1_selected.withColumnRenamed('Mean Temp (°C)','mean_temp')
data1_selected.show(5)
data1_selected.printSchema()
# other datasets do not need to be selected, which the fields in datasets are necessary.


# In[13]:


# data exploration
# just use the map description and line paint for each variable
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sn

data1 = data1.toPandas()
sn.pointplot(data1['Year'], data1['Max Temp (°C)'])
plt.title('Max Temperature Distribution')
sn.pointplot(data1['Year'], data1['Mean Temp (°C)'])
plt.title('Min Temperature Distribution')
sn.pointplot(data1['Year'], data1['Min Temp (°C)'])
plt.title('Mean Temperature Distribution')


# In[86]:


# data cleaning
# I just want to calculate the monthly average values for temperature variables to fill the some null values of daily temperature.
# which is also fulfil the demand that the variables types of other datasets belong to monthly average value types

data1_cleaning=data1_selected.select("Year","Month","max_temp","min_temp","mean_temp").groupby("Year","Month").mean("max_temp","min_temp","mean_temp")
data1_cleaning = data1_cleaning.withColumnRenamed('avg(max_temp)','max_temp')
data1_cleaning = data1_cleaning.withColumnRenamed('avg(min_temp)','min_temp')
data1_cleaning = data1_cleaning.withColumnRenamed('avg(mean_temp)','mean_temp')
data1_cleaning.show()
print('Number of data1_cleaning: ', data1_cleaning.count())


# In[87]:


data1_cleaning.describe('min_temp', 'max_temp', 'mean_temp').show(1)


# In[30]:


# data construction
# create new variable whose name is "extreme_label" 
# which value is 1 when min temperature less than -40 and 0 when min temperature more than -40
data_df=data1_selected
data_df.createOrReplaceTempView('data1_df')
data_extreme=spark.sql("select Year, Month, case when min_temp <= -40 then 1 when min_temp > -40 then 0 end as extreme_label from data1_df")
data_extreme.show(5)
print('Number of data_extreme: ', data_extreme.count())
data_extreme=data_extreme.select("Year","Month","extreme_label").groupby("Year","Month").sum("extreme_label")
data_extreme = data_extreme.withColumnRenamed('sum(extreme_label)','extreme_label')
data_extreme.show(5)
data_extreme.printSchema()
print('Number of data_extreme: ', data_extreme.count())


# In[88]:


# data integration
from pyspark.sql.functions import split, explode, concat, concat_ws

# create connection key
data1_cleaning=data1_cleaning.withColumn('label1',concat(data1_cleaning['Year'],data1_cleaning['Month']))
data3=data3.withColumn('label2',concat(data3['year'],data3['month']))
data6=data6.withColumn('label3',concat(data6['year'],data6['month']))

data1_cleaning.show(5);data1_cleaning.printSchema()
data3.show(5);data3.printSchema()
data6.show(5);data6.printSchema()

# begain to integrate
data1_cleaning=data1_cleaning.join(data3,data1_cleaning['label1']==data3['label2'],'left_outer')
data1_cleaning.show(5)


#data_combine=data1_cleaning.join(data_extreme,data1_cleaning['Year','Month']==data_extreme['Year','Month'],'left_outer')
#data1_combine.show()


# In[89]:


# continue to integrate
data2_cleaning=data1_cleaning.join(data6,data1_cleaning['label2']==data6['label3'],'left_outer')
data2_cleaning.show(5)
data3_cleaning=data2_cleaning.join(data_extreme,data2_cleaning['label3']==data_extreme['label4'],'left_outer')
data3_cleaning.show(5)


# In[90]:


# create the key for data_extreme
data_extreme=data_extreme.withColumn('label4',concat(data_extreme['Year'],data_extreme['Month']))
data_extreme.show(5)


# In[91]:


# continue to integrate
data2 = data2.withColumnRenamed('Date','label5')
data2 = data2.withColumnRenamed('Value','arc_oscil')
data4 = data4.withColumnRenamed('Date','label6')
data4 = data4.withColumnRenamed('Value','pacific')
data5 = data5.withColumnRenamed('Date','label7')
data5 = data5.withColumnRenamed('Value','southern')
data4_cleaning=data3_cleaning.join(data2,data3_cleaning['label4']==data2['label5'],'left_outer')
data5_cleaning=data4_cleaning.join(data4,data4_cleaning['label5']==data4['label6'],'left_outer')
data6_cleaning=data5_cleaning.join(data5,data5_cleaning['label6']==data5['label7'],'left_outer')
data6_cleaning.show(5)


# In[92]:


# data reduction
data1_reduction = data6_cleaning.drop('year','month','label1','label2','label3','label4','label5','label6')
data1_reduction.show(5)

data1_reduction=data1_reduction.drop('label')
data1_reduction.show(5)
print(data1_reduction.count())


# In[93]:


data1_reduction=data1_reduction.drop('label')
data1_reduction.show(5)
print(data1_reduction.count())


# In[94]:


# reduction, observe the relationship between response and explanatory variable.
# add the year and month variable first.
data_a=data_extreme.drop('extreme_label')
data_a.show(5)


# In[95]:


data2_reduction=data1_reduction.join(data_a,data1_reduction['label7']==data_a['label4'],'left_outer')
data2_reduction.show(5)


# In[96]:


data3_reduction=data2_reduction.drop('label7','label4')


# In[97]:


data3_reduction.show(5)
data3_reduction.printSchema()


# In[52]:


### doing plot to judge which variables need to be selected
data_bb = data3_reduction.toPandas()

sn.pointplot(data_bb['emission'], data_bb['mean_temp'])
plt.title('the relationship between emission and mean_temp')
print("corr:",data3_reduction.stat.corr('emission', 'mean_temp'))


# In[140]:


### add log() for emission variable
data_cc=data3_reduction
data_cc.createOrReplaceTempView('data_cc')
data_cc=spark.sql("select log(emission) as emission, mean_temp from data_cc")
data_cc.show(5)


# In[54]:


data_dd = data_cc.toPandas()
sn.pointplot(data_dd['emission'], data_dd['mean_temp'])
plt.title('the relationship between log(emission) and mean_temp')
print("corr:",data_cc.stat.corr('emission', 'mean_temp'))


# In[55]:


print("corr:",data3_reduction.stat.corr('emission', 'mean_temp'))
print("corr:",data_cc.stat.corr('emission', 'mean_temp'))


# In[56]:


sn.pointplot(data_bb['temp'], data_bb['mean_temp'])
plt.title('the relationship between surface_temp and mean_temp')
print("corr:",data3_reduction.stat.corr('temp', 'mean_temp'))


# In[57]:


sn.pointplot(data_bb['arc_oscil'], data_bb['mean_temp'])
plt.title('the relationship between arc_oscil and mean_temp')
print("corr:",data3_reduction.stat.corr('arc_oscil', 'mean_temp'))


# In[58]:


sn.pointplot(data_bb['pacific'], data_bb['mean_temp'])
plt.title('the relationship between pacific and mean_temp')
print("corr:",data3_reduction.stat.corr('pacific', 'mean_temp'))


# In[59]:


sn.pointplot(data_bb['Year'], data_bb['mean_temp'])
plt.title('the relationship between Year and mean_temp')
print("corr:",data3_reduction.stat.corr('Year', 'mean_temp'))


# In[60]:


sn.pointplot(data_bb['southern'], data_bb['mean_temp'])
plt.title('the relationship between southern and mean_temp')
print("corr:",data3_reduction.stat.corr('southern', 'mean_temp'))


# In[61]:


sn.pointplot(data_bb['Month'], data_bb['mean_temp'])
plt.title('the relationship between Month and mean_temp')
print("corr:",data3_reduction.stat.corr('Month', 'mean_temp'))


# In[98]:


# so ,finally decide reduce the variable "year" whose corr is 0.0032, and I decide to focus on study the relationship 
# between mean_temp and other variables, which would improve the efficiency
data_final=data3_reduction.drop('max_temp','min_temp','year')
data_final.show(5)
data_final.printSchema()


# In[159]:


#data_final=data_final.drop('min_temp')
#data_final.show(5)
#data_final.printSchema()
# mean_temp as the expanatory variable, emission, temp, arc_oscil, pacific, southern, Month


# In[103]:


# data projection
# I just want to divide this dataset to 2 different datasets,which is help to the following data mining process
data_extreme_final=data3_reduction.drop('mean_temp','mean_temp','mean_temp','emission','temp','arc_oscil','pacific','southern')
data_pattern_final=data_final.drop('extreme_label')
data_extreme_final.show(5)
data_extreme_final.printSchema()
data_pattern_final.show(5)
data_pattern_final.printSchema()


# In[162]:


#data_extreme_final=data_extreme_final.drop('max_temp','min_temp')
#data_extreme_final.show(5)
#data_extreme_final.printSchema()


# In[167]:


# descriptive analysis for extreme weather in Canada
# doing bar plot
data_ee=data_extreme_final.select("Year","extreme_label").groupby("Year").sum("extreme_label")
data_ee.show(5)
data_ee.printSchema()
data_ff=data_ee.toPandas()
sn.barplot(data_ff['Year'], data_ff['sum(extreme_label)'])
sn.pointplot(data_ff['Year'], data_ff['sum(extreme_label)'])
plt.title('the extreme weather distribution')


# In[165]:


#data_ee=data_extreme_final.select("Year","extreme_label").groupby("Year").sum("extreme_label")
#data_ee.show(5)
#data_ee.printSchema()


# In[104]:


# continue to data projection
from pyspark.ml.feature import VectorAssembler
featuresCol = data_pattern_final.drop('mean_temp').columns
assembler = VectorAssembler(inputCols = featuresCol, outputCol = 'features')
df_projected = assembler.transform(data_pattern_final)
final_data = df_projected.select('mean_temp', 'features')
final_data.show(5)


# In[105]:


# DM algorithm selection
# select algorithm
from pyspark.ml.regression import (RandomForestRegressor,
GBTRegressor, DecisionTreeRegressor)


# create evaluator with R2
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(labelCol='mean_temp', predictionCol='prediction',
metricName='r2')


# for performance visualization
import numpy as np
import matplotlib.pyplot as plt


# In[106]:


### model building process
#create a sample for model test
sample, x = final_data.randomSplit([0.1, 0.8])


# In[107]:


# decision trees
r2_dtr = np.zeros(10)
for i in np.arange(10):
    dtr = DecisionTreeRegressor(labelCol='mean_temp', maxDepth= (i+1)*3.)
    dtrModel = dtr.fit(sample)
    prediction_dtr = dtrModel.transform(sample)
    r2_dtr[i] = evaluator.evaluate(prediction_dtr)
plt.plot(np.arange(3, 33, 3), r2_dtr)
# so choose 10 as the maxDepth


# In[108]:


# Random Forest
r2_rfr = np.zeros(10)
for i in np.arange(10):
    rfr = RandomForestRegressor(labelCol='mean_temp', maxDepth=(i+1)*3.)
    rfrModel = rfr.fit(sample)
    prediction_rfr = rfrModel.transform(sample)
    r2_rfr[i] = evaluator.evaluate(prediction_rfr)
plt.plot(np.arange(3, 33, 3), r2_rfr)
# so select 10 as maxDepth


# In[109]:


# Gradient Boosted Trees 
r2_gbt = np.zeros(10)
for i in np.arange(10):
    gbt = GBTRegressor(labelCol='mean_temp', maxIter = (i+1)*10.)
    gbtModel = gbt.fit(sample)
    prediction_gbt = gbtModel.transform(sample)
    r2_gbt[i] = evaluator.evaluate(prediction_gbt)
plt.plot(np.arange(10, 105, 10), r2_gbt)
# so select 30 as the maxIter


# In[110]:


# data splitting
train, test = final_data.randomSplit([0.7, 0.3])


# In[ ]:


# data mining


# In[111]:


# decision tree regression model
DTR = DecisionTreeRegressor(labelCol='mean_temp', maxDepth=10)
DTRmodel = DTR.fit(train)
prediction_DTR = DTRmodel.transform(test)
# show the summary
prediction_DTR.describe().show()


# In[112]:


# random forest regression model
RFR = RandomForestRegressor(labelCol='mean_temp', maxDepth=10)
RFRmodel = RFR.fit(train)
prediction_RFR = RFRmodel.transform(test)
# show the summary
prediction_RFR.describe().show()


# In[113]:


# gradient boosted trees regression model
GBT = GBTRegressor(labelCol='mean_temp', maxIter = 30)
GBTmodel = GBT.fit(train)
prediction_GBT = GBTmodel.transform(test)
# show the summary
prediction_GBT.describe().show()


# In[ ]:


# search for patterns


# In[114]:


# check the importance of each feature
DTRmodel.featureImportances


# In[115]:


# check the importance of each feature
RFRmodel.featureImportances


# In[116]:


# check the importance of each feature
GBTmodel.featureImportances


# In[187]:


# check the decision tree
print(DTRmodel.toDebugString)


# In[117]:


# study the DM pattern
plt.plot(DTRmodel.featureImportances.values)


# In[118]:


plt.plot(RFRmodel.featureImportances.values)


# In[119]:


plt.plot(GBTmodel.featureImportances.values)


# In[120]:


features = ('emission', 'temp', 'arc_oscil', 'pacific', 'southern', 'Month')
plt.bar(features ,DTRmodel.featureImportances)


# In[213]:


plt.bar(features ,RFRmodel.featureImportances)


# In[214]:


plt.bar(features ,GBTmodel.featureImportances)


# In[219]:


# visualization the prediction outcomes
# DTR model
prediction_DTR.show(5)
prediction1_DTR=prediction_DTR.toPandas()
plt.plot(prediction1_DTR['mean_temp'])
plt.plot(prediction1_DTR['prediction'])
plt.title('the relationship between observation and prediction')


# In[220]:


# RFR model
prediction_RFR.show(5)
prediction1_RFR=prediction_RFR.toPandas()
plt.plot(prediction1_RFR['mean_temp'])
plt.plot(prediction1_RFR['prediction'])
plt.title('the relationship between observation and prediction')


# In[221]:


# GBT model
prediction_GBT.show(5)
prediction1_GBT=prediction_GBT.toPandas()
plt.plot(prediction1_GBT['mean_temp'])
plt.plot(prediction1_GBT['prediction'])
plt.title('the relationship between observation and prediction')


# In[121]:


# assess and evaluation
r2_GBT = evaluator.evaluate(prediction_GBT)
r2_DTR = evaluator.evaluate(prediction_DTR)
r2_RFR = evaluator.evaluate(prediction_RFR)
print('R2 Score of GBT Regression: ', r2_GBT)
print('R2 Score of Decision Tree Regression: ', r2_DTR)
print('R2 Score of Random Forest Regression: ', r2_RFR)


# In[ ]:


# iteration
# data description
data1 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\1633.csv", header=True)
data2 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\Arctic_oscillation.csv", header=True)
data3 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\CO2_emission.csv", header=True)
data4 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\Pacific_decadal_oscillation.csv", header=True)
data5 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\Southern_Oscillation_Index.csv", header=True)
data6 = spark.read.csv("D:\\anaconda\\project\\test\\pyspark\\model_data\\surface_temp.csv", header=True)
print("main_data");data1.show(5); data1.printSchema(); print('Number of data1_Rows: ', data1.count())
print("arc_oscil");data2.show(5); data2.printSchema(); print('Number of data2_Rows: ', data2.count())
print("CO2");data3.show(5); data3.printSchema(); print('Number of data3_Rows: ', data3.count())
print("pacific");data4.show(5); data4.printSchema(); print('Number of data4_Rows: ', data4.count())
print("soutnhern");data5.show(5); data5.printSchema(); print('Number of data5_Rows: ', data5.count())
print("surface");data6.show(5); data6.printSchema(); print('Number of data6_Rows: ', data6.count())


# In[83]:


# data selection
# data formation
from pyspark.sql.types import (StructField, StructType, StringType, IntegerType, FloatType)

data_schema = [StructField('Longitude (x)', FloatType(), True),
               StructField('Latitude (y)', FloatType(), True),
               StructField('Station Name', IntegerType(), True),
               StructField('Climate ID', IntegerType(), True),
               StructField('Date/Time', StringType(), True),
               StructField('Year', IntegerType(), True),
               StructField('Month', IntegerType(), True),
               StructField('Day', IntegerType(), True),
               StructField('Data Quality', StringType(), True),
               StructField('Max Temp (°C)', FloatType(), True),
               StructField('Max Temp Flag', StringType(), True),
               StructField('Min Temp (°C)', FloatType(), True),
               StructField('Min Temp Flag', StringType(), True),
               StructField('Mean Temp (°C)', FloatType(), True),
               StructField('Mean Temp Flag', StringType(), True),
               StructField('Heat Deg Days (°C)', FloatType(), True),
               StructField('Heat Deg Days Flag', StringType(), True),
               StructField('Cool Deg Days (°C)', FloatType(), True),
               StructField('Cool Deg Days Flag', StringType(), True),
               StructField('Total Rain (mm)', FloatType(), True),
               StructField('Total Rain Flag', StringType(), True),
               StructField('Total Snow (cm)', FloatType(), True),
               StructField('Total Snow Flag', StringType(), True),
               StructField('Total Precip (mm)', FloatType(), True),
               StructField('Total Precip Flag', StringType(), True),
               StructField('Snow on Grnd (cm)', FloatType(), True),
               StructField('Snow on Grnd Flag', StringType(), True),
               StructField('Dir of Max Gust (10s deg)', FloatType(), True),
               StructField('Dir of Max Gust Flag',StringType(), True),
               StructField('Spd of Max Gust (km/h)', FloatType(), True),
               StructField('Spd of Max Gust Flag',StringType(), True)]
final_struct = StructType(fields = data_schema)
data1 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\2402.csv', schema=final_struct, header=True)

data_schema = [StructField('Date', StringType(), True),
               StructField('Value', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data2 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\Arctic_oscillation.csv', schema=final_struct, header=True)

data_schema = [StructField('year', IntegerType(), True),
               StructField('month', IntegerType(), True),
               StructField('emission', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data3 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\CO2_emission.csv', schema=final_struct, header=True)

data_schema = [StructField('Date', StringType(), True),
               StructField('Value', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data4 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\Pacific_decadal_oscillation.csv', schema=final_struct, header=True)

data_schema = [StructField('Date', StringType(), True),
               StructField('Value', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data5 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\Southern_Oscillation_Index.csv', schema=final_struct, header=True)

data_schema = [StructField('year', IntegerType(), True),
               StructField('month', IntegerType(), True),
               StructField('temp', FloatType(), True)]
final_struct = StructType(fields = data_schema)
data6 = spark.read.csv('D:\\anaconda\\project\\test\\pyspark\\model_data\\surface_temp.csv', schema=final_struct, header=True)


# In[ ]:


# model construction
# follow the above step
# between mean_temp and other variables, which would improve the efficiency
data_final=data3_reduction.drop('max_temp','min_temp')
data_final.show(5)
data_final.printSchema()


# In[ ]:


# parameter selection
# follow the above step


# In[ ]:


# assessment
# follow the above step

