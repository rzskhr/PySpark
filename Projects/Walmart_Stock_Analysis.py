
# coding: utf-8

# # Walmart Stock Analysis

# Walmart Stock market data from the years 2012-2017.

# #### Importing spark and loading dependencies

# In[83]:


import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')
import pyspark


# #### Starting a simple Spark Session

# In[84]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('walmart_stock').getOrCreate()


# #### Loading the Walmart Stock CSV File, having Spark infer the data types.

# In[85]:


walmart_df = spark.read.csv('walmart_stock.csv', header=True, inferSchema=True)


# In[86]:


# print out number of rows and columns
print((walmart_df.count(), len(walmart_df.columns)))


# #### Printing the clomn names

# In[87]:


walmart_df.columns


# #### print the schema

# In[88]:


walmart_df.printSchema()


# #### Print out the first 5 columns.

# In[89]:


walmart_df.head(5)


# #### Use describe() to learn about the DataFrame.

# In[90]:


walmart_df.describe().show()


# #### Format the columns of walmart_df.describe() to 2 significant digits

# In[91]:


# printing the schema of walmart_df.describe()
walmart_df.describe().printSchema()


# > All the columns are of type string. We would need to cast the columns in order to do so.
# http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.cast

# In[92]:


from pyspark.sql.functions import format_number


# In[93]:


# store the description in a var
walmart_desc = walmart_df.describe()


# In[94]:


# cast the result
walmart_desc.select(walmart_desc['summary'],
                   format_number(walmart_desc['Open'].cast('float'),2).alias('Open'),
                   format_number(walmart_desc['High'].cast('float'),2).alias('High'),
                   format_number(walmart_desc['Low'].cast('float'),2).alias('Low'),
                   format_number(walmart_desc['Close'].cast('float'),2).alias('Close'),
                   walmart_desc['Volume'].cast('int').alias('Volume')
                   ).show()


# #### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.

# In[95]:


walmart_df_hv_ratio = walmart_df.withColumn('HV Ratio', (walmart_df['High'] / walmart_df['Volume']))
walmart_df_hv_ratio.select('HV Ratio').show()


# #### What day had the Peak High in Price?

# In[96]:


# order by high column and take the head
walmart_df.orderBy(walmart_df['High'].desc()).head(1)


# In[97]:


# index the object above to get the date time
walmart_df.orderBy(walmart_df['High'].desc()).head(1)[0][0]


# #### What is the mean of the Close column?

# In[98]:


from pyspark.sql.functions import mean
walmart_df.select(mean('Close')).show()


# #### What is the max and min of the Volume column?

# In[99]:


from pyspark.sql.functions import max, min


# In[100]:


walmart_df.select(max('Volume'), min('Volume')).show()


# #### How many days was the Close lower than 60 dollars?

# In[101]:


# filter the data less than 60 and count
walmart_df.filter('Close < 60').count()


# In[102]:


walmart_df.filter(walmart_df['Close'] < 60).count()


# In[103]:


# pyspark has count function as well
from pyspark.sql.functions import count
walmart_df.filter(walmart_df['Close'] < 60).select(count('Close')).show()


# #### What percentage of the time was the High greater than 80 dollars ?
# #### In other words, (Number of Days High>80)/(Total Days in the dataset)

# In[104]:


(walmart_df.filter(walmart_df['High']>80).count() / walmart_df.count())*100 #multiply 100 for percent


# #### What is the Pearson correlation between High and Volume?
# http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameStatFunctions.corr

# In[105]:


# import the correlation fucntion from pyspark
from pyspark.sql.functions import corr


# In[106]:


walmart_df.select(corr(walmart_df['High'], walmart_df['Volume'])).show()


# #### What is the max High per year?

# In[107]:


# import year
from pyspark.sql.functions import year


# In[108]:


walmart_df_year = walmart_df.withColumn('Year', year(walmart_df['Date']))
# display first five rows of new year column
walmart_df_year.select('Year').head(5)


# In[109]:


walmart_df_year_max = walmart_df_year.groupBy('Year').max()
# display head
walmart_df_year_max.head()


# In[110]:


walmart_df_year_max.select('Year', 'max(High)').show()


# #### What is the average Close for each Calendar Month?
# #### In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months. 

# In[111]:


# import month
from pyspark.sql.functions import month


# In[112]:


walmart_df_month = walmart_df.withColumn('Month', month('Date'))
# display first five rows of new month column
walmart_df_month.select('Month').head(5)


# In[113]:


walmart_df_month_avg = walmart_df_month.select(['Month', 'Close']).groupBy('Month').mean()
# show head
walmart_df_month_avg.head()


# In[114]:


walmart_df_month_avg.select(['Month', 'avg(Close)']).orderBy('Month').show()


# ---
