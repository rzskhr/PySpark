
# coding: utf-8

# # Missing Data

# In[1]:


# find and load PySpark
import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')
import pyspark


# In[2]:


# import a spark session
from pyspark.sql import SparkSession


# In[3]:


spark = SparkSession.builder.appName('missing_data').getOrCreate()


# In[4]:


# load the dataframe
df =  spark.read.csv('ContainsNull.csv', header=True, inferSchema=True)
# print the schema
df.printSchema()


# In[5]:


# show the dataframe
df.show()


# In[6]:


# DROPPIN THE MISSING DATA
df.na.drop().show()    # drops all the row having any missing data


# In[9]:


# we can setup THRESHOLD to keep rows having less than two missing values
df.na.drop(thresh=2).show()


# In[11]:


# WE CAN ALSO USE THE PARAMERTER "how"
df.na.drop(how='any').show() # drops if there is any null values


# In[12]:


df.na.drop(how='all').show()  # drops rows if there are all the values that are missing


# In[15]:


# we can also see only a column using SUBSET
df.na.drop(subset=['Sales']).show()  # checks only the sales column


# ### Substituting missing values

# In[19]:


# fill value takes the datatype and fills the matching column having same datatype
df.na.fill('String_Data').show() # columns with datatype int/double/float will not be filled


# In[21]:


df.na.fill(0).show() # columns with datatype string will not be filled


# In[23]:


# targetting the columns to fill
# It is always recommended to use the column names as subset while filling data
df.na.fill('No_Name', subset=['Name']).show()


# In[25]:


from pyspark.sql.functions import mean


# In[32]:


# lets fill the missing sales with mean value

# get the mean value
mean_value = df.select(mean(df['Sales'])).collect()
mean_value


# In[35]:


# get the number from the row object
mean_sales = mean_value[0][0]


# In[37]:


# fill the missing values with mean sales
df.na.fill(mean_sales, subset=['Sales']).show()


# ---

# ## Date and Timestamps

# In[38]:


# creating a session for data and timestamps
spark = SparkSession.builder.appName('date_time').getOrCreate()


# In[39]:


# load the stock data set
df =  spark.read.csv('appl_stock.csv', header=True, inferSchema=True)
# print the schema
df.printSchema()


# In[40]:


df.head(1)


# In[42]:


# check the date column
df.select(['Date', 'Volume']).show()


# In[43]:


# importing some important date functions
from pyspark.sql.functions import (dayofmonth, dayofyear, 
                                   year, month, hour, weekofyear, 
                                   format_number, date_format)


# In[46]:


# show only the date
df.select(dayofmonth(df['Date'])).show()


# In[50]:


# using multiple function create diff cols
df.select(dayofmonth(df['Date']), year(df['Date'])).show()


# In[58]:


# calculate average closing price per year

# creating a new column year using WITHCOLUMN
df_year = df.withColumn('Year', year(df['Date']))
df_year.head(1)[0]


# In[71]:


# group by and select two columns year and Close
avg_close_per_year = df_year.groupBy('Year').mean().select(['Year', 'avg(Close)'])


# In[72]:


# rename the columns
avg_close_per_year = avg_close_per_year.withColumnRenamed("avg(Close)", "Average Closing Price")


# In[74]:


# format the numbers
avg_close_per_year.select(['Year', format_number('Average Closing Price', 2)]).show()


# In[77]:


# use alias to rename the column
avg_close_per_year.select(['Year', format_number('Average Closing Price', 2).alias("Avg Close")]).show()

