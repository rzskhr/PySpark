
# coding: utf-8

# # Spark DataFrame Basic Operations

# In[1]:


# find and load PySpark
import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')
import pyspark


# In[2]:


# import a spark session
from pyspark.sql import SparkSession


# In[3]:


spark = SparkSession.builder.appName('ops').getOrCreate()


# In[4]:


# load the data set
df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)


# In[5]:


# print schema
df.printSchema()


# In[7]:


# display the dataframe
df.show()


# In[10]:


# display first row
df.head(1)[0] # indexing because it returns as a list of rows


# In[13]:


# we can add filters like sql
df.filter("Close < 500").show()


# In[15]:


# select the columns using select
df.filter("Close < 500").select(['Open', 'Close']).show()


# In[18]:


# performing same operation using normal python comparision operators 
df.filter(df['Close'] < 500).select(['Open', 'Close', 'Volume']).show()


# In[21]:


# using and and or operators 
df.filter((df['Close'] < 200) & (df['Open'] > 200)).select(['Open', 'Close', 'Volume']).show()


# In[24]:


# what date was the Low price 197.16?
df.filter(df['Low'] == 197.16).show()


# In[25]:


# if we want to use the above data for future use, we use the collect command
result = df.filter(df['Low'] == 197.16).collect()
result


# In[31]:


# we can covert the above results to different data structures
# for example here, a dictionary
result[0].asDict()


# ---

# ## GroupBy and Aggregate Operations

# In[32]:


# load the sales info dataset
df = spark.read.csv('sales_info.csv', inferSchema=True, header=True)


# In[33]:


df.show()


# In[34]:


df.printSchema()


# In[37]:


# lets group by company name
df.groupBy('Company')    # returns a GroupedData object on which we can perform opertions


# In[39]:


# shows the avg sales grouped by company
df.groupBy('Company').mean().show() # also we can do -> sum(), max(), min(), count() etc..


# In[42]:


# Aggregate
# agg METHOD TAKES THE INPUTS COLUMNS AND OPERATIONS AS  DICTIONARY
df.agg({'Sales': 'sum'}).show()


# In[43]:


# max sale
df.agg({'Sales': 'max'}).show()


# In[46]:


# using agg along with group by
# get max sale of each company
grouped_df = df.groupBy('Company')   # grouping df by company
grouped_df.agg({'Sales': 'max'}).show()   # taking the max out of each


# ### Importing functions from spark

# In[47]:


from pyspark.sql.functions import countDistinct, avg, stddev


# In[51]:


# lets get the distinct number of companies 
df.select(countDistinct('Company')).show()


# In[53]:


# avg sales of all 4
df.select(avg('Sales')).show()


# In[55]:


# can add alias as in sql
# avg sales of all 4
df.select(avg('Sales').alias('Average Sales')).show()


# In[57]:


# Formatting columns
df.select(stddev('Sales')).show()


# In[58]:


from pyspark.sql.functions import format_number


# In[64]:


sales_std = df.select(stddev('Sales').alias('Sales_STD'))
sales_std.select(format_number('Sales_STD', 2)).show()  # pass the column name and the number of decimals wanted
# will have to fix alias once again


# #### OrderBy

# In[66]:


df.orderBy("Sales").show()


# In[67]:


df.orderBy('Company').show()


# In[68]:


# for descending, pass the entire column object
df.orderBy(df['Company'].desc()).show()

