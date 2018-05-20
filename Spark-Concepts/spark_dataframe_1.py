
# coding: utf-8

# ## Spark DataFrame

# In[1]:


# find and load PySpark
import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')
import pyspark


# In[2]:


from pyspark.sql import SparkSession


# In[3]:


spark = SparkSession.builder.appName('Basics').getOrCreate()


# In[4]:


# file_text = """{"name":"Michael"}
# {"name":"Andy", "age":30}
# {"name":"Justin", "age":19}"""
# f = open('people.json','w')
# f.write(file_text)
# f.close()


# In[5]:


# load file as dataframe
df = spark.read.json('people.json')


# In[6]:


# display the dataframe
df.show()


# In[7]:


# schema of the dataframe
df.printSchema()


# In[8]:


# displaying the column names
df.columns


# In[9]:


# display a statiscial summary
df.describe()
# returns a dataframe


# In[10]:


# show the values in the dataframe
df.describe().show()


# In[11]:


# import the type tools
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


# In[12]:


data_schema = [StructField('age', IntegerType(), True), # age -> means the column it relates to
              StructField('name', StringType(), True)]  # IntegerType()/ StringType() -> It converts the columns ...
                                                        #                           ...to IntegerType or StringType
                                                        # True -> means the field can be null


# In[13]:


# final structure
final_struct = StructType(fields=data_schema)


# In[14]:


# lets read the same file with the schema
df = spark.read.json('people.json', schema=final_struct)


# In[15]:


# print the df and schema
df.show()
df.printSchema()


# In[16]:


# Displaying a single column
print(type(df['age']))
df['age'] # returns a column object


# In[17]:


# we need to use SELECT method to see the values of a column
df.select('age').show()
# the code in the previous cell returns the column object, 
# where as this method (select) returns a dataframe


# In[18]:


# similarly there a row object in a dataframe
df.head(2)


# In[19]:


# get the element from the 0th index of the row
df.head(2)[0]


# In[20]:


type(df.head(2)[0])


# In[21]:


# select multiple columns
df.select(['age', 'name']).show()


# In[22]:


# creating new columns using withColum
df.withColumn('double_age', df['age']*2).show()


# In[23]:


# doesn't change the original dataframe
df.show()


# In[24]:


# renaming a column
df.withColumnRenamed('age', 'my_new_age').show()


# In[25]:


# CREATE A VIEW OF THE DATAFRAME TO ACCESS AS TABLE
df.createOrReplaceTempView('people_table')


# In[26]:


results = spark.sql('SELECT * FROM people_table')


# In[27]:


results.show()


# In[28]:


new_results = spark.sql('SELECT * FROM people_table WHERE age=30')
new_results.show()

