# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Reading

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA Reading JSON

# COMMAND ----------

 df_json = spark.read.format('json').option('inferSchema',True).option('header',True).option('multiline',False).load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

df_json.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DDL SCHEMA

# COMMAND ----------

my_ddl_schema='''
                 Item_Identifier string,
                 Item_Weight string ,
                 Item_Fat_Content string ,
                 Item_Visibility double ,
                 Item_Type string ,
                 Item_MRP double ,
                 Outlet_Identifier string ,
                 Outlet_Establishment_Year int ,
                 Outlet_Size string ,
                 Outlet_Location_Type string ,
                 Outlet_Type string ,
                 Item_Outlet_Sales double 
              '''

# COMMAND ----------

df=spark.read.format('csv').schema(my_ddl_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------


my_strct_schema = StructType([ StructField('Item_Identifier',StringType(),True), StructField('Item_Weight',StringType(),True), StructField('Item_Fat_Content',StringType(),True), StructField('Item_Visibility',StringType(),True), StructField('Item_MRP',StringType(),True), StructField('Outlet_Identifier',StringType(),True), StructField('Outlet_Establishment_Year',StringType(),True), StructField('Outlet_Size',StringType(),True), StructField('Outlet_Location_Type',StringType(),True), StructField('Outlet_Type',StringType(),True), StructField('Item_Outlet_Sales',StringType(),True)

])

# COMMAND ----------

df= spark.read.format('csv').schema(my_strct_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #SELECT

# COMMAND ----------

df.select("Item_Identifier","Item_Weight","Item_Fat_Content").display()

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_Id')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #FILTER

# COMMAND ----------

# MAGIC %md
# MAGIC ##Case 1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Case 2

# COMMAND ----------

df.filter( (col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10) ).display()

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #withColumnRenamed
# MAGIC

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create new column

# COMMAND ----------

df.withColumn('flag',lit("")).display()

# COMMAND ----------

df.withColumn('multiply',col("Item_Weight")*col("Item_MRP")).display()

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg")).withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","LF")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #TYPE CASTING

# COMMAND ----------

df=df.withColumn("Item_Weight",col('Item_Weight').cast(StringType()))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Sort

# COMMAND ----------

df.sort(col('Item_MRP').desc()).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_MRP'],ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #DROP

# COMMAND ----------

df.drop("Item_Visibility").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dropping multiple column

# COMMAND ----------

df.drop('Item_MRP','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #DropDuplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Union and UnionByName

# COMMAND ----------

# MAGIC %md
# MAGIC ###Preparing dataframe
# MAGIC

# COMMAND ----------


data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Union

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####UnionByName

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #String Functions

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type')).display()

# COMMAND ----------

df.select(lower('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Date functions
# MAGIC

# COMMAND ----------

df=df.withColumn('Current date',current_date())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Date_add()

# COMMAND ----------

df=df.withColumn('Week after',date_add('Current date',7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###date_sub()

# COMMAND ----------

df.withColumn('Week before',date_sub('Week after',7)).display()

# COMMAND ----------

df.withColumn('weeek beforee',date_add('Current date',-7)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Datediff()

# COMMAND ----------

df=df.withColumn('Datedifference',datediff('Week after','Current date'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###date format
# MAGIC
# MAGIC

# COMMAND ----------

df=df.withColumn('Current date',date_format('Current date','dd-MM-yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Handling Nulls
# MAGIC

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset='Outlet_Size').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Filling Nulls
# MAGIC

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('notavailable',subset='Outlet_Size').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Split and Indexing
# MAGIC

# COMMAND ----------

df.withColumn('Item_Type',split('Item_Type',' ')).display()

# COMMAND ----------

df.withColumn('Item_Type',split('Item_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #EXPLODE

# COMMAND ----------

df_exp=df.withColumn('Item_Type',split('Item_Type',' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn('Item_Type',explode('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Array contains

# COMMAND ----------

df_exp.withColumn("ARRaYcontains",array_contains('Item_Type','Foods')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #GroupBy

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('TotalSum')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('TOTAL Sum'),avg('Item_MRP').alias('avg Sum')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Collect List

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #When_Otherwise

# COMMAND ----------

df.display()

# COMMAND ----------

df_veg=df.withColumn('VEG_CHECK',when(col('Item_Type')=='Meat','NON VEG').otherwise('VEG'))
df_veg.display()

# COMMAND ----------

df.withColumn('veg_exp_flag',when(((col('Item_Type')!='Meat') & (col('Item_MRP')<100)),'Veg_Inexpensive')\
                            .when((col('Item_Type')!='Meat') & (col('Item_MRP')>100),'Veg_Expensive')\
                            .otherwise('Non_Veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #JOINS
# MAGIC

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inner Join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Left Join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Right Join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Anti Join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Windows Function

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn("row_column",row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Rank and DenseRank
# MAGIC

# COMMAND ----------

df.withColumn("rank",rank().over(Window.orderBy(col('Item_Identifier').desc()))).withColumn('denserank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Cumulative Sum

# COMMAND ----------

df.withColumn("cumsum",sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #User Defined Function (UDF)

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

my_udf=udf(my_func)

# COMMAND ----------

df.withColumn("squaremrp",my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Writing

# COMMAND ----------

df.write.format('csv').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Append

# COMMAND ----------

df.write.format('csv').mode('append').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Overwrite

# COMMAND ----------

df.write.format('csv').mode('overwrite').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Error

# COMMAND ----------

df.write.format('csv').mode('error').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ignore
# MAGIC

# COMMAND ----------

df.write.format('csv').mode('ignore').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Parquet

# COMMAND ----------

df.write.format('parquet').mode('overwrite').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ###createTempView

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fat_Content = 'Regular'

# COMMAND ----------

df_sql=spark.sql("select * from my_view where Item_Fat_Content = 'Regular'")

# COMMAND ----------

df_sql.display()

# COMMAND ----------


