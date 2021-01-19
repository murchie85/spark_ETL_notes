# PySpark DataFrames  
  
[Home](../../README.md)  

![](https://miro.medium.com/max/2560/1*qgkjkj6BLVS1uD4mw_sTEg.png)

# DataFrame Basics  
  
## Navigation   
  
| ___ | Navigation| Links | ___ | 
|-------|-----------|---------|----------|
|[First DataFrame](#First-DataFrame)|[Amending Schema](#Amending-Schema) |[Select Operations](#Select-Operations) |[Column operations](#Column-operations)|
| [SQL](#SQL)|[DataFrame Filter Operations](#DataFrame-Filter-Operations) |[Collect & Filter](#Collect-&-Filter) |[Groupby and Aggregate](#Groupby-and-Aggregate) |
|[Rounding Numbers](#Rounding-Numbers) |[Missing Data drop/fill](#Missing-Data) |[Sort and order](#Sort-and-order) | |
| | | | |
| | | | |

**Setting up**  

```python
spark = SparkSession.builder.appName('Basics').getOrCreate() 
df = spark.read.json(peopleFile)
```  
  
**Useful commands**. 
  
```python  
df.show()
df.printSchema()
df.describe().show()
df.select('column').show()
df.select(countDistinct('sales')).show()
```

## First DataFrame
[Nav](#navigation)


```python
import findspark
findspark.init('/opt/apache-spark/spark-2.4.7-bin-hadoop2.7')
from pyspark.sql import SparkSession
peopleFile = 'in/df/people.json'
sourcePath = 'in/df/'
```


```python
spark = SparkSession.builder.appName('Basics').getOrCreate() 
```


```python
df = spark.read.json(peopleFile)
```


```python
df.show()
```

    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    



```python
df.printSchema()
```

    root
     |-- age: long (nullable = true)
     |-- name: string (nullable = true)
    



```python
df.columns # don't need ()
```




    ['age', 'name']




```python
df.describe()
```




    DataFrame[summary: string, age: string, name: string]




```python
df.describe().show()
```

    +-------+------------------+-------+
    |summary|               age|   name|
    +-------+------------------+-------+
    |  count|                 2|      3|
    |   mean|              24.5|   null|
    | stddev|7.7781745930520225|   null|
    |    min|                19|   Andy|
    |    max|                30|Michael|
    +-------+------------------+-------+
    


## Amending Schema
[Nav](#navigation)


```python
from pyspark.sql.types import StructField,StringType,IntegerType,StructType
```


```python
# struct field takes three inputs, name, datatype, nullable
print("Defining table dataschema")
data_schema = [  StructField('age',IntegerType(),True),  
                 StructField('name', StringType(),True)
              ]
```

    Defining table dataschema



```python
final_struc = StructType(fields=data_schema)
```


```python
df = spark.read.json(peopleFile,schema=final_struc)
```


```python
df.printSchema() # now our age schema has been udpated
```

    root
     |-- age: integer (nullable = true)
     |-- name: string (nullable = true)
    


## Select Operations
[Nav](#navigation)


```python
print(type(df['age']))
print(type(df.select('age')))
```

    <class 'pyspark.sql.column.Column'>
    <class 'pyspark.sql.dataframe.DataFrame'>



```python
df.head(2)
```




    [Row(age=None, name='Michael'), Row(age=30, name='Andy')]




```python
df.select('age').show()
```

    +----+
    | age|
    +----+
    |null|
    |  30|
    |  19|
    +----+
    



```python
df.select(['age','name']).show()
```

    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    


## Column operations
[Nav](#navigation)

#### Adding a Column


```python
df.withColumn('newage',df['age']).show()
```

    +----+-------+------+
    | age|   name|newage|
    +----+-------+------+
    |null|Michael|  null|
    |  30|   Andy|    30|
    |  19| Justin|    19|
    +----+-------+------+
    



```python
df.show()
```

    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    



```python
 df.withColumn('DoubleAge',df['age']*2).show()
```

    +----+-------+---------+
    | age|   name|DoubleAge|
    +----+-------+---------+
    |null|Michael|     null|
    |  30|   Andy|       60|
    |  19| Justin|       38|
    +----+-------+---------+
    


#### Renaming a Column


```python
df.withColumnRenamed('age','my_new_age').show()
```

    +----------+-------+
    |my_new_age|   name|
    +----------+-------+
    |      null|Michael|
    |        30|   Andy|
    |        19| Justin|
    +----------+-------+
    


# SQL
[Nav](#navigation)

#### Crete a view from Dataframe


```python
df.createOrReplaceTempView('people')
```

#### Run Queries


```python
results = spark.sql("SELECT * FROM people").show()
```

    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    



```python
new_results = spark.sql("SELECT * FROM people where age =30").show()
```

    +---+----+
    |age|name|
    +---+----+
    | 30|Andy|
    +---+----+
    


# DataFrame Filter Operations
[Nav](#navigation)


```python
from pyspark.sql import SparkSession
```


```python
spark = SparkSession.builder.appName('ops').getOrCreate()
```


```python
df = spark.read.csv('in/df/appl_stock.csv',inferSchema=True,header=True)
```


```python
df.printSchema()
```

    root
     |-- Date: timestamp (nullable = true)
     |-- Open: double (nullable = true)
     |-- High: double (nullable = true)
     |-- Low: double (nullable = true)
     |-- Close: double (nullable = true)
     |-- Volume: integer (nullable = true)
     |-- Adj Close: double (nullable = true)
    



```python
df.show(2)
```

    +-------------------+----------+----------+------------------+----------+---------+------------------+
    |               Date|      Open|      High|               Low|     Close|   Volume|         Adj Close|
    +-------------------+----------+----------+------------------+----------+---------+------------------+
    |2010-01-04 00:00:00|213.429998|214.499996|212.38000099999996|214.009998|123432400|         27.727039|
    |2010-01-05 00:00:00|214.599998|215.589994|        213.249994|214.379993|150476200|27.774976000000002|
    +-------------------+----------+----------+------------------+----------+---------+------------------+
    only showing top 2 rows
    



```python
df.head(3)[1]
```




    Row(Date=datetime.datetime(2010, 1, 5, 0, 0), Open=214.599998, High=215.589994, Low=213.249994, Close=214.379993, Volume=150476200, Adj Close=27.774976000000002)



### SQL Style VS Python Style


```python
df.filter("close < 500").select(['Open','close']).show(2) # SQL style
```

    +----------+----------+
    |      Open|     close|
    +----------+----------+
    |213.429998|214.009998|
    |214.599998|214.379993|
    +----------+----------+
    only showing top 2 rows
    



```python
df.filter(df['Close'] < 500).select(['Open','close']).show(2) # Python style
```

    +----------+----------+
    |      Open|     close|
    +----------+----------+
    |213.429998|214.009998|
    |214.599998|214.379993|
    +----------+----------+
    only showing top 2 rows
    


#### Multiple condition filter

**Note** you need to use   
`&` and   
`|` or  
`~` not    
  
boolean conditions


```python
df.filter((df['Close'] < 200) &  (df['Open'] > 200)).show()
```

    +-------------------+------------------+----------+----------+----------+---------+------------------+
    |               Date|              Open|      High|       Low|     Close|   Volume|         Adj Close|
    +-------------------+------------------+----------+----------+----------+---------+------------------+
    |2010-01-22 00:00:00|206.78000600000001|207.499996|    197.16|    197.75|220441900|         25.620401|
    |2010-01-28 00:00:00|        204.930004|205.500004|198.699995|199.289995|293375600|25.819922000000002|
    |2010-01-29 00:00:00|        201.079996|202.199995|190.250002|192.060003|311488100|         24.883208|
    +-------------------+------------------+----------+----------+----------+---------+------------------+
    


# Collect & Filter

[Nav](#navigation)
  
This is the more traditional approach that we will be doing - filtering a dataframe and collecting results then making display format.


```python
result = df.filter(df['Low'] == 197.16).collect()
```


```python
row = result[0]
```

### asDict 


```python
row.asDict()['Volume']
```




    220441900



## Groupby and Aggregate
[Nav](#navigation)
  
- Groupby groups rows by a given column attribute
- Aggregate could be sum of all values, average etc could be applied to groupby result also


```python
from pyspark.sql import SparkSession
```


```python
spark = SparkSession.builder.appName('aggs').getOrCreate()
```


```python
df = spark.read.csv(str(sourcePath) + 'sales_info.csv', inferSchema=True,header=True)
```


```python
df.show()
df.printSchema()
```

    +-------+-------+-----+
    |Company| Person|Sales|
    +-------+-------+-----+
    |   GOOG|    Sam|200.0|
    |   GOOG|Charlie|120.0|
    |   GOOG|  Frank|340.0|
    |   MSFT|   Tina|600.0|
    |   MSFT|    Amy|124.0|
    |   MSFT|Vanessa|243.0|
    |     FB|   Carl|870.0|
    |     FB|  Sarah|350.0|
    |   APPL|   John|250.0|
    |   APPL|  Linda|130.0|
    |   APPL|   Mike|750.0|
    |   APPL|  Chris|350.0|
    +-------+-------+-----+
    
    root
     |-- Company: string (nullable = true)
     |-- Person: string (nullable = true)
     |-- Sales: double (nullable = true)
    



```python
df.groupBy('Company')
```




    <pyspark.sql.group.GroupedData at 0x7faa48be3050>




```python
df.groupBy('Company').count().show()
```

    +-------+-----+
    |Company|count|
    +-------+-----+
    |   APPL|    4|
    |   GOOG|    3|
    |     FB|    2|
    |   MSFT|    3|
    +-------+-----+
    



```python
df.groupBy('Company').mean().show()
df.groupBy('Company').max().show()
df.groupBy('Company').min().show()
```

    +-------+-----------------+
    |Company|       avg(Sales)|
    +-------+-----------------+
    |   APPL|            370.0|
    |   GOOG|            220.0|
    |     FB|            610.0|
    |   MSFT|322.3333333333333|
    +-------+-----------------+
    
    +-------+----------+
    |Company|max(Sales)|
    +-------+----------+
    |   APPL|     750.0|
    |   GOOG|     340.0|
    |     FB|     870.0|
    |   MSFT|     600.0|
    +-------+----------+
    
    +-------+----------+
    |Company|min(Sales)|
    +-------+----------+
    |   APPL|     130.0|
    |   GOOG|     120.0|
    |     FB|     350.0|
    |   MSFT|     124.0|
    +-------+----------+
    


#### Aggregating without grouping  

Different and takes in a dict  
  
- agg all rows
- accross sales column. 
- sum them up 



```python
df.agg({'sales':'sum'}).show()
```

    +----------+
    |sum(sales)|
    +----------+
    |    4327.0|
    +----------+
    



```python
group_data = df.groupBy('Company')
```

#### Aggregating with groups


```python
group_data.agg({'sales':'sum'}).show()
```

    +-------+----------+
    |Company|sum(sales)|
    +-------+----------+
    |   APPL|    1480.0|
    |   GOOG|     660.0|
    |     FB|    1220.0|
    |   MSFT|     967.0|
    +-------+----------+
    


### Import functions from spark 

- add 
```
from pyspark.sql.functions
```  
  
then hit tab  


```python
from pyspark.sql.functions import countDistinct,avg, stddev 
```

#### Count Distinct 


```python
df.select(countDistinct('sales')).show()
```

    +---------------------+
    |count(DISTINCT sales)|
    +---------------------+
    |                   11|
    +---------------------+
    



```python
df.select(avg('sales')).show()
```

    +-----------------+
    |       avg(sales)|
    +-----------------+
    |360.5833333333333|
    +-----------------+
    


## USING ALIAS


```python
df.select(avg('sales').alias('Average Sales')).show()
```

    +-----------------+
    |    Average Sales|
    +-----------------+
    |360.5833333333333|
    +-----------------+
    



```python
df.select(stddev('sales')).show()
```

    +------------------+
    |stddev_samp(sales)|
    +------------------+
    |250.08742410799007|
    +------------------+
    


### Rounding Numbers
[Nav](#navigation)


```python
from pyspark.sql.functions import format_number
df.select(stddev('sales')).show() # Formats title 
```

    +------------------+
    |stddev_samp(sales)|
    +------------------+
    |250.08742410799007|
    +------------------+
    



```python
salesstd = df.select(stddev('sales'))
salesstd.select(format_number('stddev_samp(sales)',2).alias('std')).show()  # formats number and adds alias
```

    +------+
    |   std|
    +------+
    |250.09|
    +------+
    


## Sort and order


```python
df.orderBy('Sales').show()
```

    +-------+-------+-----+
    |Company| Person|Sales|
    +-------+-------+-----+
    |   GOOG|Charlie|120.0|
    |   MSFT|    Amy|124.0|
    |   APPL|  Linda|130.0|
    |   GOOG|    Sam|200.0|
    |   MSFT|Vanessa|243.0|
    |   APPL|   John|250.0|
    |   GOOG|  Frank|340.0|
    |     FB|  Sarah|350.0|
    |   APPL|  Chris|350.0|
    |   MSFT|   Tina|600.0|
    |   APPL|   Mike|750.0|
    |     FB|   Carl|870.0|
    +-------+-------+-----+
    


 #### Order by descending


```python
df.orderBy(df['Sales'].desc()).show()
```

    +-------+-------+-----+
    |Company| Person|Sales|
    +-------+-------+-----+
    |     FB|   Carl|870.0|
    |   APPL|   Mike|750.0|
    |   MSFT|   Tina|600.0|
    |     FB|  Sarah|350.0|
    |   APPL|  Chris|350.0|
    |   GOOG|  Frank|340.0|
    |   APPL|   John|250.0|
    |   MSFT|Vanessa|243.0|
    |   GOOG|    Sam|200.0|
    |   APPL|  Linda|130.0|
    |   MSFT|    Amy|124.0|
    |   GOOG|Charlie|120.0|
    +-------+-------+-----+
    


# Missing Data

[Nav](#navigation)
  
Three options. 
  
- Keep as nulls
- Drop 
- Fill in with other values. 



```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('miss').getOrCreate()
```


```python
df = spark.read.csv(str(sourcePath) + 'ContainsNull.csv', header = True, inferSchema = True)
```


```python
df.show()
df.printSchema()
```

    +----+-----+-----+
    |  Id| Name|Sales|
    +----+-----+-----+
    |emp1| John| null|
    |emp2| null| null|
    |emp3| null|345.0|
    |emp4|Cindy|456.0|
    +----+-----+-----+
    
    root
     |-- Id: string (nullable = true)
     |-- Name: string (nullable = true)
     |-- Sales: double (nullable = true)
    


#### Drop


```python
df.na.drop().show() # drops any row that has missing data
```

    +----+-----+-----+
    |  Id| Name|Sales|
    +----+-----+-----+
    |emp4|Cindy|456.0|
    +----+-----+-----+
    



```python
df.na.drop(thresh=2).show() ## only drop if 2 or more nulls
```

    +----+-----+-----+
    |  Id| Name|Sales|
    +----+-----+-----+
    |emp1| John| null|
    |emp3| null|345.0|
    |emp4|Cindy|456.0|
    +----+-----+-----+
    



```python
df.na.drop(how='all').show() ## only drop if all columns are null 
```

    +----+-----+-----+
    |  Id| Name|Sales|
    +----+-----+-----+
    |emp1| John| null|
    |emp2| null| null|
    |emp3| null|345.0|
    |emp4|Cindy|456.0|
    +----+-----+-----+
    



```python
df.na.drop(subset='Sales').show()  ## Only drop if sales column is null
```

    +----+-----+-----+
    |  Id| Name|Sales|
    +----+-----+-----+
    |emp3| null|345.0|
    |emp4|Cindy|456.0|
    +----+-----+-----+
    



```python

```
