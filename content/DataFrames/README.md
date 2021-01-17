# PySpark DataFrames  
  
[Home](../../README.md)  

![](https://miro.medium.com/max/2560/1*qgkjkj6BLVS1uD4mw_sTEg.png)

# DataFrame Basics  
  
### Key Points    
  
| ___ | Navigation| Links | ___ | 
|-------|-----------|---------|----------|
|[First DataFrame](#First-DataFrame)|[Amending Schema](#Amending-Schema) |[Select Operations](#Select-Operations) |[Column operations](#Column-operations)|
| [SQL](#SQL)|[DataFrame Filter Operations](#DataFrame-Filter-Operations) | | |
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
```

## First DataFrame


```python
import findspark
findspark.init('/opt/apache-spark/spark-2.4.7-bin-hadoop2.7')
from pyspark.sql import SparkSession
peopleFile = 'in/df/people.json'
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
df.show()
```

    +-------------------+------------------+------------------+------------------+------------------+---------+------------------+
    |               Date|              Open|              High|               Low|             Close|   Volume|         Adj Close|
    +-------------------+------------------+------------------+------------------+------------------+---------+------------------+
    |2010-01-04 00:00:00|        213.429998|        214.499996|212.38000099999996|        214.009998|123432400|         27.727039|
    |2010-01-05 00:00:00|        214.599998|        215.589994|        213.249994|        214.379993|150476200|27.774976000000002|
    |2010-01-06 00:00:00|        214.379993|            215.23|        210.750004|        210.969995|138040000|27.333178000000004|
    |2010-01-07 00:00:00|            211.75|        212.000006|        209.050005|            210.58|119282800|          27.28265|
    |2010-01-08 00:00:00|        210.299994|        212.000006|209.06000500000002|211.98000499999998|111902700|         27.464034|
    |2010-01-11 00:00:00|212.79999700000002|        213.000002|        208.450005|210.11000299999998|115557400|         27.221758|
    |2010-01-12 00:00:00|209.18999499999998|209.76999500000002|        206.419998|        207.720001|148614900|          26.91211|
    |2010-01-13 00:00:00|        207.870005|210.92999500000002|        204.099998|        210.650002|151473000|          27.29172|
    |2010-01-14 00:00:00|210.11000299999998|210.45999700000002|        209.020004|            209.43|108223500|         27.133657|
    |2010-01-15 00:00:00|210.92999500000002|211.59999700000003|        205.869999|            205.93|148516900|26.680197999999997|
    |2010-01-19 00:00:00|        208.330002|215.18999900000003|        207.240004|        215.039995|182501900|27.860484999999997|
    |2010-01-20 00:00:00|        214.910006|        215.549994|        209.500002|            211.73|153038200|         27.431644|
    |2010-01-21 00:00:00|        212.079994|213.30999599999998|        207.210003|        208.069996|152038600|         26.957455|
    |2010-01-22 00:00:00|206.78000600000001|        207.499996|            197.16|            197.75|220441900|         25.620401|
    |2010-01-25 00:00:00|202.51000200000001|        204.699999|        200.190002|        203.070002|266424900|26.309658000000002|
    |2010-01-26 00:00:00|205.95000100000001|        213.710005|        202.580004|        205.940001|466777500|         26.681494|
    |2010-01-27 00:00:00|        206.849995|            210.58|        199.530001|        207.880005|430642100|26.932840000000002|
    |2010-01-28 00:00:00|        204.930004|        205.500004|        198.699995|        199.289995|293375600|25.819922000000002|
    |2010-01-29 00:00:00|        201.079996|        202.199995|        190.250002|        192.060003|311488100|         24.883208|
    |2010-02-01 00:00:00|192.36999699999998|             196.0|191.29999899999999|        194.729998|187469100|         25.229131|
    +-------------------+------------------+------------------+------------------+------------------+---------+------------------+
    only showing top 20 rows
    



```python

```
