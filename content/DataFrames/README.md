# PySpark DataFrames  
  
[Home](../../README.md)  

![](https://miro.medium.com/max/2560/1*qgkjkj6BLVS1uD4mw_sTEg.png)

# DataFrame Basics  
  
### Key Points    
  
| Quick | Navigation| Jump | To | 
|-------|-----------|---------|----------|
|[Amending Schema](#Amending-Schema) |[Select Operations](#Select-Operations) |[Column operations](#Column-operations)| |
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
```


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

```
