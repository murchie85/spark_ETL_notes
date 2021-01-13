



## Learning Path

![](spark.png)
  
- [pyspark](#Pyspark) 
- RDD 
- Tableu 
- Hive Tables




## Pyspark
    
- Apache Spark is written in Scala programming language
	- Py4j Allows apache to create the python plugin
- Using PySpark, you can work with **RDDs** in Python programming language
- Pyspark runs on disributed computers  


## Install on Linux  

- Installing via direct download, untar to location
- Set spark home, python type and path variables  
  
```
spark-submit myprogram.py
```  
  




## Useful Links 

  
[pyspark](https://www.youtube.com/watch?v=XrpSRCwISdk&ab_channel=Databricks ) 
  
[SAS](https://www.sas.com/en_gb/insights/data-management/what-is-etl.html)












## Notes  

- No **dataswamps**
- Dynamic **DAGs **with Airflow
- I care about **data lifecycle**, managing transformation, batch jobs, task driven 
- upstream,downstream dependencies pipeline 
- **siem** BIG BIG DATA
- Fortnightly demos to cover scale improvements, reduction etc
- *map reduce* for hadoop data file system 
	- sharding data for processing. 

- AWS depending on use case, can be anything from file gateway to kinesis and datasync.
	- kinesis for streaming
	- filegateway for bulk transfer 
	- redshift for data warehousing
	- Athena for dat query using sql
	- Elasticseach (kibana and logstash)

## Datamovment 
	- **MSK for kafka** (use kafka apis to move and stream) 
	- Firehose
## Datalake
	- AWS datalake formation (beanstalk for datalake)




# BigData   

The role is to do with ingesting data and bringing data on to the AWS platform It involves 

- loading data  
- ETL transformation  
  
so they are keen to bring someone in with **big data** and **transformation** experience. 
 
This individual will be liaising with the **business** and testing teams **Python/Pyspark** experience is important AWS experience is also key SQL and **Airflow** are useful to have 

Infrastructure or server experience is not necessary  
  
## Answers  
  

- Create workflows for data ingestion using Pyspark  
- I've worked with various AWS ingestion methods such as: 
	- Transfer local data to SMB share presented by `file gateway`, it then stores in an s3 bucket
	- aws `datasync` to transfer in bulk 
	- `SFTP` clients to transfer data using   

- use Custom Grafana Dashboards or data studio to visualise data transfer process  


## AWS Options  
  
# ETL   

- Extract, transform, load  
- Large experience from working with map reduce such as kafka, sharding ..
- Datascience projects using pandas, DS pipelines  
 
**Requirements**  
  
- Failures
- monitoring 
- Dependencies   
- Scalability  
- Deployment



## Airflow  


**DAG** - Directed Acyclic Graph  
    

- Workflows 
- graphs 
- monitoring workflows (workflow manager like UCD etc )  
- Great for ETL 


1. Write a CSV file to a platform data container.

2. Convert the CSV file into a Parquet table.

3. Run SQL queries on the data in Parquet table.  
  
  