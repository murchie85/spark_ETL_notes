## Airflow  
     
[home](../../README.md).  
  

![](https://cdn-images-1.medium.com/max/1024/1*FJsMPN5kPMI7JuqhsaP7rA.png)
  

## Navigation  
   
- [Intro](#Intro) 
- [Setup](#Setup) 
- [Useful Commands](#Useful-Commands)
- [Architecture](#Architecture)
- [UI](#UI)


## Intro  
 
### UseCases 

Used for `orchastrating datajobs` including the logic flow, to manage failures, notifications, extract, transform, load.  
  
Airflow also critically manages `mutliple pipelines` in parallel.  
    
- Extensible (build your own plugins)
- Dashboards 
- Uses python
- Scalable.   
  

To `author`, `schedule` and `monitor` data workflows.   
It allows you to orchastrate tasks in the right way, in the right order.  
  
    
### Core Components 
  

**Web server** Flask server with Gunicorn serving the UI  
**Scheduler** Daemon in charge of managing workflows. 
**metastore**  Database where metadata is stored  
  
**executor** class defining **how** your tasks will executor 
**worker** the process executing the task.  
  

## DAG  
  
DAG (Directed Acyclic Graph) is essentially a pipeline.  
  
![](https://miro.medium.com/max/4096/1*CtoqTsvlAuXU_4V-W5VMyQ.png)
  
  
**operator** is a task, run by the worker.  
- Action Operator
- Transfer Operator
- Sensor Operator   
  

**Task** is an operator.   
**task instance** is the specific runtime for a given task.  
	
## DAGS ARE NOT...  
  
- For streaming data
- For processing data (use a `spark submit` operator instead to prevent memory overflow). 
  
## Architecture
    
![](architecture.png) 
  

**Celery** architecture is:  
  
- Scalable 
- An executor on each node 
- Celery manages nodes and needs reddis/rabbitmq to manage queues.  
- Executor pushes tasks to the queue 
- Airflow workers fetch tasks and execute them on their own machine  

  
How it works:  
  
1. Data pipelines are added to `folder Dags`. 
2. Web server and executor parses `folder dags`. 
3. `DagRun` object is created in Metastore with status `running`. 
4. A `task instance` object is created and sent to executor 
5. Eexecutor updates `task instance` object in meta store, 
6. Once complete, `task instance` object updated again, and scheduler checks it is done.  
  
  

## Setup  

Can create a VM and connect to it using ssh connection on visual studios. 
  
Or. 

1. Using `conda create --name airflowEnv python=3` to create env
2. Using `conda activate airflow` to manage dependencies.    
3. Use `conda env remove --name airflow` to delete once finished.  
4. Run `airflow db init` to start first time.  
5. Modify port on `airflow.cfg`  accordingly.  

  
But should specify the constraints file:  

```
pip install apache-airflow==2.0.0 --constraint https://gist.githubusercontent.com/marclamberti/742efaef5b2d94f44666b0aec020be7c/raw/5da51f9fe99266562723fdfb3e11d3b6ac727711/constraint.txt
```	  
  
Run to start: 
  
```sh
airflow db init 
```
  
Run to get dir path: 
  
```
airflow info
``` 

#### Airflow Dir. 
  

```
airflow.cfg		airflow.db		logs			unittests.cfg		webserver_config.py
```

- config file `airflow.cfg` 
- Database `airflow.db`. 
- Unitests `unittests.cfg` to test configuration   
- webserver settings `webserver_config.py`. 
    


# Useful Commands



## CheatSheet 
   

```sh
airflow cheatsheet
```
  
#### Create Users 

```sh
airflow users create -u admin -p admin -f adam -l mcmurchie -r Admin -e admin@airflow.com

```

#### Start Scheduler

```sh
airflow scheduler -D
```
    
- This has to run in addition to webserver so use `-D` flag.  	 

#### List DAGS

```sh
airflow dags list
```
  
#### Lists Tasks for given DAG

```sh
airflow tasks list dag_name 
  
airflow tasks list example_python_operator
```
  
#### Trigger DAG datapipeline

```sh
airflow dags trigger -e 2021-01-01 dag_name
```
  
####

```sh

```

   


#### Help Flag 

```sh
airflow users create -h
```

#### Start Webserver  
   

```sh
airflow webserver
```



#### Init DB  
   
   
```sh
airflow db init
```
    
#### Reset DB 

```sh
airflow db reset

```
 
  
  
# UI
  
![](DagView.png) 
  
- Toggle the job on/off 
- `tags` help you group pipelines. 
- Owner could be airflow, team names i.e. marketing. 
- Runs is for status. 
- Schedule   
- Recent tasks (status). 
- Actions: trigger manually, refresh, delete (doesn't delete file, just metadata) 
- links gives you access to code, view, details etc.    
  
## Tree View 

  
![](https://anchormen.nl/wp-content/uploads/2020/05/Tree-view-in-the-web-UI-to-demonstrate-Airflow-concepts..png) 

    
- Useful for history of DAG runs 
- Each verticle slice is a run 
- Square is a task 
  
## Graph View 
    
![](https://michal.karzynski.pl/images/illustrations/2017-03-19/airflow-example-dag.png). 
  
- Outline color represents the `status`
- Internal color represents the `operator` 
  
## Gantt View  
  
![](https://www.agari.com/wp-content/uploads/2015/08/Gantt_chart.png)
  
- Great for spotting bottlenecks
- helps to see how long each task is taking.  
- Allows for planning of parallel jobs.  
- Great for production.  
  

## Instance view 

![](instanceView.png). 
  

- Most important is the logs 
- `clear` task if you want to re-run 
- `mark fail / success`    
  




# Notes.  
  
Training materials from marclaberti `https://github.com/marclamberti/training_materials` 

