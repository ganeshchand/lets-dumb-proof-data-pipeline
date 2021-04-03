

What does a pipeline need (destructure a pipeline):

* Import statements
* library dependencies
* Reader configurations
* Writer configurations
* Runtime configurations
    * Spark Configurations for tuning, feature flag
    * Databricks Runtime configurations
    * Logging level
* Parameterized business logic (transformation)


## Version 1

* Reader:
  * .option("rowsPerSecond", 1)
  
* Writer
  * .option("checkpointLocation", "/tmp/deltalake/confix/2/rate/_checkpoint")
  * .trigger(ProcessingTime("30 seconds"))
  
  * .option("path", "/tmp/deltalake/confix/2/rate")
  
  Problem:
  * I have to change the source code when I promote this pipeline from dev to qa to prod
  * I have to change the source code if I need to tweak trigger interval and input rate in my prod environment
  

## Version 2

* Parameterize your pipeline configuration
  * get input from users
  * provide sensible defaults
  * use dbutils.widgets.{text, get}
* Catch incorrect configuration / invalid inputs as early as possible and fail the pipeline  

Problem

* what happens if you have twenty or more pipeline parameters
* every input value is String. What about arrays, map, nested input


## Version 3
* JSON string as input
* Notebook job parameters only takes valid String inputs. You cannot pass JSON string as input. Read from a config json file?

Option 1: Use Json format  
* Parse JSON string? Yes, it's tempting but no need. Use configuration library already available in runtime.

```scala
        dbutils.fs.put(
"/tmp/deltalake/config/ingest_stream_rate_sample.json",
"""{
  "pipelineConfig":{
  "triggerInterval":"30 seconds",
  "checkpointLocation":"\"\"",
  "outputPath":"/tmp/deltalake/sample/1/rate",
  "rowsPerSecond":"1"
  }
  }""", true)
```
  Option 2: Use HOCON format


```scala

dbutils.fs.put(
"/tmp/deltalake/config/ingest_stream_rate_sample.conf",
s"""{
pipeline.conf {
triggerInterval: 30 seconds
outputPath: /tmp/deltalake/sample/1/rate
checkpointLocation: ${outputPath}"/_checkpoint"
rowsPerSecond: 1
}
}""", true)
```

* 


Databricks Runtime 8.x, 7.x amd 6.x uses com.typesafe.config: 1.2.1
v1.2.1 was released on May 2 2014
v1.4.1 latest version released on Oct 22 2020 




## Generalization vs parameterization

Generalization is for code re-use. You can generalize your pipeline code so you can run multiple pipelines using the code just by passing the required parameters.
Parameterization is for making your code environment agnostic and to avoid code changes and deployment to re-configure your pipeline



https://hocon-playground.herokuapp.com/
https://transform.tools/json-to-scala-case-class or https://json2caseclass.cleverapps.io/


