---
layout: docs
title:  Tool classes in Kylin
categories: tutorial
permalink: /docs/tutorial/tools.html
---
Kylin has many tool class. This document will introduce the following class: KylinConfigCLI.java, CubeMetaExtractor.java, CubeMetaIngester.java, CubeMigrationCLI.java and CubeMigrationCheckCLI.java. Before using these tools, you have to switch to the KYLIN_HOME directory. 

## KylinConfigCLI.java

### Intention
KylinConfigCLI.java outputs the value of Kylin properties. 

### How to use 
After the class name, you can only write one parameter, `conf_name` which is the parameter name that you want to know its value.
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.KylinConfigCLI <conf_name>
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.KylinConfigCLI kylin.server.mode
{% endhighlight %}
Result:
{% highlight Groff markup %}
all
{% endhighlight %}

If you do not know the full parameter name, you can use the following command, then all parameters prefixed by this prefix will be listed:
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.KylinConfigCLI <prefix>.
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.KylinConfigCLI kylin.job.
{% endhighlight %}
Result:
{% highlight Groff markup %}
max-concurrent-jobs=10
retry=3
sampling-percentage=100
{% endhighlight %}

## CubeMetaExtractor.java

### Intention
CubeMetaExtractor.java is to extract Cube related info for debugging / distributing purpose.  

### How to use
At least two parameters should be followed. 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMetaExtractor -<conf_name> <conf_value> -destDir <your_dest_dir>
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMetaExtractor -cube querycube -destDir /root/newconfigdir1
{% endhighlight %}
Result:
After the command is successfully executed, the Cube / project / hybrid you want to extract will exist in the destDir.

All supported parameters are listed below:  

| Parameter                                             | Description                                                                                         |
| ----------------------------------------------------- | :-------------------------------------------------------------------------------------------------- |
| allProjects                                           | Specify realizations in all projects to extract                                                     |
| compress <compress>                                   | Specify whether to compress the output with zip. Default true.                                      | 
| cube <cube>                                           | Specify which Cube to extract                                                                       |
| destDir <destDir>                                     | (Required) Specify the dest dir to save the related information                                     |
| engineType <engineType>                               | Specify the engine type to overwrite. Default is empty, keep origin.                                |
| hybrid <hybrid>                                       | Specify which hybrid to extract                                                                     |
| includeJobs <includeJobs>                             | Set this to true if want to extract job info/outputs too. Default false                             |
| includeSegmentDetails <includeSegmentDetails>         | Set this to true if want to extract segment details too, such as dict, tablesnapshot. Default false |
| includeSegments <includeSegments>                     | Set this to true if want extract the segments info. Default true                                    |
| onlyOutput <onlyOutput>                               | When include jobs, only extract output of job. Default true                                         |
| packagetype <packagetype>                             | Specify the package type                                                                            |
| project <project>                                     | Specify realizations in which project to extract                                                    |
| storageType <storageType>                             | Specify the storage type to overwrite. Default is empty, keep origin.                               |
| submodule <submodule>                                 | Specify whether this is a submodule of other CLI tool. Default false.                               |

## CubeMetaIngester.java

### Intention
CubeMetaIngester.java is to ingest the extracted Cube meta into another metadata store. It only supports ingest cube now. 

### How to use
At least two parameters should be followed. Please make sure the cube you want to ingest does not exist in the target project. Note: The zip file must contain only one directory after it has been decompressed.
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMetaIngester -project <target_project> -srcPath <your_src_dir>
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMetaIngester -project querytest -srcPath /root/newconfigdir1/cubes.zip
{% endhighlight %}
Result:
After the command is successfully executed, the Cube you want to ingest will exist in the srcPath.

All supported parameters are listed below:

| Parameter                         | Description                                                                                                                                                                                        |
| --------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| forceIngest <forceIngest>         | Skip the target Cube, model and table check and ingest by force. Use in caution because it might break existing cubes! Suggest to backup metadata store first. Default false.                      |
| overwriteTables <overwriteTables> | If table meta conflicts, overwrite the one in metadata store with the one in srcPath. Use in caution because it might break existing cubes! Suggest to backup metadata store first. Default false. |
| project <project>                 | (Required) Specify the target project for the new cubes.                                                                                                                                           |
| srcPath <srcPath>                 | (Required) Specify the path to the extracted Cube metadata zip file.                                                                                                                               |

## CubeMigrationCLI.java

### Intention
CubeMigrationCLI.java serves for the purpose of migrating cubes. e.g. upgrade Cube from dev env to test(prod) env, or vice versa. Note that different envs are assumed to share the same Hadoop cluster, including HDFS, HBase and HIVE.  

### How to use
The first eight parameters must have and the order cannot be changed.
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMigrationCLI <srcKylinConfigUri> <dstKylinConfigUri> <cubeName> <projectName> <copyAclOrNot> <purgeOrNot> <overwriteIfExists> <realExecute> <migrateSegmentOrNot>
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMigrationCLI /root/apache-kylin-2.5.0-bin-hbase1x/conf/kylin.properties /root/me/apache-kylin-2.5.0-bin-hbase1x/conf/kylin.properties querycube IngesterTest true false false true false
{% endhighlight %}
After the command is successfully executed, please reload metadata, the Cube you want to migrate will exist in the target project.

All supported parameters are listed below:
　If you use `cubeName`, and the model of Cube you want to migrate in target environment does not exist, it will also migrate the model resource.
　If you set `overwriteIfExists` to false, and the Cube exists in the target environment, when you run the command, the prompt message will show.
　If you set `migrateSegmentOrNot` to true, please make sure the metadata HDFS dir of Kylin exists and the Cube status is READY.

| Parameter           | Description                                                                                |
| ------------------- | :----------------------------------------------------------------------------------------- |
| srcKylinConfigUri   | The KylinConfig of the Cube’s source                                                      |
| dstKylinConfigUri   | The KylinConfig of the Cube’s new home                                                    |
| cubeName            | the name of Cube to be migrated.(Make sure it exist)                                       |
| projectName         | The target project in the target environment.(Make sure it exist)                          |
| copyAclOrNot        | True or false: whether copy Cube ACL to target environment.                                |
| purgeOrNot          | True or false: whether purge the Cube from src server after the migration.                 |
| overwriteIfExists   | Overwrite Cube if it already exists in the target environment.                             |
| realExecute         | If false, just print the operations to take, if true, do the real migration.               |
| migrateSegmentOrNot | (Optional) true or false: whether copy segment data to target environment. Default true.   |

## CubeMigrationCheckCLI.java

### Intention
CubeMigrationCheckCLI.java serves for the purpose of checking the "KYLIN_HOST" property to be consistent with the dst's MetadataUrlPrefix for all of Cube segments' corresponding HTables after migrating a Cube. CubeMigrationCheckCLI.java will be called in CubeMigrationCLI.java and is usually not used separately. 

### How to use
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMigrationCheckCLI -fix <conf_value> -dstCfgUri <dstCfgUri_value> -cube <cube_name>
{% endhighlight %}
For example: 
{% highlight Groff markup %}
./bin/kylin.sh org.apache.kylin.tool.CubeMigrationCheckCLI -fix true -dstCfgUri /root/me/apache-kylin-2.5.0-bin-hbase1x/conf/kylin.properties -cube querycube
{% endhighlight %}
All supported parameters are listed below:

| Parameter           | Description                                                                   |
| ------------------- | :---------------------------------------------------------------------------- |
| fix                 | Fix the inconsistent Cube segments' HOST, default false                       |
| dstCfgUri           | The KylinConfig of the Cube’s new home                                       |
| cube                | The name of Cube migrated                                                     |