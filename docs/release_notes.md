<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache Kylin Release Notes

## v0.7.1 (First Apache Release) - 2015-05-29
_Tag:_ [kylin-0.7.1-incubating](https://github.com/apache/incubator-kylin/tree/kylin-0.7.1-incubating)

This is first Apache Kylin release, including many changes after migrated to Apache.

__Main Changes:__

* Package renamed from com.kylinolap to org.apache.kylin
* Binary pacakge avaliable download from: [http://kylin.incubator.apache.org/download](http://kylin.incubator.apache.org/download/)


__JIRA Tickets__


** New Feature
    * [KYLIN-327] - Binary distribution 
    * [KYLIN-368] - Move MailService to Common module
    * [KYLIN-540] - Data model upgrade for legacy cube descs
    * [KYLIN-576] - Refactor expansion rate expression

** Task
    * [KYLIN-361] - Rename package name with Apache Kylin
    * [KYLIN-531] - Rename package name to org.apache.kylin
    * [KYLIN-533] - Job Engine Refactoring
    * [KYLIN-585] - Simplify deployment
    * [KYLIN-586] - Add Apache License header in each source file
    * [KYLIN-587] - Remove hard copy of javascript libraries
    * [KYLIN-624] - Add dimension and metric info into DataModel
    * [KYLIN-650] - Move all document from github wiki to code repository (using md file)
    * [KYLIN-669] - Release v0.7.1 as first apache release

** Sub-task
    * [KYLIN-670] - Update pom with "incubating" in version number
    * [KYLIN-737] - Generate and sign release package for review and vote
    * [KYLIN-795] - Release after success vote

** Bug
    * [KYLIN-132] - Job framework
    * [KYLIN-194] - Dict & ColumnValueContainer does not support number comparison, they do string comparison right now
    * [KYLIN-220] - Enable swap column of Rowkeys in Cube Designer
    * [KYLIN-230] - Error when create HTable
    * [KYLIN-255] - Error when a aggregated function appear twice in select clause
    * [KYLIN-383] - Sample Hive EDW database name should be replaced by "default" in the sample
    * [KYLIN-399] - refreshed segment not correctly published to cube
    * [KYLIN-412] - No exception or message when sync up table which can't access
    * [KYLIN-421] - Hive table metadata issue
    * [KYLIN-436] - Can't sync Hive table metadata from other database rather than "default"
    * [KYLIN-508] - Too high cardinality is not suitable for dictionary!
    * [KYLIN-509] - Order by on fact table not works correctly
    * [KYLIN-517] - Always delete the last one of Add Lookup page buttom even if deleting the first join condition
    * [KYLIN-524] - Exception will throw out if dimension is created on a lookup table, then deleting the lookup table.
    * [KYLIN-547] - Create cube failed if column dictionary sets false and column length value greater than 0
    * [KYLIN-556] - error tip enhance when cube detail return empty
    * [KYLIN-570] - Need not to call API before sending login request
    * [KYLIN-571] - Dimensions lost when creating cube though Joson Editor
    * [KYLIN-572] - HTable size is wrong
    * [KYLIN-581] - unable to build cube
    * [KYLIN-583] - Dependency of Hive conf/jar in II branch will affect auto deploy
    * [KYLIN-588] - Error when run package.sh
    * [KYLIN-593] - angular.min.js.map and angular-resource.min.js.map are missing in kylin.war
    * [KYLIN-594] - Making changes in build and packaging with respect to apache release process
    * [KYLIN-595] - Kylin JDBC driver should not assume Kylin server listen on either 80 or 443
    * [KYLIN-605] - Issue when install Kylin on a CLI which does not have yarn Resource Manager
    * [KYLIN-614] - find hive dependency shell fine is unable to set the hive dependency correctly
    * [KYLIN-615] - Unable add measures in Kylin web UI
    * [KYLIN-619] - Cube build fails with hive+tez
    * [KYLIN-620] - Wrong duration number
    * [KYLIN-621] - SecurityException when running MR job
    * [KYLIN-627] - Hive tables' partition column was not sync into Kylin
    * [KYLIN-628] - Couldn't build a new created cube
    * [KYLIN-629] - Kylin failed to run mapreduce job if there is no mapreduce.application.classpath in mapred-site.xml
    * [KYLIN-630] - ArrayIndexOutOfBoundsException when merge cube segments 
    * [KYLIN-638] - kylin.sh stop not working
    * [KYLIN-639] - Get "Table 'xxxx' not found while executing SQL" error after a cube be successfully built
    * [KYLIN-640] - sum of float not working
    * [KYLIN-642] - Couldn't refresh cube segment
    * [KYLIN-643] - JDBC couldn't connect to Kylin: "java.sql.SQLException: Authentication Failed"
    * [KYLIN-644] - join table as null error when build the cube
    * [KYLIN-652] - Lookup table alias will be set to null
    * [KYLIN-657] - JDBC Driver not register into DriverManager
    * [KYLIN-658] - java.lang.IllegalArgumentException: Cannot find rowkey column XXX in cube CubeDesc
    * [KYLIN-659] - Couldn't adjust the rowkey sequence when create cube
    * [KYLIN-666] - Select float type column got class cast exception
    * [KYLIN-681] - Failed to build dictionary if the rowkey's dictionary property is "date(yyyy-mm-dd)"
    * [KYLIN-682] - Got "No aggregator for func 'MIN' and return type 'decimal(19,4)'" error when build cube
    * [KYLIN-684] - Remove holistic distinct count and multiple column distinct count from sample cube
    * [KYLIN-691] - update tomcat download address in download-tomcat.sh
    * [KYLIN-696] - Dictionary couldn't recognize a value and throw IllegalArgumentException: "Not a valid value"
    * [KYLIN-703] - UT failed due to unknown host issue
    * [KYLIN-711] - UT failure in REST module
    * [KYLIN-739] - Dimension as metrics does not work with PK-FK derived column
    * [KYLIN-761] - Tables are not shown in the "Query" tab, and couldn't run SQL query after cube be built

** Improvement
    * [KYLIN-168] - Installation fails if multiple ZK
    * [KYLIN-182] - Validation Rule: columns used in Join condition should have same datatype
    * [KYLIN-204] - Kylin web not works properly in IE
    * [KYLIN-217] - Enhance coprocessor with endpoints 
    * [KYLIN-251] - job engine refactoring
    * [KYLIN-261] - derived column validate when create cube
    * [KYLIN-317] - note: grunt.json need to be configured when add new javascript or css file
    * [KYLIN-324] - Refactor metadata to support InvertedIndex
    * [KYLIN-407] - Validation: There's should no Hive table column using "binary" data type
    * [KYLIN-445] - Rename cube_desc/cube folder
    * [KYLIN-452] - Automatically create local cluster for running tests
    * [KYLIN-498] - Merge metadata tables 
    * [KYLIN-532] - Refactor data model in kylin front end
    * [KYLIN-539] - use hbase command to launch tomcat
    * [KYLIN-542] - add project property feature for cube
    * [KYLIN-553] - From cube instance, couldn't easily find the project instance that it belongs to
    * [KYLIN-563] - Wrap kylin start and stop with a script 
    * [KYLIN-567] - More flexible validation of new segments
    * [KYLIN-569] - Support increment+merge job
    * [KYLIN-578] - add more generic configuration for ssh
    * [KYLIN-601] - Extract content from kylin.tgz to "kylin" folder
    * [KYLIN-616] - Validation Rule: partition date column should be in dimension columns
    * [KYLIN-634] - Script to import sample data and cube metadata
    * [KYLIN-636] - wiki/On-Hadoop-CLI-installation is not up to date
    * [KYLIN-637] - add start&end date for hbase info in cubeDesigner
    * [KYLIN-714] - Add Apache RAT to pom.xml
    * [KYLIN-753] - Make the dependency on hbase-common to "provided"
    * [KYLIN-758] - Updating port forwarding issue Hadoop Installation on Hortonworks Sandbox.
    * [KYLIN-779] - [UI] jump to cube list after create cube
    * [KYLIN-796] - Add REST API to trigger storage cleanup/GC

** Wish
    * [KYLIN-608] - Distinct count for ii storage

