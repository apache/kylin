## Background
What is hadoop-client docker images? 
And why we need this?

## Build Step
1. Place Spark binary(*spark-2.3.2-bin-hadoop2.7.tgz*) into dir `provided-binary`.
2. Depend on which Hadoop Distribution you are use, prepare `Dockerfile`, please take `CDH-5.7` as an example
3. Run `build-image.sh` to build image.

