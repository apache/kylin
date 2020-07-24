## Background
What is kylin-client docker images? And why we need this?
kylin-client is a based docker image which based on hadoop-client, it will provided the 
flexibility of upgrade of Apache Kylin.

## Build Step
1. Place Kylin binary(*apache-kylin-3.0.1-bin-cdh57*) and uncompress it into current dir.
2. Modify `Dockerfile` , change the value of `KYLIN_VERSION` and the name of base image(hadoop-client).
3. Run `build-image.sh` to build image.