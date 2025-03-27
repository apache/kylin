# Preview latest Kylin (5.x)

## [Image Tag Information](https://hub.docker.com/r/apachekylin/apache-kylin-standalone)
| Tag                  | Image Contents                                                                 | Comment & Publish Date                                                                                                                                   |
|----------------------|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| 5.0.2-GA             | [**Recommended for users**] The official 5.0.2-GA with Spark & Gluten bundled. | Uploaded at 2025-03-31                                                                                                                                   |
| kylin-4.0.1-mondrian | The official Kylin 4.0.1 with **MDX** function enabled                         | Uploaded at 2022-05-13                                                                                                                                   | 

## Why you need Kylin 5

These are the highlight features of Kylin 5, if you are interested, please visit https://kylin.apache.org/5.0/ for detail information.

#### More flexible and enhanced data model
- Allow adding new dimensions and measures to the existing data model
- The model adapts to table schema changes while retaining the existing index at the best effort
- Support last-mile data transformation using Computed Column
- Support raw query (non-aggregation query) using Table Index
- Support changing dimension table (SCD2)
#### Simplified metadata design
- Merge DataModel and CubeDesc into new DataModel
- Add DataFlow for more generic data sequence, e.g. streaming alike data flow
- New metadata AuditLog for better cache synchronization
#### More flexible index management (was cuboid)
- Add IndexPlan to support flexible index management
- Add IndexEntity to support different index type
- Add LayoutEntity to support different storage layouts of the same Index
#### Toward a native and vectorized query engine
- Experiment: Integrate with a native execution engine, leveraging Gluten
- Support async query
- Enhance cost-based index optimizer
#### More
- Build engine refactoring and performance optimization
- New WEB UI based on Vue.js, a brand new front-end framework, to replace AngularJS
- Smooth modeling process in one canvas

### Attention!
After the time of release of Kylin 5.0.0 , **Kylin 4.X and older version** will be set in **retired** status(NOT in active development).

## How to preview Kylin 5

Deploy a Kylin 5.X instance without any pre-deployed hadoop component by following command:

```shell
docker run -d \
    --name Kylin5-Machine \
    --hostname localhost \
    -e TZ=UTC \
    -m 10G \
    -p 7070:7070 \
    -p 8088:8088 \
    -p 9870:9870 \
    -p 8032:8032 \
    -p 8042:8042 \
    -p 2181:2181 \
    apachekylin/apache-kylin-standalone:5.0.2-GA

docker logs --follow Kylin5-Machine
```

The following message indicates that the Kylin is ready :

```
Kylin service is already available for you to preview.
```

After that, please press `Ctrl + C` to exit `docker logs`, and visit Kylin web UI.


| Service Name | URL                         |
|--------------|-----------------------------|
| Kylin        | http://localhost:7070/kylin |
| Yarn         | http://localhost:8088       |
| HDFS         | http://localhost:9870       |

When you log in Kylin web UI, please remember your username is **ADMIN** , and password is **KYLIN** .


To stop and remove the container, use these commands.
```
docker stop Kylin5-Machine
docker rm Kylin5-Machine
```


### Notes

If you are using mac docker desktop, please ensure that you have set Resources: Memory=8GB and Cores=6 cores at least,
so that can run kylin standalone on docker well.

If you are interested in `Dockerfile`, please visit https://github.com/apache/kylin/blob/kylin5/dev-support/release-manager/standalone-docker/all-in-one/Dockerfile .

If you want to configure and restart Kylin instance,
you can use `docker exec -it Kylin5-Machine bash` to login the container.
Kylin is deployed at `/home/kylin/apache-kylin-{VERSION}-bin`.

If you find some issues, please send email to Kylin's user mailing list.

For how to connect to PowerBI, here is  [a discussion in mailing list](https://lists.apache.org/thread/74pxjcx58t3m83r6o9b1hrzjjd40lhy4) .

---------
# Preview Inactive version (4.x & 3.x)

After the time of release of Kylin 5.0.0 , **Kylin 4.X and older version** will be set in **retired** status(NOT in active development),
following is the command for Kylin 4.X:

```
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 2181:2181 \
apachekylin/apache-kylin-standalone:4.0.0
```

and the command for Kylin 4.X with mondrian:

```
docker run -d \
-m 8G \
-p 7070:7070 \
-p 7080:7080 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 2181:2181 \
apachekylin/apache-kylin-standalone:kylin-4.0.1-mondrian
```

and the command for Kylin 3.X:

```sh
docker run -d \
-m 8G \
-p 7070:7070 \
-p 8088:8088 \
-p 50070:50070 \
-p 8032:8032 \
-p 8042:8042 \
-p 16010:16010 \
apachekylin/apache-kylin-standalone:3.1.0
```

----- 

## Note

If you are using mac docker desktop, please ensure that you have set Resources: Memory=8GB and Cores=6 cores at least so that can run kylin standalone on docker well.

如果是使用 mac docker desktop 的用户，请将 docker desktop 中 Resource 的内存至少设置为 8gb 以及 6 core，以保证能流畅运行 kylin standalone on docker.

-----

For user, please visit http://kylin.apache.org/docs/install/kylin_docker.html for detail.

对于中国用户，请参阅 http://kylin.apache.org/cn/docs/install/kylin_docker.html

------

JIRA : https://issues.apache.org/jira/browse/KYLIN-4114.

For any suggestion, please contact us via Kylin's mailing list: user@kylin.apache.org.
