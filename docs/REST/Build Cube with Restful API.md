
### 1.	Authentication
*   Currently, Kylin uses [basic authentication](http://en.wikipedia.org/wiki/Basic_access_authentication).
*   Add `Authorization` header to first request for authentication
*   Or you can do a specific request by `POST http://localhost:7070/kylin/api/user/authentication`
*   Once authenticated, client can go subsequent requests with cookies.
*   Example
```
    POST http://localhost:7070/kylin/api/user/authentication

    Authorization:Basic xxxxJD124xxxGFxxxSDF
    Content-Type: application/json;charset=UTF-8
```

### 2.	Get details of cube. 
*   `GET http://localhost:7070/kylin/api/cubes?cubeName={cube_name}&limit=15&offset=0`
*   Client can find cube segment date ranges in returned cube detail.
*   Example
```
    GET http://localhost:7070/kylin/api/cubes?cubeName=test_kylin_cube_with_slr&limit=15&offset=0

    Authorization:Basic xxxxJD124xxxGFxxxSDF
    Content-Type: application/json;charset=UTF-8
```

### 3.	Then submit a build job of the cube. 
*   `PUT http://localhost:7070/kylin/api/cubes/{cube_name}/rebuild`
*   For put request body detail please refer to [service doc](https://github.com/KylinOLAP/Kylin/wiki/Restful-Service-Doc). 
    *   `startTime` and `endTime` should be utc timestamp.
    *   `buildType` can be `BUILD` or `MERGE`. `BUILD` is for building a new segment or refreshing an existing segment. `MERGE` is for merging multiple existing segments into one bigger segment.
*   This method will return a newly created job instance, in which the uuid is the identity of job to track job status.
*   Example
```
    PUT http://localhost:7070/kylin/api/cubes/test_kylin_cube_with_slr/rebuild

    Authorization:Basic xxxxJD124xxxGFxxxSDF
    Content-Type: application/json;charset=UTF-8
    
    {
    	"startTime": 0,
    	"endTime": 1388563200000,
    	"buildType": "BUILD"
    }
```

### 4.	Track job status. 
*   `GET http://localhost:7070/kylin/api/jobs/{job_uuid}`
*   Returned `job_status` represents current status of job.

### 5.	If the job failed, you can resume the job. 
*   `PUT http://localhost:7070/kylin/api/jobs/{job_uuid}/resume`
