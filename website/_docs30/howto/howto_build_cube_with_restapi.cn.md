---
layout: docs30-cn
title:  用 API 构建 Cube
categories: 帮助
permalink: /cn/docs30/howto/howto_build_cube_with_restapi.html
---

### 1. 认证
*   目前Kylin使用[basic authentication](http://en.wikipedia.org/wiki/Basic_access_authentication)。
*   给第一个请求加上用于认证的 Authorization 头部。
*   或者进行一个特定的请求: POST http://localhost:7070/kylin/api/user/authentication 。
*   完成认证后, 客户端可以在接下来的请求里带上cookie。
{% highlight Groff markup %}
POST http://localhost:7070/kylin/api/user/authentication

Authorization:Basic xxxxJD124xxxGFxxxSDF
Content-Type: application/json;charset=UTF-8
{% endhighlight %}

### 2. 获取Cube的详细信息
*   `GET http://localhost:7070/kylin/api/cubes?cubeName={cube_name}&limit=15&offset=0`
*   用户可以在返回的cube详细信息里找到cube的segment日期范围。
{% highlight Groff markup %}
GET http://localhost:7070/kylin/api/cubes?cubeName=test_kylin_cube_with_slr&limit=15&offset=0

Authorization:Basic xxxxJD124xxxGFxxxSDF
Content-Type: application/json;charset=UTF-8
{% endhighlight %}

### 3.	然后提交cube构建任务
*   `PUT http://localhost:7070/kylin/api/cubes/{cube_name}/rebuild`
*   关于 put 的请求体细节请参考 Build Cube API
    *   `startTime` 和 `endTime` 应该是utc时间。
    *   `buildType` 可以是 `BUILD` 、 `MERGE` 或 `REFRESH`。 `BUILD` 用于构建一个新的segment， `REFRESH` 用于刷新一个已有的segment， `MERGE` 用于合并多个已有的segment生成一个较大的segment。
*   这个方法会返回一个新建的任务实例，它的uuid是任务的唯一id，用于追踪任务状态。
{% highlight Groff markup %}
PUT http://localhost:7070/kylin/api/cubes/test_kylin_cube_with_slr/rebuild

Authorization:Basic xxxxJD124xxxGFxxxSDF
Content-Type: application/json;charset=UTF-8
    
{
    "startTime": 0,
    "endTime": 1388563200000,
    "buildType": "BUILD"
}
{% endhighlight %}

### 4.	跟踪任务状态 
*   `GET http://localhost:7070/kylin/api/jobs/{job_uuid}`
*   返回的 `job_status` 代表job的当前状态。

### 5.	如果构建任务出现错误，可以重新开始它
*   `PUT http://localhost:7070/kylin/api/jobs/{job_uuid}/resume`
