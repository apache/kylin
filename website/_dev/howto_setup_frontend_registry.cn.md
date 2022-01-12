---
layout: dev
title:  如何设置Kylin的前端仓库
categories: development
permalink: /cn/development/howto_setup_frontend_registry.html
---

如果安装Kylin的前端依赖时, 因网络问题或者包在默认仓库内无法获取导致安装失败, 可以配置仓库为[http://150.158.20.97:8081/#browse/browse](http://150.158.20.97:8081/#browse/browse). 以下为配置bower和npm仓库的方法。

#### 设置bower仓库
- 确认包'bower-nexus3-resolver'已安装, 若未装, 使用命令`npm install -g bower-nexus3-resolver`安装.
- 修改路径为`$KYLIN_SOURCE/webapp/.bowerrc`的配置文件
    ```
    {
        "directory":"app/components",
        "registry":{
            "search":[
                "http://150.158.20.97:8081/repository/group-bower"
            ]
        },
        "resolvers":[
            "bower-nexus3-resolver"
        ],
        "timeout":60000
    }
    ```

#### 设置npm仓库
执行以下命令
```
npm config set registry http://150.158.20.97:8081/repository/group-npm/
```