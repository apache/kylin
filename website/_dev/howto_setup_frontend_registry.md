---
layout: dev
title:  How to Set up Frontend Registry
categories: development
permalink: /development/howto_setup_frontend_registry.html
---

If errors occur during installing Kylin's frontend dependencies due to network latency or some packages not obtainable by default registry, bower and npm repositories for Kylin are available at [http://150.158.20.97:8081/#browse/browse](http://150.158.20.97:8081/#browse/browse). Below are some guides about how to set bower and npm repositories.

#### Set up bower repository
- Make sure package 'bower-nexus3-resolver' has been stalled on your machine, if not, install it by running `npm install -g bower-nexus3-resolver`.
- Alter the file at `$KYLIN_SOURCE/webapp/.bowerrc` to
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

#### Set up npm repository
Run command below
```
npm config set registry http://150.158.20.97:8081/repository/group-npm/
```