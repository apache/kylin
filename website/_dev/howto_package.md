---
layout: dev
title:  How to Build Binary Package
categories: development
permalink: /development/howto_package.html
---

### Generate Binary Package
This document talks about how to build binary package from source code.

#### Download source code
You can download Apache Kylin source code from github repository.

```
git clone https://github.com/apache/kylin kylin
```

#### Build Binary Package

In order to generate binary package, **maven** and **npm** are pre-requisites.

**(Optional)** If you're behind a proxy server, both npm and bower need be told with the proxy info before running ./script/package.sh:

```
export http_proxy=http://your-proxy-host:port
npm config set proxy http://your-proxy-host:port
```

##### Build Package for HBase 1.x
```
cd kylin
build/script/package.sh
```

##### Build Package for CDH 5.7
```
cd kylin
build/script/package.sh -P cdh5.7
```
