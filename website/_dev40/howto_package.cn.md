---
layout: dev40-cn
title:  如何打二进制包
categories: development
permalink: /cn/development40/howto_package.html
---

### 生成二进制包
本文档讲述的是如何从源码构建 Kylin4 的二进制包

#### 下载源码
您可以从 github 仓库下载 Apache Kylin 源码。

```
git clone https://github.com/apache/kylin kylin
```

#### 构建二进制包

为了生成二进制包，需要预先准备好 **maven** 和 **npm**。

**(可选)** 如果您在代理服务器后面，在运行 ./script/package.sh 之前，需要将代理信息告知 npm 和 bower：

```
export http_proxy=http://your-proxy-host:port
npm config set proxy http://your-proxy-host:port
```

##### 运行打包命令
```
cd kylin
build/script/package.sh
```
