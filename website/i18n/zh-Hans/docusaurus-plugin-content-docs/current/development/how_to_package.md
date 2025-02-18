---
title: 如何打包 Kylin
language: zh-Hans
sidebar_label: 如何打包 Kylin
pagination_label: 如何打包 Kylin
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/how_to_test
pagination_next: development/how_to_subscribe_mailing_list
keywords:
  - package
draft: false
last_update:
  date: 08/22/2022
  author: Xiaoxiang Yu, Jiang Longfei
---

# 如何打包 Kylin

### <span id="software_reqiurement">软件环境要求</span>

| 软件           | 描述                 | 版本                                       | 下载连接                                                                                     |
|--------------|--------------------|------------------------------------------|------------------------------------------------------------------------------------------|
| Git          | 获取最新提交的名称和哈希值      | 2.30.1 or later                          | latest                                                                                   | https://git-scm.com/book/en/v2/Getting-Started-Installing-Git                            |
| Apache Maven | 编译 JAVA 和 Scala 源码 | 3.8.2 or latest                          | https://maven.apache.org/download.cgi                                                    |
| Node.js      | 编译前端源码             | 16.20.2 is recommended ( or 16.x ~ 18.x) | [如何安装其他版本 node.js](development/how_to_package.md#install_other_node)                     |
| JDK          | JAVA 开发环境          | JDK 1.8.x                                | https://www.oracle.com/java/technologies/javase/javase8u211-later-archive-downloads.html |

安装上述软件后，请通过以下命令验证 **软件环境要求**：

```shell
$ java -version
java version "1.8.0_301"
Java(TM) SE Runtime Environment (build 1.8.0_301-b09)
Java HotSpot(TM) 64-Bit Server VM (build 25.301-b09, mixed mode)

$ mvn -v
Apache Maven 3.8.2 (ea98e05a04480131370aa0c110b8c54cf726c06f)
Maven home: /Users/xiaoxiang.yu/LacusDir/lib/apache-maven-3.8.2
Java version: 1.8.0_301, vendor: Oracle Corporation, runtime: /Library/Java/JavaVirtualMachines/jdk1.8.0_301.jdk/Contents/Home/jre
Default locale: en_CN, platform encoding: UTF-8
OS name: "mac os x", version: "10.16", arch: "x86_64", family: "mac"

$ node -v
v16.20.2

$ git version
git version 2.30.1 (Apple Git-130)
```

### 打包脚本的选项

| 选项            | 描述                                                      |
|---------------|---------------------------------------------------------|
| -official     | 如果添加此选项，包名称将不包含时间戳                                      |
| -noThirdParty | 如果添加此选项，第三方包不会被打包，目前它们是 inflxDB、grafana 和 PostgreSQL    |
| -noSpark      | 如果添加此选项，spark 不会被打包进 Kylin 5.0 安装包，需要手动安装 spark         |                                                                         |
| -noHive1      | 默认情况下 Kylin 5.0 将支持 Hive 1.2，如果添加此选项，该二进制文件将支持Hive 2.3+ |
| -skipFront    | 如果添加此选项，前端源码不会被构建和打包                                    |
| -skipCompile  | 如果添加此选项，Java源代码将不会编译                                    |

### 其他打包选项

| 选项         | 描述                                  | 
|------------|-------------------------------------|
| -P hadoop3 | 打包 Kylin 5.0 软件包以在 Hadoop 3.0+ 上运行。 |

### 包内容

| 选项          | 描述                                | 
|-------------|-----------------------------------|
| VERSION     | `Apache Kylin ${release_version}` |
| commit_SHA1 | `${HASH_COMMIT}@${BRANCH_NAME}`   |

### 包名称

包名为 `apache-kylin-${release_version}.tar.gz`，其中 `${release_version}` 默认是 `{project.version}.YYYYmmDDHHMMSS` 。
例如，一个非官方包可能是 `apache-kylin-5.0.0-SNAPSHOT.20220812161045.tar.gz` ，而一个官方包可能是 `apache-kylin-5.0.0.tar.gz`

### 开发人员和发布的示例

```shell

## Case 1: For the developer who wants to package for testing purposes
./build/release/release.sh 

## Case 2: Official apache release,  Kylin binary for deployment on Hadoop3+ and Hive2.3+, 
# and the third party cannot be distributed because of apache distribution policy(size and license)
./build/release/release.sh -noSpark -official 

## Case 3: A package for Apache Hadoop 3 platform
./build/release/release.sh -P hadoop3
```

### <span id="install_other_node">如何安装特殊版本 node.js</span>

1. 请访问 https://nodejs.org/en/download/ 下载和安装最新版 node.js。
安装后，您可以使用以下命令用于验证正在使用的 node.js 的版本

```shell
$ node -v
v20.18.0
```

2. 使用类似 https://github.com/nvm-sh/nvm 安装和使用特殊版本的 node.js

```shell
## Switch to specific version using nvm
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
nvm install 16.20.2

## Before packaging, please switch to a specific version
nvm use nvm install 16.20.2
```

您可以使用如下命令, 检测正在使用的 node.js 版本:

```shell
$ node -v
v16.20.2
```

3. 切换到最新的 node.js

```shell
## switch to the original version
nvm use system
```
