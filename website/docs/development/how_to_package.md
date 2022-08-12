---
sidebar_position: 1
---

# How to package

### Environment Requirement

| Software      | Comment              |
|---------------| ---------------------|
| Git           |  Fetch branch name and hash of latest commit      | 
| Apache Maven  |  Build Java and Scala source code   |
| Node.js       |  Build front end   |


### Options for Packaging Script

|         Option       |     Comment    | 
|--------------------  | ---------------|
| -official            | Should package name contains timestamp, default no | 
| -noThirdParty        | Should third party binary be packaging into binary, current they are influxdb,grafana and postgresql, default no |
| -noSpark             | Should spark be packaging into Kylin binary, default yes |
| -noHive1             | Should this binary support Hive 1.2, by default kylin will support Hive 2.3+ |
| -skipFront           | Should build front end, default yes |
| -skipCompile         | Should build back end, default yes |

### Package Content

|         Option       |     Comment    | 
|--------------------  | ---------------|
| VERSION              | `Apache Kylin ${release_version}`  |
| commit_SHA1          |  `${HASH_COMMIT}@${BRANCH_NAME}` |

### Package Name convention

Package name is `apache-kylin-${release_version}.tar.gz`, while `${release_version}` is `{project.version}.YYYYmmDDHHMMSS` by default.
For example, an unofficial package could be `apache-kylin-5.0.0-SNAPSHOT.20220812161045.tar.gz` while an official package could be `apache-kylin-5.0.0.tar.gz`

### Example for developer and release manager

```shell

## Case 1: For developer who want to package for testing purpose
cd build/release
./release.sh

## Case 2: Official apache release,  kylin binary for deploy on Hadoop3+ and Hive2.3+, 
# and third party cannot be distributed because of apache distribution policy(size and license)
cd build/release
./release.sh -noThirdParty -noSpark -official 
```