## Background

The docker image is used to provide an **easy and standard way** 
for [release manager](https://infra.apache.org/release-publishing.html#releasemanager) 
to complete [apache release process](https://www.apache.org/legal/release-policy.html) 
and obey [apache release policy](https://www.apache.org/legal/release-policy.html).

For maven artifacts, please check [publishing-maven-artifacts](https://infra.apache.org/publishing-maven-artifacts.html).

Some source code are modified from [apache spark release](https://github.com/apache/spark/tree/master/dev/create-release) scripts.
Kylin project use [maven-release-plugin](https://maven.apache.org/maven-release/maven-release-plugin/) to release source code and maven artifacts

It also provided a way to publish documentation for Kylin 5.

## How to release

### What you need to prepare

| Item                                                                    | Used for                                                                                            | Reference                                                                                             |
|-------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| Apache Account<br/>(Should belongs to Kylin PMC) <br> (id and password) | 1. Write access to ASF's Gitbox service and SVN service <br> 2. Send email for release announcement | https://id.apache.org                                                                                 |
| GPG Key <br> (key files and GPG_PASSPHRASE)                             | Sign and distribute your released files(binary and compressed source files)                         | https://infra.apache.org/release-signing.html <br> https://infra.apache.org/release-distribution.html |
| user and password for nexus server                                      | upload artifacts to ASF's nexus server                                                              |                                                                                                       |
| Laptop which installed Docker                                           | The place you run release scripts, should install Docker Engine                                     | N/A                                                                                                   |


-[ ] Update `CURRENT_KYLIN_VERSION` in `KylinVersion.java` .

```bash
docker start release-machine-1 \
    -p 4040:4040 \
    bash 
```

| Name            | Comment                                                                                                                                                                          |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ASF_USERNAME    | ID of Apache Account                                                                                                                                                             |
| ASF_PASSWORD    | (**Never leak this**)Password of Apache Account                                                                                                                                  |
| GPG_PASSPHRASE  | (**Never leak this**)PASSPHRASE of GPG Key                                                                                                                                       |
| GIT_BRANCH      | Branch which you used to release, default is **kylin5**                                                                                                                          |
| RELEASE_VERSION | Which version you want to release, default is **kylin5.0.0-alpha**                                                                                                               |
| NEXT_VERSION    | Next version you want to use after released, default is **kylin5.0.0-beta**                                                                                                      |
| RELEASE_STEP    | (default is **publish-rc**)<br/>Legal values are <br/> publish-rc : upload binary to release candidate folder <br> publish : publish release binary officially after vote passed |


#### Set user and password in **servers** of `~/.m2/settings.xml`

Otherwise, you will fail in maven-deploy-plugin with http 401 error.

### Step 4 : Publish Release Candidate

```bash
bash release-publish.sh publish-snapshot
```

### Step 5 : Vote for Release Candidate

- Prepare vote template for voting

### Step 6 : Publish Release Candidate

```bash
bash release-publish.sh publish-release
```

- Prepare vote template for announcement
- Close maven repository in Web UI

### Step 7 : Remove Docker container

```bash
docker rm release-machine
```

## Publish Official Documentation