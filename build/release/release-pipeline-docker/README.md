## Background

The docker image is used to provide an **easy and standard way** 
for [release manager](https://infra.apache.org/release-publishing.html#releasemanager) 
to complete [apache release process](https://www.apache.org/legal/release-policy.html) 
and obey [apache release policy](https://www.apache.org/legal/release-policy.html).

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

### Step 1 : Configure Basic Info and Copy GPG Private Key

-  Start docker container

```bash
docker run --name release-machine --hostname release-machine -i -t apachekylin/release-machine:latest  bash
# docker ps -f name=release-machine
```

- Copy GPG Private Key from your laptop into container

```bash
docker cp ~/XXX.private.key release-machine:/root
```

### Step 2 : Configure setenv.sh

- Set correct values for all variables in `/root/scripts/setenv.sh`, such as **ASF_PASSWORD** and **GPG_PASSPHRASE**.

#### Variables in setenv.sh

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

### Step 3 : Install GPG Private Key

```bash
gpg --import XXX.private.key
```

```bash
gpg --list-sigs {NAME of Your Key}
```

### Step 4 : Publish Release Candidate

```bash
export RELEASE_STEP=publish-rc
bash release-publish.sh
```

### Step 5 : Vote for Release Candidate

- Prepare vote template for voting

### Step 6 : Publish Release Candidate

```bash
export RELEASE_STEP=publish
bash release-publish.sh
```

- Prepare vote template for announcement
- Close maven repository in Web UI

### Step 7 : Remove Docker container

```bash
docker rm release-machine
```

## Publish Official Documentation