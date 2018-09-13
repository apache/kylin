---
layout: dev-cn
title:  如何发布
categories: development
permalink: /cn/development/howto_release.html
---

_本教程只适用于 Apache Kylin Committers。_  
_以在 Mac OS X 上的 Shell 命令作为样例。_  
_对于中国用户，请谨慎使用代理以避免潜在的防火墙问题。_  

## 建立账户
确保您有可使用的账号且对以下应用有权限:

* Apache 账户: [https://id.apache.org](https://id.apache.org/)    
* Apache Kylin git repo (main cobe base): [https://github.com/apache/kylin](https://github.com/apache/kylin)  
* Apache Kylin svn 仓库 (只针对网站): [https://svn.apache.org/repos/asf/kylin](https://svn.apache.org/repos/asf/kylin)  
* Apache Nexus (maven 仓库): [https://repository.apache.org](https://repository.apache.org)  
* Apache Kylin dist 仓库: [https://dist.apache.org/repos/dist/dev/kylin](https://dist.apache.org/repos/dist/dev/kylin)  

## 安装使用 Java 8 和 Maven 3.5.3+
开始之前，确保已经安装了 Java 8，以及 Maven 3.5.3 或更高版本。

## 设置 GPG 签名密钥  
按照 [http://www.apache.org/dev/release-signing](http://www.apache.org/dev/release-signing) 上的说明创建密钥对  
安装 gpg (以 Mac OS X 为例):  
`brew install gpg`

生成 gpg 密钥:  
参考: [https://www.gnupg.org/gph/en/manual/c14.html](https://www.gnupg.org/gph/en/manual/c14.html)  
_生成的所有新 RSA 密钥应至少为 4096 位。不要生成新的 DSA 密钥_  
`gpg --full-generate-key`  

验证您的密钥:  
`gpg --list-sigs YOUR_NAME`

获取密钥的指纹:
`gpg --fingerprint YOUR_NAME`

它将显示指纹，如 "Key fingerprint = XXXX XXXX ..."，然后在 [https://id.apache.org/](https://id.apache.org/) 上的"OpenPGP Public Key Primary Fingerprint"字段处将指纹添加到您的 apache 帐户；等待几个小时，密钥将添加到 [https://people.apache.org/keys/](https://people.apache.org/keys/)，例如:
[https://people.apache.org/keys/committer/lukehan.asc](https://people.apache.org/keys/committer/lukehan.asc)

生成 ASCII Amromed 键:  
`gpg -a --export YOUR_MAIL_ADDRESS > YOUR_NAME.asc &`

上传密钥到公共服务器:  
`gpg --send-keys YOUR_KEY_HASH`

或通过 web 提交密钥:  
打开并提交到 [http://pool.sks-keyservers.net:11371](http://pool.sks-keyservers.net:11371) (您可以选择任意一个有效的公钥服务器)

一旦您的密钥提交到服务器，您可以通过使用以下命令验证:  
`gpg --recv-keys YOUR_KEY_HASH`
举例:  
`gpg --recv-keys 027DC364`

按照 KEYS 文件中的说明将公钥添加到 KEYS 文件:  
_KEYS 文件位于:_ __${kylin}/KEYS__  
例如:  
`(gpg --list-sigs YOURNAME && gpg --armor --export YOURNAME) >> KEYS`

提交您的改动。

## 准备 release 的工件  
__开始前:__

* 如上所述设置签名密钥。
* 确保您使用的是 JDK 1.8。
* 确保您使用的是 GIT 2.7.2 或更高版本。
* 确保您使用的是正确的 release 版本号。
* 确保每个“resolved”的 JIRA 案例（包括重复案例）都分配了修复版本。
* 确保你在干净的目录工作

__在 Maven 中配置 Apache 存储库服务器__
如果您是第一次发布，您需要在 ~/.m2/settings.xml 中服务器授权信息；如果该文件不存在，从 $M2_HOME/conf/settings.xml 拷贝一个模板;

在“服务器”部分中，确保添加以下服务器，并将 #YOUR_APACHE_ID#, #YOUR_APACHE_PWD#, #YOUR_GPG_PASSPHRASE# 替换为您的 ID，密码和口令:
{% highlight bash %}
  <servers>
    <!-- To publish a snapshot of some part of Maven -->
    <server>
      <id>apache.snapshots.https</id>
      <username>#YOUR_APACHE_ID#</username>
      <password>#YOUR_APACHE_PWD#</password>
    </server>
    <!-- To stage a release of some part of Maven -->
    <server>
      <id>apache.releases.https</id>
      <username>#YOUR_APACHE_ID#</username>
      <password>#YOUR_APACHE_PWD#</password>
    </server>
    
    <!-- To publish a website of some part of Maven -->
    <server>
      <id>apache.website</id>
      <username>#YOUR_APACHE_ID#</username>
      <password>#YOUR_APACHE_PWD#</password>
      <!-- Either
      <privateKey>...</privateKey>
      --> 
      <filePermissions>664</filePermissions>
      <directoryPermissions>775</directoryPermissions>
    </server>
    
    <!-- To stage a website of some part of Maven -->
    <server>
      <id>stagingSite</id> 
      <!-- must match hard-coded repository identifier in site:stage-deploy -->
      <username>#YOUR_APACHE_ID#</username>
      <filePermissions>664</filePermissions>
      <directoryPermissions>775</directoryPermissions>
    </server>
    <server>
      <id>gpg.passphrase</id>
      <passphrase>#YOUR_GPG_PASSPHRASE#</passphrase>
    </server>
  </servers>
{% endhighlight %}

__修复许可证问题__
{% highlight bash %}
# Set passphrase variable without putting it into shell history
$ read -s GPG_PASSPHRASE

# Make sure that there are no junk files in the sandbox
$ git clean -xf
$ mvn clean

# Make sure all unit tests are passed
$ mvn test

# Check the `org.apache.kylin.common.KylinVersion` class, ensure the value of `CURRENT_KYLIN_VERSION` is the release version. 

# Fix any license issues as reported by target/rat.txt
$ mvn -Papache-release -DskipTests -Dgpg.passphrase=${GPG_PASSPHRASE} install
{% endhighlight %}

可选的，当 dry-run 成功了，将安装变为部署:
{% highlight bash %}
$ mvn -Papache-release -DskipTests -Dgpg.passphrase=${GPG_PASSPHRASE} deploy
{% endhighlight %}

__准备__
检查并确保你可以 ssh 连接到 github:
{% highlight bash %}
ssh -T git@github.com
{% endhighlight %}

基于要当前的开发分支，创建一个以 release 版本号命名的发布分支，例如，v2.5.0-release，并将其推到服务器端。  
{% highlight bash %}
$ git checkout -b vX.Y.Z-release
$ git push -u origin vX.Y.Z-release
{% endhighlight %}

如果任何步骤失败，请清理（见下文），解决问题，然后从头重新开始。  
{% highlight bash %}
# Set passphrase variable without putting it into shell history
$ read -s GPG_PASSPHRASE

# Make sure that there are no junk files in the sandbox
$ git clean -xf
$ mvn clean

# 可选的, do a dry run of the release:prepare step, which sets version numbers.
$ mvn -DdryRun=true -DskipTests -DreleaseVersion=X.Y.Z -DdevelopmentVersion=(X.Y.Z+1)-SNAPSHOT -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" release:prepare 2>&1 | tee /tmp/prepare-dry.log
{% endhighlight %}

__查看 dry run 输出:__

* 在 `target` 目录中应该是这 8 个文件，其中包括：
  * apache-kylin-X.Y.Z-SNAPSHOT-source-release.zip
  * apache-kylin-X.Y.Z-SNAPSHOT-source-release.zip.asc
  * apache-kylin-X.Y.Z-SNAPSHOT-source-release.zip.asc.sha256
  * apache-kylin-X.Y.Z-SNAPSHOT-source-release.zip.sha256
* 移除 .zip.asc.sha256 文件因为不需要。
* 注意文件名以 `apache-kylin-` 开始
* 在源发行版 `.zip` 文件中，检查所有文件是否属于名为 `apache-kylin-X.Y.Z-SNAPSHOT` 的目录。
* 该目录必须包含 `NOTICE`, `LICENSE`, `README.md` 文件
* 按[此](https://httpd.apache.org/dev/verification.html)检查 PGP。

__运行真实的 release:__
现在真正开始 release  
{% highlight bash %}
# Prepare sets the version numbers, creates a tag, and pushes it to git.
$ mvn -DskipTests -DreleaseVersion=X.Y.Z -DdevelopmentVersion=(X.Y.Z+1)-SNAPSHOT -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" release:prepare

# Perform checks out the tagged version, builds, and deploys to the staging repository
$ mvn -DskipTests -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" release:perform
{% endhighlight %}

__一个失败的 release 尝试后进行清理:__
{% highlight bash %}
# Make sure that the tag you are about to generate does not already
# exist (due to a failed release attempt)
$ git tag

# If the tag exists, delete it locally and remotely
$ git tag -d kylin-X.Y.Z
$ git push origin :refs/tags/kylin-X.Y.Z

# Remove modified files
$ mvn release:clean

# Check whether there are modified files and if so, go back to the
# original git commit
$ git status
$ git reset --hard HEAD
{% endhighlight %}

__关闭 Nexus 仓库中的阶段性工件:__

* 输入 [https://repository.apache.org/](https://repository.apache.org/) 并登陆
* 在 `Build Promotion` 下，点击 `Staging Repositories`
* 在 `Staging Repositories` 选项卡中，应该有一个包含配置文件 `org.apache.kylin` 的行
* 浏览工件树并确保存在 .jar，.pom，.asc 文件
* 选中行第一列中的复选框，点击 'Close' 按钮发布仓库到
  [https://repository.apache.org/content/repositories/orgapachekylin-1006](https://repository.apache.org/content/repositories/orgapachekylin-1006)
  (或相似的 URL)

__上传到临时区域:__  
通过 subversion 将工件上传到临时区域，https://dist.apache.org/repos/dist/dev/kylin/apache-kylin-X.Y.Z-rcN:
{% highlight bash %}
# Create a subversion workspace, if you haven't already
$ mkdir -p ~/dist/dev
$ pushd ~/dist/dev
$ svn co https://dist.apache.org/repos/dist/dev/kylin
$ popd

## Move the files into a directory
$ cd target
$ mkdir ~/dist/dev/kylin/apache-kylin-X.Y.Z-rcN
$ mv apache-kylin-* ~/dist/dev/kylin/apache-kylin-X.Y.Z-rcN

## Remove the .zip.asc.sha256 file as it is not needed.
$ rm ~/dist/dev/kylin/apache-kylin-X.Y.Z-rcN/apache-kylin-X.Y.Z-SNAPSHOT-source-release.zip.asc.sha256

## Check in
$ cd ~/dist/dev/kylin
$ svn add apache-kylin-X.Y.Z-rcN
$ svn commit -m 'Upload release artifacts to staging' --username <YOUR_APACHE_ID>
{% endhighlight %}

# 验证 release
{% highlight bash %}
# Check unit test
$ mvn test

# Check that the signing key (e.g. 2AD3FAE3) is pushed
$ gpg --recv-keys key

# Check keys
$ curl -O https://dist.apache.org/repos/dist/release/kylin/KEYS

# Sign/check sha256 hashes
# (Assumes your O/S has a 'shasum' command.)
function checkHash() {
  cd "$1"
  for i in *.{pom,gz}; do
    if [ ! -f $i ]; then
      continue
    fi
    if [ -f $i.sha256 ]; then
      if [ "$(cat $i.sha256)" = "$(shasum -a 256 $i)" ]; then
        echo $i.sha256 present and correct
      else
        echo $i.sha256 does not match
      fi
    else
      shasum -a 256 $i > $i.sha256
      echo $i.sha256 created
    fi
  done
};
$ checkHash apache-kylin-X.Y.Z-rcN
{% endhighlight %}

## Apache 投票过程  

__在 Apache Kylin dev 邮件列表上投票__  
在 dev 邮件列表上进行 release 投票，使用由 Maven release plugin 生成的 commit id，其消息看起来像 "[maven-release-plugin] prepare release kylin-x.x.x"：

{% highlight text %}
To: dev@kylin.apache.org
Subject: [VOTE] Release apache-kylin-X.Y.Z (RC[N])

Hi all,

I have created a build for Apache Kylin X.Y.Z, release candidate N.

Changes highlights:
...

Thanks to everyone who has contributed to this release.
Here’s release notes:
https://github.com/apache/kylin/blob/XXX/docs/release_notes.md

The commit to be voted upon:

https://github.com/apache/kylin/commit/xxx

Its hash is xxx.

The artifacts to be voted on are located here:
https://dist.apache.org/repos/dist/dev/kylin/apache-kylin-X.Y.Z-rcN/

The hash of the artifact is as follows:
apache-kylin-X.Y.Z-source-release.zip.sha256 xxx

A staged Maven repository is available for review at:
https://repository.apache.org/content/repositories/orgapachekylin-XXXX/

Release artifacts are signed with the following key:
https://people.apache.org/keys/committer/lukehan.asc

Please vote on releasing this package as Apache Kylin X.Y.Z.

The vote is open for the next 72 hours and passes if a majority of
at least three +1 PPMC votes are cast.

[ ] +1 Release this package as Apache Kylin X.Y.Z
[ ]  0 I don't feel strongly about it, but I'm okay with the release
[ ] -1 Do not release this package because...


Here is my vote:

+1 (binding)


{% endhighlight %}

投票完成后，发出结果：  
{% highlight text %}
Subject: [RESULT][VOTE] Release apache-kylin-X.Y.Z (RC[N])
To: dev@kylin.apache.org

Thanks to everyone who has tested the release candidate and given
their comments and votes.

The tally is as follows.

N binding +1s:

N non-binding +1s:

No 0s or -1s.

Therefore I am delighted to announce that the proposal to release
Apache-Kylin-X.Y.Z has passed.


{% endhighlight %}

## 发布  
成功发布投票后，我们需要推动发行到镜像，以及其它任务。

在 JIRA 中，搜索
[all issues resolved in this release](https://issues.apache.org/jira/issues/?jql=project%20%3D%20KYLIN%20),
并进行批量更新，将它们的状态更改为“关闭”，
并加上更改的评论
"Resolved in release X.Y.Z (YYYY-MM-DD)"
(填写适当的发布号和日期)。  
__取消 "Send mail for this update"。__

标记 JIRA 系统中发布的版本，[管理版本](https://issues.apache.org/jira/plugins/servlet/project-config/KYLIN/versions)。

推广分阶段的 nexus 工件。

* 转到 [https://repository.apache.org/](https://repository.apache.org/) 并登陆
* 在 "Build Promotion" 下点击 "Staging Repositories"
* 在 "orgapachekylin-xxxx" 行中，选中框
* 点击 "Release" 按钮

将工件检入 svn。
{% highlight bash %}
# Get the release candidate.
$ mkdir -p ~/dist/dev
$ cd ~/dist/dev
$ svn co https://dist.apache.org/repos/dist/dev/kylin

# Copy the artifacts. Note that the copy does not have '-rcN' suffix.
$ mkdir -p ~/dist/release
$ cd ~/dist/release
$ svn co https://dist.apache.org/repos/dist/release/kylin
$ cd kylin
$ cp -rp ../../dev/kylin/apache-kylin-X.Y.Z-rcN apache-kylin-X.Y.Z
$ svn add apache-kylin-X.Y.Z

# Check in.
svn commit -m 'checkin release artifacts'
{% endhighlight %}

Svnpubsub 将会发布到
[https://dist.apache.org/repos/dist/release/kylin](https://dist.apache.org/repos/dist/release/kylin) 并会在 24 小时内传播到
[http://www.apache.org/dyn/closer.cgi/kylin](http://www.apache.org/dyn/closer.cgi/kylin)。

如果现在有超过 2 个版本，请清除最旧的版本：

{% highlight bash %}
cd ~/dist/release/kylin
svn rm apache-kylin-X.Y.Z
svn commit -m 'Remove old release'
{% endhighlight %}

旧版本将保留在 [release archive](http://archive.apache.org/dist/kylin/).

在 JIRA 中发布相同版本，检查最新发布版本的更改日志。

## 构建和上传二进制包
发布后，您需要生成二进制包并将它们放入到 VPN 发布库中；

* 使用 `git fetch --all --prune --tags` 来同步您本地和远程的仓库。
* Git 检出当前发布的标签；
* 通过参考[此文档](howto_package.html)制作二进制包;
* 使用 gpg 对生成的二进制包进行签名，例如：
  {% highlight bash %}
  gpg --armor --output apache-kylin-2.5.0-bin.tar.gz.asc --detach-sig apache-kylin-2.5.0-bin.tar.gz
  {% endhighlight %}
* 生成二进制包的 sha256 文件，例如：
  {% highlight bash %}
  shasum -a 256 apache-kylin-2.5.0-bin.tar.gz > apache-kylin-2.5.0-bin.tar.gz.sha256
  {% endhighlight %}
* 将二进制包，签名文件和 sha256 文件推送到 svn __dev__ 仓库，然后运行 `svn mv <files-in-dev> <files-in-release>` 命令将他们移动到 svn __release__ 仓库。
* 对于不同的 Hadoop/HBase 版本，您可能需要上述步骤；
* 添加文件，然后将更改提交 svn。 


## 更新源码
发布后，您需要手动更新一些源代码：

* 更新 `KylinVersion` 类，将 `CURRENT_KYLIN_VERSION` 的值更改为当前开发版本。

## 发布网站  
更多细节参考[如何写文档](howto_docs.html)。

## 发送通知邮件到邮件列表
发送一个邮件主题如 "[Announce] Apache Kylin x.y.z released" 到以下列表：

* Apache Kylin Dev 邮箱列表: dev@kylin.apache.org
* Apache Kylin User 邮箱列表: user@kylin.apache.org
* Apache Announce 邮箱列表: announce@apache.org
  请注意始终使用您的 Apache 邮件地址发送;

这是一个公告电子邮件的样本（通过研究 Kafka):

{% highlight text %} 
The Apache Kylin team is pleased to announce the immediate availability of the 2.5.0 release. 

This is a major release after 2.4, with more than 100 bug fixes and enhancements; All of the changes in this release can be found in:
https://kylin.apache.org/docs/release_notes.html

You can download the source release and binary packages from Apache Kylin's download page: https://kylin.apache.org/download/

Apache Kylin is an open source Distributed Analytics Engine designed to provide SQL interface and multi-dimensional analysis (OLAP) on Apache Hadoop, supporting extremely large datasets.

Apache Kylin lets you query massive data set at sub-second latency in 3 steps:
1. Identify a star schema or snowflake schema data set on Hadoop.
2. Build Cube on Hadoop.
3. Query data with ANSI-SQL and get results in sub-second, via ODBC, JDBC or RESTful API.

Thanks everyone who have contributed to the 2.1.0 release.

We welcome your help and feedback. For more information on how to
report problems, and to get involved, visit the project website at
https://kylin.apache.org/

{% endhighlight %}

# 感谢  
本指南起草于 [Apache Calcite](http://calcite.apache.org) Howto doc 的参考资料，非常感谢。

