---
layout: dev
title:  How to Making a Release
categories: development
permalink: /development/howto_release.html
---

_This guide is for Apache Kylin Committers only._  
_Shell commands is on Mac OS X as sample._  
_For people in China, please aware using proxy to avoid potential firewall issue._  

## Setup Account
Make sure you have avaliable account and privilege for following applications:

* Apache account: [https://id.apache.org](https://id.apache.org/)    
* Apache Kylin git repo (main cobe base): [https://git-wip-us.apache.org/repos/asf/kylin.git](https://git-wip-us.apache.org/repos/asf/kylin.git)  
* Apache Kylin svn repo (for website only): [https://svn.apache.org/repos/asf/kylin](https://svn.apache.org/repos/asf/kylin)  
* Apache Nexus (maven repo): [https://repository.apache.org](https://repository.apache.org)  
* Apache Kylin dist repo: [https://dist.apache.org/repos/dist/dev/kylin](https://dist.apache.org/repos/dist/dev/kylin)  

## Setup PGP signing keys  
Follow instructions at [http://www.apache.org/dev/release-signing](http://www.apache.org/dev/release-signing) to create a key pair  
Install gpg (On Mac OS X as sample):  
`brew install gpg and gpg --gen-key`

Generate gpg key:  
Reference: [https://www.gnupg.org/gph/en/manual/c14.html](https://www.gnupg.org/gph/en/manual/c14.html)  
_All new RSA keys generated should be at least 4096 bits. Do not generate new DSA keys_  
`gpg --gen-key`  

Verify your key:  
`gpg --list-sigs YOUR_NAME`

Get the fingerprint of your key:
`gpg --fingerprint YOUR_NAME`

It will display the fingerprint like "Key fingerprint = XXXX XXXX ...", then add the fingerprint to your apache account at [https://id.apache.org/](https://id.apache.org/) in "OpenPGP Public Key Primary Fingerprint" field; wait for a few hours the key will added to [https://people.apache.org/keys/](https://people.apache.org/keys/), for example:
[https://people.apache.org/keys/committer/lukehan.asc](https://people.apache.org/keys/committer/lukehan.asc)

Generate ASCII Amromed Key:  
`gpg -a --export YOUR_MAIL_ADDRESS > YOUR_NAME.asc &`

Upload key to public server:  
`gpg --send-keys YOUR_KEY_HASH`

or Submit key via web:  
Open and Submit to [http://pool.sks-keyservers.net:11371](http://pool.sks-keyservers.net:11371) (you can pickup any available public key server)

Once your key submitted to server, you can verify using following command:  
`gpg --recv-keys YOUR_KEY_HASH`
for example:  
`gpg --recv-keys 027DC364`

Add your public key to the KEYS file by following instructions in the KEYS file.:  
_KEYS file location:_ __${kylin}/KEYS__  
For example:  
`(gpg --list-sigs YOURNAME && gpg --armor --export YOURNAME) >> KEYS`

Commit your changes.

## Prepare artifacts for release  
__Before you start:__

* Set up signing keys as described above.
* Make sure you are using JDK 1.7 (not 1.8).
* Make sure you are working on right release version number.
* Make sure that every “resolved” JIRA case (including duplicates) has a fix version assigned.

__Configure Apache repository server in Maven__
If you're the first time to do release, you need update the server authentication information in ~/.m2/settings.xml; If this file doesn't exist, copy a template from $M2_HOME/conf/settings.xml;

In the "servers" section, make sure the following servers be added, and replace #YOUR_APACHE_ID#, #YOUR_APACHE_PWD#, #YOUR_GPG_PASSPHRASE# with your ID, password, and passphrase:
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

__Fix license issues and make a snapshot__
{% highlight bash %}
# Set passphrase variable without putting it into shell history
$ read -s GPG_PASSPHRASE

# Make sure that there are no junk files in the sandbox
$ git clean -xn
$ mvn clean

# Fix any license issues as prompted
$ mvn -Papache-release -Dgpg.passphrase=${GPG_PASSPHRASE} install
{% endhighlight %}

Optionally, when the dry-run has succeeded, change install to deploy:
{% highlight bash %}
$ mvn -Papache-release -Dgpg.passphrase=${GPG_PASSPHRASE} deploy
{% endhighlight %}

__Making a release__

Create a release branch named after the release, e.g. v0.7.2-release, and push it to Apache.  
{% highlight bash %}
$ git checkout -b vX.Y.Z-release
$ git push -u origin vX.Y.Z-release
{% endhighlight %}
We will use the branch for the entire the release process. Meanwhile, we do not allow commits to the master branch. After the release is final, we can use `git merge --ff-only` to append the changes on the release branch onto the master branch. (Apache does not allow reverts to the master branch, which makes it difficult to clean up the kind of messy commits that inevitably happen while you are trying to finalize a release.)

Now, set up your environment and do a dry run. The dry run will not commit any changes back to git and gives you the opportunity to verify that the release process will complete as expected.

If any of the steps fail, clean up (see below), fix the problem, and start again from the top.  
{% highlight bash %}
# Set passphrase variable without putting it into shell history
$ read -s GPG_PASSPHRASE

# Make sure that there are no junk files in the sandbox
$ git clean -xn
$ mvn clean

# Do a dry run of the release:prepare step, which sets version numbers.
$ mvn -DdryRun=true -DskipTests -DreleaseVersion=X.Y.Z -DdevelopmentVersion=(X.Y.Z+1)-SNAPSHOT -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE}" release:prepare 2>&1 | tee /tmp/prepare-dry.log
{% endhighlight %}

__Check the artifacts:__

* In the `target` directory should be these 8 files, among others:
  * apache-kylin-X.Y.Z-SNAPSHOT-src.tar.gz
  * apache-kylin-X.Y.Z-SNAPSHOT-src.tar.gz.asc
  * apache-kylin-X.Y.Z-SNAPSHOT-src.tar.gz.md5
  * apache-kylin-X.Y.Z-SNAPSHOT-src.tar.gz.sha1
  * apache-kylin-X.Y.Z-SNAPSHOT-src.zip
  * apache-kylin-X.Y.Z-SNAPSHOT-src.zip.asc
  * apache-kylin-X.Y.Z-SNAPSHOT-src.zip.md5
  * apache-kylin-X.Y.Z-SNAPSHOT-src.zip.sha1
* Remove the .zip, .zip.asc, .zip.md5 and zip.sha1 file as they are not needed.
* Note that the file names start `apache-kylin-`.
* In the source distro `.tar.gz`, check that all files belong to a directory called
  `apache-kylin-X.Y.Z-src`.
* That directory must contain files `NOTICE`, `LICENSE`, `README.md`
* Check PGP, per [this](https://httpd.apache.org/dev/verification.html)

__Run real release:__
Now, run the release for real.  
{% highlight bash %}
# Prepare sets the version numbers, creates a tag, and pushes it to git.
$ mvn -DskipTests -DreleaseVersion=X.Y.Z -DdevelopmentVersion=(X.Y.Z+1)-SNAPSHOT -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE}" release:prepare

# Perform checks out the tagged version, builds, and deploys to the staging repository
$ mvn -DskipTests -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests" release:perform
{% endhighlight %}

__Close the staged artifacts in the Nexus repository:__

* Go to [https://repository.apache.org/](https://repository.apache.org/) and login
* Under `Build Promotion`, click `Staging Repositories`
* In the `Staging Repositories` tab there should be a line with profile `org.apache.kylin`
* Navigate through the artifact tree and make sure the .jar, .pom, .asc files are present
* Check the box on in the first column of the row, and press the 'Close' button to publish the repository at
  [https://repository.apache.org/content/repositories/orgapachekylin-1006](https://repository.apache.org/content/repositories/orgapachekylin-1006)
  (or a similar URL)

__Upload to staging area:__  
Upload the artifacts via subversion to a staging area, https://dist.apache.org/repos/dist/dev/kylin/apache-kylin-X.Y.Z-rcN:
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

## Check in
$ cd ~/dist/dev/kylin
$ svn add apache-kylin-X.Y.Z-rcN
$ svn commit -m 'Upload release artifacts to staging' --username <YOUR_APACHE_ID>
{% endhighlight %}

__Cleaning up after a failed release attempt:__
{% highlight bash %}
# Make sure that the tag you are about to generate does not already
# exist (due to a failed release attempt)
$ git tag

# If the tag exists, delete it locally and remotely
$ git tag -d apache-kylin-X.Y.Z
$ git push origin :refs/tags/apache-kylin-X.Y.Z

# Remove modified files
$ mvn release:clean

# Check whether there are modified files and if so, go back to the
# original git commit
$ git status
$ git reset --hard HEAD
{% endhighlight %}

# Validate a release
{% highlight bash %}
# Check unit test
$ mvn test

# Check that the signing key (e.g. 2AD3FAE3) is pushed
$ gpg --recv-keys key

# Check keys
$ curl -O https://dist.apache.org/repos/dist/release/kylin/KEYS

## Sign/check md5 and sha1 hashes
 _(Assumes your O/S has 'md5' and 'sha1' commands.)_
function checkHash() {
  cd "$1"
  for i in *.{zip,gz}; do
    if [ ! -f $i ]; then
      continue
    fi
    if [ -f $i.md5 ]; then
      if [ "$(cat $i.md5)" = "$(md5 -q $i)" ]; then
        echo $i.md5 present and correct
      else
        echo $i.md5 does not match
      fi
    else
      md5 -q $i > $i.md5
      echo $i.md5 created
    fi
    if [ -f $i.sha1 ]; then
      if [ "$(cat $i.sha1)" = "$(sha1 -q $i)" ]; then
        echo $i.sha1 present and correct
      else
        echo $i.sha1 does not match
      fi
    else
      sha1 -q $i > $i.sha1
      echo $i.sha1 created
    fi
  done
}
$ checkHash apache-kylin-X.Y.Z-rcN
{% endhighlight %}

## Apache voting process  

__Vote on Apache Kylin dev mailing list__  
Release vote on dev list, use the commit id that generated by Maven release plugin, whose message looks like "[maven-release-plugin] prepare release kylin-x.x.x":  

{% highlight text %}
To: dev@kylin.apache.org
Subject: [VOTE] Release apache-kylin-X.Y.Z (release candidate N)

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

The hashes of the artifacts are as follows:
src.zip.md5 xxx
src.zip.sha1 xxx
src.tar.gz.md5 xxx
src.tar.gz.sha1 xxx

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

Luke

{% endhighlight %}

After vote finishes, send out the result:  
{% highlight text %}
Subject: [RESULT] [VOTE] Release apache-kylin-X.Y.Z (release candidate N)
To: dev@kylin.apache.org

Thanks to everyone who has tested the release candidate and given
their comments and votes.

The tally is as follows.

N binding +1s:

N non-binding +1s:

No 0s or -1s.

Therefore I am delighted to announce that the proposal to release
Apache-Kylin-X.Y.Z has passed.

I'll now start a vote on the general list. Those of you in the IPMC,
please recast your vote on the new thread.

Luke

{% endhighlight %}

## Publishing a release  
After a successful release vote, we need to push the release
out to mirrors, and other tasks.

In JIRA, search for
[all issues resolved in this release](https://issues.apache.org/jira/issues/?jql=project%20%3D%20KYLIN%20),
and do a bulk update changing their status to "Closed",
with a change comment
"Resolved in release X.Y.Z (YYYY-MM-DD)"
(fill in release number and date appropriately).  
__Uncheck "Send mail for this update".__

Promote the staged nexus artifacts.

* Go to [https://repository.apache.org/](https://repository.apache.org/) and login
* Under "Build Promotion" click "Staging Repositories"
* In the line with "orgapachekylin-xxxx", check the box
* Press "Release" button

Check the artifacts into svn.
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

Svnpubsub will publish to
[https://dist.apache.org/repos/dist/release/kylin](https://dist.apache.org/repos/dist/release/kylin) and propagate to
[http://www.apache.org/dyn/closer.cgi/kylin](http://www.apache.org/dyn/closer.cgi/kylin) within 24 hours.

If there are now more than 2 releases, clear out the oldest ones:

{% highlight bash %}
cd ~/dist/release/kylin
svn rm apache-kylin-X.Y.Z
svn commit -m 'Remove old release'
{% endhighlight %}

The old releases will remain available in the
[release archive](http://archive.apache.org/dist/kylin/).

Release same version in JIRA, check [Change Log](https://issues.apache.org/jira/browse/KYLIN/?selectedTab=com.atlassian.jira.jira-projects-plugin:changelog-panel) for the latest released version.

## Publishing the web site  
Refer to [How to document](howto_docs.html) for more detail.

## Send announcement mail to mailing list
Send one mail with subject like "[Announce] Apache Kylin x.y released" to following list:
* Apache Kylin Dev mailing list: dev@kylin.apache.org
* Apache Kylin User mailing list: user@kylin.apache.org
* Apache Announce mailing list: announce@apache.org
Please notice to always use your Apache mail address to send this

# Thanks  
This guide drafted with reference from [Apache Calcite](http://calcite.apache.org) Howto doc, Thank you very much.

