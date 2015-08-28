---
layout: docs
title:  "Advance Settings of Kylin Environment"
categories: install
permalink: /docs/install/advance_settings.html
version: v0.7.2
since: v0.7.1
---

## Enabling LZO compression

LZO compression can be leveraged to compress the output of MR jobs, as well as hbase table storage, reducing the storage overhead. By default we do not enable LZO compression in Kylin because hadoop sandbox venders tend to not include LZO in their distributions due to license(GPL) issues.

To enable LZO in Kylin, follow these steps:

### Make sure LZO is working in your environment

We have a simple tool to test whether LZO is well installed on EVERY SERVER in hbase cluster (http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.1.2/bk_installing_manually_book/content/rpm-chap2-3.html), and restart the cluster.
To test it on the hadoop CLI that you deployed Kylin, Just run

{% highlight Groff markup %}
hbase org.apache.hadoop.hbase.util.CompressionTest file:///PATH-TO-A-LOCAL-TMP-FILE lzo
{% endhighlight %}

If no exception is printed, you're good to go. Otherwise you'll need to first install LZO properly on this server.
To test if the hbase cluster is ready to create LZO compressed tables, test following hbase command:

{% highlight Groff markup %}
create 'lzoTable', {NAME => 'colFam',COMPRESSION => 'LZO'}
{% endhighlight %}

### Modify kylin_job_conf.xml

You'll need to stop Kylin first by running `./kylin.sh stop`, and then modify $KYLIN_HOME/conf/kylin_job_conf.xml by uncommenting some configuration entries related to LZO compression. 
After this, you need to run `./kylin.sh start` to start Kylin again. Now Kylin will use LZO to compress MR outputs and hbase tables.


## Enable LDAP authentication

Kylin supports LDAP authentication for enterprise or production deployment; This is implemented based on Spring Security framework; Before enable LDAP, please contact your LDAP administrator to get necessary information, like LDAP server URL, username/password, search patterns, etc;

## Configure LDAP properties conf/kylin.properties

Firstly, provide your LDAP URL, and username/password if the LDAP server is secured;

```
ldap.server=ldap://<your_ldap_host>:<port>
ldap.username=<your_user_name>
ldap.password=<your_password>
```

Secondly, provide the user search patterns, this is by your LDAP design, here is just a sample:


```
ldap.user.searchBase=OU=UserAccounts,DC=mycompany,DC=com
ldap.user.searchPattern=(&(AccountName={0})(memberOf=CN=MYCOMPANY-USERS,DC=mycompany,DC=com))
ldap.user.groupSearchBase=OU=Group,DC=mycompany,DC=com
```

If you have service accounts (e.g, for system integration) which also need be authenticated, you can configure them in ldap.service.*; If not, leave them be empty; 


### Enable LDAP mode

Set "kylin.sandbox=true" in conf/kylin.properties, then restart Kylin server; In the Login page, use a LDAP account name/password to login.