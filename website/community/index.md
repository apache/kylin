---
layout: default
title: Community
permalink: /community/index.html
---

### Powered By Apache Kylin™
For information about who are using Apache Kylin™, please refer to [Powered By](/community/poweredby.html) page.


### Apache Kylin Mailing List

These are the mailing lists that have been established for this project. For each list, there is a subscribe, unsubscribe, and an archive link.

| Name  | Subscribe | Unsubscribe | Post | Archive |
|------ |-----------|-------------|------|---------|
| User Mailing List | [Subscribe](mailto:user-subscribe@kylin.apache.org) | [Unsubscribe](mailto:user-unsubscribe@kylin.apache.org) | [Post](mailto:user@kylin.apache.org) | [mail-archives.apache.org](http://mail-archives.apache.org/mod_mbox/kylin-user/) |
| Developers Mailing List | [Subscribe](mailto:dev-subscribe@kylin.apache.org) | [Unsubscribe](mailto:dev-unsubscribe@kylin.apache.org) | [Post](mailto:dev@kylin.apache.org) | [mail-archives.apache.org](http://mail-archives.apache.org/mod_mbox/kylin-dev/) |
| Issues Mailing List | [Subscribe](mailto:issues-subscribe@kylin.apache.org) | [Unsubscribe](mailto:issues-unsubscribe@kylin.apache.org) | N/A | [mail-archives.apache.org](http://mail-archives.apache.org/mod_mbox/kylin-issues/) |
| Commits Mailing List | [Subscribe](mailto:commits-subscribe@kylin.apache.org) | [Unsubscribe](mailto:commits-unsubscribe@kylin.apache.org) | N/A | [mail-archives.apache.org](http://mail-archives.apache.org/mod_mbox/kylin-commits/) |

### Mailing List Archives
For convenience, there's a forum style mailing list archives which not part of offical Apache archives:

* [Developer List archive on Nabble](http://apache-kylin.74782.x6.nabble.com)

### Social Media 
The official Kylin Twitter account: [@ApacheKylin](https://twitter.com/ApacheKylin)

## Apache Kylin Team
A successful project requires many people to play many roles. Some members write code, provide project mentorship, or author documentation. Others are valuable as testers, submitting patches and suggestions.

### PMC Members

| Name  | Apache ID    | Github    |  Role |
|:----- |:-------------|:----------|:------|
{% for c in site.data.contributors %}  | {{ c.name }} | <a href="http://home.apache.org/phonebook.html?uid={{ c.apacheId }}">{{ c.apacheId }}</a> | <a href="http://github.com/{{ c.githubId }}"><img width="48" src="{% unless c.avatar %}http://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> |  {{ c.role }} |
{% endfor %}

### Contributors
Contributors has commited code could be found [here](https://github.com/apache/kylin/graphs/contributors).
__Other contributors__

| Name  | Github    |   |
|:----- |:----------|:------|
|Rui Feng | [fengrui129](https://github.com/fengrui129) | Website Design, Kylin Logo|
|Luffy Xiao | [luffy-xiao](http://github.com/luffy-xiao) | Kylin Web application, REST service |
|Kejia Wang |  [Kejia-Wang](https://github.com/Kejia-Wang)  | Web aplication, Website|
|Yue Yang |  | Web aplication UI design |

### Credits

* Thanks [eBay Inc.](https://www.ebayinc.com/) to donated this project to open source community, first announement at [eBay Techblog](http://www.ebaytechblog.com/2014/10/20/announcing-kylin-extreme-olap-engine-for-big-data/).  
* Thanks [JetBrains](https://www.jetbrains.com/) for providing us a free license of [IntelliJ IDEA](https://www.jetbrains.com/idea/).
* Thanks to [Vikash Agarwal](vikash_agarwal@hotmail.com), his artical __[ODBC Driver Development](http://www.drdobbs.com/windows/odbc-driver-development/184416434?pgno=5)__ and sample code introdued the basic idea about how to write an ODBC driver from scratch.





