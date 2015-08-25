---
layout: default
title: Community
permalink: /community/index.html
---

### Apache Kylin Mailing List

These are the mailing lists that have been established for this project. For each list, there is a subscribe, unsubscribe, and an archive link.

| Name  | Subscribe | Unsubscribe | Post | Archive |
|------ |-----------|-------------|------|---------|
| User Mailing List | [Subscribe](mailto:user-subscribe@kylin.incubator.apache.org) | [Unsubscribe](mailto:user-unsubscribe@kylin.incubator.apache.org) | [Post](mailto:user@kylin.incubator.apache.org) | [mail-archives.apache.org](http://mail-archives.apache.org/mod_mbox/kylin-user/) |
| Developers Mailing List | [Subscribe](mailto:dev-subscribe@kylin.incubator.apache.org) | [Unsubscribe](mailto:dev-unsubscribe@kylin.incubator.apache.org) | [Post](mailto:dev@kylin.incubator.apache.org) | [mail-archives.apache.org](http://mail-archives.apache.org/mod_mbox/kylin-dev/) |
| Issues Mailing List | [Subscribe](mailto:issues-subscribe@kylin.incubator.apache.org) | [Unsubscribe](mailto:issues-unsubscribe@kylin.incubator.apache.org) | N/A | [mail-archives.apache.org](http://mail-archives.apache.org/mod_mbox/kylin-issues/) |
| Commits Mailing List | [Subscribe](mailto:commits-subscribe@kylin.incubator.apache.org) | [Unsubscribe](mailto:commits-unsubscribe@kylin.incubator.apache.org) | [Post](mailto:commits@kylin.incubator.apache.org) | [mail-archives.apache.org](http://mail-archives.apache.org/mod_mbox/kylin-commits/) |

### Mailing List Archives
For convenience, there's a forum style mailing list archives which not part of offical Apache archives:

* [Developer List archive on Nabble](http://apache-kylin-incubating.74782.x6.nabble.com)

### Social Media 
The official Kylin Twitter account: [@ApacheKylin](https://twitter.com/ApacheKylin)

## Apache Kylin Team
A successful project requires many people to play many roles. Some members write code, provide project mentorship, or author documentation. Others are valuable as testers, submitting patches and suggestions.

### PMC Members

| Name  | Apache ID    | Github    |  Role |
|:----- |:-------------|:----------|:------|
{% for c in site.data.contributors %}  | {{ c.name }} | <a href="http://people.apache.org/committer-index#{{ c.apacheId }}">{{ c.apacheId }}</a> | <a href="http://github.com/{{ c.githubId }}"><img width="48" src="{% unless c.avatar %}http://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> |  {{ c.role }} |
{% endfor %}





