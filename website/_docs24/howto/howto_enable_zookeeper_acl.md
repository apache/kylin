---
layout: docs24
title:  Enable Zookeeper ACL
categories: howto
permalink: /docs24/howto/howto_enable_zookeeper_acl.html
---

Edit $KYLIN_HOME/conf/kylin.properties to add following configuration item:

* Add "kylin.env.zookeeper.zk-auth". It is the configuration item you can specify the zookeeper authenticated information. Its formats is "scheme:id". The value of scheme that the zookeeper supports is "world", "auth", "digest", "ip" or "super". The "id" is the authenticated information of the scheme. For example:

    `kylin.env.zookeeper.zk-auth=digest:ADMIN:KYLIN`

    The scheme equals to "digest". The id equals to "ADMIN:KYLIN", which expresses the "username:password".

* Add "kylin.env.zookeeper.zk-acl". It is the configuration item you can set access permission. Its formats is "scheme:id:permissions". The value of permissions that the zookeeper supports is "READ", "WRITE", "CREATE", "DELETE" or "ADMIN". For example, we configure that everyone has all the permissions:

    `kylin.env.zookeeper.zk-acl=world:anyone:rwcda`

    The scheme equals to "world". The id equals to "anyone" and the permissions equals to "rwcda".
