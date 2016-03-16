---
layout: docs
title:  Enable Security with LDAP
categories: howto
permalink: /docs/howto/howto_ldap_and_sso.html
---

## Enable LDAP authentication

Kylin supports LDAP authentication for enterprise or production deployment; This is implemented with Spring Security framework; Before enable LDAP, please contact your LDAP administrator to get necessary information, like LDAP server URL, username/password, search patterns;

#### Configure LDAP server info

Firstly, provide LDAP URL, and username/password if the LDAP server is secured; The password in kylin.properties need be encrypted with AES with a given key; to encrypt it, you download latest Kylin source code and then run org.apache.kylin.rest.security.PasswordPlaceholderConfigurer in your IDE, passing "AES" as the first parameter, your plain password as the second parameter to get an encrypted password.

```
ldap.server=ldap://<your_ldap_host>:<port>
ldap.username=<your_user_name>
ldap.password=<your_password_encrypted>
```

Secondly, provide the user search patterns, this is by LDAP design, here is just a sample:

```
ldap.user.searchBase=OU=UserAccounts,DC=mycompany,DC=com
ldap.user.searchPattern=(&(AccountName={0})(memberOf=CN=MYCOMPANY-USERS,DC=mycompany,DC=com))
ldap.user.groupSearchBase=OU=Group,DC=mycompany,DC=com
```

If you have service accounts (e.g, for system integration) which also need be authenticated, configure them in ldap.service.*; Otherwise, leave them be empty;

### Configure the administrator group and default role

To map an LDAP group to the admin group in Kylin, need set the "acl.adminRole" to "ROLE_" + GROUP_NAME. For example, in LDAP the group "KYLIN-ADMIN-GROUP" is the list of administrators, here need set it as:

```
acl.adminRole=ROLE_KYLIN-ADMIN-GROUP
acl.defaultRole=ROLE_ANALYST,ROLE_MODELER
```

The "acl.defaultRole" is a list of the default roles that grant to everyone, keep it as-is.

#### Enable LDAP

Set "kylin.sandbox=false" in conf/kylin.properties, then restart Kylin server.
