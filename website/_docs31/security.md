---
layout: docs31
title:  Security Issues
categories: docs
permalink: /docs31/security.html
---

### [CVE-2020-13937](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-13937)

__Severity__

Important

__Versions Affected__

Kylin 2.0.0, 2.1.0, 2.2.0, 2.3.0, 2.3.1, 2.3.2, 2.4.0, 2.4.1, 2.5.0, 2.5.1, 2.5.2, 2.6.0, 2.6.1, 2.6.2, 2.6.3, 2.6.4, 2.6.5, 2.6.6, 3.0.0-alpha, 3.0.0-alpha2, 3.0.0-beta, 3.0.0, 3.0.1, 3.0.2, 3.1.0, 4.0.0-alpha.

__Description__

Kylin has one restful api which exposed Kylin's configuration information without any authentication, so it is dangerous because some confidential information entries will be disclosed to everyone.

__Mitigation__

Users of all previous versions after 2.0 should upgrade to 3.1.0.

Users could edit `$KYLIN_HOME/WEB-INF/classes/kylinSecurity.xml`, and remove this line `<scr:intercept-url pattern="/api/admin/config" access="permitAll"/>`. After that,  restart all Kylin instances to make it effective.

Otherwise, you can upgrade Kylin to 3.1.1.

__Credit__

This issue was discovered by Ngo Wei Lin (@Creastery) of STAR Labs (@starlabs_sg).

### [CVE-2020-13926](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-13926)

__Severity__

Important

__Versions Affected__

Kylin 2.0.0, 2.1.0, 2.2.0, 2.3.0, 2.3.1, 2.3.2, 2.4.0, 2.4.1, 2.5.0, 2.5.1, 2.5.2, 2.6.0, 2.6.1, 2.6.2, 2.6.3, 2.6.4, 2.6.5, 2.6.6, 3.0.0-alpha, 3.0.0-alpha2, 3.0.0-beta, 3.0.0, 3.0.1 3.0.2

__Description__

Kylin concatenates and executes some Hive SQL statements in Hive CLI or beeline when building new segments; some parts of the SQL are from system configurations, while the configuration can be overwritten by certain rest API, which make SQL injection attack is possible.

__Mitigation__

Users of all previous versions after 2.0 should upgrade to 3.1.0.

__Credit__

We would like to thank Rupeng Wang from Kyligence for reporting and fix this issue.


### [CVE-2020-13925](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-13925)

__Severity__

Important

__Versions Affected__

Kylin 2.3.0, 2.3.1, 2.3.2, 2.4.0, 2.4.1, 2.5.0, 2.5.1, 2.5.2, 2.6.0, 2.6.1, 2.6.2, 2.6.3, 2.6.4, 2.6.5, 2.6.6, 3.0.0-alpha, 3.0.0-alpha2, 3.0.0-beta, 3.0.0, 3.0.1 3.0.2

__Description__

Similar to CVE-2020-1956, Kylin has one more restful API which concatenates the API inputs into OS commands and then executes them on the server; while the reported API misses necessary input validation, which causes the hackers have the possibility to execute OS command remotely.

__Mitigation__

Users of all previous versions after 2.3 should upgrade to 3.1.0.

__Credit__

We would like to thank Clancey <clanceyz@protonmail.com> for reporting this issue.


### [CVE-2020-1937](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-1937) Apache Kylin SQL injection vulnerability

__Severity__

Important

__Versions Affected__

Kylin 2.3.0 to 2.3.2, 2.4.0 to 2.4.1, 2.5.0 to 2.5.2, 2.6.0 to 2.6.4, 3.0.0-alpha, 3.0.0-alpha2, 3.0.0-beta, 3.0.0

__Description__

Kylin has some restful apis which will concat sqls with the user input string, a user is likely to be able to run malicious database queries.

__Mitigation__

Users should upgrade to 3.0.1 or 2.6.5

__Credit__

This issue was discovered by ﻿Jonathan Leitschuh

### [CVE-2020-1956](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-1956) Apache Kylin command injection vulnerability

__Severity__

Important

__Versions Affected__

Kylin 2.3.0 to 2.3.2, 2.4.0 to 2.4.1, 2.5.0 to 2.5.2, 2.6.0 to 2.6.5, 3.0.0-alpha, 3.0.0-alpha2, 3.0.0-beta, 3.0.0, 3.0.1

__Description__

Kylin has some restful api which will concat os command with the user input string, a user is likely to be able to execute any os command without any protection or validation.

__Mitigation__

Users should upgrade to 3.0.2 or 2.6.6 or set kylin.tool.auto-migrate-cube.enabled to false to disable command execution. 

__Credit__

This issue was discovered by ﻿Johannes Dahse

### [CVE-2021-27738](https://cveprocess.apache.org/cve/CVE-2021-27738) Improper Access Control to Streaming Coordinator & SSRF

__Severity__

Moderate

__Versions Affected__

Kylin 3.0.0-alpha to 3.1.2

__Description__

All request mappings in `StreamingCoordinatorController.java` handling `/kylin/api/streaming_coordinator/*` REST API endpoints did not include any security checks, which allowed an unauthenticated user to issue arbitrary requests, such as assigning/unassigning of streaming cubes, creation/modification and deletion of replica sets, to the Kylin Coordinator.

For endpoints accepting node details in HTTP message body, unauthenticated (but limited) server-side request forgery (SSRF) can be achieved.

__Mitigation__

Users of Kylin 3.x should upgrade to 3.1.3 or apply patch https://github.com/apache/kylin/pull/1646.

__Credit__

This issue was discovered by Wei Lin Ngo

### [CVE-2021-31522](https://cveprocess.apache.org/cve/CVE-2021-31522) Apache Kylin unsafe class loading

__Severity__

Moderate

__Versions Affected__

Kylin 2.0.0 to 2.6.6, 3.0.0-alpha to 3.1.2, 4.0.0-alpha to 4.0.0

__Description__

Kylin can receive user input and load any class through Class.forName(...).

__Mitigation__

Users of Kylin 2.x & Kylin 3.x should upgrade to 3.1.3 or apply patch https://github.com/apache/kylin/pull/1695.
Users of Kylin 4.x should upgrade to 4.0.1 or apply patch https://github.com/apache/kylin/pull/1763.

__Credit__

This issue was discovered by bo yu

### [CVE-2021-36774](https://cveprocess.apache.org/cve/CVE-2021-36774) Mysql JDBC Connector Deserialize RCE

__Severity__

Moderate

__Versions Affected__

Kylin 2.0.0 to 2.6.6, 3.0.0-alpha to 3.1.2

__Description__

Apache Kylin allows users to read data from other database systems using JDBC. The MySQL JDBC driver supports certain properties, which, if left unmitigated, can allow an attacker to execute arbitrary code from a hacker-controlled malicious MySQL server within Kylin server processes.

__Mitigation__

Users of Kylin 2.x & Kylin 3.x should upgrade to 3.1.3 or apply patch https://github.com/apache/kylin/pull/1694.

__Credit__

This issue was discovered by jinchen sheng

### [CVE-2021-45456](https://cveprocess.apache.org/cve/CVE-2021-45456) Command injection

__Severity__

Moderate

__Versions Affected__

Kylin 4.0.0

__Description__

Apache kylin checks the legitimacy of the project before executing some commands with the project name passed in by the user. There is a mismatch between what is being checked and what is being used as the shell command argument in DiagnosisService. This may cause an illegal project name to pass the check and perform the following steps, resulting in a command injection vulnerability.

__Mitigation__

Users of Kylin 4.0.0 should upgrade to 4.0.1 or apply patch https://github.com/apache/kylin/pull/1781.

__Credit__

This issue was discovered by Alvaro Munoz

### [CVE-2021-45457](https://cveprocess.apache.org/cve/CVE-2021-45457) Overly broad CORS configuration

__Severity__

Moderate

__Versions Affected__

Kylin 2.0.0 to 2.6.6, 3.0.0-alpha to 3.1.2, 4.0.0-alpha to 4.0.0

__Description__

Cross-origin requests with credentials are allowed to be sent from any origin.

Kylin reflects the `Origin` header and allow credentials to be sent cross-origin in the default configuration. The preflight OPTIONS request:
```
OPTIONS /kylin/api/projects HTTP/1.1
Host: localhost:7070
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:94.0) Gecko/20100101 Firefox/94.0
Accept: */*
Accept-Language: en-US
Accept-Encoding: gzip, deflate
Access-Control-Request-Method: POST
Access-Control-Request-Headers: content-type
Referer: http://b49b-95-62-58-48.ngrok.io/
Origin: http://b49b-95-62-58-48.ngrok.io
Connection: keep-alive
Cache-Control: max-age=0
```

Will be replied with:

```
HTTP/1.1 200 OK
Server: Apache-Coyote/1.1
Access-Control-Allow-Origin: http://b49b-95-62-58-48.ngrok.io
Access-Control-Allow-Credentials: true
Vary: Origin
Access-Control-Allow-Methods: DELETE, POST, GET, OPTIONS, PUT
Access-Control-Allow-Headers: Authorization, Origin, No-Cache, X-Requested-With, Cache-Control, Accept, X-E4m-With, If-Modified-Since, Pragma, Last-Modified, Expires, Content-Type
Content-Length: 0
```

__Mitigation__

Users of Kylin 2.x & Kylin 3.x should upgrade to 3.1.3 or apply patch https://github.com/apache/kylin/pull/1782.
Users of Kylin 4.x should upgrade to 4.0.1 or apply patch https://github.com/apache/kylin/pull/1781.

__Credit__

This issue was discovered by Alvaro Munoz

### [CVE-2021-45458](https://cveprocess.apache.org/cve/CVE-2021-45458) Hardcoded credentials

__Severity__

Moderate

__Versions Affected__

Kylin 2.0.0 to 2.6.6, 3.0.0-alpha to 3.1.2, 4.0.0-alpha to 4.0.0

__Description__

Apache Kylin provides encryption classes PasswordPlaceholderConfigurer to help users encrypt their passwords. In the encryption algorithm used by this encryption class, the cipher is initialized with a hardcoded key and IV.  If users use class PasswordPlaceholderConfigurer to encrypt their password and configure it into kylin's configuration file, there is a risk that the password may be decrypted.

__Mitigation__

Users of Kylin 2.x & Kylin 3.x should upgrade to 3.1.3 or apply patch https://github.com/apache/kylin/pull/1782.
Users of Kylin 4.x should upgrade to 4.0.1 or apply patch https://github.com/apache/kylin/pull/1781.

__Credit__

This issue was discovered by Alvaro Munoz
