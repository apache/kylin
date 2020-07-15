---
layout: docs
title:  Security Issues
categories: docs
permalink: /docs/security.html
---

### [CVE-2020-13926](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-13926)

__Severity__

Important

__Vendor__

The Apache Software Foundation

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

__Vendor__

The Apache Software Foundation

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

__Vendor__

The Apache Software Foundation


__Versions Affected__

Kylin 2.3.0 to 2.3.2

Kylin 2.4.0 to 2.4.1

Kylin 2.5.0 to 2.5.2

Kylin 2.6.0 to 2.6.4

Kylin 3.0.0-alpha, Kylin 3.0.0-alpha2, Kylin 3.0.0-beta, Kylin 3.0.0

__Description__

Kylin has some restful apis which will concat sqls with the user input string, a user is likely to be able to run malicious database queries.

__Mitigation__

Users should upgrade to 3.0.1 or 2.6.5

__Credit__

This issue was discovered by ﻿Jonathan Leitschuh

### [CVE-2020-1956](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-1956) Apache Kylin command injection vulnerability

__Severity__


Important

__Vendor__

The Apache Software Foundation

__Versions Affected__

Kylin 2.3.0 to 2.3.2

Kylin 2.4.0 to 2.4.1

Kylin 2.5.0 to 2.5.2

Kylin 2.6.0 to 2.6.5

Kylin 3.0.0-alpha, Kylin 3.0.0-alpha2, Kylin 3.0.0-beta, Kylin 3.0.0, Kylin 3.0.1

__Description__

Kylin has some restful api which will concat os command with the user input string, a user is likely to be able to execute any os command without any protection or validation.

__Mitigation__

Users should upgrade to 3.0.2 or 2.6.6 or set kylin.tool.auto-migrate-cube.enabled to false to disable command execution. 

__Credit__

This issue was discovered by ﻿Johannes Dahse
