---
layout: docs30
title:  Security Issues
categories: docs30
permalink: /docs30/security.html
---

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
