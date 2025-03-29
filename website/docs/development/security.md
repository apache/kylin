---
title: Kylin Security
language: en
sidebar_label: Kylin Security
pagination_label: Kylin Security
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/how_to_understand_kylin_design
pagination_next: null
showLastUpdateAuthor: true
showLastUpdateTime: true
keywords:
  - security
draft: false
last_update:
  date: 02/13/2025
  author: Jiang Longfei
---

# Apache Kylin Security
The Apache Software Foundation takes security issues very seriously. 
Apache Kylin specifically offers security features and is responsive to issues around its features. 
If you have any concern around Kylin Security or believe you have uncovered a vulnerability, 
we suggest that you get in touch via the e-mail address ```security@apache.org```. 
In the message, try to provide a description of the issue and ideally a way of reproducing it. 
The security team will get back to you after assessing the description.

Note that this security address should be used only for undisclosed vulnerabilities. 
Dealing with fixed issues or general questions on how to use the security features 
should be handled regularly via the user and the dev lists. 
Please report any security problems to the project security address before disclosing it publicly.

The ASF Security team maintains a page with a description of how vulnerabilities are handled, 
check their [Web page](http://apache.org/security/) for more information.

# Known Security Issues
## CVE-2024-48944: Apache Kylin: SSRF vulnerability in the diagnosis api 
Severity: low

Affected versions:

- Apache Kylin 5.0.0 through 5.0.1

Description:

Server-Side Request Forgery (SSRF) vulnerability in Apache Kylin. Through a kylin server, an attacker may forge a request to invoke "/kylin/api/xxx/diag" api on another internal host and possibly get leaked information. There are two preconditions: 1) The attacker has got admin access to a kylin server; 2) Another internal host has the "/kylin/api/xxx/diag" api

endpoint open for service.


This issue affects Apache Kylin: from 5.0.0
through

5.0.1.

Users are recommended to upgrade to version 5.0.2, which fixes the issue.

This issue is being tracked as KYLIN-5644 

Credit:

- Zevi (finder)

## CVE-2025-30067: Apache Kylin: The remote code execution via jdbc url 

Severity: low

Affected versions:

- Apache Kylin 4.0.0 through 5.0.1

Description:

Improper Control of Generation of Code ('Code Injection') vulnerability in Apache Kylin.
If an attacker gets access to Kylin's system or project admin permission, the JDBC connection configuration maybe altered to execute arbitrary code from the remote. You are fine as long as the Kylin's system and project admin access is well protected.

This issue affects Apache Kylin: from 4.0.0 through 5.0.1.

Users are recommended to upgrade to version 5.0.2 or above, which fixes the issue.

This issue is being tracked as KYLIN-5994

Credit:

Pho3n1x (finder)

