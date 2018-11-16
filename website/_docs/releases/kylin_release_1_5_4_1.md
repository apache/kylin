---
layout: docs
title:  Kylin Release 1.5.4.1
categories: releases
permalink: /docs/releases/kylin_release_1_5_4_1.html
---

_Tag:_ [kylin-1.5.4.1](https://github.com/apache/kylin/tree/kylin-1.5.4.1)

This version fixes two major bugs introduced in 1.5.4; The metadata and HBase coprocessor is compatible with 1.5.4.

### Bug fix

* [KYLIN-2010] - Date dictionary return wrong SQL result
* [KYLIN-2026] - NPE occurs when build a cube without partition column
* [KYLIN-2032] - Cube build failed when partition column isn't in dimension list
