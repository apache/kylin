---
title: How to test
language: en
sidebar_label: How to test
pagination_label: How to test
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/how_to_debug_kylin_in_ide
pagination_next: development/how_to_package
showLastUpdateAuthor: true
showLastUpdateTime: true
keywords:
  - testing
draft: false
last_update:
  date: 08/24/2022
---

# How to run tests

```shell
bash dev-support/unit_testing.sh
```

This scripts will finish in about 1~1.5 hour. The output will be saved in ci-results-YYYY-mm-dd.txt .

If all testes passed, console will print:

```text
...
[INFO] --- maven-surefire-plugin:3.0.0-M5:test (default-test) @ kylin-sparder ---
[INFO] --- maven-surefire-plugin:3.0.0-M5:test (default-test) @ kylin-spark-common ---
[INFO] --- maven-surefire-plugin:3.0.0-M5:test (default-test) @ kylin-spark-it ---
<Failed test on following module>
<Failed cases statistics>
```
