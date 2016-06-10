---
layout: post-blog
title:  Use Count Distinct in Apache Kylin
date:   2016-06-10 18:30:00
author: Yerui Sun 
categories: blog
---

Since v.1.5.3

## Background
Count Distinct is a commonly measure in OLAP analyze, usually used for uv, etc. Apache Kylin offers two kinds of count distinct, approximately and precisely, differs on resource and performance.

## Approximately Count Distinct
Apache Kylin implements approximately count distinct using HyperLogLog algorithm, offered serveral precision, with the error rates from 9.75% to 1.22%. 
The result of measure has theorically upper limit in size, as 2^N bytes. For the max precision N=16, the upper limit is 64KB, and the max error rate is 1.22%. 
This implementation's pros is fast caculating and storage resource saving, but can't be used for precisely requirements.

## Precisely Count Distinct
Apache Kylin also implements precisely count distinct based on bitmap. For the data with type tiny int(byte), small int(short) and int, project the value into the bitmap directly. For the data with type long, string and others, encode the value as String into a dict, and project the dict id into the bitmap.
The result of measure is the serialized data of bitmap, not just the count value. This makes sure that the rusult is always right with any roll-up, even across segments.
This implementation's pros is precesily result, without error, but needs more storage resources. One result size maybe hundreds of MB, when the count distinct value over millions.

## Global Dictionary
Apache Kylin encode value into dictionay at the segment level by default. That means one same value in different segments maybe encoded into different id, which means the result of precisely count distinct maybe not correct.
We introduced Global Dictionary with ensurance that one same value always encode into same id in different segments, to resolve this problem. Meanwhile, the capacity of dict has expanded dramatically, upper to support 2G values in one dict. It can also be used to replace default dictionary which has 5M values limitation.
Current version has no UI for global dictionary yet, and the cube desc json shoule be modified to enable it:

```
"dictionaries": [
    {
          "column": "SUCPAY_USERID",
	  "reuse": "USER_ID",
          "builder": "org.apache.kylin.dict.GlobalDictionaryBuilder"
    }
]
```

The `column` means the column which to be encoded, the `builder` specifies the dictionary builder, only `org.apache.kylin.dict.GlobalDictionaryBuilder` is available for now.
The 'reuse` is used to optimize the dict of more than one columns based on one dataset, please refer the next section 'Example' for more details.
The global dictionay can't be used for dimensiion encoding for now, that means if one column is used for dimension and count distinct measure in one cube, the dimension encoding should be others but not dict.

## Example
Here's some example data:
| DT           | USER\_ID | FLAG1 | FLAG2 | USER\_ID\_FLAG1 | USER\_ID\_FLAG2 |
| :----------: | :------: | :---: | :---: | :-------------: | :-------------: |
| 2016-06-08   | AAA      | 1     | 1     | AAA             | AAA             |
| 2016-06-08   | BBB      | 1     | 1     | BBB             | BBB             |
| 2016-06-08   | CCC      | 0     | 1     | NULL            | CCC             |
| 2016-06-09   | AAA      | 0     | 1     | NULL            | AAA             |
| 2016-06-09   | CCC      | 1     | 0     | CCC             | NULL            |
| 2016-06-10   | BBB      | 0     | 1     | NULL            | BBB             |

There's basic columns `DT`, `USER_ID`, `FLAG1`, `FLAG2`, and condition columns `USER_ID_FLAG1=if(FLAG1=1,USER_ID,null)`, `USER_ID_FLAG2=if(FLAG2=1,USER_ID,null)`. Supposed the cube is builded by day, has 3 segments.

Without the global dictionay, the precisely count distinct in semgent is correct, but the roll-up acrros segments result is wrong. Here's an example:

```
select count(distinct user_id_flag1) from table where dt in ('2016-06-08', '2016-06-09')
```
The result is 2 but not 3. The reason is that the dict in 2016-06-08 segment is AAA=>1, BBB=>1, and the dict in 2016-06-09 segment is CCC=> 1.
With global dictionary config as below, the dict became as AAA=>1, BBB=>2, CCC=>3, that will procude correct result.
```
"dictionaries": [
    {
      "column": "USER_ID_FLAG1",
      "builder": "org.apache.kylin.dict.GlobalDictionaryBuilder"
    }
]
```

Actually, the data of USER_ID_FLAG1 and USER_ID_FLAG2 both are a subset of USER_ID dataset, that made the dictionary re-using possible. Just encode the USER_ID dataset, and config USER_ID_FLAG1 and USER_ID_FLAG2 resue USER_ID dict:
```
"dictionaries": [
    {
      "column": "USER_ID",
      "builder": "org.apache.kylin.dict.GlobalDictionaryBuilder"
    },
    {
      "column": "USER_ID_FLAG1",
      "reuse": "USER_ID",
      "builder": "org.apache.kylin.dict.GlobalDictionaryBuilder"
    },
    {
      "column": "USER_ID_FLAG2",
      "reuse": "USER_ID",
      "builder": "org.apache.kylin.dict.GlobalDictionaryBuilder"
    }
]
```

## Conclusions
Here's some basically pricipal to decide which kind of count distinct will be used:
 - If the result with error rate is acceptable, approximately way is always an better way
 - If you need precisely result, the only way is precisely count distinct
 - If you don't need roll-up across segments, or the column data type is tinyint/smallint/int, or the values count is less than 5M, just use default dictionary; otherwise the global dictionary should be configured, and consider the reuse column optimization
