---
layout: docs30
title:  Use Hive to build global dictionary
categories: howto
permalink: /docs30/howto/howto_use_hive_mr_dict.html
---

## Global Dictionary in Hive
Count distinct(bitmap) measure is very important for many scenario, such as PageView statistics, and Kylin support count distinct since 1.5.3 .
Apache Kylin implements precisely count distinct measure based on bitmap, and use global dictionary to encode string value into integer. 
Currently we have to build global dictionary in single process/JVM, which may take a lot of time and memory for UHC. By this feature(KYLIN-3841), we use Hive, a distributed SQL engine to build global dictionary.

This will help to:
1. Reduce memory pressure of Kylin process, MapReduce(or other engine which hive used) will be used to build dict instead of Kylin process itself.
2. Make build base cuboid quicker, because string value has been encoded in previous step.
3. Make global dictionary reusable.
4. Make global dictionary readable and bijective, you may use global dictionary outside Kylin, this maybe useful in many scenario.

### Step by step Analysis
This feature will add three additional steps in cube building if enabled, let us try to understand what Kylin do in these steps.

1. Global Dict Mr/Hive extract dict_val from Data

    - Create a Hive table for store global dictionary if it is not exists, table name should be *CubeName_Suffix*. This table has two normal column and one partition column, two normal columns are `dict_key` and `dict_value`, which for origin value and encoded integer respectively.
    - Create a temporary table with "__group_by" as its suffix, which used to store distinct value for specific column. This table has one normal column and one partition column, normal column is `dict_key` which used to store origin value.
    - Insert distinct value into temporary table created above for each column by using a hive query "select cloA from flatTable group by cloA".

    When this step finished, you should get a temporary table contains distinct values, each partition for specific Count_Distinct column.

2. Global Dict Mr/Hive build dict_val

    - Find all fresh distinct value which never exists in any older segments by *LEFT JOIN* between global dictionary table and temporary table.
    - Append all fresh distinct value to the tail of global dictionary table by *UNION*. By the power of `row_number` function in Hive, added value will be encoded with integer in incremental way.

    When this step finished, all distinct value for all Count_Distinct column will be encoded correctly in global dictionary table.

3. Global Dict Mr/Hive replace dict_val to Data

    - Using *LEFT JOIN* to replace original string value with encoded integer on flat table which used to build cuboid later.

    When this step finished, all string value which belong to Count_Distinct column will be updated with encoded integer in flat hive table.

----

## How to use

If you have some count distinct(bitmap) measure, and data type of that column is String, you may need Hive Global Dictionary. Says columns name are PV_ID and USER_ID, and table name is USER_ACTION, you may add cube-level configuration `kylin.dictionary.mr-hive.columns=USER_ACTION_PV_ID,USER_ACTION_USER_ID` to enable this feature.

Please don't use hive global dictionary on integer type column, you have to know that the value will be replaced with encoded integer in flat hive table. If you have sum/max/min measure on the same column, you will get wrong result in these measures.

And you should know this feature is conflicted with shrunken global dictionary(KYLIN-3491) because they fix the same thing in different way.

### Configuration

- `kylin.dictionary.mr-hive.columns` is used to specific which columns need to use Hive-MR dict, should be *TABLE1_COLUMN1,TABLE2_COLUMN2*. Better configured in cube level, default value is empty.
- `kylin.dictionary.mr-hive.database` is used to specific which database Hive-MR dict table located, default value is *default*.
- `kylin.hive.union.style` Sometime sql which used to build global dict table may have problem in union syntax, you may refer to Hive Doc for more detail. The default value is *UNION*, using lower version of Hive should change to *UNION ALL*.
- `kylin.dictionary.mr-hive.table.suffix` is used to specific suffix of global dict table, default value is *_global_dict*.

----

## Screenshot

#### SQL in new added step Add count_distinct(bitmap) measure

![add_count_distinct_bitmap](/images/Hive-Global-Dictionary/cube-level-config.png)

#### SQL in new added step Set hive-dict-column in cube level config

![set-hive-dict-column](/images/Hive-Global-Dictionary/set-hive-dict-column.png)

#### SQL in new added step Three added steps of cubing job

![three-added-steps](/images/Hive-Global-Dictionary/three-added-steps.png)

#### SQL in new added step Hive Global Dictionary Table

![hive-global-dict-table](/images/Hive-Global-Dictionary/hive-global-dict-table.png)

#### SQL in new added step

- Global Dict Mr/Hive extract dict_val from Data

    {% highlight Groff markup %}
    CREATE TABLE IF NOT EXISTS lacus.KYLIN_SALE_HIVE_DICT_HIVE_GLOBAL
    ( dict_key STRING COMMENT '',
    dict_val INT COMMENT ''
    )
    COMMENT ''
    PARTITIONED BY (dict_column string)
    STORED AS TEXTFILE;
    DROP TABLE IF EXISTS kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195__group_by;
    CREATE TABLE IF NOT EXISTS kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195__group_by
    (
     dict_key STRING COMMENT ''
    )
    COMMENT ''
    PARTITIONED BY (dict_column string)
    STORED AS SEQUENCEFILE
    ;
    INSERT OVERWRITE TABLE kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195__group_by
    PARTITION (dict_column = 'KYLIN_SALES_LSTG_FORMAT_NAME')
    SELECT
    KYLIN_SALES_LSTG_FORMAT_NAME
    FROM kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195
    GROUP BY KYLIN_SALES_LSTG_FORMAT_NAME
    ;
    INSERT OVERWRITE TABLE kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195__group_by
    PARTITION (dict_column = 'KYLIN_SALES_OPS_REGION')
    SELECT
    KYLIN_SALES_OPS_REGION
    FROM kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195
    GROUP BY KYLIN_SALES_OPS_REGION ;
    {% endhighlight %}

- Global Dict Mr/Hive build dict_val

    {% highlight Groff markup %}
    INSERT OVERWRITE TABLE lacus.KYLIN_SALE_HIVE_DICT_HIVE_GLOBAL
    PARTITION (dict_column = 'KYLIN_SALES_OPS_REGION')
    SELECT dict_key, dict_val FROM lacus.KYLIN_SALE_HIVE_DICT_HIVE_GLOBAL
    WHERE dict_column = 'KYLIN_SALES_OPS_REGION'
    UNION ALL
    SELECT a.dict_key as dict_key, (row_number() over(order by a.dict_key asc)) + (0) as dict_val
    FROM
    (
     SELECT dict_key FROM default.kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195__group_by WHERE dict_column = 'KYLIN_SALES_OPS_REGION' AND dict_key is not null
    ) a
    LEFT JOIN
    (
    SELECT dict_key, dict_val FROM lacus.KYLIN_SALE_HIVE_DICT_HIVE_GLOBAL WHERE dict_column = 'KYLIN_SALES_OPS_REGION'
    ) b
    ON a.dict_key = b.dict_key
    WHERE b.dict_val is null;

    INSERT OVERWRITE TABLE lacus.KYLIN_SALE_HIVE_DICT_HIVE_GLOBAL
    PARTITION (dict_column = 'KYLIN_SALES_LSTG_FORMAT_NAME')
    SELECT dict_key, dict_val FROM lacus.KYLIN_SALE_HIVE_DICT_HIVE_GLOBAL
    WHERE dict_column = 'KYLIN_SALES_LSTG_FORMAT_NAME'
    UNION ALL
    SELECT a.dict_key as dict_key, (row_number() over(order by a.dict_key asc)) + (0) as dict_val
    FROM
    (
     SELECT dict_key FROM default.kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195__group_by WHERE dict_column = 'KYLIN_SALES_LSTG_FORMAT_NAME' AND dict_key is not null
    ) a
    LEFT JOIN
    (
    SELECT dict_key, dict_val FROM lacus.KYLIN_SALE_HIVE_DICT_HIVE_GLOBAL WHERE dict_column = 'KYLIN_SALES_LSTG_FORMAT_NAME'
    ) b
    ON a.dict_key = b.dict_key
    WHERE b.dict_val is null;
{% endhighlight %}

- Global Dict Mr/Hive replace dict_val to Data

{% highlight Groff markup %}
    INSERT OVERWRITE TABLE default.kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195
    SELECT
    a.KYLIN_SALES_TRANS_ID
    ,a.KYLIN_SALES_PART_DT
    ,a.KYLIN_SALES_LEAF_CATEG_ID
    ,a.KYLIN_SALES_LSTG_SITE_ID
    ,a.KYLIN_SALES_SELLER_ID
    ,a.KYLIN_SALES_BUYER_ID
    ,a.BUYER_ACCOUNT_ACCOUNT_COUNTRY
    ,a.SELLER_ACCOUNT_ACCOUNT_COUNTRY
    ,a.KYLIN_SALES_PRICE
    ,a.KYLIN_SALES_ITEM_COUNT
    ,a.KYLIN_SALES_LSTG_FORMAT_NAME
    ,b. dict_val
    FROM default.kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195 a
    LEFT OUTER JOIN
    (
    SELECT dict_key, dict_val FROM lacus.KYLIN_SALE_HIVE_DICT_HIVE_GLOBAL WHERE dict_column = 'KYLIN_SALES_OPS_REGION'
    ) b
     ON a.KYLIN_SALES_OPS_REGION = b.dict_key;
    INSERT OVERWRITE TABLE default.kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195
    SELECT
    a.KYLIN_SALES_TRANS_ID
    ,a.KYLIN_SALES_PART_DT
    ,a.KYLIN_SALES_LEAF_CATEG_ID
    ,a.KYLIN_SALES_LSTG_SITE_ID
    ,a.KYLIN_SALES_SELLER_ID
    ,a.KYLIN_SALES_BUYER_ID
    ,a.BUYER_ACCOUNT_ACCOUNT_COUNTRY
    ,a.SELLER_ACCOUNT_ACCOUNT_COUNTRY
    ,a.KYLIN_SALES_PRICE
    ,a.KYLIN_SALES_ITEM_COUNT
    ,b. dict_val
    ,a.KYLIN_SALES_OPS_REGION
    FROM default.kylin_intermediate_kylin_sale_hive_dict_921b0a15_d7cd_a2e6_6852_4ce44158f195 a
    LEFT OUTER JOIN
    (
    SELECT dict_key, dict_val FROM lacus.KYLIN_SALE_HIVE_DICT_HIVE_GLOBAL WHERE dict_column = 'KYLIN_SALES_LSTG_FORMAT_NAME'
    ) b
     ON a.KYLIN_SALES_LSTG_FORMAT_NAME = b.dict_key;
{% endhighlight %}

### Reference Link

- https://issues.apache.org/jira/browse/KYLIN-3491
- https://issues.apache.org/jira/browse/KYLIN-3841
- https://issues.apache.org/jira/browse/KYLIN-3905
- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Union
- http://kylin.apache.org/blog/2016/08/01/count-distinct-in-kylin/