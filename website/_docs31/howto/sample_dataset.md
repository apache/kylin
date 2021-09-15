---
layout: docs31
title:  Sample Dataset
categories: tutorial
permalink: /docs31/howto/sample_dataset.html
---

Kylin binary package contains a sample dataset for testing. It consists of five tables, including the fact table which has 10,000 rows. Because of the small data size, it is convenient to carry out as a test in the virtual machine. You can import the Kylin built-in sample data into Hive using executable script.

### Import Sample Dataset into Hive

The script is `sample.sh`. you can find it under `$KYLIN_HOME/bin`.

```sh
$KYLIN_HOME/bin/sample.sh
```

Once the script is complete, execute the following commands to enter Hive. Then you can confirm whether the tables are imported successfully.

```sh
hive
```

By default, the script imports 5 tables into Hive's `default` database. You can check the tables imported into Hive or query some tables:

```sql
hive> use default;
hive> show tables;
hive> select count(*) from kylin_sales;
```

> Tip: If you need to import the table to the specified database in Hive, you can modify the configuration item `kylin.source.hive.database-for-flat-table` in the Kylin configuration file `$KYLIN_HOME/conf/kylin.properties` to the specified Hive database.

### Table Introduction

Kylin supports both star schema and snowflake data model. In this manual, we will use a typical snowflake data model as our sample data set which contains five tables:

- **KYLIN_SALES** This is the fact table, it contains detail information of sales orders. Each row holds information such as the seller, the commodity classification, the amount of orders, the quantity of goods, etc. Each row corresponds to a transaction.
- **KYLIN_CATEGORY_GROUPINGS** This is a dimension table, it represents details of commodity classification, such as, name of commodity category, etc.
- **KYLIN_CAL_DT** This is another dimension table which extends information of dates, such as beginning date of the year, beginning date of the month, beginning date of the week.
- **KYLIN_ACCOUNT** This is the user account table. Each row represents a user who could be a buyer and/or a seller of a specific transaction, which links to **KYLIN_SALES** through the BUYER_ID or SELLER_ID.
- **KYLIN_COUNTRY** This is the country dimension table linking to **KYLIN_ACCOUNT**.

The five tables together constitute the structure of the entire snowflake data model. Below is a relational diagram of them.

![Sample Table](/images/SampleDataset/dataset.png)

### Data Dictionary

The generated hive tables contains too many columns which may confused you. So the following tables will list some key columns which referred in Kylin's Model/Cube, and describe the underlying business meaning of them.

| Table                    | Field                | Description                      |
| :----------------------- | :------------------- | :------------------------------- |
| KYLIN_SALES              | TRANS_ID             | Order ID                         |
| KYLIN_SALES              | PART_DT              | Order Date                       |
| KYLIN_SALES              | LEAF_CATEG_ID        | ID Of Commodity Category         |
| KYLIN_SALES              | LSTG_SITE_ID         | Site ID                          |
| KYLIN_SALES              | SELLER_ID            | Account ID Of Seller             |
| KYLIN_SALES              | BUYER_ID             | Account ID Of Buyer              |
| KYLIN_SALES              | PRICE                | Order Amount                     |
| KYLIN_SALES              | ITEM_COUNT           | The Number Of Purchased Goods    |
| KYLIN_SALES              | LSTG_FORMAT_NAME     | Order Transaction Type           |
| KYLIN_SALES              | OPS_USER_ID          | System User ID                   |
| KYLIN_SALES              | OPS_REGION           | System User Region               |
| KYLIN_CATEGORY_GROUPINGS | USER_DEFINED_FIELD1  | User Defined Fields 1            |
| KYLIN_CATEGORY_GROUPINGS | USER_DEFINED_FIELD3  | User Defined Fields 3            |
| KYLIN_CATEGORY_GROUPINGS | UPD_DATE             | Update Date                      |
| KYLIN_CATEGORY_GROUPINGS | UPD_USER             | Update User                      |
| KYLIN_CATEGORY_GROUPINGS | META_CATEG_NAME      | Level 1 Category                 |
| KYLIN_CATEGORY_GROUPINGS | CATEG_LVL2_NAME      | Level 2 Category                 |
| KYLIN_CATEGORY_GROUPINGS | CATEG_LVL3_NAME      | Level 3 Category                 |
| KYLIN_CAL_DT             | CAL_DT               | Date                             |
| KYLIN_CAL_DT             | WEEK_BEG_DT          | Week Beginning Date              |
| KYLIN_CAL_DT             | MONTH_BEG_DT         | Month Beginning Date             |
| KYLIN_CAL_DT             | YEAR_BEG_DT          | Year Beginning Date              |
| KYLIN_ACCOUNT            | ACCOUNT_ID           | ID Number Of Account             |
| KYLIN_ACCOUNT            | ACCOUNT_COUNTRY      | Country ID Where Account Resides |
| KYLIN_ACCOUNT            | ACCOUNT_BUYER_LEVEL  | Buyer Account Level              |
| KYLIN_ACCOUNT            | ACCOUNT_SELLER_LEVEL | Seller Account Level             |
| KYLIN_ACCOUNT            | ACCOUNT_CONTACT      | Contact of Account               |
| KYLIN_COUNTRY            | COUNTRY              | Country ID                       |
| KYLIN_COUNTRY            | NAME                 | Descriptive Name Of Country      |