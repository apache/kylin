/* eslint-disable */
export default {
  "uuid": "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
  "last_modified": 1537497663000,
  "version": "3.0.0.0",
  "name": "nmodel_basic",
  "alias": "nmodel_basic",
  "owner": "ADMIN",
  "is_draft": false,
  "description": null,
  "fact_table": "DEFAULT.TEST_KYLIN_FACT",
  "management_type": "TABLE_ORIENTED",
  "lookups": [{
      "table": "DEFAULT.TEST_ORDER",
      "kind": "LOOKUP",
      "alias": "TEST_ORDER",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_ORDER.ORDER_ID"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.ORDER_ID"
        ]
      }
    },
    {
      "table": "EDW.TEST_SELLER_TYPE_DIM",
      "kind": "LOOKUP",
      "alias": "TEST_SELLER_TYPE_DIM",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.SLR_SEGMENT_CD"
        ]
      }
    },
    {
      "table": "EDW.TEST_CAL_DT",
      "kind": "LOOKUP",
      "alias": "TEST_CAL_DT",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_CAL_DT.CAL_DT"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.CAL_DT"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_CATEGORY_GROUPINGS",
      "kind": "LOOKUP",
      "alias": "TEST_CATEGORY_GROUPINGS",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID",
          "TEST_CATEGORY_GROUPINGS.SITE_ID"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.LEAF_CATEG_ID",
          "TEST_KYLIN_FACT.LSTG_SITE_ID"
        ]
      }
    },
    {
      "table": "EDW.TEST_SITES",
      "kind": "LOOKUP",
      "alias": "TEST_SITES",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_SITES.SITE_ID"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.LSTG_SITE_ID"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_ACCOUNT",
      "kind": "LOOKUP",
      "alias": "SELLER_ACCOUNT",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "SELLER_ACCOUNT.ACCOUNT_ID"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.SELLER_ID"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_ACCOUNT",
      "kind": "LOOKUP",
      "alias": "BUYER_ACCOUNT",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "BUYER_ACCOUNT.ACCOUNT_ID"
        ],
        "foreign_key": [
          "TEST_ORDER.BUYER_ID"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_COUNTRY",
      "kind": "LOOKUP",
      "alias": "SELLER_COUNTRY",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "SELLER_COUNTRY.COUNTRY"
        ],
        "foreign_key": [
          "SELLER_ACCOUNT.ACCOUNT_COUNTRY"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_COUNTRY",
      "kind": "LOOKUP",
      "alias": "BUYER_COUNTRY",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "BUYER_COUNTRY.COUNTRY"
        ],
        "foreign_key": [
          "BUYER_ACCOUNT.ACCOUNT_COUNTRY"
        ]
      }
    }
  ],
  "dimensions": [{
      "table": "TEST_KYLIN_FACT",
      "columns": [
        "ORDER_ID",
        "SLR_SEGMENT_CD",
        "CAL_DT",
        "LEAF_CATEG_ID",
        "LSTG_SITE_ID",
        "SELLER_ID"
      ]
    },
    {
      "table": "TEST_ORDER",
      "columns": [
        "ORDER_ID",
        "BUYER_ID"
      ]
    },
    {
      "table": "TEST_SELLER_TYPE_DIM",
      "columns": [
        "SELLER_TYPE_CD"
      ]
    },
    {
      "table": "TEST_CAL_DT",
      "columns": [
        "CAL_DT"
      ]
    },
    {
      "table": "TEST_CATEGORY_GROUPINGS",
      "columns": [
        "LEAF_CATEG_ID",
        "SITE_ID"
      ]
    },
    {
      "table": "TEST_SITES",
      "columns": [
        "SITE_ID"
      ]
    },
    {
      "table": "SELLER_ACCOUNT",
      "columns": [
        "ACCOUNT_ID",
        "ACCOUNT_COUNTRY"
      ]
    },
    {
      "table": "BUYER_ACCOUNT",
      "columns": [
        "ACCOUNT_ID",
        "ACCOUNT_COUNTRY"
      ]
    },
    {
      "table": "SELLER_COUNTRY",
      "columns": [
        "COUNTRY"
      ]
    },
    {
      "table": "BUYER_COUNTRY",
      "columns": [
        "COUNTRY"
      ]
    }
  ],
  "metrics": [],
  "filter_condition": null,
  "partition_desc": {
    "partition_date_column": "TEST_KYLIN_FACT.CAL_DT",
    "partition_time_column": null,
    "partition_date_start": 0,
    "partition_date_format": "yyyy-MM-dd",
    "partition_time_format": "HH:mm:ss",
    "partition_type": "APPEND",
    "partition_condition_builder": "org.apache.kylin.metadata.model.PartitionDesc$DefaultPartitionConditionBuilder"
  },
  "capacity": "MEDIUM",
  "auto_merge": true,
  "auto_merge_time_ranges": [
    "WEEK",
    "MONTH"
  ],
  "volatile_range": {
    "volatileRangeType": "DAY",
    "volatile_range_number": 0,
    "volatile_range_available": true,
    "volatile_range_type": "DAY"
  },
  "table_positions": [{
    "table1": {
      "x_position": 2,
      "y_position": 2,
      "width": 2,
      "height": 2
    }
  }],
  "scale": 0,
  "simplified_dimensions": [{
      "id": 0,
      "name": "SITE_NAME",
      "column": "TEST_SITES.SITE_NAME",
      "is_dimension": false
    },
    {
      "id": 1,
      "name": "TRANS_ID",
      "column": "TEST_KYLIN_FACT.TRANS_ID",
      "is_dimension": false
    },
    {
      "id": 2,
      "name": "CAL_DT",
      "column": "TEST_KYLIN_FACT.CAL_DT",
      "is_dimension": true
    },
    {
      "id": 3,
      "name": "LSTG_FORMAT_NAME",
      "column": "TEST_KYLIN_FACT.LSTG_FORMAT_NAME",
      "is_dimension": false
    },
    {
      "id": 4,
      "name": "LSTG_SITE_ID",
      "column": "TEST_KYLIN_FACT.LSTG_SITE_ID",
      "is_dimension": true
    },
    {
      "id": 5,
      "name": "META_CATEG_NAME",
      "column": "TEST_CATEGORY_GROUPINGS.META_CATEG_NAME",
      "is_dimension": false
    },
    {
      "id": 6,
      "name": "CATEG_LVL2_NAME",
      "column": "TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME",
      "is_dimension": false
    },
    {
      "id": 7,
      "name": "CATEG_LVL3_NAME",
      "column": "TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME",
      "is_dimension": false
    },
    {
      "id": 8,
      "name": "LEAF_CATEG_ID",
      "column": "TEST_KYLIN_FACT.LEAF_CATEG_ID",
      "is_dimension": true
    },
    {
      "id": 9,
      "name": "SELLER_ID",
      "column": "TEST_KYLIN_FACT.SELLER_ID",
      "is_dimension": true
    },
    {
      "id": 10,
      "name": "WEEK_BEG_DT",
      "column": "TEST_CAL_DT.WEEK_BEG_DT",
      "tomb": true,
      "is_dimension": false
    },
    {
      "id": 11,
      "name": "PRICE",
      "column": "TEST_KYLIN_FACT.PRICE",
      "is_dimension": false
    },
    {
      "id": 12,
      "name": "ITEM_COUNT",
      "column": "TEST_KYLIN_FACT.ITEM_COUNT",
      "is_dimension": false
    },
    {
      "id": 13,
      "name": "ORDER_ID",
      "column": "TEST_KYLIN_FACT.ORDER_ID",
      "is_dimension": true
    },
    {
      "id": 14,
      "name": "TEST_DATE_ENC",
      "column": "TEST_ORDER.TEST_DATE_ENC",
      "is_dimension": false
    },
    {
      "id": 15,
      "name": "TEST_TIME_ENC",
      "column": "TEST_ORDER.TEST_TIME_ENC",
      "is_dimension": false
    },
    {
      "id": 16,
      "name": "SLR_SEGMENT_CD",
      "column": "TEST_KYLIN_FACT.SLR_SEGMENT_CD",
      "is_dimension": true
    },
    {
      "id": 17,
      "name": "BUYER_ID",
      "column": "TEST_ORDER.BUYER_ID",
      "is_dimension": true
    },
    {
      "id": 18,
      "name": "SELLER_BUYER_LEVEL",
      "column": "SELLER_ACCOUNT.ACCOUNT_BUYER_LEVEL",
      "is_dimension": false
    },
    {
      "id": 19,
      "name": "SELLER_SELLER_LEVEL",
      "column": "SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL",
      "is_dimension": false
    },
    {
      "id": 20,
      "name": "SELLER_COUNTRY",
      "column": "SELLER_ACCOUNT.ACCOUNT_COUNTRY",
      "is_dimension": false
    },
    {
      "id": 21,
      "name": "SELLER_COUNTRY_NAME",
      "column": "SELLER_COUNTRY.NAME",
      "is_dimension": false
    },
    {
      "id": 22,
      "name": "BUYER_BUYER_LEVEL",
      "column": "BUYER_ACCOUNT.ACCOUNT_BUYER_LEVEL",
      "is_dimension": false
    },
    {
      "id": 23,
      "name": "BUYER_SELLER_LEVEL",
      "column": "BUYER_ACCOUNT.ACCOUNT_SELLER_LEVEL",
      "is_dimension": false
    },
    {
      "id": 24,
      "name": "BUYER_COUNTRY",
      "column": "BUYER_ACCOUNT.ACCOUNT_COUNTRY",
      "is_dimension": false
    },
    {
      "id": 25,
      "name": "BUYER_COUNTRY_NAME",
      "column": "BUYER_COUNTRY.NAME",
      "is_dimension": false
    },
    {
      "id": 26,
      "name": "TEST_COUNT_DISTINCT_BITMAP",
      "column": "TEST_KYLIN_FACT.TEST_COUNT_DISTINCT_BITMAP",
      "is_dimension": false
    }
  ],
  "all_measures": [{
      "name": "TRANS_CNT",
      "function": {
        "expression": "COUNT",
        "parameter": {
          "type": "constant",
          "value": "1"
        },
        "returntype": "bigint"
      },
      "id": 1000
    },
    {
      "name": "GMV_SUM",
      "function": {
        "expression": "SUM",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE"
        },
        "returntype": "decimal(19,4)"
      },
      "id": 1001
    },
    {
      "name": "GMV_MIN",
      "function": {
        "expression": "MIN",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE"
        },
        "returntype": "decimal(19,4)"
      },
      "id": 1002
    },
    {
      "name": "GMV_MAX",
      "function": {
        "expression": "MAX",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE"
        },
        "returntype": "decimal(19,4)"
      },
      "id": 1003
    },
    {
      "name": "ITEM_COUNT_SUM",
      "function": {
        "expression": "SUM",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.ITEM_COUNT"
        },
        "returntype": "bigint"
      },
      "id": 1004
    },
    {
      "name": "ITEM_COUNT_MAX",
      "function": {
        "expression": "MAX",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.ITEM_COUNT"
        },
        "returntype": "bigint"
      },
      "id": 1005
    },
    {
      "name": "ITEM_COUNT_MIN",
      "function": {
        "expression": "MIN",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.ITEM_COUNT"
        },
        "returntype": "bigint"
      },
      "id": 1006,
      "tomb": true
    },
    {
      "name": "SELLER_HLL",
      "function": {
        "expression": "COUNT_DISTINCT",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.SELLER_ID"
        },
        "returntype": "hllc(10)"
      },
      "id": 1007
    },
    {
      "name": "COUNT_DISTINCT",
      "function": {
        "expression": "COUNT_DISTINCT",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.LSTG_FORMAT_NAME"
        },
        "returntype": "hllc(10)"
      },
      "id": 1008
    },
    {
      "name": "TOP_SELLER",
      "function": {
        "expression": "TOP_N",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE",
          "next_parameter": {
            "type": "column",
            "value": "TEST_KYLIN_FACT.SELLER_ID"
          }
        },
        "returntype": "topn(100, 4)",
        "configuration": {
          "topn.encoding.TEST_KYLIN_FACT.SELLER_ID": "int:4"
        }
      },
      "id": 1009
    },
    {
      "name": "TEST_COUNT_DISTINCT_BITMAP",
      "function": {
        "expression": "COUNT_DISTINCT",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.TEST_COUNT_DISTINCT_BITMAP"
        },
        "returntype": "bitmap"
      },
      "id": 1010
    },
    {
      "name": "GVM_PERCENTILE",
      "function": {
        "expression": "PERCENTILE_APPROX",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE"
        },
        "returntype": "percentile(100)"
      },
      "id": 1011
    }
  ],
  "column_correlations": [{
      "name": "CATEGORY_HIERARCHY",
      "correlation_type": "hierarchy",
      "columns": [
        "TEST_CATEGORY_GROUPINGS.META_CATEG_NAME",
        "TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME",
        "TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME"
      ]
    },
    {
      "name": "DATE_HIERARCHY",
      "correlation_type": "hierarchy",
      "columns": [
        "TEST_CAL_DT.WEEK_BEG_DT",
        "TEST_KYLIN_FACT.CAL_DT"
      ]
    },
    {
      "name": "SITE_JOINT",
      "correlation_type": "joint",
      "columns": [
        "TEST_KYLIN_FACT.LSTG_SITE_ID",
        "TEST_SITES.SITE_NAME"
      ]
    }
  ],
  "multilevel_partition_cols": [],
  "computed_columns": [],
  "status": "READY",
  "segment_ranges": {},
  "simple_tables": [{
      "table": "DEFAULT.TEST_KYLIN_FACT",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar"
      }
    },
    {
      "table": "DEFAULT.TEST_ORDER",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar"

      }
    },
    {
      "table": "EDW.TEST_SELLER_TYPE_DIM",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint"

      }
    },
    {
      "table": "EDW.TEST_CAL_DT",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint",
        "DIM_CRE_USER": "varchar",
        "WEEK_BEG_DT": "date"

      }
    },
    {
      "table": "DEFAULT.TEST_CATEGORY_GROUPINGS",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint"

      }
    },
    {
      "table": "EDW.TEST_SITES",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint",
        "DIM_CRE_USER": "varchar",
        "WEEK_BEG_DT": "date",
        "CAL_DT_MNS_1QTR_DT": "varchar",
        "WEEK_IN_YEAR_ID": "varchar",
        "CATEG_LVL2_NAME": "varchar",
        "MONTH_BEG_DT": "date"

      }
    },
    {
      "table": "DEFAULT.TEST_ACCOUNT",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint",
        "DIM_CRE_USER": "varchar",
        "WEEK_BEG_DT": "date"

      }
    },
    {
      "table": "DEFAULT.TEST_ACCOUNT",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint"

      }
    },
    {
      "table": "DEFAULT.TEST_COUNTRY",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer"

      }
    },
    {
      "table": "DEFAULT.TEST_COUNTRY",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint"

      }
    }
  ]
}