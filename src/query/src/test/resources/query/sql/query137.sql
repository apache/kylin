-- KE14922 support WVARCHAR, SQL_WVARCHAR by calcite

select
    cast( "LSTG_FORMAT_NAME" as WVARCHAR ),
    { fn convert( "LSTG_FORMAT_NAME", WVARCHAR ) } as LSTG_FORMAT_NAME2,
    { fn convert( "LSTG_FORMAT_NAME", SQL_WVARCHAR ) } as LSTG_FORMAT_NAME3
from test_kylin_fact