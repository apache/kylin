--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--


USE TPCDS;

-- CREATE EXTERNAL TABLE IF NOT EXISTS dbgen_version
-- (
--     dv_version      varchar(16),
--     dv_create_date  date,
--     dv_create_time time,
--     dv_cmdline_args varchar(200)
-- )
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
-- STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS customer_address
(
    ca_address_sk    int ,
    ca_address_id    char(16),
    ca_street_number char(10),
    ca_street_name   varchar(60),
    ca_street_type   char(15),
    ca_suite_number  char(10),
    ca_city          varchar(60),
    ca_county        varchar(30),
    ca_state         char(2),
    ca_zip           char(10),
    ca_country       varchar(20),
    ca_gmt_offset    decimal(5, 2),
    ca_location_type char(20)
--     primary key (ca_address_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS customer_demographics
(
    cd_demo_sk            int,
    cd_gender             char(1),
    cd_marital_status     char(1),
    cd_education_status   char(20),
    cd_purchase_estimate  int,
    cd_credit_rating      char(10),
    cd_dep_count          int,
    cd_dep_employed_count int,
    cd_dep_college_count  int
--     primary key (cd_demo_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS date_dim
(
    d_date_sk           int ,
    d_date_id           char(16),
    d_date              date,
    d_month_seq         int,
    d_week_seq          int,
    d_quarter_seq       int,
    d_year              int,
    d_dow               int,
    d_moy               int,
    d_dom               int,
    d_qoy               int,
    d_fy_year           int,
    d_fy_quarter_seq    int,
    d_fy_week_seq       int,
    d_day_name          char(9),
    d_quarter_name      char(6),
    d_holiday           char(1),
    d_weekend           char(1),
    d_following_holiday char(1),
    d_first_dom         int,
    d_last_dom          int,
    d_same_day_ly       int,
    d_same_day_lq       int,
    d_current_day       char(1),
    d_current_week      char(1),
    d_current_month     char(1),
    d_current_quarter   char(1),
    d_current_year      char(1)
--     primary key (d_date_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse
(
    w_warehouse_sk    int ,
    w_warehouse_id    char(16),
    w_warehouse_name  varchar(20),
    w_warehouse_sq_ft int,
    w_street_number   char(10),
    w_street_name     varchar(60),
    w_street_type     char(15),
    w_suite_number    char(10),
    w_city            varchar(60),
    w_county          varchar(30),
    w_state           char(2),
    w_zip             char(10),
    w_country         varchar(20),
    w_gmt_offset      decimal(5, 2)
--     primary key (w_warehouse_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS ship_mode
(
    sm_ship_mode_sk int ,
    sm_ship_mode_id char(16),
    sm_type         char(30),
    sm_code         char(10),
    sm_carrier      char(20),
    sm_contract     char(20)
--     primary key (sm_ship_mode_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS time_dim
(
    t_time_sk   int ,
    t_time_id   char(16),
    t_time      int,
    t_hour      int,
    t_minute    int,
    t_second    int,
    t_am_pm     char(2),
    t_shift     char(20),
    t_sub_shift char(20),
    t_meal_time char(20)
--     primary key (t_time_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS reason
(
    r_reason_sk   int ,
    r_reason_id   char(16),
    r_reason_desc char(100)
--     primary key (r_reason_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS income_band
(
    ib_income_band_sk int,
    ib_lower_bound    int,
    ib_upper_bound    int
--     primary key (ib_income_band_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS item
(
    i_item_sk        int ,
    i_item_id        char(16),
    i_rec_start_date date,
    i_rec_end_date   date,
    i_item_desc      varchar(200),
    i_current_price  decimal(7, 2),
    i_wholesale_cost decimal(7, 2),
    i_brand_id       int,
    i_brand          char(50),
    i_class_id       int,
    i_class          char(50),
    i_category_id    int,
    i_category       char(50),
    i_manufact_id    int,
    i_manufact       char(50),
    i_size           char(20),
    i_formulation    char(20),
    i_color          char(20),
    i_units          char(10),
    i_container      char(10),
    i_manager_id     int,
    i_product_name   char(50)
--     primary key (i_item_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS store
(
    s_store_sk         int ,
    s_store_id         char(16),
    s_rec_start_date   date,
    s_rec_end_date     date,
    s_closed_date_sk   int,
    s_store_name       varchar(50),
    s_number_employees int,
    s_floor_space      int,
    s_hours            char(20),
    s_manager          varchar(40),
    s_market_id        int,
    s_geography_class  varchar(100),
    s_market_desc      varchar(100),
    s_market_manager   varchar(40),
    s_division_id      int,
    s_division_name    varchar(50),
    s_company_id       int,
    s_company_name     varchar(50),
    s_street_number    varchar(10),
    s_street_name      varchar(60),
    s_street_type      char(15),
    s_suite_number     char(10),
    s_city             varchar(60),
    s_county           varchar(30),
    s_state            char(2),
    s_zip              char(10),
    s_country          varchar(20),
    s_gmt_offset       decimal(5, 2),
    s_tax_precentage   decimal(5, 2)
--     primary key (s_store_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS call_center
(
    cc_call_center_sk int ,
    cc_call_center_id char(16),
    cc_rec_start_date date,
    cc_rec_end_date   date,
    cc_closed_date_sk int,
    cc_open_date_sk   int,
    cc_name           varchar(50),
    cc_class          varchar(50),
    cc_employees      int,
    cc_sq_ft          int,
    cc_hours          char(20),
    cc_manager        varchar(40),
    cc_mkt_id         int,
    cc_mkt_class      char(50),
    cc_mkt_desc       varchar(100),
    cc_market_manager varchar(40),
    cc_division       int,
    cc_division_name  varchar(50),
    cc_company        int,
    cc_company_name   char(50),
    cc_street_number  char(10),
    cc_street_name    varchar(60),
    cc_street_type    char(15),
    cc_suite_number   char(10),
    cc_city           varchar(60),
    cc_county         varchar(30),
    cc_state          char(2),
    cc_zip            char(10),
    cc_country        varchar(20),
    cc_gmt_offset     decimal(5, 2),
    cc_tax_percentage decimal(5, 2)
--     primary key (cc_call_center_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS customer
(
    c_customer_sk          int ,
    c_customer_id          char(16),
    c_current_cdemo_sk     int,
    c_current_hdemo_sk     int,
    c_current_addr_sk      int,
    c_first_shipto_date_sk int,
    c_first_sales_date_sk  int,
    c_salutation           char(10),
    c_first_name           char(20),
    c_last_name            char(30),
    c_preferred_cust_flag  char(1),
    c_birth_day            int,
    c_birth_month          int,
    c_birth_year           int,
    c_birth_country        varchar(20),
    c_login                char(13),
    c_email_address        char(50),
    c_last_review_date_sk  int
--     primary key (c_customer_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS web_site
(
    web_site_sk        int ,
    web_site_id        char(16),
    web_rec_start_date date,
    web_rec_end_date   date,
    web_name           varchar(50),
    web_open_date_sk   int,
    web_close_date_sk  int,
    web_class          varchar(50),
    web_manager        varchar(40),
    web_mkt_id         int,
    web_mkt_class      varchar(50),
    web_mkt_desc       varchar(100),
    web_market_manager varchar(40),
    web_company_id     int,
    web_company_name   char(50),
    web_street_number  char(10),
    web_street_name    varchar(60),
    web_street_type    char(15),
    web_suite_number   char(10),
    web_city           varchar(60),
    web_county         varchar(30),
    web_state          char(2),
    web_zip            char(10),
    web_country        varchar(20),
    web_gmt_offset     decimal(5, 2),
    web_tax_percentage decimal(5, 2)
--     primary key (web_site_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS store_returns
(
    sr_returned_date_sk   int,
    sr_return_time_sk     int,
    sr_item_sk            int,
    sr_customer_sk        int,
    sr_cdemo_sk           int,
    sr_hdemo_sk           int,
    sr_addr_sk            int,
    sr_store_sk           int,
    sr_reason_sk          int,
    sr_ticket_number      int,
    sr_return_quantity    int,
    sr_return_amt         decimal(7, 2),
    sr_return_tax         decimal(7, 2),
    sr_return_amt_inc_tax decimal(7, 2),
    sr_fee                decimal(7, 2),
    sr_return_ship_cost   decimal(7, 2),
    sr_refunded_cash      decimal(7, 2),
    sr_reversed_charge    decimal(7, 2),
    sr_store_credit       decimal(7, 2),
    sr_net_loss           decimal(7, 2)
--     primary key (sr_item_sk, sr_ticket_number)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS household_demographics
(
    hd_demo_sk        int,
    hd_income_band_sk int,
    hd_buy_potential  char(15),
    hd_dep_count      int,
    hd_vehicle_count  int
--     primary key (hd_demo_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS web_page
(
    wp_web_page_sk      int ,
    wp_web_page_id      char(16),
    wp_rec_start_date   date,
    wp_rec_end_date     date,
    wp_creation_date_sk int,
    wp_access_date_sk   int,
    wp_autogen_flag     char(1),
    wp_customer_sk      int,
    wp_url              varchar(100),
    wp_type             char(50),
    wp_char_count       int,
    wp_link_count       int,
    wp_image_count      int,
    wp_max_ad_count     int
--     primary key (wp_web_page_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS promotion
(
    p_promo_sk        int ,
    p_promo_id        char(16),
    p_start_date_sk   int,
    p_end_date_sk     int,
    p_item_sk         int,
    p_cost            decimal(15, 2),
    p_response_target int,
    p_promo_name      char(50),
    p_channel_dmail   char(1),
    p_channel_email   char(1),
    p_channel_catalog char(1),
    p_channel_tv      char(1),
    p_channel_radio   char(1),
    p_channel_press   char(1),
    p_channel_event   char(1),
    p_channel_demo    char(1),
    p_channel_details varchar(100),
    p_purpose         char(15),
    p_discount_active char(1)
--     primary key (p_promo_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS catalog_page
(
    cp_catalog_page_sk     int ,
    cp_catalog_page_id     char(16),
    cp_start_date_sk       int,
    cp_end_date_sk         int,
    cp_department          varchar(50),
    cp_catalog_number      int,
    cp_catalog_page_number int,
    cp_description         varchar(100),
    cp_type                varchar(100)
--     primary key (cp_catalog_page_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS inventory
(
    inv_date_sk          int,
    inv_item_sk          int,
    inv_warehouse_sk     int,
    inv_quantity_on_hand int
--     primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS catalog_returns
(
    cr_returned_date_sk      int,
    cr_returned_time_sk      int,
    cr_item_sk               int,
    cr_refunded_customer_sk  int,
    cr_refunded_cdemo_sk     int,
    cr_refunded_hdemo_sk     int,
    cr_refunded_addr_sk      int,
    cr_returning_customer_sk int,
    cr_returning_cdemo_sk    int,
    cr_returning_hdemo_sk    int,
    cr_returning_addr_sk     int,
    cr_call_center_sk        int,
    cr_catalog_page_sk       int,
    cr_ship_mode_sk          int,
    cr_warehouse_sk          int,
    cr_reason_sk             int,
    cr_order_number          int,
    cr_return_quantity       int,
    cr_return_amount         decimal(7, 2),
    cr_return_tax            decimal(7, 2),
    cr_return_amt_inc_tax    decimal(7, 2),
    cr_fee                   decimal(7, 2),
    cr_return_ship_cost      decimal(7, 2),
    cr_refunded_cash         decimal(7, 2),
    cr_reversed_charge       decimal(7, 2),
    cr_store_credit          decimal(7, 2),
    cr_net_loss              decimal(7, 2)
--     primary key (cr_item_sk, cr_order_number)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS web_returns
(
    wr_returned_date_sk      int,
    wr_returned_time_sk      int,
    wr_item_sk               int,
    wr_refunded_customer_sk  int,
    wr_refunded_cdemo_sk     int,
    wr_refunded_hdemo_sk     int,
    wr_refunded_addr_sk      int,
    wr_returning_customer_sk int,
    wr_returning_cdemo_sk    int,
    wr_returning_hdemo_sk    int,
    wr_returning_addr_sk     int,
    wr_web_page_sk           int,
    wr_reason_sk             int,
    wr_order_number          int,
    wr_return_quantity       int,
    wr_return_amt            decimal(7, 2),
    wr_return_tax            decimal(7, 2),
    wr_return_amt_inc_tax    decimal(7, 2),
    wr_fee                   decimal(7, 2),
    wr_return_ship_cost      decimal(7, 2),
    wr_refunded_cash         decimal(7, 2),
    wr_reversed_charge       decimal(7, 2),
    wr_account_credit        decimal(7, 2),
    wr_net_loss              decimal(7, 2)
--     primary key (wr_item_sk, wr_order_number)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS web_sales
(
    ws_sold_date_sk          int,
    ws_sold_time_sk          int,
    ws_ship_date_sk          int,
    ws_item_sk               int,
    ws_bill_customer_sk      int,
    ws_bill_cdemo_sk         int,
    ws_bill_hdemo_sk         int,
    ws_bill_addr_sk          int,
    ws_ship_customer_sk      int,
    ws_ship_cdemo_sk         int,
    ws_ship_hdemo_sk         int,
    ws_ship_addr_sk          int,
    ws_web_page_sk           int,
    ws_web_site_sk           int,
    ws_ship_mode_sk          int,
    ws_warehouse_sk          int,
    ws_promo_sk              int,
    ws_order_number          int,
    ws_quantity              int,
    ws_wholesale_cost        decimal(7, 2),
    ws_list_price            decimal(7, 2),
    ws_sales_price           decimal(7, 2),
    ws_ext_discount_amt      decimal(7, 2),
    ws_ext_sales_price       decimal(7, 2),
    ws_ext_wholesale_cost    decimal(7, 2),
    ws_ext_list_price        decimal(7, 2),
    ws_ext_tax               decimal(7, 2),
    ws_coupon_amt            decimal(7, 2),
    ws_ext_ship_cost         decimal(7, 2),
    ws_net_paid              decimal(7, 2),
    ws_net_paid_inc_tax      decimal(7, 2),
    ws_net_paid_inc_ship     decimal(7, 2),
    ws_net_paid_inc_ship_tax decimal(7, 2),
    ws_net_profit            decimal(7, 2)
--     primary key (ws_item_sk, ws_order_number)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS catalog_sales
(
    cs_sold_date_sk          int,
    cs_sold_time_sk          int,
    cs_ship_date_sk          int,
    cs_bill_customer_sk      int,
    cs_bill_cdemo_sk         int,
    cs_bill_hdemo_sk         int,
    cs_bill_addr_sk          int,
    cs_ship_customer_sk      int,
    cs_ship_cdemo_sk         int,
    cs_ship_hdemo_sk         int,
    cs_ship_addr_sk          int,
    cs_call_center_sk        int,
    cs_catalog_page_sk       int,
    cs_ship_mode_sk          int,
    cs_warehouse_sk          int,
    cs_item_sk               int,
    cs_promo_sk              int,
    cs_order_number          int,
    cs_quantity              int,
    cs_wholesale_cost        decimal(7, 2),
    cs_list_price            decimal(7, 2),
    cs_sales_price           decimal(7, 2),
    cs_ext_discount_amt      decimal(7, 2),
    cs_ext_sales_price       decimal(7, 2),
    cs_ext_wholesale_cost    decimal(7, 2),
    cs_ext_list_price        decimal(7, 2),
    cs_ext_tax               decimal(7, 2),
    cs_coupon_amt            decimal(7, 2),
    cs_ext_ship_cost         decimal(7, 2),
    cs_net_paid              decimal(7, 2),
    cs_net_paid_inc_tax      decimal(7, 2),
    cs_net_paid_inc_ship     decimal(7, 2),
    cs_net_paid_inc_ship_tax decimal(7, 2),
    cs_net_profit            decimal(7, 2)
--    primary key (cs_item_sk, cs_order_number)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

-- IF NOT EXISTS
CREATE EXTERNAL TABLE IF NOT EXISTS store_sales
(
    ss_sold_date_sk       int,
    ss_sold_time_sk       int,
    ss_item_sk            int,
    ss_customer_sk        int,
    ss_cdemo_sk           int,
    ss_hdemo_sk           int,
    ss_addr_sk            int,
    ss_store_sk           int,
    ss_promo_sk           int,
    ss_ticket_number      int,
    ss_quantity           int,
    ss_wholesale_cost     decimal(7, 2),
    ss_list_price         decimal(7, 2),
    ss_sales_price        decimal(7, 2),
    ss_ext_discount_amt   decimal(7, 2),
    ss_ext_sales_price    decimal(7, 2),
    ss_ext_wholesale_cost decimal(7, 2),
    ss_ext_list_price     decimal(7, 2),
    ss_ext_tax            decimal(7, 2),
    ss_coupon_amt         decimal(7, 2),
    ss_net_paid           decimal(7, 2),
    ss_net_paid_inc_tax   decimal(7, 2),
    ss_net_profit         decimal(7, 2)
--     primary key (ss_item_sk, ss_ticket_number)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;


LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/customer_address.dat' OVERWRITE INTO TABLE customer_address;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/customer_demographics.dat' OVERWRITE INTO TABLE customer_demographics;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/date_dim.dat' OVERWRITE INTO TABLE date_dim;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/warehouse.dat' OVERWRITE INTO TABLE warehouse;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/ship_mode.dat' OVERWRITE INTO TABLE ship_mode;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/time_dim.dat' OVERWRITE INTO TABLE time_dim;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/reason.dat' OVERWRITE INTO TABLE reason;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/income_band.dat' OVERWRITE INTO TABLE income_band;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/item.dat' OVERWRITE INTO TABLE item;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/store.dat' OVERWRITE INTO TABLE store;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/call_center.dat' OVERWRITE INTO TABLE call_center;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/customer.dat' OVERWRITE INTO TABLE customer;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/web_site.dat' OVERWRITE INTO TABLE web_site;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/store_returns.dat' OVERWRITE INTO TABLE store_returns;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/household_demographics.dat' OVERWRITE INTO TABLE household_demographics;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/web_page.dat' OVERWRITE INTO TABLE web_page;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/promotion.dat' OVERWRITE INTO TABLE promotion;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/catalog_page.dat' OVERWRITE INTO TABLE catalog_page;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/inventory.dat' OVERWRITE INTO TABLE inventory;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/catalog_returns.dat' OVERWRITE INTO TABLE catalog_returns;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/web_returns.dat' OVERWRITE INTO TABLE web_returns;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/web_sales.dat' OVERWRITE INTO TABLE web_sales;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/catalog_sales.dat' OVERWRITE INTO TABLE catalog_sales;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/tpcds/data/store_sales.dat' OVERWRITE INTO TABLE store_sales;
