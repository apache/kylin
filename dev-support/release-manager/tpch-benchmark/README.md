# Modeling of Kylin 5.0.0 for TPC-H

---

# Background

### What is TPC-H ?

![TPC-H Schema](resources/TPCH-Schema.png)

### Query 1 : Pricing Summary Report Query

#### Original Version


```sql
/**
 * TPC-H Query 1 : Pricing Summary Report Query / 价格统计报告查询
 * 在单个表lineitem上查询某个时间段内，对已经付款的、已经运送的等各类商品进行统计，包括业务量的计费、发货、折扣、税、平均价格等信息。
 * 带有分组、排序、聚集操作并存的单表查询操作。这个查询会导致表上的数据有95%到97%行被读取到。
 */
select /*+ MODEL_PRIORITY(Lineitem) */
       l_returnflag,
       l_linestatus,
       sum(l_quantity)                                       as sum_qty,
       sum(l_extendedprice)                                  as sum_base_price,
       sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       avg(l_quantity)                                       as avg_qty,
       avg(l_extendedprice)                                  as avg_price,
       avg(l_discount)                                       as avg_disc,
       count(*)                                              as count_order
from tpch_cn.lineitem
where l_shipdate <= '1998-12-01' - interval '90' day -- DELTA is randomly selected within [60. 120].
group by l_returnflag,
         l_linestatus
order by l_returnflag,
         l_linestatus;
```

#### Modified Version

```sql
select l_returnflag,
       l_linestatus,
       sum(l_quantity)                                       as sum_qty,
       sum(l_extendedprice)                                  as sum_base_price,
       sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       avg(l_quantity)                                       as avg_qty,
       avg(l_extendedprice)                                  as avg_price,
       avg(l_discount)                                       as avg_disc,
       count(*)                                              as count_order
from tpch_cn.lineitem
where l_shipdate <= '1998-09-01'
group by l_returnflag,
         l_linestatus
order by l_returnflag,
         l_linestatus;
```


### Query 2 : Minimum Cost Supplier Query

#### Original Version

```sql
/**
 * TPC-H Query 2 : Minimum Cost Supplier Query / 最小代价供货商查询
 * Return the first 100 selected rows
 * 得到给定的区域内，对于指定的零件（某一类型和大小的零件），哪个供应者能以最低的价格供应它，就可以选择哪个供应者来订货。
 * 带有排序、聚集操作、子查询并存的多表查询操作。
 */
select s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
from tpch_cn.part,
     tpch_cn.supplier,
     tpch_cn.partsupp,
     tpch_cn.nation,
     tpch_cn.region
where p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15          -- SIZE is randomly selected within [1. 50];
  and p_type like '%BRASS' -- TYPE is randomly selected within the list Syllable 3 defined for Types in Clause 4.2.2.13;
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'    -- REGION is randomly selected within the list of values defined for R_NAME in 4.2.3.
  and ps_supplycost
    = ( -- 关连子查询
          select min(ps_supplycost)
          from tpch_cn.partsupp,
               tpch_cn.supplier,
               tpch_cn.nation,
               tpch_cn.region
          where p_partkey = ps_partkey -- p_partkey 是外部的 Join Key
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'EUROPE')
order by s_acctbal desc,
         n_name,
         s_name, 
         p_partkey
limit 100;
```

#### Modified Version
```sql
select s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
from
    tpch_cn.partsupp,
    tpch_cn.part,
    tpch_cn.supplier,
    tpch_cn.nation,
    tpch_cn.region
where p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15    
  and p_type like '%BRASS'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
  and ps_supplycost
    = (
          select min(ps_supplycost)
          from tpch_cn.partsupp,
               tpch_cn.supplier,
               tpch_cn.nation,
               tpch_cn.region
          where p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'EUROPE')
order by s_acctbal desc,
         n_name,
         s_name,
         p_partkey
limit 100;

select min(ps_supplycost)
from tpch_cn.partsupp,
     tpch_cn.supplier,
     tpch_cn.nation,
     tpch_cn.region
where s_suppkey = ps_suppkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE';

select s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
from
    tpch_cn.partsupp,
    tpch_cn.part,
    tpch_cn.supplier,
    tpch_cn.nation,
    tpch_cn.region
where p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15
  and p_type like '%BRASS'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
order by s_acctbal desc,
         n_name,
         s_name,
         p_partkey
limit 100;
  
```


### Query 3

#### Original Version

```sql
/**
 * TPC-H Query 3 : Shipping Priority Query / 运送优先级查询
 * Return the first 10 selected rows
 * 查询得到收入在前10位的尚未运送的订单。在指定的日期之前还没有运送的订单中具有最大收入的订单的运送优先级（订单按照收入的降序排序）和潜在的收入（潜在的收入为l_extendedprice * (1-l_discount)的和）。
 * 带有分组、排序、聚集操作并存的三表查询操作。
 */
select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate,
       o_shippriority
from tpch_cn.customer,
     tpch_cn.orders,
     tpch_cn.lineitem
where c_mktsegment = 'BUILDING'  -- SEGMENT is randomly selected within the list of values defined for Segments in Clause 4.2.2.13;
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < '1995-03-15' -- DATE is a randomly selected day within [1995-03-01 .. 1995-03-31].
  and l_shipdate > '1995-03-15'
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc, o_orderdate
limit 10;
```


```sql
select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate,
       o_shippriority
from tpch_cn.lineitem 
        join tpch_cn.orders on  l_orderkey = o_orderkey
        join tpch_cn.customer on c_custkey = o_custkey
where c_mktsegment = 'BUILDING'  -- SEGMENT is randomly selected within the list of values defined for Segments in Clause 4.2.2.13;
  and o_orderdate < '1995-03-15' -- DATE is a randomly selected day within [1995-03-01 .. 1995-03-31].
  and l_shipdate > '1995-03-15'
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc, o_orderdate
limit 10;
```


### Query 4

#### Original Version

```sql
/**
 * TPC-H Query 4 : Order Priority Checking Query / 订单优先级查询
 * 查询得到订单优先级统计值。计算给定的某三个月的订单的数量，在每个订单中至少有一行由顾客在它的提交日期之后收到。
 * 带有分组、排序、聚集操作、子查询并存的单表查询操作。子查询是相关子查询。
 */
select o_orderpriority,
       count(*) as order_count
from tpch_cn.orders
where o_orderdate >= '1993-07-01' -- DATE is the first day of a randomly selected month between the first month of 1993 and the 10th month of 1997.
  and o_orderdate < DATE '1993-07-01' + interval '3' month
  and exists ( -- exists 子查询
    select *
    from tpch_cn.lineitem
    where l_orderkey = o_orderkey
      and l_commitdate < l_receiptdate
    )
group by o_orderpriority
order by o_orderpriority; 
```

### Query 5

#### Original Version

```sql
/**
 * TPC-H Query 5 : Local Supplier Volume Query / 某地区供货商为公司带来的收入查询
 * 通过某个地区零件供货商而获得的收入（收入按sum(l_extendedprice * (1 -l_discount))计算）统计信息。可用于决定在给定的区域是否需要建立一个当地分配中心
 * 带有分组、排序、聚集操作、子查询并存的多表连接查询操作。
 */
select n_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue
from tpch_cn.customer,
     tpch_cn.orders,
     tpch_cn.lineitem,
     tpch_cn.supplier,
     tpch_cn.nation,
     tpch_cn.region
where c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey  -- 这里形成一个环， Kylin 不支持这样的 Join Graph
  and s_nationkey = n_nationkey 
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'                  -- REGION is randomly selected within the list of values defined for R_NAME in C;aise 4.2.3;
  and o_orderdate >= date '1994-01-01' -- DATE is the first of January of a randomly selected year within [1993 .. 1997].
  and o_orderdate < date '1994-01-01' + interval '1' year
group by n_name
order by revenue desc;
```

```sql
select n1.n_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue
from tpch_cn.lineitem 
    join tpch_cn.orders on l_orderkey = o_orderkey
    join tpch_cn.customer on o_custkey = c_custkey
    join tpch_cn.supplier on l_suppkey = s_suppkey
    join tpch_cn.nation as n1 on s_nationkey = n1.n_nationkey
    join tpch_cn.region as r1 on n1.n_regionkey = r1.r_regionkey
    join tpch_cn.nation as n2 on c_nationkey = n2.n_nationkey
    join tpch_cn.region as r2 on n2.n_regionkey = r2.r_regionkey
where 
    r1.r_name = 'ASIA'              
    and o_orderdate >= date '1994-01-01' 
    and o_orderdate < date '1995-01-01' 
group by n1.n_name
order by revenue desc;
```


### Query 6

#### Original Version

```sql
/**
 * TPC-H Query 6 : Forecasting Revenue Change Query / 预测收入变化查询
 * 得到某一年中通过变换折扣带来的增量收入。这是典型的“what-if”判断，用来寻找增加收入的途径。
 * 预测收入变化查询考虑了指定的一年中折扣在“DISCOUNT-0.01”和“DISCOUNT＋0.01”之间的已运送的所有订单，求解把l_quantity小于quantity的订单的折扣消除之后总收入增加的数量。
 * 带有聚集操作的单表查询操作。查询语句使用了BETWEEN-AND操作符，有的数据库可以对BETWEEN-AND进行优化。
 */
select sum(l_extendedprice * l_discount) as revenue -- 潜在的收入增加量
from tpch_cn.lineitem
where l_shipdate >= '1994-01-01'                     -- DATE is the first of January of a randomly selected year within [1993 .. 1997];
  and l_shipdate < DATE '1994-01-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01 -- DISCOUNT is randomly selected within [0.02 .. 0.09];
  and l_quantity < 24 -- QUANTITY is randomly selected within [24 .. 25].
;
```

```sql
select sum(l_extendedprice * l_discount) as revenue -- 潜在的收入增加量
from tpch_cn.lineitem
where l_shipdate >= '1994-01-01'                     -- DATE is the first of January of a randomly selected year within [1993 .. 1997];
  and l_shipdate < DATE '1995-01-01'
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01 
  and l_quantity < 24
;
```


### Query 7(Skipped)

#### Original Version

```sql
/**
 * TPC-H Query 7 : Volume Shipping Query / 货运盈利情况查询
 * 从供货商国家与销售商品的国家之间通过销售获利情况的查询。此查询确定在两国之间货运商品的量用以帮助重新谈判货运合同。
 * 带有分组、排序、聚集、子查询操作并存的多表查询操作。子查询的父层查询不存在其他查询对象，是格式相对简单的子查询。
 */
select supp_nation,
       cust_nation,
       l_year,
       sum(volume) as revenue
from ( -- 子查询
         select n1.n_name                          as supp_nation,
                n2.n_name                          as cust_nation,
                l_shipdate                         as l_year,
                l_extendedprice * (1 - l_discount) as volume
         from tpch_cn.supplier,
              tpch_cn.lineitem,
              tpch_cn.orders,
              tpch_cn.customer,
              tpch_cn.nation n1,
              tpch_cn.nation n2
         where s_suppkey = l_suppkey
           and o_orderkey = l_orderkey
           and c_custkey = o_custkey
           and s_nationkey = n1.n_nationkey
           and c_nationkey = n2.n_nationkey
           and (
                 (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') -- NATION1 is randomly selected within the list of values defined for N_NAME in Clause 4.2.3;
                 or
                 (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') -- NATION2 is randomly selected within the list of values defined for N_NAME in Clause 4.2.3 and must be dif- ferent from the value selected for NATION1 in item 1 above.
             )
           and l_shipdate between '1995-01-01' and '1996-12-31'
    ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year;
```

### Query 8

#### Original Version

```sql
/**
 * TPC-H Query 8 : National Market Share Query / 国家市场份额查询
 * 在过去的两年中一个给定零件类型在某国某地区市场份额的变化情况。
 * 带有分组、排序、聚集、子查询操作并存的查询操作。子查询的父层查询不存在其他查询对象，是格式相对简单的子查询，但子查询自身是多表连接的查询。
 */
select o_year,
       sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) -- NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3;
           as mkt_share
from ( -- 子查询
         select year(o_orderdate)                  as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name                          as nation
         from tpch_cn.part,
              tpch_cn.supplier,
              tpch_cn.lineitem,
              tpch_cn.orders,
              tpch_cn.customer,
              tpch_cn.nation n1,
              tpch_cn.nation n2,
              tpch_cn.region
         where p_partkey = l_partkey
           and s_suppkey = l_suppkey
           and l_orderkey = o_orderkey
           and o_custkey = c_custkey
           and c_nationkey = n1.n_nationkey
           and n1.n_regionkey = r_regionkey
           and r_name = 'AMERICA' -- REGION is the value defined in Clause 4.2.3 for R_NAME where R_REGIONKEY corresponds to N_REGIONKEY for the selected NATION in item 1 above;
           and s_nationkey = n2.n_nationkey
           and o_orderdate between '1995-01-01' and '1996-12-31'
           and p_type = 'ECONOMY ANODIZED STEEL' -- TYPE is randomly selected within the list of 3-syllable strings defined for Types in Clause 4.2.2.13.
    ) as all_nations 
group by o_year
order by o_year;
```


### Query 9

#### Original Version
```sql
/**
 * TPC-H Query 9 : Product Type Profit Measure Query / 产品类型利润估量查询
 * 查询每个国家每一年所有被定购的零件在一年中的总利润。
 * 带有分组、排序、聚集、子查询操作并存的查询操作。子查询的父层查询不存在其他查询对象，是格式相对简单的子查询，但子查询自身是多表连接的查询。
 * 子查询中使用了LIKE操作符，有的查询优化器不支持对LIKE操作符进行优化。
 */
select nation,
       o_year,
       sum(amount) as sum_profit
from ( -- 子查询
         select n_name                                                          as nation,
                year(o_orderdate)                                               as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
         from tpch_cn.part,
              tpch_cn.supplier,
              tpch_cn.lineitem,
              tpch_cn.partsupp,
              tpch_cn.orders,
              tpch_cn.nation
         where s_suppkey = l_suppkey
           and ps_suppkey = l_suppkey
           and ps_partkey = l_partkey
           and p_partkey = l_partkey
           and o_orderkey = l_orderkey
           and s_nationkey = n_nationkey
           and p_name like '%green%') as profit -- COLOR is randomly selected within the list of values defined for the generation of P_NAME in Clause 4.2.3.
group by nation,
         o_year
order by nation,
         o_year
        desc;
```

#### Modified Version

```sql
 select n_name as nation,
        year(o_orderdate)  as o_year,
        sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as amount_profit
 from tpch_cn.part,
      tpch_cn.supplier,
      tpch_cn.lineitem,
      tpch_cn.partsupp,
      tpch_cn.orders,
      tpch_cn.nation
 where s_suppkey = l_suppkey
   and ps_suppkey = l_suppkey
   and ps_partkey = l_partkey
   and p_partkey = l_partkey
   and o_orderkey = l_orderkey
   and s_nationkey = n_nationkey
   and p_name like '%green%'
group by n_name, year(o_orderdate)
order by n_name, year(o_orderdate);
```


### Query 10

#### Original Version
```sql
/**
 * TPC-H Query 10 : Returned Item Reporting Query / 货运存在问题的查询
 * The Returned Item Reporting Query finds the top 20 customers
 * 每个国家在某时刻起的三个月内货运存在问题的客户和造成的损失。
 * 带有分组、排序、聚集操作并存的多表连接查询操作。
 */
select c_custkey,
       c_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal,
       n_name,
       c_address,
       c_phone,
       c_comment
from tpch_cn.customer,
     tpch_cn.orders,
     tpch_cn.lineitem,
     tpch_cn.nation
where c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-10-01' -- DATE is the first day of a randomly selected month from the second month of 1993 to the first month of 1995.
  and o_orderdate < date '1993-10-01' + interval '3' month
  and l_returnflag = 'R'               -- 被退货
  and c_nationkey = n_nationkey
group by c_custkey,
         c_name,
         c_acctbal,
         c_phone,
         n_name,
         c_address,
         c_comment
order by revenue desc
limit 20;
```


### Query 11

#### Original Version
```sql
/**
 * TPC-H Query 11 : Important Stock Identification Query / 库存价值查询
 * 查询库存中某个国家供应的零件的价值。
 * 带有分组、排序、聚集、子查询操作并存的多表连接查询操作。子查询位于分组操作的HAVING条件中。
 */
select ps_partkey,
       sum(ps_supplycost * ps_availqty) as value -- 聚集操作，商品的总价值
from tpch_cn.partsupp,
     tpch_cn.supplier,
     tpch_cn.nation
where ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY' -- NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3;
group by ps_partkey
having sum(ps_supplycost * ps_availqty) > ( -- 带有HAVING子句的分组操作
    select sum(ps_supplycost * ps_availqty) * (0.0001 / 50) -- FRACTION is chosen as 0.0001 / SF.
    from tpch_cn.partsupp,
         tpch_cn.supplier,
         tpch_cn.nation
    where ps_suppkey = s_suppkey
      and s_nationkey = n_nationkey
      and n_name = 'GERMANY' -- 与父查询的 WHERE 条件一致
)
order by value desc;
 ```

### Query 12

#### Original Version

```sql
/**
 * TPC-H Query 12 : Shipping Modes and Order Priority Query / 货运模式和订单优先级查询
 * 查询获得货运模式和订单优先级。可以帮助决策：选择便宜的货运模式是否会导致消费者更多的在合同日期之后收到货物，而对紧急优先命令产生负面影响。
 * 带有分组、排序、聚集操作并存的两表连接查询操作。
 */
select l_shipmode,
       sum(case
               when o_orderpriority = '1-URGENT'
                   or o_orderpriority = '2-HIGH' then 1
               else 0
           end) as high_line_count,
       sum(case
               when o_orderpriority <> '1-URGENT'
                   and o_orderpriority <> '2-HIGH' then 1
               else 0
           end) as low_line_count
from tpch_cn.orders,
     tpch_cn.lineitem
where o_orderkey = l_orderkey
  and l_shipmode in ('MAIL', 'SHIP') -- SHIPMODE1 is randomly selected within the list of values defined for Modes in Clause 4.2.2.13; SHIPMODE2 is randomly selected within the list of values defined for Modes in Clause 4.2.2.13 and must be different from the value selected for SHIPMODE1 in item 1;
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= '1994-01-01'  -- DATE is the first of January of a randomly selected year within [1993 .. 1997].
  and l_receiptdate < date '1995-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode;
```

### Query 13

#### Original Version

```sql
/**
 * TPC-H Query 13 : Customer Distribution Query
 * 查询获得消费者的订单数量，包括过去和现在都没有订单记录的消费者
 * 带有分组、排序、聚集、子查询、左外连接操作并存的查询操作
 */
select c_count,
       count(*) as custdist
from 
( -- 子查询
   select c_custkey, count(o_orderkey) as c_count
   from tpch_cn.customer left outer join tpch_cn.orders -- 子查询中包括左外连接操作
         on c_custkey = o_custkey and o_comment not like '%special%requests%'
        -- WORD1 is randomly selected from 4 possible values: special, pending, unusual, express.
        -- WORD2 is randomly selected from 4 possible values: packages, requests, accounts, deposits.
   group by c_custkey
) as c_orders
group by 
    c_count
order by 
    custdist desc,
    c_count desc;
```


### Query 14

#### Original Version

 ```sql
/**
 * TPC-H Query 14 : Promotion Effect Query / 促销效果查询
 * 查询获得某一个月的收入中有多大的百分比是来自促销零件。用以监视促销带来的市场反应。
 */
select 100.00 * sum(case
                        when p_type like 'PROMO%' -- 促销零件
                            then l_extendedprice * (1 - l_discount)
                        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from tpch_cn.lineitem,
     tpch_cn.part
where l_partkey = p_partkey
  and l_shipdate >= '1995-09-01' -- DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
  and l_shipdate < '1995-10-01'
```


### Query 15

#### Original Version

```sql
/**
 * TPC-H Query 15 : Top Supplier Query
 * 查询获得某段时间内为总收入贡献最多的供货商（排名第一）的信息。可用以决定对哪些头等供货商给予奖励、给予更多订单、给予特别认证、给予鼓舞等激励。
 * 带有分排序、聚集、聚集子查询操作并存的普通表与视图的连接操作。
 */
WITH revenue(supplier_no, total_revenue) as ( -- 复杂视图
    SELECT l_suppkey,
           SUM(l_extendedprice * (1 - l_discount))
    FROM tpch_cn.lineitem
    WHERE l_shipdate >= '1996-01-01' -- DATE is the first day of a randomly selected month between the first month of 1993 and the 10th month of 1997.
      AND l_shipdate < date '1996-01-01' + interval '3' month
    GROUP BY l_suppkey)
SELECT s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
FROM tpch_cn.supplier,
     revenue
WHERE s_suppkey = supplier_no
  AND total_revenue = ( -- 非关联子查询
    SELECT MAX(total_revenue)
    FROM revenue)
ORDER BY s_suppkey;
```


```sql
WITH revenue(supplier_no, total_revenue) as ( 
    -- OLAPContext start
    SELECT l_suppkey,
           SUM(l_extendedprice * (1 - l_discount))
    FROM tpch_cn.lineitem
    WHERE l_shipdate >= '1996-01-01'
      AND l_shipdate < '1996-04-01'
    GROUP BY l_suppkey
    -- OLAPContext end
)
SELECT s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
FROM tpch_cn.supplier,
     revenue
WHERE s_suppkey = supplier_no
  AND total_revenue = (
    -- OLAPContext start
    SELECT MAX(total_revenue)
    FROM revenue
    -- OLAPContext end
    )
ORDER BY s_suppkey;
```

### Query 16

#### Original Version

```sql
/**
 * TPC-H Query 16 : Parts/Supplier Relationship Query / 零件/供货商关系查询
 * 查询获得能够以指定的贡献条件供应零件的供货商数量。可用于决定在订单量大，任务紧急时，是否有充足的供货商。
 * 带有分组、排序、聚集、去重、NOT IN 子查询操作并存的两表连接操作。
 */
select p_brand,
       p_type,
       p_size,
       count(distinct ps_suppkey) as supplier_cnt
from tpch_cn.partsupp,
     tpch_cn.part
where p_partkey = ps_partkey
  and p_brand <> 'Brand#45'                    -- BRAND = Brand#MN where M and N are two single character strings representing two numbers randomly and independently selected within [1 .. 5];
  and p_type not like 'MEDIUM POLISHED%'       -- TYPE is made of the first 2 syllables of a string randomly selected within the list of 3-syllable strings defined for Types in Clause 4.2.2.13;
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9) -- SIZE is randomly selected as a set of eight different values within [1 .. 50]
  and ps_suppkey not in (-- NOT IN子查询，消费者排除某些供货商
    select s_suppkey
    from tpch_cn.supplier
    where s_comment like '%Customer%Complaints%')
group by p_brand,
         p_type,
         p_size
order by supplier_cnt desc,
         p_brand,
         p_type,
         p_size;
```

### Query 17

#### Original Version

```sql
/**
 * TPC-H Query 17 : Small-Quantity-Order Revenue Query / 小订单收入查询
 * 查询获得比平均供货量的百分之二十还低的小批量订单。对于指定品牌和指定包装类型的零件，决定在一个七年数据库的所有订单中这些订单零件的平均项目数量（过去的和未决的）。
 * 如果这些零件中少于平均数20％的订单不再被接纳，那平均一年会损失多少呢？所以此查询可用于计算出如果没有没有小量订单，平均年收入将损失多少（因为大量商品的货运，将降低管理费用）。
 * 带有聚集、聚集子查询操作并存的两表连接操作。
 */
select sum(l_extendedprice) / 7.0 as avg_yearly
from tpch_cn.lineitem,
     tpch_cn.part
where p_partkey = l_partkey
  and p_brand = 'Brand#23'    -- BRAND = 'Brand#MN' where MN is a two character string representing two numbers randomly and independently selected within [1 .. 5];
  and p_container = 'MED BOX' -- CONTAINER is randomly selected within the list of 2-syllable strings defined for Containers in Clause 4.2.2.13.
  and l_quantity < (-- 关联子查询
    select 0.2 * avg(l_quantity)
    from tpch_cn.lineitem
    where l_partkey = p_partkey);
```

### Query 18

#### Original Version

```sql
/**
 * TPC-H Query 18 : Large Volume Customer Query / 大订单顾客查询
 * Return the first 100 selected rows
 * 查询获得比指定供货量大的供货商信息。可用于决定在订单量大，任务紧急时，验证否有充足的供货商。
 * 带有分组、排序、聚集、IN子查询操作并存的三表连接操作
 */
select c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
from tpch_cn.customer,
     tpch_cn.orders,
     tpch_cn.lineitem
where o_orderkey in (-- 带有分组操作的IN子查询
      select l_orderkey
      from tpch_cn.lineitem
      group by l_orderkey
      having sum(l_quantity) > 312 -- QUANTITY is randomly selected within [312..315]
    ) 
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
order by o_totalprice desc,
         o_orderdate
limit 100;
```

### Query 19

#### Original Version

```sql
/**
 * TPC-H Query 19 : Discounted Revenue Query / 折扣收入查询
 * BRAND1, BRAND2, BRAND3 = 'Brand#MN' where each MN is a two character string representing two numbers randomly and independently selected within [1 .. 5]
 * 查询得到对一些空运或人工运输零件三个不同种类的所有订单的总折扣收入。零件的选择考虑特定品牌、包装和尺寸范围。本查询是用数据挖掘工具产生格式化代码的一个例子。
 * 带有分组、排序、聚集、IN子查询操作并存的三表连接操作。
 */
select sum(l_extendedprice * (1 - l_discount)) as revenue
from tpch_cn.lineitem,
     tpch_cn.part
where (
            p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 5 -- QUANTITY1 is randomly selected within [1..10].
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
   or (
            p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 15 -- QUANTITY2 is randomly selected within [10..20].
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
   or (
            p_partkey = l_partkey
        and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 25 -- QUANTITY3 is randomly selected within [20..30].
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );
```

### Query 20

#### Original Version

```sql
/**
 * TPC-H Query 20 : Potential Part Promotion Query / 供货商竞争力查询
 * 查询确定在某一年内，找出指定国家的能对某一零件商品提供更有竞争力价格的供货货。所谓更有竞争力的供货商，是指那些零件有过剩的供货商，超过供或商在某一年中货运给定国的某一零件的50％则为过剩。
 * 带有排序、聚集、IN子查询、普通子查询操作并存的两表连接操作。
 */
select s_name,
       s_address
from tpch_cn.supplier,
     tpch_cn.nation
where s_suppkey in ( -- 第一层子查询
    select ps_suppkey
    from tpch_cn.partsupp
    where ps_partkey in (  -- 第二层子查询，非关联子查询
            select p_partkey
            from tpch_cn.part
            where p_name like 'forest%'
        )
        and ps_availqty > ( -- 第二层子查询，关联子查询
            select 0.5 * sum(l_quantity)
            from tpch_cn.lineitem
            where l_partkey = ps_partkey
                and l_suppkey = ps_suppkey
                and l_shipdate >= date('1994-01-01') -- DATE is the first of January of a randomly selected year within 1993..1997.
                and l_shipdate < date('1994-01-01') + interval '1' year
        )
    ) -- 第一层子查询结束
    and s_nationkey = n_nationkey
    and n_name = 'CANADA' -- NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3.
order by s_name;
```
  
```sql

with part_quantity as ( -- model1
select
    l_partkey, 
    l_suppkey, 
    0.5 * sum(l_quantity) as half_sum_quantity
from
    tpch_cn.lineitem
where l_shipdate >= '1994-01-01'
    and l_shipdate < '1995-01-01'
group by
    l_partkey,
    l_suppkey
)

with forest_suppilers as(
select ps_suppkey
from tpch_cn.partsupp 
    join part_quantity 
        on partsupp.ps_partkey = part_quantity.l_suppkey
               and partsupp.ps_suppkey = part_quantity.l_suppkey
    join tpch_cn.part
        on partsupp.ps_partkey = part.p_partkey
where ps_availqty > half_sum_quantity 
    and p_name like 'forest%'
)
select s_name, s_address
from tpch_cn.supplier 
    join tpch_cn.nation on s_nationkey = n_nationkey
    join forest_suppilers on supplier.s_suppkey= forest_suppilers.ps_suppkey
where n_name = 'CANADA'
;
```

### Query 21 : Suppliers Who Kept Orders Waiting Query

```sql
/**
 * TPC-H Query 21 : Suppliers Who Kept Orders Waiting Query / 不能按时交货供货商查询
 * Return the first 100 selected rows.
 * 查询获得不能及时交货的供货商。
 * 带有分组、排序、聚集、EXISTS子查询、NOT EXISTS子查询操作并存的四表连接操作。查询语句没有从语法上限制返回多少条元组，但是TPC-H标准规定，查询结果只返回前100行（通常依赖于应用程序实现）。
 */
select s_name,
       count(*) as numwait
from tpch_cn.supplier,
     tpch_cn.lineitem l1,
     tpch_cn.orders,
     tpch_cn.nation
where s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists ( -- Exist 子查询
    select *
    from tpch_cn.lineitem l2
    where l2.l_orderkey = l1.l_orderkey
      and l2.l_suppkey <> l1.l_suppkey)
  and not exists ( -- Not Exist 子查询
    select *
    from tpch_cn.lineitem l3
    where l3.l_orderkey = l1.l_orderkey
      and l3.l_suppkey <> l1.l_suppkey
      and l3.l_receiptdate > l3.l_commitdate)
  and s_nationkey = n_nationkey
  and n_name = 'SAUDI ARABIA' -- NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3.
group by s_name
order by numwait desc,
         s_name;
```


### Query 22 : Global Sales Opportunity Query


```sql
/**
* TPC-H Query 22 : Global Sales Opportunity Query / 全球销售机会查询
* 查询获得消费者可能购买的地理分布。本查询计算在指定的国家，比平均水平更持肯定态度但还没下七年订单的消费者数量。能反应出普通消费者的的态度，即购买意向。
* 带有分组、排序、聚集、EXISTS子查询、NOT EXISTS子查询操作并存的四表连接操作。
*/
select cntrycode,
      count(*)       as numcust,
      sum(c_acctbal) as totacctbal
from ( -- 第一层子查询
        select substring(c_phone from 1 for 2) as cntrycode,
               c_acctbal
        from tpch_cn.customer
             -- I1 ... I7 are randomly selected without repetition from the possible values for Country code as defined in Clause 4.2.2.9.
        where substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17')
          and c_acctbal > ( -- 第二层子查询
            select avg(c_acctbal)
            from tpch_cn.customer
            where c_acctbal > 0.00
              and substring(c_phone from 1 for 2)
                in ('13', '31', '23', '29', '30', '18', '17'))
          and not exists ( -- 第二层子查询
            select *
            from tpch_cn.orders
            where o_custkey = c_custkey)
        -- 第一层子查询结束
    ) as custsale
group by cntrycode
order by cntrycode;
```