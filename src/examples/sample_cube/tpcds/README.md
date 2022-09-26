## TPC-DS Dataset


### Table List


#### Fact Tables
The schema includes seven fact tables:

A pair of fact tables focused on the product sales and returns for each of the three channels. ie:
1. Store Sales and returns(实体店)
2. Catalog Sales and returns(电话订购)
3. Web Sales and returns(网购)

A single fact table that models inventory for the catalog and internet sales channels.
1. Inventory(商品库存)

#### Dimension Tables

In addition, the schema includes 17 dimension tables that are associated with all sales channels. 

| Type	      | Name	                  | Chinese Comment     |
|------------|------------------------|---------------------|
| Dimension  | Store                  | 	实体店铺               |
| Dimension	 | Call Center            | 	电话销售的呼叫中心, 和电话销售有关 |
| Dimension	 | Catalog Page           | 	电话销售相关             |
| Dimension	 | Web Site               | 	网店网站               |
| Dimension	 | Web Page               | 	网店网页               |
| Dimension	 | Warehouse              | 	物流仓库               |
| Dimension	 | Customer               | 	顾客基础信息             |
| Dimension	 | Customer Address       | 	顾客地址               |
| Dimension	 | Customer Demographics  | 	顾客信息               |
| Dimension	 | Date Dim               | 	日期表                |
| Dimension	 | Household Demographics | 	家庭信息               |
| Dimension	 | Item                   | 	商品                 |
| Dimension	 | Income Band            | 	收入阶层               |
| Dimension	 | Promotion              | 	销售促销               |
| Dimension	 | Reason                 | 	退货原因               |
| Dimension	 | Ship Mode              | 	配送方式               |
| Dimension	 | Time Dim               | 	时刻表                |

#### ER-Diagram

[check this link](https://bbs.huaweicloud.com/blogs/detail/198087)