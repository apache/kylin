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



with  W1_inner  as  (select week_beg_dt as week_beg_dt,
						   CAL_DT as CAL_DT,
						   ITEM_COUNT as ITEM_COUNT,
						   TRANS_ID as TRANS_ID,
						   SLR_SEGMENT_CD as SLR_SEGMENT_CD,
						   sum(sum_price) * 1.3 as my_sum_price1
					from (select test_cal_dt.week_beg_dt as week_beg_dt,
							     test_kylin_fact.CAL_DT as CAL_DT,
							     ITEM_COUNT,
							     ORDER_ID,
							     TRANS_ID,
							     SLR_SEGMENT_CD,
							     sum(price) as sum_price
						  from test_kylin_fact
					      inner JOIN edw.test_cal_dt as test_cal_dt
					      ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt
					      inner JOIN test_category_groupings
					      ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
					      inner JOIN edw.test_sites as test_sites
					      ON test_kylin_fact.lstg_site_id = test_sites.site_id
					      group by test_cal_dt.week_beg_dt,test_kylin_fact.CAL_DT,ITEM_COUNT,ORDER_ID,TRANS_ID,SLR_SEGMENT_CD
					      order by CAL_DT,ITEM_COUNT
					      limit 10
					      )
					group by week_beg_dt,CAL_DT,ITEM_COUNT,TRANS_ID,SLR_SEGMENT_CD),

	 W2_inner  as   (select week_beg_dt as week_beg_dt,
						    CAL_DT as CAL_DT,
						    TRANS_ID as TRANS_ID,
						    SLR_SEGMENT_CD as SLR_SEGMENT_CD,
						    sum(my_sum_price1) as my_sum_price2
				     from W1_inner
				     group by week_beg_dt,CAL_DT,TRANS_ID,SLR_SEGMENT_CD),

	 W3_inner  as   (select W1_inner.*
	 	             from W2_inner
	 	             inner JOIN W1_inner
	 	             on W1_inner.week_beg_dt = W2_inner.CAL_DT),

	 W4_final_union as (
	 					(
	 						select  week_beg_dt as week_beg_dt,
						   			CAL_DT as CAL_DT,
						   			'l1' as exp_dimension,
						   			ITEM_COUNT as exp_int,
	 								sum(my_sum_price1) as measure
	 						from W3_inner
	 						group by week_beg_dt,CAL_DT,'l1',ITEM_COUNT
	 					)
	 					union all
	 					(
	 						select  week_beg_dt as week_beg_dt,
						   			CAL_DT as CAL_DT,
						   			'l1' as exp_dimension,
						   			TRANS_ID as exp_int,
	 								sum(my_sum_price1) as measure
	 						from W3_inner
	 						group by week_beg_dt,CAL_DT,'l1',TRANS_ID
	 					)
	 					union all
	 					(
	 						select  week_beg_dt as week_beg_dt,
						   			CAL_DT as CAL_DT,
						   			'l1' as exp_dimension,
						   			SLR_SEGMENT_CD as exp_int,
	 								sum(my_sum_price1) as measure
	 						from W3_inner
	 						group by week_beg_dt,CAL_DT,'l1',SLR_SEGMENT_CD
	 					)

	 				   ),

	W5_table as (
					select * from (select * from W4_final_union) where exp_int > 0

				)


select week_beg_dt,CAL_DT,exp_dimension,exp_int,measure
from W5_table
order by CAL_DT




