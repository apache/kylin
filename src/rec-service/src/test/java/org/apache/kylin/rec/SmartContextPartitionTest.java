/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rec;

import java.util.Map;

import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SmartContextPartitionTest extends AutoTestOnLearnKylinData {

    @Test
    public void testAllFullLoadTableJoin() {
        /*
         * case 1: both are full load tables, result have two contexts
         * --     join
         * --    /    \
         * --  join    A
         * --  /  \
         * -- A    B
         */
        String[] sqls = new String[] {
                "SELECT buyer_account.account_country AS b_country FROM kylin_account buyer_account\n"
                        + "JOIN kylin_country buyer_country ON buyer_account.account_country = buyer_country.country\n"
                        + "JOIN kylin_account seller_account ON buyer_account.account_country = seller_account.account_country\n"
                        + "LIMIT 500" };
        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(2, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 2: both are full load tables, but result have one context
         * --     join
         * --    /    \
         * --  join    A
         * --  /  \
         * -- B    A
         */
        sqls = new String[] { "SELECT buyer_account.account_country AS b_country FROM kylin_account buyer_account\n"
                + "JOIN kylin_country buyer_country ON buyer_account.account_country = buyer_country.country\n"
                + "JOIN kylin_country seller_country ON buyer_country.country = seller_country.country\n"
                + "LIMIT 500" };
        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(1, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(1, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 3: all tables are full load table, should have three contexts
         * --             join
         * --            /    \
         * --         join    join
         * --         /  \    /   \
         * --        A    B join  join
         * --               / \    /  \
         * --              A   C  E   join
         * --                         /  \
         * --                        D    A
         */
        sqls = new String[] { "SELECT SUM(price) AS sum_price\n" //
                + "FROM kylin_sales\n" //
                + "\tJOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt\n" //
                + "\tJOIN (\n" //
                + "\t\tSELECT t1.seller_id\n" //
                + "\t\tFROM (\n" //
                + "\t\t\tSELECT kylin_sales.seller_id AS seller_id\n" //
                + "\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\tJOIN kylin_category_groupings ON kylin_category_groupings.site_id = kylin_sales.lstg_site_id\n"
                + "\t\t) t1\n" //
                + "\t\t\tJOIN (\n" //
                + "\t\t\t\tSELECT kylin_country.country AS country, t3.account_id AS buyer_id\n" //
                + "\t\t\t\tFROM kylin_country\n" //
                + "\t\t\t\t\tJOIN (\n" //
                + "\t\t\t\t\t\tSELECT kylin_account.account_id AS account_id, kylin_account.account_country AS account_country\n"
                + "\t\t\t\t\t\tFROM kylin_account\n"
                + "\t\t\t\t\t\t\tJOIN kylin_sales ON kylin_sales.seller_id = kylin_account.account_id\n"
                + "\t\t\t\t\t) t3\n" //
                + "\t\t\t\t\tON kylin_country.country = t3.account_country\n" //
                + "\t\t\t) t2\n" //
                + "\t\t\tON t1.seller_id = t2.buyer_id\n" //
                + "\t) t5\n" //
                + "\tON t5.seller_id = kylin_sales.seller_id\n" + "WHERE kylin_sales.part_dt < '2012-09-10'" };
        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(3, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(3, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 4: all tables are full load table, should have one context
         * --        join
         * --       /    \
         * --    join    join
         * --    /  \    /   \
         * --   A    B join  join
         * --         /  \   /  \
         * --        B    D  B   D
         */
        sqls = new String[] { "SELECT kylin_cal_dt.cal_dt AS cal_dt, SUM(kylin_sales.price) AS price\n" //
                + "FROM kylin_cal_dt\n" //
                + "\tJOIN kylin_sales ON kylin_cal_dt.cal_dt = kylin_sales.part_dt\n" //
                + "\tJOIN (\n" //
                + "\t\tSELECT t1.seller_id, t1.part_dt AS part_dt\n" //
                + "\t\tFROM (\n" //
                + "\t\t\tSELECT kylin_sales.seller_id AS seller_id, kylin_sales.part_dt AS part_dt\n" //
                + "\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\tJOIN kylin_account ON kylin_sales.seller_id = kylin_account.account_id\n" //
                + "\t\t) t1\n" //
                + "\t\t\tJOIN (\n" //
                + "\t\t\t\tSELECT kylin_sales.buyer_id AS buyer_id\n" //
                + "\t\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\t\tJOIN kylin_account ON kylin_sales.buyer_id = kylin_account.account_id\n" //
                + "\t\t\t\tWHERE kylin_sales.part_dt > '2014-01-01'\n" //
                + "\t\t\t) t2\n" //
                + "\t\t\tON t1.seller_id = t2.buyer_id\n" //
                + "\t) t5\n" //
                + "\tON t5.part_dt = kylin_sales.part_dt\n" //
                + "GROUP BY kylin_cal_dt.cal_dt" };
        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(1, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(1, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }
        log.debug("ending testAllFullLoadTableJoin()");
    }

    @Test
    public void testIsRightSideIncrementalLoadTable() {

        /*
         * case 1: incremental table marks with '*', should have three contexts, but only two layouts
         * --     join
         * --    /    \
         * --  join    *A
         * --  /  \
         * -- B    *A
         */
        String[] sqls = new String[] {
                "SELECT buyer_account.account_country AS b_country FROM kylin_account buyer_account\n"
                        + "JOIN kylin_country buyer_country ON buyer_account.account_country = buyer_country.country\n"
                        + "JOIN kylin_account seller_account ON buyer_account.account_country = seller_account.account_country\n"
                        + "LIMIT 500" };

        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), proj);
        TableDesc kylinCountry = tableManager.getTableDesc("DEFAULT.KYLIN_COUNTRY");
        kylinCountry.setIncrementLoading(true);
        tableManager.updateTableDesc(kylinCountry);

        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(3, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 2: incremental load table marks with '*', should have five contexts, and five layouts
         * --             join
         * --            /    \
         * --         join    join
         * --         /  \    /   \
         * --       *A    B join  join
         * --               / \    /  \
         * --             *A   C *E   join
         * --                         /  \
         * --                        D   *A
         */
        sqls = new String[] { "SELECT SUM(price) AS sum_price\n" //
                + "FROM kylin_sales\n" //
                + "\tJOIN kylin_cal_dt ON kylin_sales.part_dt = kylin_cal_dt.cal_dt\n" //
                + "\tJOIN (\n" //
                + "\t\tSELECT t1.seller_id\n" //
                + "\t\tFROM (\n" //
                + "\t\t\tSELECT kylin_sales.seller_id AS seller_id\n" //
                + "\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\tJOIN kylin_category_groupings ON kylin_category_groupings.site_id = kylin_sales.lstg_site_id\n"
                + "\t\t) t1\n" //
                + "\t\t\tJOIN (\n" //
                + "\t\t\t\tSELECT kylin_country.country AS country, t3.account_id AS buyer_id\n" //
                + "\t\t\t\tFROM kylin_country\n" //
                + "\t\t\t\t\tJOIN (\n" //
                + "\t\t\t\t\t\tSELECT kylin_account.account_id AS account_id, kylin_account.account_country AS account_country\n"
                + "\t\t\t\t\t\tFROM kylin_account\n"
                + "\t\t\t\t\t\t\tJOIN kylin_sales ON kylin_sales.seller_id = kylin_account.account_id\n"
                + "\t\t\t\t\t) t3\n" //
                + "\t\t\t\t\tON kylin_country.country = t3.account_country\n" //
                + "\t\t\t) t2\n" //
                + "\t\t\tON t1.seller_id = t2.buyer_id\n" //
                + "\t) t5\n" //
                + "\tON t5.seller_id = kylin_sales.seller_id\n" + "WHERE kylin_sales.part_dt < '2012-09-10'" };

        TableDesc kylinSales = tableManager.getTableDesc("DEFAULT.KYLIN_SALES");
        kylinSales.setIncrementLoading(true);
        tableManager.updateTableDesc(kylinSales);
        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(5, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(5, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * case 3: incremental load table marks with '*', should have five contexts, but five layouts
         * --        join
         * --       /    \
         * --    left    join
         * --    /  \    /   \
         * --   A   *B join   join
         * --         /  \    /  \
         * --        *B   D   D   *B
         */
        sqls = new String[] { "SELECT kylin_cal_dt.cal_dt AS cal_dt, SUM(kylin_sales.price) AS price\n" //
                + "FROM kylin_cal_dt\n" //
                + "\tLEFT JOIN kylin_sales ON kylin_cal_dt.cal_dt = kylin_sales.part_dt\n" //
                + "\tJOIN (\n" //
                + "\t\tSELECT t1.seller_id, t1.part_dt AS part_dt\n" //
                + "\t\tFROM (\n" //
                + "\t\t\tSELECT kylin_sales.seller_id AS seller_id, kylin_sales.part_dt AS part_dt\n" //
                + "\t\t\tFROM kylin_sales\n" //
                + "\t\t\t\tJOIN kylin_account ON kylin_sales.seller_id = kylin_account.account_id\n" //
                + "\t\t) t1\n" //
                + "\t\t\tJOIN (\n" //
                + "\t\t\t\tSELECT kylin_sales.buyer_id AS buyer_id\n" //
                + "\t\t\t\tFROM kylin_account\n" //
                + "\t\t\t\t\tJOIN kylin_sales ON kylin_account.account_id = kylin_sales.buyer_id\n" //
                + "\t\t\t\tWHERE kylin_sales.part_dt > '2014-01-01'\n" //
                + "\t\t\t) t2\n" //
                + "\t\t\tON t1.seller_id = t2.buyer_id\n" //
                + "\t) t5\n" //
                + "\tON t5.part_dt = kylin_sales.part_dt\n" //
                + "GROUP BY kylin_cal_dt.cal_dt" };
        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(5, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(5, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        kylinCountry = tableManager.getTableDesc("DEFAULT.KYLIN_COUNTRY");
        kylinSales = tableManager.getTableDesc("DEFAULT.KYLIN_SALES");
        kylinCountry.setIncrementLoading(false);
        kylinSales.setIncrementLoading(false);
        tableManager.updateTableDesc(kylinCountry);
        tableManager.updateTableDesc(kylinSales);
        log.debug("ending testIsRightSideIncrementalLoadTable()");
    }

    @Test
    public void testCrossJoin() {
        /*
         * -- case 1: inner join as cross join
         * --    join
         * --   /    \
         * --  A      B
         */
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " inner join kylin_cal_dt on part_dt = '2012-01-01' group by part_dt, lstg_format_name" };

        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(2, context.getModelContexts().size());
            Assert.assertEquals(2, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * -- case 2: cross join
         * --    join
         * --   /    \
         * --  A      B
         */
        sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from \n"
                + " kylin_sales, kylin_cal_dt where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(2, collectAllOlapContexts(context).size());
            Assert.assertEquals(2, context.getModelContexts().size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }
        log.debug("ending testCrossJoin()");
    }

    @Test
    public void testAllIncrementalLoadTableJoin() {

        /*
         * -- case 1: self join
         * --    join
         * --   /    \
         * --  A      A (alias B)
         */
        String[] sqls = new String[] { "SELECT t1.seller_id, t2.part_dt FROM kylin_sales t1\n"
                + "JOIN kylin_sales t2 ON t1.seller_id = t2.seller_id LIMIT 500" };

        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), proj);
        TableDesc kylinSalesTable = tableManager.getTableDesc("DEFAULT.KYLIN_SALES");
        kylinSalesTable.setIncrementLoading(true);
        tableManager.updateTableDesc(kylinSalesTable);

        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(1, context.getModelContexts().size());
            Assert.assertEquals(2, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        /*
         * -- case 2: both table are incremental load, result have two context
         * --    join
         * --   /    \
         * --  A      B
         */
        sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " inner join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };

        TableDesc kylinCalDtTable = tableManager.getTableDesc("DEFAULT.KYLIN_CAL_DT");
        kylinCalDtTable.setIncrementLoading(true);
        tableManager.updateTableDesc(kylinCalDtTable);
        {
            val context = AccelerationUtil.runWithSmartContext(getTestConfig(), proj, sqls, true);
            Assert.assertEquals(2, collectAllOlapContexts(context).size());

            final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            Assert.assertEquals(2, accelerateInfoMap.get(sqls[0]).getRelatedLayouts().size());
        }

        // reset to initial state
        kylinSalesTable = tableManager.getTableDesc("DEFAULT.KYLIN_SALES");
        kylinCalDtTable = tableManager.getTableDesc("DEFAULT.KYLIN_CAL_DT");
        kylinSalesTable.setIncrementLoading(false);
        kylinCalDtTable.setIncrementLoading(false);
        tableManager.updateTableDesc(kylinSalesTable);
        tableManager.updateTableDesc(kylinCalDtTable);
        log.debug("ending testAllIncrementalLoadTableJoin()");
    }
}
