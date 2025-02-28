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

package org.apache.kylin.query.util;

import org.junit.Assert;
import org.junit.Test;

public class OlapRelUtilTest {

    @Test
    public void removeDigestCtxValueTest() {
        String digestSrc = "OlapLimitRel(ctx=[], fetch=[500])\n"
                + "    OlapProjectRel(针梭织__0=[$0], 封样合格率（款）__max__1=[$1], 实际收货配套累计量__sum__2=[$2], EXPR$3=[$3], __grouping_id=[$4], ctx=[])\n"
                + "      OlapUnionRel(all=[true], ctx=[], all=[true])\n"
                + "        OlapProjectRel(针梭织__0=[$0], 封样合格率（款）__max__1=[$1], 实际收货配套累计量__sum__2=[$2], EXPR$3=[$3], __grouping_id=[$4], ctx=[])\n"
                + "          OlapProjectRel(针梭织__0=[$0], 封样合格率（款）__max__1=[$1], 实际收货配套累计量__sum__2=[$2], EXPR$3=[$3], __grouping_id=[0], ctx=[])\n"
                + "            OlapProjectRel(LSTG_FORMAT_NAME=[$0], 封样合格率（款）__max__1=[$1], AGG$0=[$3], AGG$1=[$4], ctx=[])\n"
                + "              OlapJoinRel(condition=[=($0, $2)], joinType=[inner], ctx=[])\n"
                + "                OlapAggregateRel(group-set=[[0]], groups=[null], 封样合格率（款）__max__1=[COUNT(DISTINCT $1)], ctx=[0@null])\n"
                + "                  OlapProjectRel(LSTG_FORMAT_NAME=[$0], $f1=[CASE(=($1, 100), $0, null)], $f2=[CAST(0):BIGINT], $f3=[CAST(0):BIGINT], ctx=[0@null])\n"
                + "                    OlapFilterRel(condition=[>($1, 0)], ctx=[0@null])\n"
                + "                      OlapProjectRel(LSTG_FORMAT_NAME=[$3], ORDER_ID=[$1], TRANS_ID=[$0], ctx=[0@null])\n"
                + "                        OlapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[0@null], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]])\n"
                + "                OlapAggregateRel(group-set=[[0]], groups=[null], AGG$0=[SUM($1)], AGG$1=[SUM($2)], ctx=[])\n"
                + "                  OlapProjectRel(LSTG_FORMAT_NAME=[$0], 实际收货配套累计量__sum__2=[$2], $f2=[CASE(>($1, 0), $3, $4)], ctx=[])\n"
                + "                    OlapAggregateRel(group-set=[[0, 1]], groups=[null], TOP_AGG$0=[SUM($2)], TOP_AGG$1=[SUM($3)], TOP_AGG$2=[SUM($4)], ctx=[])\n"
                + "                      OlapProjectRel(LSTG_FORMAT_NAME=[$0], TRANS_ID=[$1], 实际收货配套累计量__sum__2=[$2], $f3=[*(0, $3)], SUM_CASE$0$1=[$4], ctx=[1@null])\n"
                + "                        OlapAggregateRel(group-set=[[0, 1]], groups=[null], 实际收货配套累计量__sum__2=[SUM($2)], SUM_CONST$1=[COUNT()], SUM_CASE$0$1=[SUM($3)], ctx=[1@null])\n"
                + "                          OlapProjectRel(LSTG_FORMAT_NAME=[$0], TRANS_ID=[$2], TRANS_ID0=[$2], ORDER_ID=[$1], ctx=[1@null])\n"
                + "                            OlapFilterRel(condition=[>($1, 0)], ctx=[1@null])\n"
                + "                              OlapProjectRel(LSTG_FORMAT_NAME=[$3], ORDER_ID=[$1], TRANS_ID=[$0], ctx=[1@null])\n"
                + "                                OlapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[1@null], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]])\n"
                + "        OlapProjectRel(针梭织__0=[$0], 封样合格率（款）__max__1=[$1], 实际收货配套累计量__sum__2=[$2], 实际收货配套累计占比__max__3=[$3], __grouping_id=[$4], ctx=[2@null])\n"
                + "          OlapProjectRel(针梭织__0=['aa'], 封样合格率（款）__max__1=[0.1], 实际收货配套累计量__sum__2=[0.2], 实际收货配套累计占比__max__3=[33], __grouping_id=[1], ctx=[2@null])\n"
                + "            OlapValuesRel(tuples=[[{ 0 }]])";
        String digestResult = OlapRelUtil.replaceDigestCtxValueByLayoutIdAndModelId(digestSrc, 10001,
                "6748hdsjf-e612-1b91-e7a0-1237f02ec9i2");
        String digestExpected = "OlapLimitRel(ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001, fetch=[500])\n"
                + "    OlapProjectRel(针梭织__0=[$0], 封样合格率（款）__max__1=[$1], 实际收货配套累计量__sum__2=[$2], EXPR$3=[$3], __grouping_id=[$4], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "      OlapUnionRel(all=[true], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001, all=[true])\n"
                + "        OlapProjectRel(针梭织__0=[$0], 封样合格率（款）__max__1=[$1], 实际收货配套累计量__sum__2=[$2], EXPR$3=[$3], __grouping_id=[$4], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "          OlapProjectRel(针梭织__0=[$0], 封样合格率（款）__max__1=[$1], 实际收货配套累计量__sum__2=[$2], EXPR$3=[$3], __grouping_id=[0], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "            OlapProjectRel(LSTG_FORMAT_NAME=[$0], 封样合格率（款）__max__1=[$1], AGG$0=[$3], AGG$1=[$4], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "              OlapJoinRel(condition=[=($0, $2)], joinType=[inner], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                OlapAggregateRel(group-set=[[0]], groups=[null], 封样合格率（款）__max__1=[COUNT(DISTINCT $1)], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                  OlapProjectRel(LSTG_FORMAT_NAME=[$0], $f1=[CASE(=($1, 100), $0, null)], $f2=[CAST(0):BIGINT], $f3=[CAST(0):BIGINT], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                    OlapFilterRel(condition=[>($1, 0)], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                      OlapProjectRel(LSTG_FORMAT_NAME=[$3], ORDER_ID=[$1], TRANS_ID=[$0], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                        OlapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001, fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]])\n"
                + "                OlapAggregateRel(group-set=[[0]], groups=[null], AGG$0=[SUM($1)], AGG$1=[SUM($2)], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                  OlapProjectRel(LSTG_FORMAT_NAME=[$0], 实际收货配套累计量__sum__2=[$2], $f2=[CASE(>($1, 0), $3, $4)], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                    OlapAggregateRel(group-set=[[0, 1]], groups=[null], TOP_AGG$0=[SUM($2)], TOP_AGG$1=[SUM($3)], TOP_AGG$2=[SUM($4)], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                      OlapProjectRel(LSTG_FORMAT_NAME=[$0], TRANS_ID=[$1], 实际收货配套累计量__sum__2=[$2], $f3=[*(0, $3)], SUM_CASE$0$1=[$4], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                        OlapAggregateRel(group-set=[[0, 1]], groups=[null], 实际收货配套累计量__sum__2=[SUM($2)], SUM_CONST$1=[COUNT()], SUM_CASE$0$1=[SUM($3)], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                          OlapProjectRel(LSTG_FORMAT_NAME=[$0], TRANS_ID=[$2], TRANS_ID0=[$2], ORDER_ID=[$1], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                            OlapFilterRel(condition=[>($1, 0)], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                              OlapProjectRel(LSTG_FORMAT_NAME=[$3], ORDER_ID=[$1], TRANS_ID=[$0], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "                                OlapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001, fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]])\n"
                + "        OlapProjectRel(针梭织__0=[$0], 封样合格率（款）__max__1=[$1], 实际收货配套累计量__sum__2=[$2], 实际收货配套累计占比__max__3=[$3], __grouping_id=[$4], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "          OlapProjectRel(针梭织__0=['aa'], 封样合格率（款）__max__1=[0.1], 实际收货配套累计量__sum__2=[0.2], 实际收货配套累计占比__max__3=[33], __grouping_id=[1], ctx=6748hdsjf-e612-1b91-e7a0-1237f02ec9i2_10001)\n"
                + "            OlapValuesRel(tuples=[[{ 0 }]])";
        Assert.assertEquals(digestExpected, digestResult);
    }

}
