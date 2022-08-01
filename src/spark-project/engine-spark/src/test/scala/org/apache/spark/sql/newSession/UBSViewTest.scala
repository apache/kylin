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
package org.apache.spark.sql.newSession

import org.apache.kylin.engine.spark.mockup.external.UBSViewCatalog

class UBSViewTest extends WithKylinExternalCatalog {

  override protected def externalCatalog: String = classOf[UBSViewCatalog].getName

  protected def checkExternalCatalogBeforeAll(): Unit = {}


  val createFactTable: String =
    s"""create table fact (
       |   id bigint,
       |   dimid int,
       |   YZT_Mrgn_Sngn_Flg Boolean,
       |   Abrd_Br_Ast_Hld_Flg Boolean,
       |   Data_Dt timestamp,
       |   Marriage_Stat_Cd string,
       |   yyyy Decimal(38,0),
       |   zzzz Decimal(38,0),
       |   t0 int,
       |   t1 int,
       |   date1 date,
       |   date2 date
       | ) USING PARQUET OPTIONS ('compression'='snappy')""".stripMargin

  val createDimTable: String =
    s"""create table dim (
       |   id int,
       |   YZT_Mrgn_Sngn_Flg Boolean,
       |   Abrd_Br_Ast_Hld_Flg Boolean,
       |   Data_Dt timestamp,
       |   Marriage_Stat_Cd string,
       |   yyyy Decimal(38,0),
       |   zzzz Decimal(38,0),
       |   t0 int,
       |   t1 int,
       |   date1 date,
       |   date2 date
       | ) USING PARQUET OPTIONS ('compression'='snappy')""".stripMargin


  test("this is simple test") {
    spark.conf.set("spark.sql.view-cache-enabled", false);
    spark.sql("show databases")
    spark.sql(createFactTable)
    spark.sql(createDimTable)
    val viewName = UBSViewCatalog.VIEW_NAME

    val testSql =
      s"""select * from fact
         | inner join $viewName on fact.dimid = $viewName.id
         | inner join $viewName as Y on fact.dimid = Y.id""".stripMargin

    spark.sql(testSql)
  }
}
