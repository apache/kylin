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
package org.apache.kylin.spark.ddl;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import lombok.val;
import scala.collection.Seq;

public class SourceTableCheck implements DDLCheck {

  @Override
  public String[] description(String project) {
    return new String[] {
        "The source table used to define the view needs to be loaded into the data source already",
        "定义 view 用到的来源表需要已经加载到数据源"
    };
  }

  @Override
  public void check(DDLCheckContext context) {
    val spark = SparderEnv.getSparkSession();
    LogicalPlan logicalPlan = null;
    try {
      logicalPlan = spark.sessionState().sqlParser().parsePlan(context.getSql());
    } catch (Throwable t) {
      throwException(t.getMessage());
    }
    val tableManager = NTableMetadataManager.getInstance(
        KylinConfig.getInstanceFromEnv(),
        context.getProject());
    if (!AclPermissionUtil.hasProjectAdminPermission(context.getProject(), context.getGroups())) {
      throwException(MsgPicker.getMsg().getDDLPermissionDenied());
    }
    Seq<LogicalPlan> relationLeaves = logicalPlan.collectLeaves();
    if (relationLeaves == null) {
      return;
    }
    for (LogicalPlan plan : scala.collection.JavaConverters.seqAsJavaListConverter(relationLeaves).asJava()) {
      if (plan instanceof UnresolvedRelation) {
        val tableName = ((UnresolvedRelation) plan).tableName();
        List<TableDesc> loadTable = tableManager.listAllTables().stream()
            .filter(table -> table.getTableAlias().equalsIgnoreCase(tableName))
            .collect(Collectors.toList());
        if (loadTable.isEmpty()) {
          throwException(MsgPicker.getMsg().getDDLTableNotLoad(tableName));
        }
        TableDesc table = loadTable.get(0);
        if (ISourceAware.ID_HIVE != table.getSourceType()
            && ISourceAware.ID_SPARK != table.getSourceType()) {
          throwException(MsgPicker.getMsg().getDDLTableNotSupport(tableName));
        }
      }
    }
  }
}
