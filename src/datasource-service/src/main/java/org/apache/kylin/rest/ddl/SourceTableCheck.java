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
package org.apache.kylin.rest.ddl;

import static org.apache.spark.ddl.DDLConstant.SOURCE_TABLE_RULE_PRIORITY;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.spark.ddl.DDLCheck;
import org.apache.spark.ddl.DDLCheckContext;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import lombok.val;
import scala.collection.Seq;

public class SourceTableCheck implements DDLCheck {

    @Override
    public String[] description(String project, String pageType) {
        if ("hive".equalsIgnoreCase(pageType)) {
            return new String[] {
                    "The source table used to define the view needs to be loaded into the data source already",
                    "定义 view 用到的来源表需要已经加载到数据源" };
        } else {
            return new String[] {
                    "The source tables in Logical View  should already be loaded into the project data source."
                            + "Users can only load Logical View created in the same project into the data source",
                    "定义 Logical View 用到的来源表需要已经加载到数据源,且用户仅能加载同一项目下创建的Logical View" };
        }
    }

    @Override
    public int priority() {
        return SOURCE_TABLE_RULE_PRIORITY;
    }

    @Override
    public void check(DDLCheckContext context) {
        val spark = SparderEnv.getSparkSession();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        LogicalPlan logicalPlan = null;
        checkACLPermission(context);
        try {
            logicalPlan = spark.sessionState().sqlParser().parsePlan(context.getSql());
        } catch (Throwable t) {
            throwException(t.getMessage());
        }
        val tableManager = NTableMetadataManager.getInstance(config, context.getProject());
        Seq<LogicalPlan> relationLeaves = logicalPlan.collectLeaves();
        if (relationLeaves == null) {
            return;
        }
        List<TableDesc> allTablesInProject = tableManager.listAllTables();
        for (LogicalPlan plan : scala.collection.JavaConverters.seqAsJavaListConverter(relationLeaves).asJava()) {
            if (plan instanceof UnresolvedRelation) {
                val tableName = ((UnresolvedRelation) plan).tableName();
                List<TableDesc> loadTable = allTablesInProject.stream()
                        .filter(table -> table.getTableAlias().equalsIgnoreCase(tableName))
                        .collect(Collectors.toList());
                if (loadTable.isEmpty()) {
                    throwException(MsgPicker.getMsg().getDDLTableNotLoad(tableName));
                }
                TableDesc table = loadTable.get(0);
                if (ISourceAware.ID_HIVE != table.getSourceType() && ISourceAware.ID_SPARK != table.getSourceType()
                        && ISourceAware.ID_JDBC != table.getSourceType()) {
                    throwException(MsgPicker.getMsg().getDDLTableNotSupport(tableName));
                }
                if (context.isLogicalViewCommand()
                        && table.getDatabase().equalsIgnoreCase(config.getDDLLogicalViewDB())) {
                    throwException(MsgPicker.getMsg().getDDLLogicalViewSourceTableError(tableName));
                }
            }
        }
    }

    private void checkACLPermission(DDLCheckContext context) {
        if (context.isHiveCommand()
                && !AclPermissionUtil.hasProjectAdminPermission(context.getProject(), context.getGroups())) {
            throwException("Only project administrator can do Hive operations");
        }
        if (!context.isHiveCommand()
                && (!AclPermissionUtil.hasProjectAdminPermission(context.getProject(), context.getGroups())
                        && !AclPermissionUtil.isSpecificPermissionInProject(context.getProject(), context.getGroups(),
                                AclPermission.MANAGEMENT))) {
            throwException("Only project administrator or modeler can do Logical View operations");
        }
    }
}
