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
package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.DDL_CHECK_ERROR;
import static org.apache.spark.ddl.DDLConstant.CREATE_LOGICAL_VIEW;
import static org.apache.spark.ddl.DDLConstant.DROP_LOGICAL_VIEW;
import static org.apache.spark.ddl.DDLConstant.REPLACE_LOGICAL_VIEW;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.LogicalViewBroadcastNotifier;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.view.LogicalView;
import org.apache.kylin.metadata.view.LogicalViewManager;
import org.apache.kylin.rest.ddl.SourceTableCheck;
import org.apache.kylin.rest.ddl.ViewCheck;
import org.apache.kylin.rest.request.ViewRequest;
import org.apache.kylin.rest.response.LogicalViewResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;

import org.apache.spark.ddl.DDLCheck;
import org.apache.spark.ddl.DDLCheckContext;
import org.apache.spark.sql.LogicalViewLoader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;

import org.springframework.stereotype.Service;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SparkDDLService extends BasicService {

  private final List<DDLCheck> ddlChecks = Lists.newArrayList(new SourceTableCheck(), new ViewCheck());

  public String executeSQL(ViewRequest request) {
    if (!KylinConfig.getInstanceFromEnv().isDDLEnabled()) {
      throw new KylinException(DDL_CHECK_ERROR, "DDL function has not been turned on.");
    }
    LogicalViewLoader.checkConfigIfNeed();
    val groups = getCurrentUserGroups();
    val context = new DDLCheckContext(request.getSql(), request.getDdlProject(), request.getRestrict(),
        AclPermissionUtil.getCurrentUsername(),
        groups, UserGroupInformation.isSecurityEnabled());

    ArrayList<DDLCheck> ddlCheckers = Lists.newArrayList(this.ddlChecks.iterator());
    Collections.sort(ddlCheckers);
    for (DDLCheck checker : ddlCheckers) {
      checker.check(context);
    }
    final StringBuilder result = new StringBuilder();
    List<Row> rows = SparderEnv.getSparkSession().sql(request.getSql()).collectAsList();
    rows.forEach(row -> result.append(row.get(0).toString()).append("\n"));
    if (context.isLogicalViewCommand()) {
      /**
       * Request MUST be handled by global owner node.
       */
      switch (context.getCommandType()) {
        case REPLACE_LOGICAL_VIEW:
        case CREATE_LOGICAL_VIEW:
          saveLogicalView(context);
          break;
        case DROP_LOGICAL_VIEW:
          dropLogicalView(context);
          break;
        default:
          break;
      }
      EventBusFactory.getInstance().postAsync(new LogicalViewBroadcastNotifier());
    }
    return result.toString();
  }

  public List<List<String>> pluginsDescription(String project, String pageType) {
    if (!KylinConfig.getInstanceFromEnv().isDDLEnabled()) {
      throw new KylinException(DDL_CHECK_ERROR, "DDL function has not been turned on.");
    }
    LogicalViewLoader.checkConfigIfNeed();
    List<String> descriptionEN = Lists.newArrayList();
    List<String> descriptionCN = Lists.newArrayList();
    for (DDLCheck checker : ddlChecks) {
      String[] description = checker.description(project, pageType);
      descriptionEN.addAll(Arrays.asList(description[0].split("\t")));
      descriptionCN.addAll(Arrays.asList(description[1].split("\t")));
    }
    return Lists.newArrayList(descriptionEN, descriptionCN);
  }

  private void saveLogicalView(DDLCheckContext context) {
    EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
      LogicalViewManager manager = LogicalViewManager.getInstance(KylinConfig.getInstanceFromEnv());
      LogicalView logicalView = new LogicalView(context.getLogicalViewName(), context.getSql(), context.getUserName(),
          context.getProject());
      manager.update(logicalView);
      return null;
    }, UnitOfWork.GLOBAL_UNIT);
    LogicalViewLoader.loadView(context.getLogicalViewName(), false, SparderEnv.getSparkSession());
  }

  private void dropLogicalView(DDLCheckContext context) {
    EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
      LogicalViewManager manager = LogicalViewManager.getInstance(KylinConfig.getInstanceFromEnv());
      manager.delete(context.getLogicalViewName());
      return null;
    }, UnitOfWork.GLOBAL_UNIT);
    LogicalViewLoader.unloadView(context.getLogicalViewName(), SparderEnv.getSparkSession());
  }

  public List<LogicalViewResponse> listAll(String project, String tableName) {
    List<LogicalView> logicalViews = LogicalViewManager.getInstance(KylinConfig.getInstanceFromEnv()).list();
    if (StringUtils.isNotBlank(tableName)) {
      logicalViews = logicalViews.stream()
          .filter(table -> table.getTableName().toLowerCase().contains(tableName.toLowerCase()))
          .collect(Collectors.toList());
    }
    List<LogicalViewResponse> viewResponses = Lists.newArrayList();
    List<LogicalViewResponse> viewResponsesInProject =
        logicalViews.stream()
            .filter(table -> table.getCreatedProject().equalsIgnoreCase(project))
            .map(LogicalViewResponse::new)
            .collect(Collectors.toList());
    List<LogicalViewResponse> viewResponsesNotInProject =
        logicalViews.stream()
            .filter(table -> !table.getCreatedProject().equalsIgnoreCase(project))
            .map(LogicalViewResponse::new)
            .collect(Collectors.toList());
    viewResponsesNotInProject.forEach(table -> table.setCreatedSql("***"));
    Collections.sort(viewResponsesInProject);
    Collections.sort(viewResponsesNotInProject);
    viewResponses.addAll(viewResponsesInProject);
    viewResponses.addAll(viewResponsesNotInProject);
    return viewResponses;
  }
}