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
package org.apache.spark.sql;

import static org.apache.kylin.common.exception.ServerErrorCode.DDL_CHECK_ERROR;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.view.LogicalView;
import org.apache.kylin.metadata.view.LogicalViewManager;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class LogicalViewLoader {
  public static final Logger LOGGER = LoggerFactory.getLogger(LogicalViewLoader.class);

  public static final ConcurrentMap<String, LogicalView> LOADED_LOGICAL_VIEWS = Maps.newConcurrentMap();
  public static final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("logical_view"));
  private static ScheduledFuture<?> syncViewScheduler;
  private static boolean hasChecked = false;
  private static boolean isLogicalViewConfigLegal = true;

  public static void initScheduler() {
    LOADED_LOGICAL_VIEWS.clear();
    LOGGER.info("Start sync logical view...");
    stopScheduler();
    syncViewScheduler = executorService.scheduleAtFixedRate(() -> {
      try {
        syncViewFromDB();
      } catch (Throwable e) {
        LOGGER.error("Error when sync logical view", e);
      }
    }, 0, KylinConfig.getInstanceFromEnv().getDDLLogicalViewCatchupInterval(), TimeUnit.SECONDS);
  }

  public static void syncViewAsync() {
    executorService.schedule(LogicalViewLoader::syncViewFromDB, 0, TimeUnit.SECONDS);
  }

  public static synchronized void loadView(String viewName, boolean loadBySpark, SparkSession spark) {
    LOGGER.info("start load new logical view, view name is {}", viewName);
    LogicalViewManager viewManager = LogicalViewManager.getInstance(KylinConfig.getInstanceFromEnv());
    LogicalView toLoadView = viewManager.get(viewName);
    try {
      if (toLoadView == null) {
        LOGGER.warn("failed to find logical view {} ", viewName);
        return;
      }
      if (loadBySpark) {
        dropLogicalViewIfExist(toLoadView.getTableName(), spark);
        spark.sql(toLoadView.getCreatedSql());
      }
      LOADED_LOGICAL_VIEWS.put(toLoadView.getTableName().toUpperCase(), toLoadView);
      LOGGER.info("The new table loaded successfully is {}", viewName);
    } catch (Throwable e) {
      LOGGER.error("Error when load new Logical View {}", viewName, e);
    }
  }

  public static synchronized void unloadView(String viewName, SparkSession spark) {
    LOADED_LOGICAL_VIEWS.remove(viewName.toUpperCase());
    dropLogicalViewIfExist(viewName, spark);
  }

  public static synchronized void syncViewFromDB() {
    checkConfigIfNeed();
    long start = System.currentTimeMillis();
    LogicalViewManager viewManager = LogicalViewManager.getInstance(KylinConfig.getInstanceFromEnv());
    Set<LogicalView> toLoadViews = Sets.newHashSet();
    Set<LogicalView> toReplaceViews = Sets.newHashSet();
    Set<LogicalView> toRemoveViews = Sets.newHashSet();
    Set<String> successLoadViews = Sets.newHashSet();
    Set<String> successReplaceViews = Sets.newHashSet();
    Set<String> successRemoveViews = Sets.newHashSet();

    viewManager.list().forEach(view -> {
      if (LOADED_LOGICAL_VIEWS.containsKey(view.getTableName())) {
        LogicalView viewLoaded = LOADED_LOGICAL_VIEWS.get(view.getTableName());
        if (viewLoaded.getLastModified() != view.getLastModified()) {
          toReplaceViews.add(view);
        }
      } else {
        toLoadViews.add(view);
      }
    });
    LOADED_LOGICAL_VIEWS.keySet().forEach(table -> {
      if (viewManager.get(table) == null) {
        toRemoveViews.add(LOADED_LOGICAL_VIEWS.get(table));
      }
    });

    SparkSession spark = SparderEnv.getSparkSession();
    toLoadViews.forEach(view -> {
      try {
        dropLogicalViewIfExist(view.getTableName(), spark);
        spark.sql(view.getCreatedSql());
        LOADED_LOGICAL_VIEWS.put(view.getTableName(), view);
        successLoadViews.add(view.getTableName());
      } catch (Throwable e) {
        LOGGER.error("Error when load new Logical View {}", view.getTableName());
      }
    });
    toReplaceViews.forEach(view -> {
      try {
        dropLogicalViewIfExist(view.getTableName(), spark);
        spark.sql(view.getCreatedSql());
        LOADED_LOGICAL_VIEWS.put(view.getTableName(), view);
        successReplaceViews.add(view.getTableName());
      } catch (Throwable e) {
        LOGGER.error("Error when replace new Logical View {}", view.getTableName());
      }
    });
    toRemoveViews.forEach(view -> {
      try {
        dropLogicalViewIfExist(view.getTableName(), spark);
        LOADED_LOGICAL_VIEWS.remove(view.getTableName());
        successRemoveViews.add(view.getTableName());
      } catch (Throwable e) {
        LOGGER.error("Error when remove Logical View {}", view.getTableName());
      }
    });
    long costTime = (System.currentTimeMillis() - start) / 1000;
    LOGGER.info("End sync logical view, cost time is {}, "
            + "\tsuccess loaded views: {},"
            + "\tsuccess replaced views: {},"
            + "\tsuccess removed views: {}.", costTime,
        successLoadViews, successReplaceViews, successRemoveViews);
  }

  private static void dropLogicalViewIfExist(String tableName, SparkSession spark) {
    String logicalViewDatabase = KylinConfig.getInstanceFromEnv().getDDLLogicalViewDB();
    spark.sql("DROP LOGICAL VIEW IF EXISTS " + logicalViewDatabase + "." + tableName);
  }

  public static void checkConfigIfNeed() {
    if (!KylinConfig.getInstanceFromEnv().isDDLLogicalViewEnabled()) {
      return;
    }
    if (!hasChecked) {
      try {
        // check if logical view database is duplicated with hive databases
        SourceFactory.getSparkSource().getSourceMetadataExplorer().listDatabases();
      } catch (Exception e) {
        LOGGER.warn("Error when list databases....", e);
        isLogicalViewConfigLegal = false;
      } finally {
        hasChecked = true;
      }
    }
    if (!isLogicalViewConfigLegal) {
      throw new KylinException(DDL_CHECK_ERROR, "Logical view database should not be duplicated with normal "
          + "hive database!!!");
    }
  }

  public static void stopScheduler() {
    try {
      if (null != syncViewScheduler && !syncViewScheduler.isCancelled()) {
        syncViewScheduler.cancel(true);
      }
    } catch (Exception e) {
      LOGGER.error("Error when cancel syncViewScheduler", e);
    }
  }

  private LogicalViewLoader() {}
}
