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

package org.apache.kylin.metadata.view;

import static org.apache.kylin.common.persistence.ResourceStore.VIEW_ROOT;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class LogicalViewManager {

  private static final Logger logger = LoggerFactory.getLogger(LogicalViewManager.class);

  public static LogicalViewManager getInstance(KylinConfig config) {
    return config.getManager(LogicalViewManager.class);
  }

  // called by reflection
  static LogicalViewManager newInstance(KylinConfig config) {
    return new LogicalViewManager(config);
  }

  // ============================================================================
  private KylinConfig config;
  private CachedCrudAssist<LogicalView> crud;

  public LogicalViewManager(KylinConfig config) {
    if (!UnitOfWork.isAlreadyInTransaction()) {
      logger.info("Initializing LogicalView with KylinConfig Id: {}", System.identityHashCode(config));
    }
    this.config = config;
    this.crud = new CachedCrudAssist<LogicalView>(getStore(), VIEW_ROOT, "", LogicalView.class) {
      @Override
      protected LogicalView initEntityAfterReload(LogicalView view, String resourceName) {
        return view;
      }
    };
  }

  public LogicalView copyForWrite(LogicalView view) {
    return crud.copyForWrite(view);
  }

  public KylinConfig getConfig() {
    return config;
  }

  public ResourceStore getStore() {
    return ResourceStore.getKylinMetaStore(this.config);
  }

  public LogicalView get(String name) {
    return crud.get(name.toUpperCase());
  }

  public List<LogicalView> list() {
    return crud.listAll();
  }

  public void update(LogicalView view) {
    LogicalView exist = get(view.getTableName());
    LogicalView copy = copyForWrite(view);
    if (exist != null) {
      copy.setLastModified(exist.getLastModified());
      copy.setMvcc(exist.getMvcc());
    }
    crud.save(copy);
  }

  public void delete(String tableName) {
    crud.delete(tableName.toUpperCase());
  }

  public boolean exists(String tableName) {
    return get(tableName) != null;
  }

  public List<LogicalView> findLogicalViewsInModel(String project, String dataflowId) {
    List<LogicalView> viewsInModel = Lists.newArrayList();
    NDataflow df = NDataflowManager.getInstance(config, project).getDataflow(dataflowId);
    if (df == null) {
      return viewsInModel;
    }
    String logicalViewDB = KylinConfig.getInstanceFromEnv().getDDLLogicalViewDB();
    NDataModel model = df.getModel();
    model.getAllTableRefs().forEach(tableRef -> {
      if (logicalViewDB.equalsIgnoreCase(tableRef.getTableDesc().getDatabase())
          && get(tableRef.getTableName()) != null) {
        viewsInModel.add(get(tableRef.getTableName()));
      }
    });
    return viewsInModel;
  }

  public LogicalView findLogicalViewInProject(String project, String tableName) {
    NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(config, project);
    TableDesc table = tblMgr.getTableDesc(tableName);
    if (table == null || !table.isLogicalView()) {
      return null;
    }
    LogicalView logicalView = get(table.getName());
    if (logicalView != null && logicalView.getCreatedProject()
        .equalsIgnoreCase(project)) {
      return logicalView;
    }
    return null;
  }
}
