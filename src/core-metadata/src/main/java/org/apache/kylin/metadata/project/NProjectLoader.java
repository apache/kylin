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

package org.apache.kylin.metadata.project;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NRealizationRegistry;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.realization.HybridRealization;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NProjectLoader {

    private final NProjectManager mgr;

    private static final ThreadLocal<ProjectBundle> cache = new ThreadLocal<>();

    public static void updateCache(@Nullable String project) {
        if (StringUtils.isNotEmpty(project) && !project.startsWith("_")) {
            val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            val projectLoader = new NProjectLoader(projectManager);
            if (projectManager.getProject(project) == null) {
                log.debug("project {} not exist", project);
                return;
            }
            val bundle = projectLoader.load(project);
            log.trace("set project {} cache {}, prev is {}", project, bundle, cache.get());
            cache.set(bundle);
        }
    }

    public static void removeCache() {
        log.trace("clear cache {}", cache.get());
        cache.remove();
    }

    public NProjectLoader(NProjectManager mgr) {
        this.mgr = mgr;
    }

    public Set<IRealization> listAllRealizations(String project) {
        ProjectBundle prjCache = load(project);
        return Collections.unmodifiableSet(
                prjCache.realizationsByTable.values().stream().flatMap(Set::stream).collect(Collectors.toSet()));
    }

    public Set<IRealization> getRealizationsByTable(String project, String table) {
        Set<IRealization> realizationsByTable = load(project).realizationsByTable.get(table);
        if (realizationsByTable == null)
            return Collections.emptySet();
        else
            return Collections.unmodifiableSet(realizationsByTable);
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String table, boolean onlyRewriteMeasure) {
        Set<IRealization> realizations = getRealizationsByTable(project, table);
        Set<String> modelIds = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .listAllModelIds();
        List<MeasureDesc> result = Lists.newArrayList();
        List<IRealization> existingRealizations = realizations.stream().filter(r -> modelIds.contains(r.getUuid()))
                .collect(Collectors.toList());
        for (IRealization r : existingRealizations) {
            if (!r.isReady())
                continue;
            NDataModel model = r.getModel();
            for (MeasureDesc m : r.getMeasures()) {
                FunctionDesc func = m.getFunction();
                if (belongToFactTable(table, model) && (!onlyRewriteMeasure || func.needRewrite())) {
                    result.add(m);
                }
            }
        }
        return result;
    }

    private boolean belongToFactTable(String table, NDataModel model) {
        // measure belong to the fact table
        return model.getRootFactTable().getTableIdentity().equals(table);
    }

    // ============================================================================
    // build the cache
    // ----------------------------------------------------------------------------

    private ProjectBundle load(String project) {
        if (cache.get() != null) {
            return cache.get();
        }
        ProjectBundle projectBundle = new ProjectBundle(project);

        ProjectInstance pi = mgr.getProject(project);
        Preconditions.checkNotNull(pi, "Project '{}' does not exist.");

        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(mgr.getConfig(), project);
        Map<String, TableDesc> projectAllTables = metaMgr.getAllTablesMap();
        NRealizationRegistry registry = NRealizationRegistry.getInstance(mgr.getConfig(), project);

        // before parallel stream, should use outside KylinConfig.getInstanceFromEnv()
        // in case of load is executed in thread.
        // eg. io.kyligence.kap.smart.query.AbstractQueryRunner.SUGGESTION_EXECUTOR_POOL
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        pi.getRealizationEntries().parallelStream().forEach(entry -> {
            IRealization realization = registry.getRealization(entry.getType(), entry.getRealization());
            if (realization == null) {
                log.warn("Realization '{}' defined under project '{}' is not found or it's broken.", entry, project);
                return;
            }
            NDataflow dataflow = (NDataflow) realization;
            if (dataflow.getModel().isFusionModel() && dataflow.isStreaming()) {
                FusionModel fusionModel = FusionModelManager.getInstance(kylinConfig, project)
                        .getFusionModel(dataflow.getModel().getFusionId());
                if (fusionModel != null) {
                    val batchModel = fusionModel.getBatchModel();
                    if (batchModel.isBroken()) {
                        log.warn("Realization '{}' defined under project '{}' is not found or it's broken.", entry,
                                project);
                        return;
                    }
                    String batchDataflowId = batchModel.getUuid();
                    NDataflow batchRealization = NDataflowManager.getInstance(kylinConfig, project)
                            .getDataflow(batchDataflowId);
                    HybridRealization hybridRealization = new HybridRealization(batchRealization, realization, project);
                    hybridRealization.setConfig(dataflow.getConfig());
                    if (sanityCheck(hybridRealization, projectAllTables)) {
                        mapTableToRealization(projectBundle, hybridRealization);
                    }
                }
            }

            if (sanityCheck(realization, projectAllTables)) {
                mapTableToRealization(projectBundle, realization);
            }
        });

        return projectBundle;
    }

    // check all columns reported by realization does exists
    private boolean sanityCheck(IRealization realization, Map<String, TableDesc> projectAllTables) {
        if (realization == null)
            return false;

        Set<TblColRef> allColumns = realization.getAllColumns();

        if (allColumns.isEmpty() && realization.getMeasures().isEmpty()) {
            return false;
        }

        return allColumns.parallelStream().allMatch(col -> {
            TableDesc table = projectAllTables.get(col.getTable());
            if (table == null) {
                log.error("Realization '{}' reports column '{}', but related table is not found by MetadataManager.",
                        realization.getCanonicalName(), col.getCanonicalName());
                return false;
            }

            if (!col.getColumnDesc().isComputedColumn()) {
                ColumnDesc foundCol = table.findColumnByName(col.getOriginalName());
                if (!col.getColumnDesc().equals(foundCol)) {
                    log.error("Realization '{}' reports column '{}', but found '{}' according to MetadataManager.",
                            realization.getCanonicalName(), col.getCanonicalName(), foundCol);
                    return false;
                }
            }
            return true;
        });
    }

    private void mapTableToRealization(ProjectBundle prjCache, IRealization realization) {
        final Set<TableRef> allTables = realization.getModel().getAllTables();
        for (TableRef tbl : allTables) {
            prjCache.realizationsByTable.computeIfAbsent(tbl.getTableIdentity(),
                    value -> ConcurrentHashMap.newKeySet());
            prjCache.realizationsByTable.get(tbl.getTableIdentity()).add(realization);
        }
    }

    private static class ProjectBundle {
        private String project;
        private final Map<String, Set<IRealization>> realizationsByTable = new ConcurrentHashMap<>();

        ProjectBundle(String project) {
            this.project = project;
        }
    }

}
