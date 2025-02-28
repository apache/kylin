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

import com.alibaba.ttl.TransmittableThreadLocal;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.realization.IRealization;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NProjectLoader {

    private static final TransmittableThreadLocal<ProjectBundle> cache = new TransmittableThreadLocal<>();

    public static void updateCache(@Nullable String project) {
        if (StringUtils.isNotEmpty(project) && !project.startsWith("_")) {
            val projectLoader = new NProjectLoader(KylinConfig.getInstanceFromEnv());
            val bundle = projectLoader.load(project);
            if (!bundle.isEmpty()) {
                log.trace("set project {} cache {}, prev is {}", project, bundle, cache.get());
                cache.set(bundle);
            }
        }
    }

    public static void removeCache() {
        log.trace("clear cache {}", cache.get());
        cache.remove();
    }

    private final KylinConfig kylinConfig;

    public NProjectLoader(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
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

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String table) {
        List<MeasureDesc> effectiveRewriteMeasures = load(project).tableToMeasuresMap.get(StringUtils.upperCase(table));
        return effectiveRewriteMeasures == null ? Collections.emptyList()
                : Collections.unmodifiableList(effectiveRewriteMeasures);
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

        ProjectInstance pi = NProjectManager.getInstance(kylinConfig).getProject(project);
        if (pi == null) {
            log.debug("Project `{}` doest not exist.", project);
            return projectBundle;
        }

        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(kylinConfig, project);
        Map<String, TableDesc> projectAllTables = tableMgr.getAllTablesMap();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        FusionModelManager fusionModelMgr = FusionModelManager.getInstance(kylinConfig, project);

        pi.getRealizationEntries().parallelStream().forEach(entry -> {
            IRealization realization = dfMgr.getRealization(entry.getRealization());
            if (realization == null) {
                log.warn("Realization '{}' defined under project '{}' is not found or it's broken.", entry, project);
                return;
            }
            NDataflow dataflow = (NDataflow) realization;
            if (dataflow.getModel().isFusionModel() && dataflow.isStreaming()) {
                FusionModel fusionModel = fusionModelMgr.getFusionModel(dataflow.getModel().getFusionId());
                if (fusionModel != null) {
                    val batchModel = fusionModel.getBatchModel();
                    if (batchModel.isBroken()) {
                        log.warn("Realization '{}' defined under project '{}' is not found or it's broken.", entry,
                                project);
                        return;
                    }
                    String batchDataflowId = batchModel.getUuid();
                    NDataflow batchRealization = dfMgr.getDataflow(batchDataflowId);
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
        mapEffectiveRewriteMeasuresByTable(projectBundle, projectAllTables);
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

    private void mapEffectiveRewriteMeasuresByTable(ProjectBundle prjCache, Map<String, TableDesc> projectAllTables) {
        Set<String> modelIds = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), prjCache.getProject())
                .listAllModelIds();

        projectAllTables.forEach((tableKey, tableDesc) -> {
            Set<IRealization> realizations = prjCache.realizationsByTable.get(tableKey);
            if (realizations == null) {
                return;
            }
            List<IRealization> existingRealizations = realizations.stream()
                    .filter(realization -> modelIds.contains(realization.getUuid())).collect(Collectors.toList());
            List<MeasureDesc> measureDescs = Lists.newArrayList();
            for (IRealization realization : existingRealizations) {
                if (!realization.isOnline()) {
                    continue;
                }
                NDataModel model = realization.getModel();
                if (model == null || model.isBroken()) {
                    continue;
                }
                for (MeasureDesc measureDesc : realization.getMeasures()) {
                    FunctionDesc func = measureDesc.getFunction();
                    if (belongToFactTable(tableKey, model) && func.needRewrite()) {
                        measureDescs.add(measureDesc);
                    }
                }
            }
            prjCache.tableToMeasuresMap.put(tableDesc.getIdentity(), measureDescs);
        });
    }

    @Getter
    private static class ProjectBundle {
        private final String project;
        private final Map<String, Set<IRealization>> realizationsByTable = new ConcurrentHashMap<>();
        private final Map<String, List<MeasureDesc>> tableToMeasuresMap = new ConcurrentHashMap<>();

        ProjectBundle(String project) {
            this.project = project;
        }

        public boolean isEmpty() {
            return realizationsByTable.isEmpty();
        }
    }

}
