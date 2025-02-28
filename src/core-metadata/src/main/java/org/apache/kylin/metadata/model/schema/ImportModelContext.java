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

package org.apache.kylin.metadata.model.schema;

import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_METADATA_FILE_ERROR;
import static org.apache.kylin.common.persistence.MetadataType.PROJECT;
import static org.apache.kylin.common.persistence.MetadataType.WITH_PROJECT_PREFIX_METADATA;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.metadata.Manager;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.RecItemSet;
import org.apache.kylin.metadata.recommendation.entity.RecItemV2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImportModelContext implements AutoCloseable {

    public static final String MODEL_REC_PATH = MetadataType.TMP_REC + "/%s.%s";

    @Getter
    private final String targetProject;
    @Getter
    private final KylinConfig importKylinConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
    private final ResourceStore importResourceStore = new InMemResourceStore(importKylinConfig);
    @Getter
    private final KylinConfig targetKylinConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
    private final ResourceStore targetResourceStore;
    private final NDataModelManager originalDataModelManager;
    private final NDataflowManager originalDataflowManager;
    private final NDataModelManager targetDataModelManager;
    private final NTableMetadataManager targetTableMetadataManager;
    private final NIndexPlanManager targetIndexPlanManger;
    private final NDataModelManager importDataModelManager;
    private final NTableMetadataManager importTableMetadataManager;
    private final NIndexPlanManager importIndexPlanManager;

    @Getter
    private final List<TableDesc> targetMissTableList;
    @Getter
    private final List<TableDesc> loadTableList;
    @Getter
    private final Map<String, String> newModels;
    private final List<String> unImportModels;

    public ImportModelContext(String targetProject, String srcProject, Map<String, RawResource> rawResourceMap) {
        this(targetProject, srcProject, rawResourceMap, Maps.newHashMap(), Lists.newArrayList());
    }

    public ImportModelContext(String targetProject, String srcProject, Map<String, RawResource> rawResourceMap,
            Map<String, String> newModels, List<String> unImportModels) {
        this.targetProject = targetProject;
        this.newModels = newModels;
        this.unImportModels = unImportModels;
        ResourceStore.setRS(importKylinConfig, importResourceStore);

        targetResourceStore = ResourceStore.getKylinMetaStore(targetKylinConfig);

        rawResourceMap.forEach((resPath, raw) -> {
            ByteSource wrap;
            try {
                if (!targetProject.equals(raw.getProject())) {
                    raw.setProject(targetProject);
                    MetadataType metaType = raw.getMetaType();
                    if (WITH_PROJECT_PREFIX_METADATA.contains(metaType)) {
                        raw.setMetaKey(raw.getMetaKey().replaceFirst(srcProject + "\\.", targetProject + "."));
                        resPath = raw.generateKeyWithType();
                    }
                    JsonNode je = JsonUtil.readValue(raw.getContent(), JsonNode.class);
                    ObjectNode contentJsonMap = JsonUtil.valueToTree(je);
                    contentJsonMap.put("project", targetProject);
                    wrap = ByteSource.wrap(JsonUtil.writeValueAsIndentBytes(contentJsonMap));
                } else {
                    wrap = raw.getByteSource();
                }
                importResourceStore.putResourceWithoutCheck(resPath, wrap, raw.getTs(), 0);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write resource, this context is broken", e);
            }
        });

        String targetProjectPath = MetadataType.mergeKeyWithType(targetProject, PROJECT);
        // put target project into importResourceStore in case of broken IndexPlan.initConfig4IndexPlan
        importResourceStore.checkAndPutResource(targetProjectPath,
                targetResourceStore.getResource(targetProjectPath).getByteSource(), -1);

        originalDataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), targetProject);
        originalDataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), targetProject);

        targetDataModelManager = NDataModelManager.getInstance(targetKylinConfig, targetProject);
        targetTableMetadataManager = NTableMetadataManager.getInstance(targetKylinConfig, targetProject);
        targetIndexPlanManger = NIndexPlanManager.getInstance(targetKylinConfig, targetProject);

        importDataModelManager = NDataModelManager.getInstance(importKylinConfig, targetProject);
        importTableMetadataManager = NTableMetadataManager.getInstance(importKylinConfig, targetProject);
        importIndexPlanManager = NIndexPlanManager.getInstance(importKylinConfig, targetProject);

        targetKylinConfig.setProperty("kylin.metadata.validate-computed-column", "false");

        val pairTable = getPairTable();
        targetMissTableList = pairTable.getFirst();
        loadTableList = pairTable.getSecond();
        loadTable();
        loadModel();
    }

    private Pair<List<TableDesc>, List<TableDesc>> getPairTable() {
        List<TableDesc> missTables = Lists.newArrayList();
        List<TableDesc> loadTables = Lists.newArrayList();

        List<TableDesc> tables = importTableMetadataManager.listAllTables();
        for (TableDesc tableDesc : tables) {
            TableDesc newTable = targetTableMetadataManager.copy(tableDesc);
            TableDesc originalTable = targetTableMetadataManager.getTableDesc(newTable.getIdentity());
            newTable.setLastModified(System.currentTimeMillis());
            if (Objects.isNull(originalTable)) {
                newTable.setMvcc(-1);
                missTables.add(newTable);
            } else {
                newTable.setMvcc(originalTable.getMvcc());
            }
            loadTables.add(newTable);
        }
        return Pair.newPair(missTables, loadTables);
    }

    private void loadTable() {
        for (TableDesc tableDesc : loadTableList) {
            targetTableMetadataManager.saveSourceTable(tableDesc);
        }
    }

    private void createNewModel(NDataModel newDataModel, NDataModel importModel) throws IOException {
        newDataModel.setProject(targetProject);
        newDataModel.setAlias(newModels.getOrDefault(importModel.getAlias(), newDataModel.getAlias()));
        newDataModel.setUuid(RandomUtil.randomUUIDStr());
        newDataModel.setMvcc(-1);
        newDataModel.setLastModified(System.currentTimeMillis());
        Manager<ComputedColumnDesc> ccManager = targetKylinConfig.getManager(targetProject,
                ComputedColumnManager.class);
        updateCcMvccIfNeed(newDataModel, ccManager);
        targetDataModelManager.createDataModelDesc(newDataModel, "");

        IndexPlan indexPlan = importIndexPlanManager.getIndexPlanByModelAlias(importModel.getAlias()).copy();
        indexPlan.setUuid(newDataModel.getUuid());
        indexPlan = targetIndexPlanManger.copy(indexPlan);
        indexPlan.setProject(targetProject);
        indexPlan.setMvcc(-1);
        indexPlan.setLastModified(System.currentTimeMillis());
        targetIndexPlanManger.createIndexPlan(indexPlan);
        reorderRecommendations(importModel.getUuid(), newDataModel.getUuid(), Collections.emptyMap());
    }

    /**
     *
     * @param originalDataModel model from current env
     * @param newDataModel model from import
     */
    private static Map<Integer, Integer> prepareIdChangedMap(NDataModel originalDataModel, NDataModel newDataModel) {
        Map<Integer, Integer> idChangedMap = new HashMap<>();

        int columnMaxId = originalDataModel.getAllNamedColumns().stream().map(NDataModel.NamedColumn::getId)
                .mapToInt(Integer::intValue).max().orElse(1);
        int measureMaxId = originalDataModel.getAllMeasures().stream().map(NDataModel.Measure::getId)
                .mapToInt(Integer::intValue).max().orElse(NDataModel.MEASURE_ID_BASE);
        Set<Integer> existIds = Sets.newHashSet();

        for (NDataModel.NamedColumn namedColumn : newDataModel.getAllNamedColumns()) {
            val exists = originalDataModel.getAllNamedColumns().stream()
                    .anyMatch(original -> Objects.equals(original.getId(), namedColumn.getId())
                            && Objects.equals(original.getAliasDotColumn(), namedColumn.getAliasDotColumn())
                            && Objects.equals(original.getStatus(), namedColumn.getStatus()));

            if (!exists) {
                int id = originalDataModel.getAllNamedColumns().stream()
                        .filter(original -> original.getAliasDotColumn().equals(namedColumn.getAliasDotColumn())
                                && original.isExist() == namedColumn.isExist()
                                && !idChangedMap.containsValue(original.getId())
                                && !existIds.contains(original.getId()))
                        .mapToInt(NDataModel.NamedColumn::getId).findFirst().orElse(++columnMaxId);
                if (!Objects.equals(id, namedColumn.getId())) {
                    idChangedMap.put(namedColumn.getId(), id);
                    namedColumn.setId(id);
                }
            }
            existIds.add(namedColumn.getId());
        }

        for (NDataModel.Measure measure : newDataModel.getAllMeasures()) {
            val exists = originalDataModel.getAllMeasures().stream()
                    .anyMatch(original -> Objects.equals(original.getId(), measure.getId())
                            && Objects.equals(original.getName(), measure.getName())
                            && Objects.equals(original.isTomb(), measure.isTomb()));

            if (!exists) {
                val id = originalDataModel.getAllMeasures().stream()
                        .filter(original -> original.getName().equals(measure.getName())
                                && original.isTomb() == measure.isTomb()
                                && !idChangedMap.containsValue(original.getId())
                                && !existIds.contains(original.getId()))
                        .map(NDataModel.Measure::getId).findFirst().orElse(++measureMaxId);

                if (!Objects.equals(id, measure.getId())) {
                    idChangedMap.put(measure.getId(), id);
                    measure.setId(id);
                }
            }
            existIds.add(measure.getId());
        }

        return idChangedMap;
    }

    private void updateModel(NDataModel newDataModel, NDataModel originalDataModel, boolean hasModelOverrideProps) {
        newDataModel.setUuid(originalDataModel.getUuid());
        newDataModel.setProject(targetProject);
        newDataModel.setLastModified(System.currentTimeMillis());
        newDataModel.setMvcc(originalDataModel.getMvcc());
        if (!hasModelOverrideProps) {
            newDataModel.setSegmentConfig(originalDataModel.getSegmentConfig());
        }
        updateDataModelDescAndCC(newDataModel);
    }

    private void updateDataModelDescAndCC(NDataModel importModel) {
        String uuid = importModel.getUuid();
        val dataModelDesc = targetDataModelManager.getDataModelDesc(uuid);

        if (dataModelDesc != null) {
            importModel.init(targetKylinConfig, targetProject, targetDataModelManager.getCCRelatedModels(importModel));
            Manager<ComputedColumnDesc> ccManager = targetKylinConfig.getManager(targetProject,
                    ComputedColumnManager.class);
            NDataModelManager.NDataModelUpdater updater = (copy) -> {
                importModel.copyPropertiesTo(copy);
                updateCcMvccIfNeed(copy, ccManager);
            };
            targetDataModelManager.updateDataModel(importModel.getUuid(), updater);
        } else {
            throw new IllegalArgumentException("Model '" + uuid + "' does not exist.");
        }
    }

    private static void updateCcMvccIfNeed(NDataModel copy, Manager<ComputedColumnDesc> ccManager) {
        if (copy.getComputedColumnUuids() != null) {
            List<ComputedColumnDesc> ccList = new ArrayList<>();
            copy.getComputedColumnDescs().forEach(cc -> {
                ComputedColumnDesc copiedCc = ccManager.copy(cc);
                copiedCc.setMvcc(ccManager.get(cc.getUuid()).map(RootPersistentEntity::getMvcc).orElse(-1L));
                ccList.add(copiedCc);
            });
            copy.setComputedColumnDescs(ccList);
        }
    }

    private void updateIndexPlan(NDataModel originalDataModel, IndexPlan targetIndexPlan,
            boolean hasModelOverrideProps) {
        targetIndexPlanManger.updateIndexPlan(originalDataModel.getUuid(), copyForWrite -> {
            if (targetIndexPlan.getRuleBasedIndex() != null) {
                copyForWrite.setRuleBasedIndex(targetIndexPlan.getRuleBasedIndex());
            } else {
                copyForWrite.setRuleBasedIndex(new RuleBasedIndex());
            }

            if (targetIndexPlan.getIndexes() != null) {
                copyForWrite.setIndexes(targetIndexPlan.getIndexes());
            } else {
                copyForWrite.setIndexes(Lists.newArrayList());
            }

            copyForWrite.getToBeDeletedIndexes().clear();

            if (targetIndexPlan.getToBeDeletedIndexes() != null) {
                copyForWrite.getToBeDeletedIndexes().addAll(targetIndexPlan.getToBeDeletedIndexes());
            } else {
                copyForWrite.getToBeDeletedIndexes().clear();
            }

            if (hasModelOverrideProps) {
                copyForWrite.setOverrideProps(targetIndexPlan.getOverrideProps());
            }

            if (targetIndexPlan.getAggShardByColumns() != null) {
                copyForWrite.setAggShardByColumns(targetIndexPlan.getAggShardByColumns());
            }
        });
    }

    private void loadModel() {
        Map<String, Exception> exceptionMap = new HashMap<>();
        importDataModelManager.listAllModels().stream()
                .filter(dataModel -> !unImportModels.contains(dataModel.getAlias())).forEach(dataModel -> {
                    try {
                        NDataModel newDataModel = importDataModelManager.copyForWrite(dataModel);
                        NDataflow df = originalDataflowManager.getDataflowByModelAlias(newDataModel.getAlias());

                        NDataModel originalDataModel;
                        if (df != null && df.checkBrokenWithRelatedInfo()) {
                            originalDataModel = originalDataModelManager.getDataModelDescWithoutInit(df.getUuid());
                            originalDataModel.setBroken(true);
                        } else {
                            originalDataModel = originalDataModelManager
                                    .getDataModelDescByAlias(newDataModel.getAlias());
                        }

                        if (newModels.containsKey(dataModel.getAlias()) || originalDataModel == null) {
                            createNewModel(newDataModel, dataModel);
                        } else {
                            Map<Integer, Integer> idChangedMap = prepareIdChangedMap(originalDataModel, newDataModel);

                            IndexPlan targetIndexPlan = importIndexPlanManager
                                    .getIndexPlanByModelAlias(newDataModel.getAlias()).copy();

                            boolean hasModelOverrideProps = (newDataModel.getSegmentConfig() != null
                                    && newDataModel.getSegmentConfig().getAutoMergeEnabled() != null
                                    && newDataModel.getSegmentConfig().getAutoMergeEnabled())
                                    || (!targetIndexPlan.getOverrideProps().isEmpty());

                            updateModel(newDataModel, originalDataModel, hasModelOverrideProps);

                            reorderIndexPlan(targetIndexPlan, idChangedMap);
                            reorderRecommendations(dataModel.getUuid(), newDataModel.getUuid(), idChangedMap);
                            updateIndexPlan(originalDataModel, targetIndexPlan, hasModelOverrideProps);
                        }
                    } catch (Exception e) {
                        log.warn("Import model {} exception", dataModel.getAlias(), e);
                        exceptionMap.put(dataModel.getAlias(), e);
                    }
                });

        if (!exceptionMap.isEmpty()) {
            String details = exceptionMap.entrySet().stream()
                    .map(entry -> handleException(entry.getKey(), entry.getValue())).collect(Collectors.joining("\n"));

            throw new KylinException(MODEL_METADATA_FILE_ERROR,
                    String.format(Locale.ROOT, "%s%n%s", MsgPicker.getMsg().getImportModelException(), details),
                    exceptionMap.values());
        }
    }

    private void reorderIndexPlan(IndexPlan copy, Map<Integer, Integer> idChangedMap) {
        if (idChangedMap.isEmpty()) {
            return;
        }

        if (copy.getAggShardByColumns() != null) {
            copy.setAggShardByColumns(copy.getAggShardByColumns().stream().map(id -> idChangedMap.getOrDefault(id, id))
                    .collect(Collectors.toList()));
        }

        // reorder
        RuleBasedIndex ruleBasedIndex = copy.getRuleBasedIndex();
        if (ruleBasedIndex != null) {
            ruleBasedIndex.setDimensions(ruleBasedIndex.getDimensions().stream()
                    .map(id -> idChangedMap.getOrDefault(id, id)).collect(Collectors.toList()));

            ruleBasedIndex.setMeasures(ruleBasedIndex.getMeasures().stream()
                    .map(id -> idChangedMap.getOrDefault(id, id)).collect(Collectors.toList()));

            for (NAggregationGroup aggregationGroup : ruleBasedIndex.getAggregationGroups()) {
                aggregationGroup.setIncludes(Arrays.stream(aggregationGroup.getIncludes())
                        .map(id -> idChangedMap.getOrDefault(id, id)).toArray(Integer[]::new));

                aggregationGroup.setMeasures(Arrays.stream(aggregationGroup.getMeasures())
                        .map(id -> idChangedMap.getOrDefault(id, id)).toArray(Integer[]::new));

                SelectRule selectRule = aggregationGroup.getSelectRule();
                selectRule.setHierarchyDims(Arrays
                        .stream(selectRule.getHierarchyDims()).map(pair -> Arrays.stream(pair)
                                .map(id -> idChangedMap.getOrDefault(id, id)).toArray(Integer[]::new))
                        .toArray(Integer[][]::new));

                selectRule.setMandatoryDims(Arrays.stream(selectRule.getMandatoryDims())
                        .map(id -> idChangedMap.getOrDefault(id, id)).toArray(Integer[]::new));

                selectRule.setJointDims(Arrays
                        .stream(selectRule.getJointDims()).map(pair -> Arrays.stream(pair)
                                .map(id -> idChangedMap.getOrDefault(id, id)).toArray(Integer[]::new))
                        .toArray(Integer[][]::new));

            }
        }

        List<IndexEntity> indexes = copy.getIndexes();
        if (indexes != null) {
            for (IndexEntity index : indexes) {
                reorderIndex(index, idChangedMap);
            }
        }

        List<IndexEntity> toBeDeletedIndexes = copy.getToBeDeletedIndexes();
        if (toBeDeletedIndexes != null) {
            for (IndexEntity index : toBeDeletedIndexes) {
                reorderIndex(index, idChangedMap);
            }
        }
    }

    private static void reorderIndex(IndexEntity index, Map<Integer, Integer> idChangedMap) {
        index.setDimensions(index.getDimensions().stream().map(id -> idChangedMap.getOrDefault(id, id))
                .collect(Collectors.toList()));

        index.setMeasures(
                index.getMeasures().stream().map(id -> idChangedMap.getOrDefault(id, id)).collect(Collectors.toList()));

        for (LayoutEntity layout : index.getLayouts()) {
            reorderLayout(layout, idChangedMap);
        }
    }

    private static void reorderLayout(LayoutEntity layout, Map<Integer, Integer> idChangedMap) {
        layout.setColOrder(layout.getColOrder().stream().map(id -> idChangedMap.getOrDefault(id, id))
                .collect(Collectors.toList()));

        layout.setShardByColumns(layout.getShardByColumns().stream().map(id -> idChangedMap.getOrDefault(id, id))
                .collect(Collectors.toList()));

        layout.setPartitionByColumns(layout.getPartitionByColumns().stream()
                .map(id -> idChangedMap.getOrDefault(id, id)).collect(Collectors.toList()));

        layout.setSortByColumns(layout.getSortByColumns().stream().map(id -> idChangedMap.getOrDefault(id, id))
                .collect(Collectors.toList()));
    }

    private void reorderRecommendations(String modelId, String targetModelId, Map<Integer, Integer> idChangedMap)
            throws IOException {
        RawResource resource = importResourceStore
                .getResource(String.format(Locale.ROOT, MODEL_REC_PATH, targetProject, modelId));
        if (resource != null) {
            List<RawRecItem> rawRecItems = parseRawRecItems(importResourceStore, targetProject, modelId);

            reorderRecommendations(rawRecItems, idChangedMap);

            RecItemSet recItemSet = new RecItemSet(targetModelId, targetProject, rawRecItems);

            targetResourceStore.checkAndPutResource(recItemSet.getResourcePath(),
                    ByteSource.wrap(JsonUtil.writeValueAsIndentBytes(recItemSet)), -1);
        }
    }

    public static List<RawRecItem> reorderRecommendations(List<RawRecItem> rawRecItems,
            Map<Integer, Integer> idChangedMap) {
        for (RawRecItem rawRecItem : rawRecItems) {
            rawRecItem.setDependIDs(
                    IntStream.of(rawRecItem.getDependIDs()).map(id -> idChangedMap.getOrDefault(id, id)).toArray());

            RecItemV2 recEntity = rawRecItem.getRecEntity();
            switch (rawRecItem.getType()) {
            case MEASURE:
                NDataModel.Measure measure = ((MeasureRecItemV2) recEntity).getMeasure();
                measure.setId(idChangedMap.getOrDefault(measure.getId(), measure.getId()));
                break;
            case DIMENSION:
                NDataModel.NamedColumn column = ((DimensionRecItemV2) recEntity).getColumn();
                column.setId(idChangedMap.getOrDefault(column.getId(), column.getId()));
                break;
            case REMOVAL_LAYOUT:
            case ADDITIONAL_LAYOUT:
                LayoutEntity layout = ((LayoutRecItemV2) recEntity).getLayout();
                reorderLayout(layout, idChangedMap);
                break;
            default:
                // do nothing
            }
        }

        return rawRecItems;
    }

    private String handleException(String modelAlias, Exception exception) {
        if (exception instanceof RuntimeException && exception.getMessage() != null
                && exception.getMessage().contains("call on Broken Entity")) {
            return String.format(Locale.ROOT, MsgPicker.getMsg().getImportBrokenModel(), modelAlias);
        }
        return exception.getMessage();
    }

    public static List<RawRecItem> parseRawRecItems(ResourceStore resourceStore, String project, String modelId)
            throws IOException {
        List<RawRecItem> rawRecItems = new ArrayList<>();
        RawResource resource = resourceStore.getResource(String.format(Locale.ROOT, MODEL_REC_PATH, project, modelId));

        if (resource != null) {
            try (InputStream inputStream = resource.getByteSource().openStream()) {
                JsonNode recEntity = JsonUtil.readValue(inputStream, JsonNode.class);
                JsonNode rawRecItemsNode = recEntity.get("rec_items");
                if (rawRecItemsNode != null) {
                    for (JsonNode jsonNode : rawRecItemsNode) {
                        rawRecItems.add(parseRawRecItem(jsonNode));
                    }
                }
            }
        }
        return rawRecItems;
    }

    private static RawRecItem parseRawRecItem(JsonNode recItemNode) throws IOException {
        RawRecItem rawRecItem = JsonUtil.readValue(recItemNode.toString(), RawRecItem.class);
        rawRecItem.setRecEntity(
                RawRecItem.toRecItem(recItemNode.get("recEntity").toString(), (byte) rawRecItem.getType().id()));
        return rawRecItem;
    }

    @Override
    public void close() {
        if (targetResourceStore != null) {
            targetResourceStore.close();
        }
        importResourceStore.close();
    }
}
