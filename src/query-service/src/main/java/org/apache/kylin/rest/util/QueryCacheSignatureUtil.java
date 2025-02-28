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
package org.apache.kylin.rest.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.guava30.shaded.common.base.Joiner;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.query.QueryMetricsContext;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.CacheSignatureQuerySupporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.Getter;
import lombok.val;

public class QueryCacheSignatureUtil {
    private static final Logger logger = LoggerFactory.getLogger("query");

    @Getter
    @Autowired
    //@Qualifier("queryService")
    private static CacheSignatureQuerySupporter queryService;

    //    static {
    //
    //    }
    // Query Cache
    public static String createCacheSignature(SQLResponse response, String project) {
        List<String> signature = Lists.newArrayList();
        // TODO need rewrite && SpringContext.getApplicationContext() != null

        //        if (queryService == null ) {
        //
        //        }
        queryService = SpringContext.getBean(CacheSignatureQuerySupporter.class);
        try {
            signature.add(queryService.onCreateAclSignature(project));
        } catch (IOException e) {
            logger.error("Fail to get acl signature: ", e);
            return "";
        }
        val realizations = response.getNativeRealizations();
        Preconditions.checkState(CollectionUtils.isNotEmpty(realizations));
        for (NativeQueryRealization realization : realizations) {
            signature.add(generateSignature(realization, project, response.getDuration()));
        }
        // i.e. root,layout1_layout2;table1_table2;snapshot1_snapshot2;seg1_seg2,...
        return Joiner.on(",").join(signature);
    }

    // Schema Cache
    public static String createCacheSignature(List<String> tables, String project, String modelAlias) {
        val tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        List<String> signature = Lists.newArrayList();
        // TODO need rewrite
        //        if (queryService == null && SpringContext.getApplicationContext() != null) {
        //
        //        }
        queryService = SpringContext.getBean(CacheSignatureQuerySupporter.class);
        try {
            signature.add(queryService.onCreateAclSignature(project));
        } catch (Exception e) {
            logger.error("Fail to get acl signature: ", e);
            return "";
        }
        for (val table : tables) {
            TableDesc tableDesc = tableManager.getTableDesc(table);
            if (tableDesc == null) {
                return "";
            }
            signature.add(String.valueOf(tableDesc.getLastModified()));
        }
        if (modelAlias != null) {
            NDataModel modelDesc = modelManager.getDataModelDescByAlias(modelAlias);
            if (modelDesc == null) {
                return "";
            }
            signature.add(String.valueOf(modelDesc.getLastModified()));
        }
        return Joiner.on(",").join(signature);
    }

    // Query Cache
    public static boolean checkCacheExpired(SQLResponse sqlResponse, String project) {
        val signature = sqlResponse.getSignature();
        if (StringUtils.isBlank(signature)) {
            // pushdown cache is not supported by default because checkCacheExpired always return true
            return true;
        }
        // acl,realization1,realization2 ...
        if (signature.split(",").length != sqlResponse.getNativeRealizations().size() + 1) {
            return true;
        }
        val lastSignature = createCacheSignature(sqlResponse, project);
        if (!signature.equals(lastSignature)) {
            logger.info("[Signature Changed] old signature: [{}] new signature: [{}]", signature, lastSignature);
            return true;
        }

        return false;
    }

    // Schema Cache
    public static boolean checkCacheExpired(List<String> tables, String prevSignature, String project,
                                            String modelAlias) {
        if (StringUtils.isBlank(prevSignature)) {
            return true;
        }
        // acl,table1,table2 ...
        if (prevSignature.split(",").length != tables.size() + 1) {
            return true;
        }
        val currSignature = createCacheSignature(tables, project, modelAlias);
        return !prevSignature.equals(currSignature);
    }

    private static String generateSignature(NativeQueryRealization realization, String project, long sqlDuration) {
        try {
            return signature(realization, project, sqlDuration);
        } catch (NullPointerException e) {
            logger.warn("NPE occurred because metadata changed during query.", e);
            return "";
        }
    }

    private static String signature(NativeQueryRealization realization, String project, long sqlDuration) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        String realizationType = realization.getType();
        if (QueryMetricsContext.TABLE_SNAPSHOT.equals(realizationType)) {
            return processSnapshot(realization, project, kylinConfig);
        } else if (QueryMetricsContext.INTERNAL_TABLE.equals(realizationType)) {
            return processInternalTable(realization, project, kylinConfig);
        } else {
            return processIndex(realization, project, kylinConfig, sqlDuration);
        }
    }

    private static String processSnapshot(NativeQueryRealization realization,
                                          String project, KylinConfig kylinConfig) {
        // case snapshot:
        // type;snapshot1Time_snapshot2Time
        List<Long> timeOfSnapshots = new ArrayList<>();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(kylinConfig, project);
        for (String lookup : realization.getLookupTables()) {
            long lastModified = tableMgr.getTableDesc(lookup).getLastModified();
            if (lastModified == 0) {
                timeOfSnapshots.clear();
                break;
            }
            timeOfSnapshots.add(lastModified);
        }
        return timeOfSnapshots.isEmpty() ? "" : "0;" + Joiner.on("_").join(timeOfSnapshots);
    }

    private static String processInternalTable(NativeQueryRealization realization,
                                               String project, KylinConfig kylinConfig) {
        // case internalTable
        // type:internalTable1Time_internalTable2Time
        List<Long> timeOfInternalTable = new ArrayList<>();
        InternalTableManager internalTableManager = InternalTableManager.getInstance(kylinConfig, project);
        for (String internalTable : realization.getLookupTables()) {
            long lastModified = internalTableManager.getInternalTableDesc(internalTable).getLastModified();
            if (lastModified == 0) {
                timeOfInternalTable.clear();
                break;
            }
            timeOfInternalTable.add(lastModified);
        }
        return timeOfInternalTable.isEmpty() ? "" : "1;" + Joiner.on("_").join(timeOfInternalTable);
    }

    private static String processIndex(NativeQueryRealization realization, String project,
                                       KylinConfig kylinConfig, long sqlDuration) {
        // case index:
        // normal => type:layout1Time_layout2Time;table1Time_table2Time;seg1Time_seg2Time
        List<Long> allLayoutTimes = Lists.newLinkedList();
        List<Long> allSegmentTimes = Lists.newLinkedList();
        NDataflow df = NDataflowManager.getInstance(kylinConfig, project).getDataflow(realization.getModelId());
        if (df != null && !df.isOffline()) {
            for (NDataSegment seg : df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING)) {
                long now = System.currentTimeMillis();
                long latestTime = seg.getSegDetails().getLastModified();
                if (latestTime <= now && latestTime >= (now - sqlDuration)) {
                    return "";
                }
                allSegmentTimes.add(latestTime);
                Long layoutId = realization.getLayoutId();
                if (seg.getLayoutIds().contains(layoutId)) {
                    allLayoutTimes.add(seg.getLayout(layoutId).getCreateTime());
                }
            }
            String layoutSignatures = Joiner.on("_").join(allLayoutTimes);
            String segSignatures = Joiner.on("_").join(allSegmentTimes);
            String tableSignatures = df.getModel().getAllTableRefs().stream().map(TableRef::getTableDesc)
                    .map(TableDesc::getLastModified).map(String::valueOf).collect(Collectors.joining("_"));
            return "2;" + Joiner.on(";").join(layoutSignatures, tableSignatures, segSignatures);
        }
        return "";
    }
}
