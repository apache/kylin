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
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryMetricsContext;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.CacheSignatureQuerySupporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        val modelId = realization.getModelId();
        Long layoutId = realization.getLayoutId();
        try {
            val dataflow = NDataflowManager.getInstance(kylinConfig, project).getDataflow(modelId);
            if (dataflow.getStatus().toString().equals("OFFLINE")) {
                return "";
            }
            List<Long> allLayoutTimes = Lists.newLinkedList();
            List<Long> allSnapshotTimes = Lists.newLinkedList();
            List<Long> allSegmentTimes = Lists.newLinkedList();
            List<Long> allTableTimes = Lists.newLinkedList();
            NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(kylinConfig, project);
            for (String snapshot : realization.getSnapshots()) {
                long snapshotModificationTime = tableMetadataManager.getTableDesc(snapshot).getLastModified();
                if (snapshotModificationTime == 0) {
                    return "";
                } else {
                    allSnapshotTimes.add(snapshotModificationTime);
                }
            }
            if (!QueryMetricsContext.TABLE_SNAPSHOT.equals(realization.getIndexType())) {
                for (NDataSegment seg : dataflow.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING)) {
                    long now = System.currentTimeMillis();
                    long latestTime = seg.getSegDetails().getLastModified();
                    if (latestTime <= now && latestTime >= (now - sqlDuration)) {
                        return "";
                    }
                    allSegmentTimes.add(latestTime);
                    if (seg.getLayoutIds().contains(layoutId)) {
                        allLayoutTimes.add(seg.getLayout(layoutId).getCreateTime());
                    }
                }

                Set<TableRef> allTableRefs = dataflow.getModel().getAllTableRefs();
                for (TableRef tableRef : allTableRefs) {
                    allTableTimes.add(tableRef.getTableDesc().getLastModified());
                }
            }
            String allLayoutTimesSignature = Joiner.on("_").join(allLayoutTimes);
            String allTableTimesSignature = Joiner.on("_").join(allTableTimes);
            String allSnapshotTimesSignature = Joiner.on("_").join(allSnapshotTimes);
            String allSegmentTimesSignature = Joiner.on("_").join(allSegmentTimes);
            return Joiner.on(";").join(allLayoutTimesSignature, allTableTimesSignature, allSnapshotTimesSignature, allSegmentTimesSignature);
        } catch (NullPointerException e) {
            logger.warn("NPE occurred because metadata changed during query.", e);
            return "";
        }
    }
}
