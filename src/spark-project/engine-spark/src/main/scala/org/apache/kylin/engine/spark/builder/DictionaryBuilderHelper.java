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

package org.apache.kylin.engine.spark.builder;

import static org.apache.spark.dict.NGlobalDictBuilderAssist.resize;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.spark.dict.NGlobalDictMetaInfo;
import org.apache.spark.dict.NGlobalDictionaryV2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DictionaryBuilderHelper {
    protected static final Logger logger = LoggerFactory.getLogger(DictionaryBuilderHelper.class);

    /**
     * Dictionary resize in three cases
     *  #1 The number of dictionaries currently needed to be built is greater than the number of
     *  buckets multiplied by the threshold
     *  #2 After the last build, the total number of existing dictionaries is greater than the total
     *  number of buckets multiplied by the threshold
     *  #3 After the last build, the number of individual buckets in the existing dictionary is greater
     *  than the threshold multiplied by KylinConfigBase.getGlobalDictV2BucketOverheadFactor
     */
    public static int calculateBucketSize(NDataSegment seg, TblColRef col, Dataset<Row> afterDistinct)
            throws IOException {
        NGlobalDictionaryV2 globalDict = new NGlobalDictionaryV2(seg.getProject(), col.getTable(), col.getName(),
                seg.getConfig().getHdfsWorkingDirectory());
        int bucketPartitionSize = globalDict.getBucketSizeOrDefault(seg.getConfig().getGlobalDictV2MinHashPartitions());
        int bucketThreshold = seg.getConfig().getGlobalDictV2ThresholdBucketSize();
        int resizeBucketSize = bucketPartitionSize;

        if (globalDict.isFirst()) {
            long afterDisCount = afterDistinct.count();
            double loadFactor = seg.getConfig().getGlobalDictV2InitLoadFactor();
            resizeBucketSize = Math.max(Math.toIntExact(afterDisCount / (int) (bucketThreshold * loadFactor)),
                    bucketPartitionSize);
            logger.info("Building a global dictionary column first for  {} , the size of the bucket is set to {}",
                    col.getName(), bucketPartitionSize);
        } else {
            long afterDisCount = afterDistinct.count();
            NGlobalDictMetaInfo metaInfo = globalDict.getMetaInfo();
            long[] bucketCntArray = metaInfo.getBucketCount();

            double loadFactor = seg.getConfig().getGlobalDictV2InitLoadFactor();
            double bucketOverheadFactor = seg.getConfig().getGlobalDictV2BucketOverheadFactor();

            int averageBucketSize = 0;

            // rule #1
            int newDataBucketSize = Math.toIntExact(afterDisCount / bucketThreshold);
            if (newDataBucketSize > metaInfo.getBucketSize()) {
                newDataBucketSize = Math.toIntExact(afterDisCount / (int) (bucketThreshold * loadFactor));
            }

            // rule #2
            if (metaInfo.getDictCount() >= bucketThreshold * metaInfo.getBucketSize()) {
                averageBucketSize = Math.toIntExact(metaInfo.getDictCount() / (int) (bucketThreshold * loadFactor));
            }

            int peakBucketSize = 0;
            //rule #3
            for (long bucketCnt : bucketCntArray) {
                if (bucketCnt > bucketThreshold * bucketOverheadFactor) {
                    peakBucketSize = bucketPartitionSize * 2;
                    break;
                }
            }

            resizeBucketSize = Math.max(Math.max(newDataBucketSize, averageBucketSize),
                    Math.max(peakBucketSize, bucketPartitionSize));

            if (resizeBucketSize != bucketPartitionSize) {
                logger.info("Start building a global dictionary column for {}, need resize from {} to {} ",
                        col.getName(), bucketPartitionSize, resizeBucketSize);
                resize(col, seg, resizeBucketSize, afterDistinct.sparkSession());
                logger.info("End building a global dictionary column for {}, need resize from {} to {} ", col.getName(),
                        bucketPartitionSize, resizeBucketSize);
            }
        }

        return resizeBucketSize;
    }

    protected static Set<TblColRef> findNeedDictCols(List<LayoutEntity> layouts) {
        Set<TblColRef> dictColSet = Sets.newHashSet();
        for (LayoutEntity layout : layouts) {
            for (MeasureDesc measureDesc : layout.getIndex().getEffectiveMeasures().values()) {
                if (needGlobalDict(measureDesc) == null)
                    continue;
                TblColRef col = measureDesc.getFunction().getParameters().get(0).getColRef();
                dictColSet.add(col);
            }
        }
        return dictColSet;
    }

    public static Set<TblColRef> extractTreeRelatedGlobalDictToBuild(NDataSegment seg,
            Collection<IndexEntity> toBuildIndexEntities) {
        List<LayoutEntity> toBuildCuboids = Lists.newArrayList();
        for (IndexEntity desc : toBuildIndexEntities) {
            toBuildCuboids.addAll(desc.getLayouts());
        }

        List<LayoutEntity> buildedLayouts = Lists.newArrayList();
        if (seg.getSegDetails() != null) {
            for (NDataLayout cuboid : seg.getSegDetails().getWorkingLayouts()) {
                buildedLayouts.add(cuboid.getLayout());
            }
        }
        Set<TblColRef> buildedColRefSet = findNeedDictCols(buildedLayouts);
        Set<TblColRef> toBuildColRefSet = findNeedDictCols(toBuildCuboids);
        toBuildColRefSet.removeIf(col -> buildedColRefSet.contains(col));
        return toBuildColRefSet;
    }

    public static Set<TblColRef> extractTreeRelatedGlobalDicts(NDataSegment seg,
            Collection<IndexEntity> toBuildIndexEntities) {
        List<LayoutEntity> toBuildCuboids = toBuildIndexEntities.stream()
                .flatMap(entity -> entity.getLayouts().stream()).collect(Collectors.toList());
        return findNeedDictCols(toBuildCuboids);
    }

    public static TblColRef needGlobalDict(MeasureDesc measure) {
        String returnDataTypeName = measure.getFunction().getReturnDataType().getName();
        if (returnDataTypeName.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)) {
            List<TblColRef> cols = measure.getFunction().getColRefs();
            Preconditions.checkArgument(cols.size() >= 1);
            return cols.get(0);
        }
        return null;
    }
}
