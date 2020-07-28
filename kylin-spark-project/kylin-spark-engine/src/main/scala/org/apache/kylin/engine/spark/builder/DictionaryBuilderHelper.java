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

import java.io.IOException;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.ColumnDesc;
import org.apache.spark.dict.NGlobalDictMetaInfo;
import org.apache.spark.dict.NGlobalDictionary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.dict.NGlobalDictBuilderAssist.resize;

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
    public static int calculateBucketSize(SegmentInfo desc, ColumnDesc col, Dataset<Row> afterDistinct) throws IOException {
        NGlobalDictionary globalDict = new NGlobalDictionary(desc.project(), col.tableAliasName(), col.columnName(),
                desc.kylinconf().getHdfsWorkingDirectory());
        int bucketPartitionSize = globalDict.getBucketSizeOrDefault(desc.kylinconf().getGlobalDictV2MinHashPartitions());
        int bucketThreshold = desc.kylinconf().getGlobalDictV2ThresholdBucketSize();
        int resizeBucketSize = bucketPartitionSize;

        if (globalDict.isFirst()) {
            long afterDisCount = afterDistinct.count();
            double loadFactor = desc.kylinconf().getGlobalDictV2InitLoadFactor();
            resizeBucketSize = Math.max(Math.toIntExact(afterDisCount / (int) (bucketThreshold * loadFactor)),
                    bucketPartitionSize);
            logger.info("Building a global dictionary column first for  {} , the size of the bucket is set to {}",
                    col.columnName(), bucketPartitionSize);
        } else {
            long afterDisCount = afterDistinct.count();
            NGlobalDictMetaInfo metaInfo = globalDict.getMetaInfo();
            long[] bucketCntArray = metaInfo.getBucketCount();

            double loadFactor = desc.kylinconf().getGlobalDictV2InitLoadFactor();
            double bucketOverheadFactor = desc.kylinconf().getGlobalDictV2BucketOverheadFactor();

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
                logger.info("Start building a global dictionary column for {}, need resize from {} to {} ", col.columnName(),
                        bucketPartitionSize, resizeBucketSize);
                resize(col, desc, resizeBucketSize, afterDistinct.sparkSession());
                logger.info("End building a global dictionary column for {}, need resize from {} to {} ", col.columnName(),
                        bucketPartitionSize, resizeBucketSize);
            }
        }

        return resizeBucketSize;
    }

//    private static Set<ColumnDesc> findNeedDictCols(List<LayoutEntity> layouts) {
//        Set<ColumnDesc> dictColSet = Sets.newHashSet();
//        for (LayoutEntity layout : layouts) {
//            for (FunctionDesc functionDesc : layout.getOrderedMeasures().values()) {
//                if (needGlobalDict(functionDesc))
//                    continue;
//                ColumnDesc col = functionDesc.pra().head();
//                dictColSet.add(col);
//            }
//        }
//        return dictColSet;
//    }
//
//    public static Set<ColumnDesc> extractTreeRelatedGlobalDictToBuild(DataSegment seg, SpanningTree toBuildTree) {
//        Collection<IndexEntity> toBuildIndexEntities = toBuildTree.getAllIndexEntities();
//        List<LayoutEntity> toBuildCuboids = Lists.newArrayList();
//        for (IndexEntity desc : toBuildIndexEntities) {
//            toBuildCuboids.addAll(desc.getLayouts());
//        }
//        List<LayoutEntity> buildedLayouts = Lists.newArrayList();
//        if (seg.getSegDetails() != null) {
//            for (DataLayout cuboid : seg.getSegDetails().getLayouts()) {
//                buildedLayouts.add(cuboid.getLayout());
//            }
//        }
//        Set<TblColRef> buildedColRefSet = findNeedDictCols(buildedLayouts);
//        Set<TblColRef> toBuildColRefSet = findNeedDictCols(toBuildCuboids);
//        toBuildColRefSet.removeIf(col -> buildedColRefSet.contains(col));
//        return toBuildColRefSet;
//    }
//
//    public static Set<ColumnDesc> extractTreeRelatedGlobalDicts(DataSegment seg, SpanningTree toBuildTree) {
//        List<LayoutEntity> toBuildCuboids = toBuildTree.getAllIndexEntities().stream()
//                .flatMap(entity -> entity.getLayouts().stream()).collect(Collectors.toList());
//        return findNeedDictCols(toBuildCuboids);
//    }
//
//    public static Boolean needGlobalDict(FunctionDesc functionDesc) {
//        if (functionDesc.returnType().dataType().equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)) {
//            Preconditions.checkArgument(functionDesc.pra().size() == 1);
//            return true;
//        }
//        return false;
//    }
}
