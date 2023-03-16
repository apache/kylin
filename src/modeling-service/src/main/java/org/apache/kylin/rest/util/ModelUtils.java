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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARTITION_COLUMN;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.constant.ModelAttributeEnum;
import org.apache.kylin.rest.response.NDataModelResponse;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;

public class ModelUtils {

    private ModelUtils() {
    }

    public static String computeExpansionRate(long storageBytesSize, long sourceBytesSize) {
        if (storageBytesSize == 0) {
            return "0";
        }
        if (sourceBytesSize == 0) {
            return "-1";
        }
        BigDecimal divide = new BigDecimal(storageBytesSize).divide(new BigDecimal(sourceBytesSize), 4,
                BigDecimal.ROUND_HALF_UP);
        BigDecimal bigDecimal = divide.multiply(new BigDecimal(100)).setScale(2);
        return bigDecimal.toString();
    }

    public static void checkPartitionColumn(NDataModel model, PartitionDesc partitionDesc, String errMsg) {
        if (!model.isBroken() && model.isStreaming() && isPartitionEmptyAndNotTimeStamp(partitionDesc)) {
            throw new KylinException(INVALID_PARTITION_COLUMN, errMsg);
        }
    }

    private static boolean isPartitionEmptyAndNotTimeStamp(PartitionDesc partitionDesc) {
        return PartitionDesc.isEmptyPartitionDesc(partitionDesc)
                || !DateFormat.isTimestampFormat(partitionDesc.getPartitionDateFormat());
    }

    public static void checkPartitionColumn(String project, String modelId, String errMsg) {
        val config = KylinConfig.getInstanceFromEnv();

        val modelMgr = NDataModelManager.getInstance(config, project);
        val model = modelMgr.getDataModelDesc(modelId);
        if (!model.isBroken() && model.isStreaming()) {
            val partitionDesc = model.getPartitionDesc();
            checkPartitionColumn(model, partitionDesc, errMsg);
        }
    }

    public static boolean isArgMatch(String valueToMatch, boolean exactMatch, String originValue) {
        return StringUtils.isEmpty(valueToMatch) || (exactMatch && originValue.equalsIgnoreCase(valueToMatch))
                || (!exactMatch
                        && originValue.toLowerCase(Locale.ROOT).contains(valueToMatch.toLowerCase(Locale.ROOT)));

    }

    public static Set<NDataModel> getFilteredModels(String project, List<ModelAttributeEnum> modelAttributes,
            List<NDataModel> models) {
        Set<ModelAttributeEnum> modelAttributeSet = Sets
                .newHashSet(modelAttributes == null ? Collections.emptyList() : modelAttributes);
        Set<NDataModel> filteredModels = new HashSet<>();
        if (SecondStorageUtil.isProjectEnable(project)) {
            val secondStorageInfos = SecondStorageUtil.setSecondStorageSizeInfo(models);
            val it = models.listIterator();
            while (it.hasNext()) {
                val secondStorageInfo = secondStorageInfos.get(it.nextIndex());
                NDataModelResponse modelResponse = (NDataModelResponse) it.next();
                modelResponse.setSecondStorageNodes(secondStorageInfo.getSecondStorageNodes());
                modelResponse.setSecondStorageSize(secondStorageInfo.getSecondStorageSize());
                modelResponse.setSecondStorageEnabled(secondStorageInfo.isSecondStorageEnabled());
            }
            if (modelAttributeSet.contains(ModelAttributeEnum.SECOND_STORAGE)) {
                filteredModels.addAll(ModelAttributeEnum.SECOND_STORAGE.filter(models));
                modelAttributeSet.remove(ModelAttributeEnum.SECOND_STORAGE);
            }
        }
        for (val attr : modelAttributeSet) {
            filteredModels.addAll(attr.filter(models));
        }
        return filteredModels;
    }

    public static void addSecondStorageInfo(String project, List<NDataModel> models) {
        if (SecondStorageUtil.isProjectEnable(project)) {
            val secondStorageInfos = SecondStorageUtil.setSecondStorageSizeInfo(models);
            val it = models.listIterator();
            while (it.hasNext()) {
                val secondStorageInfo = secondStorageInfos.get(it.nextIndex());
                NDataModelResponse modelResponse = (NDataModelResponse) it.next();
                modelResponse.setSecondStorageNodes(secondStorageInfo.getSecondStorageNodes());
                modelResponse.setSecondStorageSize(secondStorageInfo.getSecondStorageSize());
                modelResponse.setSecondStorageEnabled(secondStorageInfo.isSecondStorageEnabled());
            }
        }
    }

}
