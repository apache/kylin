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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RealizationTimeSignatureUtil {

    public static final Logger logger = LoggerFactory.getLogger(RealizationTimeSignatureUtil.class);

    public static final long CODE_BAR = 0L;
    public static final long CODE_NOT_FOUND = -1L;
    public static final long CODE_DISABLED = -2L;

    public static long getTimeSignature(String[] realizations) {
        long signatureTime = CODE_BAR;
        for (String realizationName : realizations) {
            long realizationSignatureTime = getCubeTimeSignature(realizationName);
            if (realizationSignatureTime < CODE_BAR) {
                if (realizationSignatureTime != CODE_NOT_FOUND) {
                    return realizationSignatureTime;
                } else {
                    realizationSignatureTime = getHybridTimeSignature(realizationName);
                    if (realizationSignatureTime < CODE_BAR) {
                        return realizationSignatureTime;
                    }
                }
            }
            if (signatureTime < realizationSignatureTime) {
                signatureTime = realizationSignatureTime;
            }
        }
        return signatureTime;
    }

    public static long getHybridTimeSignature(String realizationName) {
        long lastHybridTime = CODE_DISABLED;
        HybridInstance hybridInstance = HybridManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getHybridInstance(realizationName);
        if (hybridInstance == null) {
            logger.info("Hybrid [{}] is not found or disabled, will skip query cache.", realizationName);
            return CODE_NOT_FOUND;
        }
        for (IRealization realization : hybridInstance.getRealizations()) {
            long realizationTime = CODE_BAR;
            if (realization.getType() == RealizationType.CUBE) {
                realizationTime = getCubeTimeSignature(realization.getName());
            } else if (realization.getType() == RealizationType.HYBRID) {
                realizationTime = getHybridTimeSignature(realization.getName());
            }
            if (realizationTime < CODE_BAR) {
                return realizationTime;
            }
            if (lastHybridTime < realizationTime) {
                lastHybridTime = realizationTime;
            }
        }
        return lastHybridTime;
    }

    public static long getCubeTimeSignature(String realizationName) {
        long lastCubeTime = CODE_DISABLED;
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(realizationName);
        if (cubeInstance == null) {
            logger.info("Cube [{}] is not found.", realizationName);
            return CODE_NOT_FOUND;
        } else if (cubeInstance.getStatus() == RealizationStatusEnum.DISABLED) {
            logger.info("Cube [{}] is disabled, will skip query cache.", realizationName);
            return CODE_DISABLED;
        }
        for (CubeSegment cubeSeg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
            long buildTime = cubeSeg.getLastBuildTime();
            if (lastCubeTime < buildTime) {
                lastCubeTime = buildTime;
            }
        }
        return lastCubeTime;
    }
}
