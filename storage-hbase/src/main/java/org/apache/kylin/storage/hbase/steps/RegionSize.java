/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * 
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  * 
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 * /
 */

package org.apache.kylin.storage.hbase.steps;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionSize {
    protected static final Logger logger = LoggerFactory.getLogger(CreateHTableJob.class);

    public static float getReionSize(KylinConfig config, CubeDesc cubeDesc) {
        if (cubeDesc.getRegionSize() != 0) { 
            logger.info("Region size specified in Cube desc will be used");
            return cubeDesc.getRegionSize();
        } else {
            logger.info("Region size specified in Model desc will be used");
            
            switch (cubeDesc.getModel().getCapacity().toString()) {
            case "SMALL":
                return config.getKylinHBaseRegionCutSmall();
            case "MEDIUM":
                return config.getKylinHBaseRegionCutMedium();
            case "LARGE":
                return config.getKylinHBaseRegionCutLarge();
            default:
                throw new IllegalArgumentException("Capacity not recognized: " + cubeDesc.getModel().getCapacity().toString());
            }
        }
    }

}
