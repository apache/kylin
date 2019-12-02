/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.spark.metadata.cube.model.Cube;
import org.apache.kylin.engine.spark.metadata.cube.model.CubeUpdate2;

import java.io.IOException;

public class ManagerHub {

    private ManagerHub() {}

    public static Cube getCube(KylinConfig kylinConfig, String cubeName) {
        return MetadataConverter.convertCubeInstance2Cube(
                CubeManager.getInstance(kylinConfig).getCube(cubeName)
        );
    }

    public static CubeInstance updateCube(KylinConfig kylinConfig, CubeUpdate2 cubeUpdate)
            throws IOException {
        return CubeManager.getInstance(kylinConfig).updateCube(
                MetadataConverter.convertCubeUpdate(cubeUpdate));
    }
}

