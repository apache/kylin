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

package org.apache.kylin.tool.metrics.systemcube;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.tool.metrics.systemcube.def.MetricsSinkDesc;

public class CubeInstanceCreator {

    public static void main(String[] args) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        CubeInstance cubeInstance = generateKylinCubeInstanceForMetricsQueryExecution("ADMIN", config, new MetricsSinkDesc());
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        CubeManager.CUBE_SERIALIZER.serialize(cubeInstance, dout);
        dout.close();
        buf.close();
        System.out.println(buf.toString("UTF-8"));
    }

    public static CubeInstance generateKylinCubeInstanceForMetricsQueryExecution(String owner, KylinConfig config,
            MetricsSinkDesc sinkDesc) {
        return generateKylinCubeInstance(owner, sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectQueryExecution()));
    }

    public static CubeInstance generateKylinCubeInstanceForMetricsQuerySparkJob(String owner, KylinConfig config,
            MetricsSinkDesc sinkDesc) {
        return generateKylinCubeInstance(owner,
                sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectQuerySparkJob()));
    }

    public static CubeInstance generateKylinCubeInstanceForMetricsQuerySparkStage(String owner, KylinConfig config,
            MetricsSinkDesc sinkDesc) {
        return generateKylinCubeInstance(owner,
                sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectQuerySparkStage()));
    }

    public static CubeInstance generateKylinCubeInstanceForMetricsJob(String owner, KylinConfig config,
            MetricsSinkDesc sinkDesc) {
        return generateKylinCubeInstance(owner, sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectJob()));
    }

    public static CubeInstance generateKylinCubeInstanceForMetricsJobException(String owner, KylinConfig config,
            MetricsSinkDesc sinkDesc) {
        return generateKylinCubeInstance(owner,
                sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectJobException()));
    }

    public static CubeInstance generateKylinCubeInstance(String owner, String tableName) {
        CubeInstance cubeInstance = new CubeInstance();
        cubeInstance.setName(tableName.replace('.', '_'));
        cubeInstance.setSegments(new Segments<CubeSegment>());
        cubeInstance.setDescName(tableName.replace('.', '_'));
        cubeInstance.setStatus(RealizationStatusEnum.DISABLED);
        cubeInstance.setOwner(owner);
        cubeInstance.setCreateTimeUTC(0L);
        cubeInstance.updateRandomUuid();

        return cubeInstance;
    }
}
