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
package org.apache.kylin.source.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class UpdateTimeRangeStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(UpdateTimeRangeStep.class);

    public UpdateTimeRangeStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment segment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        final TblColRef partitionCol = segment.getCubeDesc().getModel().getPartitionDesc().getPartitionDateColumnRef();
        final String outputPath = this.getParams().get(BatchConstants.CFG_OUTPUT_PATH);
        final Path outputFile = new Path(outputPath, partitionCol.getName());

        String minValue = null, maxValue = null, currentValue = null;
        try (FileSystem fs = HadoopUtil.getFileSystem(outputPath); FSDataInputStream inputStream = fs.open(outputFile); BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            minValue = currentValue = bufferedReader.readLine();
            while (currentValue != null) {
                maxValue = currentValue;
                currentValue = bufferedReader.readLine();
            }
        } catch (IOException e) {
            logger.error("fail to read file " + outputFile, e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }

        final DataType partitionColType = partitionCol.getType();
        FastDateFormat dateFormat;
        if (partitionColType.isDate()) {
            dateFormat = DateFormat.getDateFormat(DateFormat.DEFAULT_DATE_PATTERN);
        } else if (partitionColType.isDatetime() || partitionColType.isTimestamp()) {
            dateFormat = DateFormat.getDateFormat(DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        } else if (partitionColType.isStringFamily()) {
            String partitionDateFormat = segment.getCubeDesc().getModel().getPartitionDesc().getPartitionDateFormat();
            if (StringUtils.isEmpty(partitionDateFormat)) {
                partitionDateFormat = DateFormat.DEFAULT_DATE_PATTERN;
            }
            dateFormat = DateFormat.getDateFormat(partitionDateFormat);
        } else {
            return new ExecuteResult(ExecuteResult.State.ERROR, "Type " + partitionColType + " is not valid partition column type");
        }

        try {
            long startTime = dateFormat.parse(minValue).getTime();
            long endTime = dateFormat.parse(maxValue).getTime();
            CubeUpdate cubeBuilder = new CubeUpdate(cube);
            segment.setDateRangeStart(startTime);
            segment.setDateRangeEnd(endTime);
            cubeBuilder.setToUpdateSegs(segment);
            cubeManager.updateCube(cubeBuilder);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (Exception e) {
            logger.error("fail to update cube segment offset", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

}
