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

package org.apache.kylin.engine.mr.streaming;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ColumnarSplitReader<KEYIN, VALUEIN> extends RecordReader {
    private static Logger logger = LoggerFactory.getLogger(ColumnarSplitReader.class);

    //    protected AtomicInteger readCount;
    protected String cubeName;
    protected String segmentName;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;

    public ColumnarSplitReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
            InterruptedException {
        initialize(inputSplit, context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (!(split instanceof FileSplit)) {
            throw new IllegalArgumentException("Only compatible with FileSplits.");
        } else {
            logger.debug("CFG_Cube_Name: " + BatchConstants.CFG_CUBE_NAME);
            cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase(Locale.ROOT);
            segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME).toUpperCase(Locale.ROOT);

            KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
            CubeManager cubeManager = CubeManager.getInstance(config);
            cube = cubeManager.getCube(cubeName);
            cubeDesc = cube.getDescriptor();
            cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        }
    }

}
