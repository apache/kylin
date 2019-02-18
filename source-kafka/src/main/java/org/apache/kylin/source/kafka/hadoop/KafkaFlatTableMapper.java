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

package org.apache.kylin.source.kafka.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.StreamingMessageRow;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.source.kafka.model.StreamCubeFactTableDesc;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.StreamingParser;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaFlatTableMapper extends KylinMapper<LongWritable, BytesWritable, BytesWritable, Text> {

    private BytesWritable outKey = new BytesWritable();
    private static final Logger logger = LoggerFactory.getLogger(KafkaFlatTableMapper.class);
    private Text outValue = new Text();
    private KylinConfig config;
    private CubeSegment cubeSegment;
    private StreamingParser streamingParser;
    private String data;
    private String delimiter;

    @Override
    protected void doSetup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        bindCurrentConfiguration(conf);

        config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        final CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        this.cubeSegment = cube.getSegmentById(conf.get(BatchConstants.CFG_CUBE_SEGMENT_ID));
        this.delimiter = BatchConstants.SEQUENCE_FILE_DEFAULT_DELIMITER; //sequence file default delimiter
        logger.info("Use delimiter: " + delimiter);
        final KafkaConfigManager kafkaConfigManager = KafkaConfigManager.getInstance(config);
        final KafkaConfig kafkaConfig = kafkaConfigManager.getKafkaConfig(cubeSegment.getCubeInstance().getRootFactTable());

        final IJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(cubeSegment);
        final StreamCubeFactTableDesc streamFactTableDesc = new StreamCubeFactTableDesc(cubeSegment.getCubeDesc(), cubeSegment, flatTableDesc);

        final List<TblColRef> allColumns = streamFactTableDesc.getAllColumns();

        try {
            streamingParser = StreamingParser.getStreamingParser(kafkaConfig.getParserName(), kafkaConfig.getAllParserProperties(), allColumns);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(e);
        }

    }

    @Override
    public void doMap(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        ByteBuffer buffer = ByteBuffer.wrap(value.getBytes(), 0, value.getLength());
        StreamingMessageRow row = streamingParser.parse(buffer).get(0);
        if (row == null) {
            throw new IllegalArgumentException("");
        }

        data = StringUtil.join(row.getData(), delimiter);
        // output this row to value
        outValue.set(Bytes.toBytes(data));
        context.write(outKey, outValue);
    }
}
