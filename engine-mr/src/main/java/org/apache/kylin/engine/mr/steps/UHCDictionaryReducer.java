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

package org.apache.kylin.engine.mr.steps;

import static org.apache.kylin.engine.mr.steps.FactDistinctColumnsReducer.DICT_FILE_POSTFIX;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.IDictionaryBuilder;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UHCDictionaryReducer extends KylinReducer<SelfDefineSortableKey, NullWritable, NullWritable, BytesWritable> {
    private static final Logger logger = LoggerFactory.getLogger(UHCDictionaryReducer.class);

    private IDictionaryBuilder builder;
    private TblColRef col;

    private MultipleOutputs mos;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        Configuration conf = context.getConfiguration();
        mos = new MultipleOutputs(context);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();
        List<TblColRef> uhcColumns = cubeDesc.getAllUHCColumns();

        int taskId = context.getTaskAttemptID().getTaskID().getId();
        col = uhcColumns.get(taskId);
        logger.info("column name: " + col.getIdentity());

        if (cube.getDescriptor().getShardByColumns().contains(col)) {
            //for ShardByColumns
            builder = DictionaryGenerator.newDictionaryBuilder(col.getType());
            builder.init(null, 0, null);
        } else {
            //for GlobalDictionaryColumns
            String hdfsDir = conf.get(BatchConstants.CFG_GLOBAL_DICT_BASE_DIR);
            DictionaryInfo dictionaryInfo = new DictionaryInfo(col.getColumnDesc(), col.getDatatype());
            String builderClass = cubeDesc.getDictionaryBuilderClass(col);
            builder = (IDictionaryBuilder) ClassUtil.newInstance(builderClass);
            builder.init(dictionaryInfo, 0, hdfsDir);
        }
    }

    @Override
    public void doReduce(SelfDefineSortableKey skey, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Text key = skey.getText();
        String value = Bytes.toString(key.getBytes(), 1, key.getLength() - 1);
        builder.addValue(value);
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        Dictionary<String> dict = builder.build();
        outputDict(col, dict);
    }

    private void outputDict(TblColRef col, Dictionary<String> dict) throws IOException, InterruptedException {
        // output written to baseDir/colName/colName.rldict-r-00000 (etc)
        String dictFileName = col.getIdentity() + "/" + col.getName() + DICT_FILE_POSTFIX;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream outputStream = new DataOutputStream(baos);) {
            outputStream.writeUTF(dict.getClass().getName());
            dict.write(outputStream);

            mos.write(BatchConstants.CFG_OUTPUT_DICT, NullWritable.get(), new ArrayPrimitiveWritable(baos.toByteArray()), dictFileName);
        }
        mos.close();
    }
}
