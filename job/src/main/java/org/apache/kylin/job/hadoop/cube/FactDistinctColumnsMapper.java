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

package org.apache.kylin.job.hadoop.cube;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.job.hadoop.hive.CubeJoinedFlatTableDesc;

/**
 * @author yangli9
 */
public class FactDistinctColumnsMapper<KEYIN> extends KylinMapper<KEYIN, HCatRecord, ShortWritable, Text> {

    private CubeJoinedFlatTableDesc intermediateTableDesc;
    private int[] factDictColRowKeyIndexes;

    private ShortWritable outputKey = new ShortWritable();
    private Text outputValue = new Text();
    private int errorRecordCounter;

    private HCatSchema schema = null;

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();
        intermediateTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);

        String rowKeyIndexesStr = conf.get(BatchConstants.CFG_FACT_DICT_COLUMN_ROWKEY_INDEXES);
        if (!StringUtils.isEmpty(rowKeyIndexesStr)) {
            String[] rowKeyIndexes = rowKeyIndexesStr.split(",");
            factDictColRowKeyIndexes = new int[rowKeyIndexes.length];
            for (int i = 0; i < rowKeyIndexes.length; i++) {
                factDictColRowKeyIndexes[i] = Integer.parseInt(rowKeyIndexes[i]);
            }
        } else {
            factDictColRowKeyIndexes = new int[0];
        }

        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
    }

    @Override
    public void map(KEYIN key, HCatRecord record, Context context) throws IOException, InterruptedException {

        try {

            int[] flatTableIndexes = intermediateTableDesc.getRowKeyColumnIndexes();
            HCatFieldSchema fieldSchema = null;
            for (int i : factDictColRowKeyIndexes) {
                outputKey.set((short) i);
                fieldSchema = schema.get(flatTableIndexes[i]);
                Object fieldValue = record.get(fieldSchema.getName(), schema);
                if (fieldValue == null)
                    continue;
                byte[] bytes = Bytes.toBytes(fieldValue.toString());
                outputValue.set(bytes, 0, bytes.length);
                context.write(outputKey, outputValue);
            }
        } catch (Exception ex) {
            handleErrorRecord(record, ex);
        }

    }

    private void handleErrorRecord(HCatRecord record, Exception ex) throws IOException {

        System.err.println("Insane record: " + record.getAll());
        ex.printStackTrace(System.err);

        errorRecordCounter++;
        if (errorRecordCounter > BatchConstants.ERROR_RECORD_THRESHOLD) {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
