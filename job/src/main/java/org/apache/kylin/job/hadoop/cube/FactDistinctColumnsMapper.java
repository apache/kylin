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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.HiveTableReader;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author yangli9
 */
public class FactDistinctColumnsMapper<KEYIN> extends KylinMapper<KEYIN, HCatRecord, ShortWritable, Text> {

    private String cubeName;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private int[] factDictCols;

    private CubeJoinedFlatTableDesc intermediateTableDesc;

    private ShortWritable outputKey = new ShortWritable();
    private Text outputValue = new Text();
    private int errorRecordCounter;

    private HCatSchema schema = null;
    private CuboidScheduler cuboidScheduler = null;
    private List<String> rowKeyValues = null;
    private HyperLogLogPlusCounter hll;
    private long baseCuboidId;
    private int nRowKey;
    private boolean collectStatistics = false;

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        collectStatistics = Boolean.parseBoolean(conf.get(BatchConstants.CFG_STATISTICS_ENABLED));
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        intermediateTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);
        cuboidScheduler = new CuboidScheduler(cubeDesc);

        baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        List<TblColRef> columns = baseCuboid.getColumns();

        ArrayList<Integer> factDictCols = new ArrayList<Integer>();
        RowKeyDesc rowkey = cubeDesc.getRowkey();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        for (int i = 0; i < columns.size(); i++) {
            TblColRef col = columns.get(i);
            if (rowkey.isUseDictionary(col) == false)
                continue;

            String scanTable = (String) dictMgr.decideSourceData(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, null)[0];
            if (cubeDesc.getModel().isFactTable(scanTable)) {
                factDictCols.add(i);
            }
        }
        this.factDictCols = new int[factDictCols.size()];
        for (int i = 0; i < factDictCols.size(); i++)
            this.factDictCols[i] = factDictCols.get(i);

        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        rowKeyValues = Lists.newArrayList();
        nRowKey = cubeDesc.getRowkey().getRowKeyColumns().length;

        if(collectStatistics) {
            hll = new HyperLogLogPlusCounter(16);
        }
    }

    @Override
    public void map(KEYIN key, HCatRecord record, Context context) throws IOException, InterruptedException {
        try {
            int[] flatTableIndexes = intermediateTableDesc.getRowKeyColumnIndexes();
            HCatFieldSchema fieldSchema = null;
            for (int i : factDictCols) {
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

        if(collectStatistics) {
            String[] row = HiveTableReader.getRowAsStringArray(record);
            putRowKeyToHLL(row, baseCuboidId);
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

    private void putRowKeyToHLL(String[] row, long cuboidId) {
        rowKeyValues.clear();
        long mask = Long.highestOneBit(baseCuboidId);
        for (int i = 0; i < nRowKey; i++) {
            if ((mask & cuboidId) == 1) {
                rowKeyValues.add(row[intermediateTableDesc.getRowKeyColumnIndexes()[i]]);
            }
            mask = mask >> 1;
        }

        String key = StringUtils.join(rowKeyValues, ",");
        hll.add(key);

        Collection<Long> children = cuboidScheduler.getSpanningCuboid(cuboidId);
        for (Long childId : children) {
            putRowKeyToHLL(row, childId);
        }

    }

    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        if(collectStatistics) {
            // output hll to reducer, key is -1
            // keyBuf = Bytes.toBytes(-1);
            outputKey.set((short) -1);
            ByteBuffer hllBuf = ByteBuffer.allocate(64 * 1024);
            hll.writeRegisters(hllBuf);
            outputValue.set(hllBuf.array());
            context.write(outputKey, outputValue);
        }
    }


}
