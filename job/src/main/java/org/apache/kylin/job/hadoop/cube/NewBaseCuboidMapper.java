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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.lookup.HiveTable;
import org.apache.kylin.dict.lookup.LookupBytesTable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author George Song (ysong1),honma
 */
public class NewBaseCuboidMapper<KEYIN> extends KylinMapper<KEYIN, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(NewBaseCuboidMapper.class);

    private String cubeName;
    private String segmentName;
    private Cuboid baseCuboid;
    private CubeInstance cube;
    private CubeSegment cubeSegment;

    private CubeDesc cubeDesc;
    private MetadataManager metadataManager;
    private TableDesc factTableDesc;

    private boolean byteRowDelimiterInferred = false;
    private byte byteRowDelimiter;

    private int counter;
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private Object[] measures;
    private byte[][] keyBytesBuf;
    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

    private BytesSplitter bytesSplitter;
    private AbstractRowKeyEncoder rowKeyEncoder;
    private MeasureCodec measureCodec;

    // deal with table join
    private HashMap<String, LookupBytesTable> lookupTables;// name -> table
    private LinkedList<TableJoin> tableJoins;
    private LinkedList<Pair<Integer, Integer>> factTblColAsRowKey;// similar as
    // TableJoin.dimTblColAsRowKey
    private int[][] measureColumnIndice;
    private byte[] nullValue;

    private class TableJoin {
        public LinkedList<Integer> fkIndice;// zero-based join columns on fact
        // table
        public String lookupTableName;
        public String joinType;

        // Pair.first -> zero-based column index in lookup table
        // Pair.second -> zero based row key index
        public LinkedList<Pair<Integer, Integer>> dimTblColAsRowKey;

        private TableJoin(String joinType, LinkedList<Integer> fkIndice, String lookupTableName, LinkedList<Pair<Integer, Integer>> dimTblColAsRowKey) {
            this.joinType = joinType;
            this.fkIndice = fkIndice;
            this.lookupTableName = lookupTableName;
            this.dimTblColAsRowKey = dimTblColAsRowKey;
        }
    }

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        metadataManager = MetadataManager.getInstance(config);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        cubeDesc = cube.getDescriptor();
        factTableDesc = metadataManager.getTableDesc(cubeDesc.getFactTable());

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);

        // intermediateTableDesc = new
        // JoinedFlatTableDesc(cube.getDescriptor());

        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);

        measureCodec = new MeasureCodec(cubeDesc.getMeasures());
        measures = new Object[cubeDesc.getMeasures().size()];

        int colCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        keyBytesBuf = new byte[colCount][];

        bytesSplitter = new BytesSplitter(factTableDesc.getColumns().length, 4096);

        nullValue = new byte[] { (byte) '\\', (byte) 'N' };// As in Hive, null
        // value is
        // represented by \N

        prepareJoins();
        prepareMetrics();
    }

    private void prepareJoins() throws IOException {
        this.lookupTables = new HashMap<String, LookupBytesTable>();
        this.tableJoins = new LinkedList<TableJoin>();
        this.factTblColAsRowKey = new LinkedList<Pair<Integer, Integer>>();

        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            JoinDesc join = dim.getJoin();
            if (join != null) {
                String joinType = join.getType().toUpperCase();
                String lookupTableName = dim.getTable();

                // load lookup tables
                if (!lookupTables.containsKey(lookupTableName)) {
                    HiveTable htable = new HiveTable(metadataManager, lookupTableName);
                    LookupBytesTable btable = new LookupBytesTable(metadataManager.getTableDesc(lookupTableName), join.getPrimaryKey(), htable);
                    lookupTables.put(lookupTableName, btable);
                }

                // create join infos
                LinkedList<Integer> fkIndice = new LinkedList<Integer>();
                for (TblColRef colRef : join.getForeignKeyColumns()) {
                    fkIndice.add(colRef.getColumn().getZeroBasedIndex());
                }
                this.tableJoins.add(new TableJoin(joinType, fkIndice, lookupTableName, this.findColumnRowKeyRelationships(dim)));

            } else {

                this.factTblColAsRowKey.addAll(this.findColumnRowKeyRelationships(dim));
            }
        }

        // put composite keys joins ahead of single key joins
        Collections.sort(tableJoins, new Comparator<TableJoin>() {
            @Override
            public int compare(TableJoin o1, TableJoin o2) {
                return Integer.valueOf(o2.fkIndice.size()).compareTo(Integer.valueOf(o1.fkIndice.size()));
            }
        });
    }

    private LinkedList<Pair<Integer, Integer>> findColumnRowKeyRelationships(DimensionDesc dim) {
        LinkedList<Pair<Integer, Integer>> dimTblColAsRowKey = new LinkedList<Pair<Integer, Integer>>();
        for (TblColRef colRef : dim.getColumnRefs()) {
            int dimTableIndex = colRef.getColumn().getZeroBasedIndex();
            int rowKeyIndex = cubeDesc.getRowkey().getRowKeyIndexByColumnName(colRef.getName());
            dimTblColAsRowKey.add(new Pair<Integer, Integer>(dimTableIndex, rowKeyIndex));
        }
        return dimTblColAsRowKey;
    }

    private void prepareMetrics() {
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        int measureSize = measures.size();
        measureColumnIndice = new int[measureSize][];
        for (int i = 0; i < measureSize; i++) {
            FunctionDesc func = measures.get(i).getFunction();
            List<TblColRef> colRefs = func.getParameter().getColRefs();
            if (colRefs == null) {
                measureColumnIndice[i] = null;
            } else {
                measureColumnIndice[i] = new int[colRefs.size()];
                for (int j = 0; j < colRefs.size(); j++) {
                    TblColRef c = colRefs.get(j);
                    int factTblIdx = factTableDesc.findColumnByName(c.getName()).getZeroBasedIndex();
                    measureColumnIndice[i][j] = factTblIdx;
                }
            }
        }
    }

    private byte[] trimSplitBuffer(SplittedBytes splittedBytes) {
        return Arrays.copyOf(splittedBytes.value, splittedBytes.length);
    }

    private byte[] buildKey(SplittedBytes[] splitBuffers) {

        int filledDimension = 0;// debug

        // join lookup tables, and fill into RowKey the columns in lookup table
        for (TableJoin tableJoin : this.tableJoins) {
            String dimTblName = tableJoin.lookupTableName;
            LookupBytesTable dimTbl = this.lookupTables.get(dimTblName);
            ByteArray[] rawKey = new ByteArray[tableJoin.fkIndice.size()];
            for (int i = 0; i < tableJoin.fkIndice.size(); ++i) {
                rawKey[i] = new ByteArray(trimSplitBuffer(splitBuffers[tableJoin.fkIndice.get(i)]));
            }
            Array<ByteArray> key = new Array<ByteArray>(rawKey);
            ByteArray[] dimRow = dimTbl.getRow(key);
            if (dimRow == null) {
                if (tableJoin.joinType.equalsIgnoreCase("INNER")) {
                    return null;
                } else if (tableJoin.joinType.equalsIgnoreCase("LEFT")) {
                    for (Pair<Integer, Integer> relation : tableJoin.dimTblColAsRowKey) {
                        keyBytesBuf[relation.getSecond()] = nullValue;
                        filledDimension++;
                    }
                }
            } else {
                for (Pair<Integer, Integer> relation : tableJoin.dimTblColAsRowKey) {
                    keyBytesBuf[relation.getSecond()] = dimRow[relation.getFirst()].array();
                    filledDimension++;
                }
            }
        }

        // fill into RowKey the columns in fact table
        for (Pair<Integer, Integer> relation : this.factTblColAsRowKey) {
            keyBytesBuf[relation.getSecond()] = trimSplitBuffer(splitBuffers[relation.getFirst()]);
            filledDimension++;
        }

        assert filledDimension == keyBytesBuf.length;

        // all the row key slots(keyBytesBuf) should be complete now
        return rowKeyEncoder.encode(keyBytesBuf);
    }

    private void buildValue(SplittedBytes[] splitBuffers) {

        for (int i = 0; i < measures.length; i++) {
            byte[] valueBytes = getValueBytes(splitBuffers, i);
            measures[i] = measureCodec.getSerializer(i).valueOf(valueBytes);
        }

        valueBuf.clear();
        measureCodec.encode(measures, valueBuf);
    }

    private byte[] getValueBytes(SplittedBytes[] splitBuffers, int measureIdx) {
        MeasureDesc desc = cubeDesc.getMeasures().get(measureIdx);
        ParameterDesc paramDesc = desc.getFunction().getParameter();
        int[] flatTableIdx = this.measureColumnIndice[measureIdx];

        byte[] result = null;

        // constant
        if (flatTableIdx == null) {
            result = Bytes.toBytes(paramDesc.getValue());
        }
        // column values
        else {
            for (int i = 0; i < flatTableIdx.length; i++) {
                SplittedBytes split = splitBuffers[flatTableIdx[i]];
                result = Arrays.copyOf(split.value, split.length);
            }
        }

        if (desc.getFunction().isCount()) {
            result = Bytes.toBytes("1");
        }

        return result;
    }

    @Override
    public void map(KEYIN key, Text value, Context context) throws IOException, InterruptedException {
        // combining the hive table flattening logic into base cuboid building.
        // the input of this mapper is the fact table rows

        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }

        if (!byteRowDelimiterInferred)
            byteRowDelimiter = bytesSplitter.inferByteRowDelimiter(value.getBytes(), value.getLength(), factTableDesc.getColumns().length);

        bytesSplitter.split(value.getBytes(), value.getLength(), byteRowDelimiter);

        try {
            byte[] rowKey = buildKey(bytesSplitter.getSplitBuffers());
            if (rowKey == null)
                return;// skip this fact table row

            outputKey.set(rowKey, 0, rowKey.length);

            buildValue(bytesSplitter.getSplitBuffers());
            outputValue.set(valueBuf.array(), 0, valueBuf.position());

            context.write(outputKey, outputValue);

        } catch (Throwable t) {
            logger.error("", t);
            context.getCounter(BatchConstants.MAPREDUCE_COUTNER_GROUP_NAME, "Error records").increment(1L);
            return;
        }
    }
}
