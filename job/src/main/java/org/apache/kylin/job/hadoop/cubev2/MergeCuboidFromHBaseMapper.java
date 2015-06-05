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

package org.apache.kylin.job.hadoop.cubev2;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowValueDecoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author shaoshi
 */
public class MergeCuboidFromHBaseMapper extends TableMapper<ImmutableBytesWritable, Text> {

    private KylinConfig config;
    private String cubeName;
    private String segmentName;
    private CubeManager cubeManager;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment mergedCubeSegment;
    private CubeSegment sourceCubeSegment;// Must be unique during a mapper's
    // life cycle

    //    private Text outputKey = new Text();
    private ImmutableBytesWritable outputKey = new ImmutableBytesWritable();

    private byte[] newKeyBuf;
    private RowKeySplitter rowKeySplitter;

    private HashMap<TblColRef, Boolean> dictsNeedMerging = new HashMap<TblColRef, Boolean>();

    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

    private Text outputValue = new Text();
    private RowValueDecoder[] rowValueDecoders;
    private boolean simpleFullCopy = false;
    private Object[] result;
    private MeasureCodec codec;

    private Boolean checkNeedMerging(TblColRef col) throws IOException {
        Boolean ret = dictsNeedMerging.get(col);
        if (ret != null)
            return ret;
        else {
            ret = cubeDesc.getRowkey().isUseDictionary(col);
            if (ret) {
                String dictTable = (String) DictionaryManager.getInstance(config).decideSourceData(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, null)[0];
                ret = cubeDesc.getFactTable().equalsIgnoreCase(dictTable);
            }
            dictsNeedMerging.put(col, ret);
            return ret;
        }
    }

    private CubeSegment findSegmentWithHTable(String htable, CubeInstance cubeInstance) {
        for (CubeSegment segment : cubeInstance.getSegments()) {
            String segmentHtable = segment.getStorageLocationIdentifier();
            if (segmentHtable != null && segmentHtable.equalsIgnoreCase(htable)) {
                return segment;
            }
        }

        throw new IllegalStateException("No merging segment's storage location identifier equals " + htable);

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        HadoopUtil.setCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME).toUpperCase();

        config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeManager = CubeManager.getInstance(config);
        cube = cubeManager.getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        mergedCubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

        newKeyBuf = new byte[256];// size will auto-grow

        TableSplit currentSplit = (TableSplit) context.getInputSplit();
        byte[] tableName = currentSplit.getTableName();
        String htableName = Bytes.toString(tableName);
        // decide which source segment
        System.out.println("htable:" + htableName);
        sourceCubeSegment = findSegmentWithHTable(htableName, cube);
        System.out.println(sourceCubeSegment);

        this.rowKeySplitter = new RowKeySplitter(sourceCubeSegment, 65, 255);

        List<RowValueDecoder> valueDecoderList = Lists.newArrayList();
        List<InMemKeyValueCreator> keyValueCreators = Lists.newArrayList();
        List<MeasureDesc> measuresDescs = Lists.newArrayList();
        int startPosition = 0;
        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                valueDecoderList.add(new RowValueDecoder(colDesc));
                keyValueCreators.add(new InMemKeyValueCreator(colDesc, startPosition));
                startPosition += colDesc.getMeasures().length;
                for (MeasureDesc measure : colDesc.getMeasures()) {
                    measuresDescs.add(measure);
                }
            }
        }

        rowValueDecoders = valueDecoderList.toArray(new RowValueDecoder[valueDecoderList.size()]);

        simpleFullCopy = (keyValueCreators.size() == 1);
        result = new Object[measuresDescs.size()];
        codec = new MeasureCodec(measuresDescs);
    }

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        long cuboidID = rowKeySplitter.split(key.get(), key.get().length);
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidID);

        SplittedBytes[] splittedByteses = rowKeySplitter.getSplitBuffers();
        int bufOffset = 0;
        BytesUtil.writeLong(cuboidID, newKeyBuf, bufOffset, RowConstants.ROWKEY_CUBOIDID_LEN);
        bufOffset += RowConstants.ROWKEY_CUBOIDID_LEN;

        for (int i = 0; i < cuboid.getColumns().size(); ++i) {
            TblColRef col = cuboid.getColumns().get(i);

            if (this.checkNeedMerging(col)) {
                // if dictionary on fact table column, needs rewrite
                DictionaryManager dictMgr = DictionaryManager.getInstance(config);
                Dictionary<?> sourceDict = dictMgr.getDictionary(sourceCubeSegment.getDictResPath(col));
                Dictionary<?> mergedDict = dictMgr.getDictionary(mergedCubeSegment.getDictResPath(col));

                while (sourceDict.getSizeOfValue() > newKeyBuf.length - bufOffset || mergedDict.getSizeOfValue() > newKeyBuf.length - bufOffset) {
                    byte[] oldBuf = newKeyBuf;
                    newKeyBuf = new byte[2 * newKeyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBuf, 0, oldBuf.length);
                }

                int idInSourceDict = BytesUtil.readUnsigned(splittedByteses[i + 1].value, 0, splittedByteses[i + 1].length);

                int size = sourceDict.getValueBytesFromId(idInSourceDict, newKeyBuf, bufOffset);
                int idInMergedDict;
                if (size < 0) {
                    idInMergedDict = mergedDict.nullId();
                } else {
                    idInMergedDict = mergedDict.getIdFromValueBytes(newKeyBuf, bufOffset, size);
                }
                BytesUtil.writeUnsigned(idInMergedDict, newKeyBuf, bufOffset, mergedDict.getSizeOfId());

                bufOffset += mergedDict.getSizeOfId();
            } else {
                // keep as it is
                while (splittedByteses[i + 1].length > newKeyBuf.length - bufOffset) {
                    byte[] oldBuf = newKeyBuf;
                    newKeyBuf = new byte[2 * newKeyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBuf, 0, oldBuf.length);
                }

                System.arraycopy(splittedByteses[i + 1].value, 0, newKeyBuf, bufOffset, splittedByteses[i + 1].length);
                bufOffset += splittedByteses[i + 1].length;
            }
        }
        byte[] newKey = Arrays.copyOf(newKeyBuf, bufOffset);
        outputKey.set(newKey, 0, newKey.length);

        valueBuf.clear();
        if (simpleFullCopy) {
            // simple case, should only 1 hbase column
            for (Cell cell : value.rawCells()) {
                valueBuf.put(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            }
        } else {
            int position = 0;
            for (int i = 0; i < rowValueDecoders.length; i++) {
                rowValueDecoders[i].decode(value, false);
                Object[] measureValues = rowValueDecoders[i].getValues();

                for (int j = 0; j < measureValues.length; j++) {
                    result[position++] = measureValues[j];
                }
            }
            codec.encode(result, valueBuf);
        }

        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(outputKey, outputValue);
    }

}
