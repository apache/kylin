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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author ysong1, honma
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MergeCuboidMapper extends KylinMapper<Text, Text, Text, Text> {

    private KylinConfig config;
    private String cubeName;
    private String segmentID;
    private CubeManager cubeManager;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment mergedCubeSegment;
    private CubeSegment sourceCubeSegment; // Must be unique during a mapper's life cycle

    private Text outputKey = new Text();

    private byte[] newKeyBodyBuf;
    private ByteArray newKeyBuf;
    private RowKeySplitter rowKeySplitter;
    private RowKeyEncoderProvider rowKeyEncoderProvider;

    private HashMap<TblColRef, Boolean> dimensionsNeedDict = new HashMap<TblColRef, Boolean>();

    // for re-encode measures that use dictionary
    private List<Pair<Integer, MeasureIngester>> dictMeasures;
    private Map<TblColRef, Dictionary<String>> oldDicts;
    private Map<TblColRef, Dictionary<String>> newDicts;
    private List<MeasureDesc> measureDescs;
    private BufferedMeasureEncoder codec;
    private Object[] measureObjs;
    private Text outputValue;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.bindCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

        config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeManager = CubeManager.getInstance(config);
        cube = cubeManager.getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        mergedCubeSegment = cube.getSegmentById(segmentID);

        // int colCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        newKeyBodyBuf = new byte[RowConstants.ROWKEY_BUFFER_SIZE];// size will auto-grow
        newKeyBuf = ByteArray.allocate(RowConstants.ROWKEY_BUFFER_SIZE);

        // decide which source segment
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        sourceCubeSegment = findSourceSegment(fileSplit, cube);

        rowKeySplitter = new RowKeySplitter(sourceCubeSegment, 65, 255);
        rowKeyEncoderProvider = new RowKeyEncoderProvider(mergedCubeSegment);

        measureDescs = cubeDesc.getMeasures();
        codec = new BufferedMeasureEncoder(measureDescs);
        measureObjs = new Object[measureDescs.size()];
        outputValue = new Text();

        dictMeasures = Lists.newArrayList();
        oldDicts = Maps.newHashMap();
        newDicts = Maps.newHashMap();
        for (int i = 0; i < measureDescs.size(); i++) {
            MeasureDesc measureDesc = measureDescs.get(i);
            MeasureType measureType = measureDesc.getFunction().getMeasureType();
            List<TblColRef> columns = measureType.getColumnsNeedDictionary(measureDesc.getFunction());
            boolean needReEncode = false;
            for (TblColRef col : columns) {
                if (!sourceCubeSegment.getDictionary(col).equals(mergedCubeSegment.getDictionary(col))) {
                    oldDicts.put(col, sourceCubeSegment.getDictionary(col));
                    newDicts.put(col, mergedCubeSegment.getDictionary(col));
                    needReEncode = true;
                }
            }
            if (needReEncode) {
                dictMeasures.add(Pair.newPair(i, measureType.newIngester()));
            }
        }
    }

    private static final Pattern JOB_NAME_PATTERN = Pattern.compile("kylin-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    public CubeSegment findSourceSegment(FileSplit fileSplit, CubeInstance cube) {
        String filePath = fileSplit.getPath().toString();
        String jobID = extractJobIDFromPath(filePath);
        return findSegmentWithUuid(jobID, cube);
    }

    private static String extractJobIDFromPath(String path) {
        Matcher matcher = JOB_NAME_PATTERN.matcher(path);
        // check the first occurrence
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            throw new IllegalStateException("Can not extract job ID from file path : " + path);
        }
    }

    private static CubeSegment findSegmentWithUuid(String jobID, CubeInstance cubeInstance) {
        for (CubeSegment segment : cubeInstance.getSegments()) {
            String lastBuildJobID = segment.getLastBuildJobID();
            if (lastBuildJobID != null && lastBuildJobID.equalsIgnoreCase(jobID)) {
                return segment;
            }
        }
        throw new IllegalStateException("No merging segment's last build job ID equals " + jobID);
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        long cuboidID = rowKeySplitter.split(key.getBytes());
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidID);
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(cuboid);

        SplittedBytes[] splittedByteses = rowKeySplitter.getSplitBuffers();
        int bufOffset = 0;
        int bodySplitOffset = rowKeySplitter.getBodySplitOffset();

        for (int i = 0; i < cuboid.getColumns().size(); ++i) {
            int useSplit = i + bodySplitOffset;
            TblColRef col = cuboid.getColumns().get(i);

            if (this.checkNeedMerging(col)) {
                // if dictionary on fact table column, needs rewrite
                DictionaryManager dictMgr = DictionaryManager.getInstance(config);
                Dictionary<?> sourceDict = dictMgr.getDictionary(sourceCubeSegment.getDictResPath(col));
                Dictionary<?> mergedDict = dictMgr.getDictionary(mergedCubeSegment.getDictResPath(col));

                while (sourceDict.getSizeOfValue() > newKeyBodyBuf.length - bufOffset || //
                        mergedDict.getSizeOfValue() > newKeyBodyBuf.length - bufOffset || //
                        mergedDict.getSizeOfId() > newKeyBodyBuf.length - bufOffset) {
                    byte[] oldBuf = newKeyBodyBuf;
                    newKeyBodyBuf = new byte[2 * newKeyBodyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBodyBuf, 0, oldBuf.length);
                }

                int idInSourceDict = BytesUtil.readUnsigned(splittedByteses[useSplit].value, 0, splittedByteses[useSplit].length);
                int idInMergedDict;

                int size = sourceDict.getValueBytesFromId(idInSourceDict, newKeyBodyBuf, bufOffset);
                if (size < 0) {
                    idInMergedDict = mergedDict.nullId();
                } else {
                    idInMergedDict = mergedDict.getIdFromValueBytes(newKeyBodyBuf, bufOffset, size);
                }

                BytesUtil.writeUnsigned(idInMergedDict, newKeyBodyBuf, bufOffset, mergedDict.getSizeOfId());
                bufOffset += mergedDict.getSizeOfId();
            } else {
                // keep as it is
                while (splittedByteses[useSplit].length > newKeyBodyBuf.length - bufOffset) {
                    byte[] oldBuf = newKeyBodyBuf;
                    newKeyBodyBuf = new byte[2 * newKeyBodyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBodyBuf, 0, oldBuf.length);
                }

                System.arraycopy(splittedByteses[useSplit].value, 0, newKeyBodyBuf, bufOffset, splittedByteses[useSplit].length);
                bufOffset += splittedByteses[useSplit].length;
            }
        }

        int fullKeySize = rowkeyEncoder.getBytesLength();
        while (newKeyBuf.array().length < fullKeySize) {
            newKeyBuf.set(new byte[newKeyBuf.length() * 2]);
        }
        newKeyBuf.set(0, fullKeySize);

        rowkeyEncoder.encode(new ByteArray(newKeyBodyBuf, 0, bufOffset), newKeyBuf);
        outputKey.set(newKeyBuf.array(), 0, fullKeySize);

        // re-encode measures if dictionary is used
        if (dictMeasures.size() > 0) {
            codec.decode(ByteBuffer.wrap(value.getBytes(), 0, value.getLength()), measureObjs);
            for (Pair<Integer, MeasureIngester> pair : dictMeasures) {
                int i = pair.getFirst();
                MeasureIngester ingester = pair.getSecond();
                measureObjs[i] = ingester.reEncodeDictionary(measureObjs[i], measureDescs.get(i), oldDicts, newDicts);
            }
            ByteBuffer valueBuf = codec.encode(measureObjs);
            outputValue.set(valueBuf.array(), 0, valueBuf.position());
            value = outputValue;
        }

        context.write(outputKey, value);
    }

    private Boolean checkNeedMerging(TblColRef col) throws IOException {
        Boolean ret = dimensionsNeedDict.get(col);
        if (ret != null)
            return ret;
        else {
            ret = cubeDesc.getRowkey().isUseDictionary(col);
            if (ret) {
                String dictTable = DictionaryManager.getInstance(config).decideSourceData(cubeDesc.getModel(), col).getTable();
                ret = cubeDesc.getFactTable().equalsIgnoreCase(dictTable);
            }
            dimensionsNeedDict.put(col, ret);
            return ret;
        }
    }
}
