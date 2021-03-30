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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 * Re-encode the cuboid from old segment (before merge) to new segment (after merge).
 */
public class SegmentReEncoder implements Serializable {
    private volatile transient boolean initialized = false;
    private CubeSegment mergingSeg;
    private CubeSegment mergedSeg;

    private byte[] newKeyBodyBuf;
    private ByteArray newKeyBuf;
    private RowKeySplitter rowKeySplitter;
    private RowKeyEncoderProvider rowKeyEncoderProvider;

    // for re-encode measures that use dictionary
    private List<Pair<Integer, MeasureIngester>> dictMeasures;
    private Map<TblColRef, Dictionary<String>> oldDicts;
    private Map<TblColRef, Dictionary<String>> newDicts;
    private List<MeasureDesc> measureDescs;
    private BufferedMeasureCodec codec;
    private CubeDesc cubeDesc;
    private KylinConfig kylinConfig;
    private Text textValue = new Text();

    public SegmentReEncoder(CubeDesc cubeDesc, CubeSegment mergingSeg, CubeSegment mergedSeg, KylinConfig kylinConfig) {
        this.cubeDesc = cubeDesc;
        this.mergingSeg = mergingSeg;
        this.mergedSeg = mergedSeg;
        this.kylinConfig = kylinConfig;
        init();
    }

    private void init() {
        newKeyBodyBuf = new byte[RowConstants.ROWKEY_BUFFER_SIZE];// size will auto-grow
        newKeyBuf = ByteArray.allocate(RowConstants.ROWKEY_BUFFER_SIZE);

        rowKeySplitter = new RowKeySplitter(mergingSeg);
        rowKeyEncoderProvider = new RowKeyEncoderProvider(mergedSeg);

        measureDescs = cubeDesc.getMeasures();
        codec = new BufferedMeasureCodec(measureDescs);

        dictMeasures = Lists.newArrayList();
        oldDicts = Maps.newHashMap();
        newDicts = Maps.newHashMap();
        for (int i = 0; i < measureDescs.size(); i++) {
            MeasureDesc measureDesc = measureDescs.get(i);
            MeasureType measureType = measureDesc.getFunction().getMeasureType();
            List<TblColRef> columns = measureType.getColumnsNeedDictionary(measureDesc.getFunction());
            boolean needReEncode = false;
            for (TblColRef col : columns) {
                //handle the column that all records is null
                if (mergingSeg.getDictionary(col) == null) {
                    continue;
                }

                oldDicts.put(col, mergingSeg.getDictionary(col));
                newDicts.put(col, mergedSeg.getDictionary(col));
                if (!mergingSeg.getDictionary(col).equals(mergedSeg.getDictionary(col))) {
                    needReEncode = true;
                }
            }
            if (needReEncode) {
                dictMeasures.add(Pair.newPair(i, measureType.newIngester()));
            }
        }
        initialized = true;
    }

    /**
     * Re-encode with both dimension and measure in encoded (Text) format.
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    public Pair<Text, Text> reEncode(Text key, Text value) throws IOException {
        if (initialized == false) {
            throw new IllegalStateException("Not initialized");
        }
        Object[] measureObjs = new Object[measureDescs.size()];
        // re-encode measures if dictionary is used
        if (dictMeasures.size() > 0) {
            codec.decode(ByteBuffer.wrap(value.getBytes(), 0, value.getLength()), measureObjs);
            for (Pair<Integer, MeasureIngester> pair : dictMeasures) {
                int i = pair.getFirst();
                MeasureIngester ingester = pair.getSecond();
                measureObjs[i] = ingester.reEncodeDictionary(measureObjs[i], measureDescs.get(i), oldDicts, newDicts);
            }

            ByteBuffer valueBuf = codec.encode(measureObjs);
            textValue.set(valueBuf.array(), 0, valueBuf.position());
            return Pair.newPair(processKey(key), textValue);
        } else {
            return Pair.newPair(processKey(key), value);
        }
    }

    /**
     * Re-encode with measures in Object[] format.
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    public Pair<Text, Object[]> reEncode2(Text key, Text value) throws IOException {
        if (initialized == false) {
            throw new IllegalStateException("Not initialized");
        }

        Object[] measureObjs = new Object[measureDescs.size()];
        codec.decode(ByteBuffer.wrap(value.getBytes(), 0, value.getLength()), measureObjs);
        // re-encode measures if dictionary is used
        if (dictMeasures.size() > 0) {
            for (Pair<Integer, MeasureIngester> pair : dictMeasures) {
                int i = pair.getFirst();
                MeasureIngester ingester = pair.getSecond();
                measureObjs[i] = ingester.reEncodeDictionary(measureObjs[i], measureDescs.get(i), oldDicts, newDicts);
            }

        }
        return Pair.newPair(processKey(key), measureObjs);
    }

    private Text processKey(Text key) throws IOException {
        long cuboidID = rowKeySplitter.split(key.getBytes());
        Cuboid cuboid = Cuboid.findForMandatory(cubeDesc, cuboidID);
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(cuboid);

        ByteArray[] splittedByteses = rowKeySplitter.getSplitBuffers();
        int bufOffset = 0;
        int bodySplitOffset = rowKeySplitter.getBodySplitOffset();

        for (int i = 0; i < cuboid.getColumns().size(); ++i) {
            int useSplit = i + bodySplitOffset;
            TblColRef col = cuboid.getColumns().get(i);

            if (cubeDesc.getRowkey().isUseDictionary(col)) {
                // if dictionary on fact table column, needs rewrite
                DictionaryManager dictMgr = DictionaryManager.getInstance(kylinConfig);
                Dictionary<String> mergedDict = dictMgr.getDictionary(mergedSeg.getDictResPath(col));

                // handle the dict of all merged segments is null
                if (mergedDict == null) {
                    continue;
                }

                Dictionary<String> sourceDict;
                // handle the column that all records is null
                if (mergingSeg.getDictionary(col) == null) {
                    BytesUtil.writeUnsigned(mergedDict.nullId(), newKeyBodyBuf, bufOffset, mergedDict.getSizeOfId());
                    bufOffset += mergedDict.getSizeOfId();
                    continue;
                } else {
                    sourceDict = dictMgr.getDictionary(mergingSeg.getDictResPath(col));
                }

                while (sourceDict.getSizeOfValue() > newKeyBodyBuf.length - bufOffset || //
                        mergedDict.getSizeOfValue() > newKeyBodyBuf.length - bufOffset || //
                        mergedDict.getSizeOfId() > newKeyBodyBuf.length - bufOffset) {
                    byte[] oldBuf = newKeyBodyBuf;
                    newKeyBodyBuf = new byte[2 * newKeyBodyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBodyBuf, 0, oldBuf.length);
                }

                int idInSourceDict = BytesUtil.readUnsigned(splittedByteses[useSplit].array(), splittedByteses[useSplit].offset(),
                        splittedByteses[useSplit].length());
                int idInMergedDict;

                //int size = sourceDict.getValueBytesFromId(idInSourceDict, newKeyBodyBuf, bufOffset);
                String v = sourceDict.getValueFromId(idInSourceDict);
                if (v == null) {
                    idInMergedDict = mergedDict.nullId();
                } else {
                    idInMergedDict = mergedDict.getIdFromValue(v);
                }

                BytesUtil.writeUnsigned(idInMergedDict, newKeyBodyBuf, bufOffset, mergedDict.getSizeOfId());
                bufOffset += mergedDict.getSizeOfId();
            } else {
                // keep as it is
                while (splittedByteses[useSplit].length() > newKeyBodyBuf.length - bufOffset) {
                    byte[] oldBuf = newKeyBodyBuf;
                    newKeyBodyBuf = new byte[2 * newKeyBodyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBodyBuf, 0, oldBuf.length);
                }

                System.arraycopy(splittedByteses[useSplit].array(), splittedByteses[useSplit].offset(), newKeyBodyBuf, bufOffset,
                        splittedByteses[useSplit].length());
                bufOffset += splittedByteses[useSplit].length();
            }
        }

        int fullKeySize = rowkeyEncoder.getBytesLength();
        while (newKeyBuf.array().length < fullKeySize) {
            newKeyBuf = new ByteArray(newKeyBuf.length() * 2);
        }
        newKeyBuf.setLength(fullKeySize);

        rowkeyEncoder.encode(new ByteArray(newKeyBodyBuf, 0, bufOffset), newKeyBuf);

        byte[] resultKey = new byte[fullKeySize];
        System.arraycopy(newKeyBuf.array(), 0, resultKey, 0, fullKeySize);

        return new Text(resultKey);
    }
}
