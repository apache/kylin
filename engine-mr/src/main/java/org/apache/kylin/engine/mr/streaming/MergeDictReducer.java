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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.dict.DictionarySerializer;
import org.apache.kylin.dict.MultipleDictionaryValueEnumerator;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class MergeDictReducer extends KylinReducer<Text, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(MergeDictReducer.class);

    private String cubeName;
    private String segmentName;
    private CubeInstance cube;
    private CubeSegment segment;
    private Map<String, TblColRef> colNeedDictMap;

    @Override
    protected void doSetup(Context context) throws IOException, InterruptedException {
        super.bindCurrentConfiguration(context.getConfiguration());
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase(Locale.ROOT);
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        segment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        if (segment == null) {
            logger.debug("segment {} is null during setup!", segmentName);
            throw new IllegalArgumentException("Segment is null, job quit!");
        }
        colNeedDictMap = Maps.newHashMap();
        Set<TblColRef> columnsNeedDict = cube.getDescriptor().getAllColumnsNeedDictionaryBuilt();
        for (TblColRef column : columnsNeedDict) {
            colNeedDictMap.put(column.getName(), column);
        }
    }

    @Override
    protected void doReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String col = key.toString();
        logger.info("merge dictionary for column:{}", col);
        TblColRef tblColRef = colNeedDictMap.get(col);

        if (tblColRef == null) {
            logger.warn("column:{} not found in the columns need dictionary map: {}", col, colNeedDictMap.keySet());
            return;
        }

        DataType dataType = tblColRef.getType();
        List<Dictionary<String>> dicts = Lists.newLinkedList();
        for (Text value : values) {
            ByteArray byteArray = new ByteArray(value.getBytes());
            Dictionary<String> dict = (Dictionary<String>) DictionarySerializer.deserialize(byteArray);
            dicts.add(dict);
        }
        Dictionary mergedDict;
        if (dicts.size() > 1) {
            MultipleDictionaryValueEnumerator multipleDictionaryValueEnumerator = new MultipleDictionaryValueEnumerator(
                    dataType, dicts);
            mergedDict = DictionaryGenerator.buildDictionary(dataType, multipleDictionaryValueEnumerator);
        } else if (dicts.size() == 1) {
            mergedDict = dicts.get(0);
        } else {
            throw new IllegalArgumentException("Dictionary missing for column " + col);
        }
        if (mergedDict == null) {
            throw new IllegalArgumentException("Merge dictionaries error for column " + col);
        }

        TableDesc tableDesc = tblColRef.getColumnDesc().getTable();
        IReadableTable.TableSignature signature = new IReadableTable.TableSignature();
        signature.setLastModifiedTime(System.currentTimeMillis());
        signature.setPath(tableDesc.getResourcePath());

        //TODO: Table signature size?
        //        signature.setSize(mergedDict.getSize());

        DictionaryInfo dictionaryInfo = new DictionaryInfo(tblColRef.getTable(), tblColRef.getName(), tblColRef
                .getColumnDesc().getZeroBasedIndex(), tblColRef.getDatatype(), signature);
        dictionaryInfo.setDictionaryObject(mergedDict);
        dictionaryInfo.setDictionaryClass(mergedDict.getClass().getName());
        dictionaryInfo.setCardinality(mergedDict.getSize());

        ByteArrayOutputStream fulBuf = new ByteArrayOutputStream();
        DataOutputStream fulDout = new DataOutputStream(fulBuf);
        DictionaryInfoSerializer.FULL_SERIALIZER.serialize(dictionaryInfo, fulDout);

        Text outValue = new Text(fulBuf.toByteArray());
        context.write(key, outValue);
        logger.debug("output dict info of column {} to path: {}", col,
                context.getConfiguration().get(FileOutputFormat.OUTDIR));
    }

}
