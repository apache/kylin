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

package org.apache.kylin.engine.mr.common;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dict.ShrunkenDictionary;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DictionaryGetterUtil {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryGetterUtil.class);

    public static String getInputSplitSignature(CubeSegment cubeSegment, InputSplit inputSplit) {
        return MRUtil.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat().getInputSplitSignature(inputSplit);
    }

    public static Map<TblColRef, Dictionary<String>> getDictionaryMap(CubeSegment cubeSegment, InputSplit inputSplit,
                                                                      Configuration configuration) throws IOException {
        Map<TblColRef, Dictionary<String>> dictionaryMap = cubeSegment.buildDictionaryMap();

        String shrunkenDictPath = configuration.get(BatchConstants.ARG_SHRUNKEN_DICT_PATH);
        if (shrunkenDictPath == null) {
            return dictionaryMap;
        }

        // replace global dictionary with shrunken dictionary if possible
        String inputSplitSignature = getInputSplitSignature(cubeSegment, inputSplit);
        FileSystem fs = FileSystem.get(configuration);
        ShrunkenDictionary.StringValueSerializer valueSerializer = new ShrunkenDictionary.StringValueSerializer();
        for (TblColRef colRef : cubeSegment.getCubeDesc().getAllGlobalDictColumns()) {
            Path colShrunkenDictDir = new Path(shrunkenDictPath, colRef.getIdentity());
            Path colShrunkenDictPath = new Path(colShrunkenDictDir, inputSplitSignature);
            if (!fs.exists(colShrunkenDictPath)) {
                logger.warn("Shrunken dictionary for column " + colRef.getIdentity() + " in split "
                        + inputSplitSignature + " does not exist!!!");
                continue;
            }
            try (DataInputStream dis = fs.open(colShrunkenDictPath)) {
                Dictionary<String> shrunkenDict = new ShrunkenDictionary(valueSerializer);
                shrunkenDict.readFields(dis);

                dictionaryMap.put(colRef, shrunkenDict);
            }
        }

        return dictionaryMap;
    }
}
