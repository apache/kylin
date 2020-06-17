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

package org.apache.kylin.stream.core.storage.columnar;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.DictionarySerializer;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimDictionaryMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.DirectBuffer;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.io.Files;

public class FragmentData {
    private static Logger logger = LoggerFactory.getLogger(FragmentData.class);

    private ByteBuffer dataBuffer;
    private FragmentMetaInfo fragmentMetaInfo;
    private File fragmentDataFile;

    private ConcurrentMap<TblColRef, Dictionary<String>> dictionaryMap;

    public FragmentData(FragmentMetaInfo fragmentMetaInfo, File fragmentDataFile) throws IOException {
        this.dictionaryMap = Maps.newConcurrentMap();
        this.fragmentMetaInfo = fragmentMetaInfo;
        this.fragmentDataFile = fragmentDataFile;
        this.dataBuffer = Files.map(fragmentDataFile, FileChannel.MapMode.READ_ONLY);
    }

    @SuppressWarnings("unchecked")
    public Map<TblColRef, Dictionary<String>> getDimensionDictionaries(TblColRef[] dimensions) {
        Map<TblColRef, Dictionary<String>> result = Maps.newHashMap();
        ByteBuffer dictBuffer = dataBuffer.asReadOnlyBuffer();
        for (TblColRef dimension : dimensions) {
            Dictionary<String> dict = dictionaryMap.get(dimension);
            if (dict == null) {
                for (DimDictionaryMetaInfo dimDictMetaInfo : fragmentMetaInfo.getDimDictionaryMetaInfos()) {
                    if (dimDictMetaInfo.getDimName().equals(dimension.getName())) {
                        int dictLength = dimDictMetaInfo.getDictLength();
                        dictBuffer.position(dimDictMetaInfo.getStartOffset());
                        byte[] dictBytes = new byte[dictLength];
                        dictBuffer.get(dictBytes, 0, dictLength);
                        dict = (Dictionary<String>) DictionarySerializer.deserialize(new ByteArray(dictBytes));
                        dictionaryMap.put(dimension, dict);
                        break;
                    }
                }
            }
            result.put(dimension, dict);
        }
        return result;
    }

    public int getBufferCapacity() {
        return dataBuffer.capacity();
    }

    public ByteBuffer getDataReadBuffer() {
        return dataBuffer.asReadOnlyBuffer();
    }

    public FragmentMetaInfo getFragmentMetaInfo() {
        return fragmentMetaInfo;
    }

    public int getSize() {
        return dataBuffer.capacity();
    }

    public void tryForceUnMapBuffer() {
        if (dataBuffer instanceof DirectBuffer) {
            try {
                sun.misc.Cleaner cleaner = ((DirectBuffer) dataBuffer).cleaner();
                if (cleaner != null) {
                    cleaner.clean();
                    logger.debug("directBuffer cleaned for fragment data:" + fragmentDataFile.getAbsolutePath());
                }
            } catch (Exception e) {
                logger.error("error when clean the fragment data:" + fragmentDataFile.getAbsolutePath());
            }
        }
    }
}
