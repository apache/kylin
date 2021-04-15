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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.dict.DictionarySerializer;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimDictionaryMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableMap;

public class DictsReader extends ColumnarFilesReader {
    private static final Logger logger = LoggerFactory.getLogger(DictsReader.class);

    public DictsReader(Path path, FileSystem fileSystem) throws IOException {
        super(fileSystem, path);
    }

    public ImmutableMap<String, Dictionary> readDicts() throws IOException {
        try (FSDataInputStream metaInputStream = fs.open(metaFilePath);
             FSDataInputStream dataInputStream = fs.open(dataFilePath)) {
            FragmentMetaInfo fragmentMetaInfo = JsonUtil.readValue(metaInputStream, FragmentMetaInfo.class);
            List<DimDictionaryMetaInfo> dimDictMetaInfos = fragmentMetaInfo.getDimDictionaryMetaInfos();
            ImmutableMap.Builder<String, Dictionary> builder = ImmutableMap.builder();
            Dictionary dict;
            String colName;
            logger.info("Reading dictionary from {}", dataFilePath.getName());
            for (DimDictionaryMetaInfo dimDictMetaInfo : dimDictMetaInfos) {
                dataInputStream.seek(dimDictMetaInfo.getStartOffset());
                dict = DictionarySerializer.deserialize(dataInputStream);
                colName = dimDictMetaInfo.getDimName();
                logger.info("Add dict for {}", colName);
                builder.put(colName, dict);
            }
            return builder.build();
        }
    }

    @Override
    public void close() throws IOException{

    }
}
