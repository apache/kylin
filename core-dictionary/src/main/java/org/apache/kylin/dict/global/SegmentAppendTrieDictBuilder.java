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

package org.apache.kylin.dict.global;

import java.io.IOException;

import java.util.Locale;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.IDictionaryBuilder;

/**
 * SegmentAppendTrieDictBuilder based on one segment.
 * SegmentAppendTrieDictBuilder only used for count distinct measure that needn't rollup among segments.
 * SegmentAppendTrieDictBuilder could avoid AppendTrieDictionary infinite growth.
 * SegmentAppendTrieDictBuilder doesn't support merge.
 */
public class SegmentAppendTrieDictBuilder implements IDictionaryBuilder {
    private AppendTrieDictionaryBuilder builder;
    private int baseId;
    private String sourceColumn;

    @Override
    public void init(DictionaryInfo dictInfo, int baseId, String hdfsDir) throws IOException {
        sourceColumn = dictInfo.getSourceTable() + "." + dictInfo.getSourceColumn();

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int maxEntriesPerSlice = config.getAppendDictEntrySize();
        if (hdfsDir == null) {
            //build in Kylin job server
            hdfsDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        }

        //use UUID to make each segment dict in different HDFS dir and support concurrent build
        //use timestamp to make the segment dict easily to delete
        String baseDir = hdfsDir + "resources/SegmentDict" + dictInfo.getResourceDir() + "/"
                + RandomUtil.randomUUID().toString() + "_" + System.currentTimeMillis() + "/";

        this.builder = new AppendTrieDictionaryBuilder(baseDir, maxEntriesPerSlice, false);
        this.baseId = baseId;
    }

    @Override
    public boolean addValue(String value) {
        if (value == null) {
            return false;
        }

        try {
            builder.addValue(value);
        } catch (Throwable e) {
            throw new RuntimeException(
                    String.format(Locale.ROOT, "Failed to create global dictionary on %s ", sourceColumn), e);
        }

        return true;
    }

    @Override
    public Dictionary<String> build() throws IOException {
        return builder.build(baseId);
    }

    @Override
    public void clear() {

    }
}
