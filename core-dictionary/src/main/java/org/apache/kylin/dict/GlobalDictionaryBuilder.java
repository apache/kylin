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

package org.apache.kylin.dict;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GlobalDictinary based on whole cube, to ensure one value has same dict id in different segments.
 * GlobalDictinary mainly used for count distinct measure to support rollup among segments.
 * Created by sunyerui on 16/5/24.
 */
public class GlobalDictionaryBuilder implements IDictionaryBuilder {
    private static final Logger logger = LoggerFactory.getLogger(GlobalDictionaryBuilder.class);

    @Override
    public Dictionary<String> build(DictionaryInfo dictInfo, IDictionaryValueEnumerator valueEnumerator, int baseId, int nSamples, ArrayList<String> returnSamples) throws IOException {
        if (dictInfo == null) {
            throw new IllegalArgumentException("GlobalDictinaryBuilder must used with an existing DictionaryInfo");
        }
        String dictDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "resources/GlobalDict" + dictInfo.getResourceDir() + "/";

        // Try to load the existing dict from cache, making sure there's only the same one object in memory
        NavigableSet<String> dicts = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getStore().listResources(dictInfo.getResourceDir());
        ArrayList<String> appendDicts = new ArrayList<>();
        if (dicts != null && !dicts.isEmpty()) {
            for (String dict : dicts) {
                DictionaryInfo info = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getStore().getResource(dict, DictionaryInfo.class, DictionaryInfoSerializer.INFO_SERIALIZER);
                if (info.getDictionaryClass().equals(AppendTrieDictionary.class.getName())) {
                    appendDicts.add(dict);
                }
            }
        }

        AppendTrieDictionary.Builder<String> builder;
        if (appendDicts.isEmpty()) {
            logger.info("GlobalDict {} is empty, create new one", dictInfo.getResourceDir());
            builder = AppendTrieDictionary.Builder.create(dictDir);
        } else if (appendDicts.size() == 1) {
            logger.info("GlobalDict {} exist, append value", appendDicts.get(0));
            AppendTrieDictionary dict = (AppendTrieDictionary) DictionaryManager.getInstance(KylinConfig.getInstanceFromEnv()).getDictionary(appendDicts.get(0));
            builder = AppendTrieDictionary.Builder.create(dict);
        } else {
            throw new IllegalStateException(String.format("GlobalDict %s should have 0 or 1 append dict but %d", dictInfo.getResourceDir(), appendDicts.size()));
        }

        byte[] value;
        while (valueEnumerator.moveNext()) {
            value = valueEnumerator.current();
            if (value == null) {
                continue;
            }
            String v = Bytes.toString(value);
            builder.addValue(v);
            if (returnSamples.size() < nSamples && returnSamples.contains(v) == false)
                returnSamples.add(v);
        }
        return builder.build(baseId);
    }
}
