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
package org.apache.spark.dict;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

public class NBucketDictionary {

    protected static final Logger logger = LoggerFactory.getLogger(NBucketDictionary.class);

    private String workingDir;

    private int bucketId;

    private Object2LongMap<String> absoluteDictMap;
    // Relative dictionary needs to calculate dictionary code according to NGlobalDictMetaInfo's bucketOffsets
    private Object2LongMap<String> relativeDictMap;

    NBucketDictionary(String baseDir, String workingDir, int bucketId, NGlobalDictMetaInfo metainfo)
            throws IOException {
        this.workingDir = workingDir;
        this.bucketId = bucketId;
        final NGlobalDictStore globalDictStore = NGlobalDictStoreFactory.getResourceStore(baseDir);
        Long[] versions = globalDictStore.listAllVersions();
        logger.debug("versions.length is {}", versions.length);
        if (versions.length == 0) {
            this.absoluteDictMap = new Object2LongOpenHashMap<>();
        } else {
            this.absoluteDictMap = globalDictStore.getBucketDict(versions[versions.length - 1], metainfo, bucketId);
        }
        this.relativeDictMap = new Object2LongOpenHashMap<>();
    }

    NBucketDictionary(String workingDir) {
        this.workingDir = workingDir;
        this.absoluteDictMap = new Object2LongOpenHashMap<>();
        this.relativeDictMap = new Object2LongOpenHashMap<>();
    }

    public void addRelativeValue(String value) {
        if (null == value) {
            return;
        }
        if (absoluteDictMap.containsKey(value)) {
            return;
        }
        relativeDictMap.put(value, relativeDictMap.size() + 1L);
    }

    public void addAbsoluteValue(String value, long encodeValue) {
        absoluteDictMap.put(value, encodeValue);
    }

    public long encode(Object value) {
        return absoluteDictMap.getLong(value.toString());
    }

    public void saveBucketDict(int bucketId) throws IOException {
        writeBucketCurrDict(bucketId);
        writeBucketPrevDict(bucketId);
    }

    private void writeBucketPrevDict(int bucketId) throws IOException {
        if (absoluteDictMap.isEmpty())
            return;
        NGlobalDictStore globalDictStore = NGlobalDictStoreFactory.getResourceStore(workingDir);
        globalDictStore.writeBucketPrevDict(workingDir, bucketId, absoluteDictMap);
    }

    private void writeBucketCurrDict(int bucketId) throws IOException {
        if (relativeDictMap.isEmpty())
            return;
        NGlobalDictStore globalDictStore = NGlobalDictStoreFactory.getResourceStore(workingDir);
        globalDictStore.writeBucketCurrDict(workingDir, bucketId, relativeDictMap);
    }

    public Object2LongMap<String> getAbsoluteDictMap() {
        return absoluteDictMap;
    }

    public Object2LongMap<String> getRelativeDictMap() {
        return relativeDictMap;
    }

    public int getBucketId() {
        return bucketId;
    }

    public void setBucketId(int bucketId) {
        this.bucketId = bucketId;
    }
}
