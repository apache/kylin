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
import java.io.Serializable;

import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NGlobalDictionary implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(NGlobalDictionary.class);

    private final static String WORKING_DIR = "working";

    public static final String SEPARATOR = "_0_DOT_0_";

    private NGlobalDictMetaInfo metadata;

    private String baseDir;
    private String project;
    private String sourceTable;
    private String sourceColumn;
    private boolean isFirst = true;
    private String skewDictStorageFile;

    public String getResourceDir() {
        return "/" + project + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT + "/" + sourceTable + "/" + sourceColumn + "/";
    }

    private String getWorkingDir() {
        return baseDir + WORKING_DIR;
    }

    public NGlobalDictionary(String project, String sourceTable, String sourceColumn, String baseDir)
            throws IOException {
        this.project = project;
        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.baseDir = baseDir + getResourceDir();
        this.metadata = getMetaInfo();
        if (metadata != null) {
            isFirst = false;
        }
    }

    public NGlobalDictionary(String dictParams) throws IOException {
        String[] dictInfo = dictParams.split(SEPARATOR);
        this.project = dictInfo[0];
        this.sourceTable = dictInfo[1];
        this.sourceColumn = dictInfo[2];
        this.baseDir = dictInfo[3];
        this.baseDir = baseDir + getResourceDir();
        this.metadata = getMetaInfo();
        if (metadata != null) {
            isFirst = false;
        }
        if (dictInfo.length >= 5) {
            skewDictStorageFile = dictInfo[4];
        }
    }

    public NBucketDictionary loadBucketDictionary(int bucketId) throws IOException {
        if (null == metadata) {
            metadata = getMetaInfo();
        }
        return new NBucketDictionary(baseDir, getWorkingDir(), bucketId, metadata, skewDictStorageFile);
    }

    public NBucketDictionary createNewBucketDictionary() {
        return new NBucketDictionary(getWorkingDir());
    }

    public void prepareWrite() throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        globalDictStore.prepareForWrite(getWorkingDir());
    }

    public void writeMetaDict(int bucketSize, int maxVersions, long versionTTL) throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        globalDictStore.writeMetaInfo(bucketSize, getWorkingDir());
        commit(maxVersions, versionTTL);
    }

    public NGlobalDictMetaInfo getMetaInfo() throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        NGlobalDictMetaInfo metadata;
        Long[] versions = globalDictStore.listAllVersions();

        if (versions.length == 0) {
            return null;
        } else {
            metadata = globalDictStore.getMetaInfo(versions[versions.length - 1]);
        }
        return metadata;
    }

    public int getBucketSizeOrDefault(int defaultSize) {
        int bucketPartitionSize;
        if (metadata == null) {
            bucketPartitionSize = defaultSize;
        } else {
            bucketPartitionSize = metadata.getBucketSize();
        }

        return bucketPartitionSize;
    }

    public boolean isFirst() {
        return isFirst;
    }

    public void setFirst(boolean first) {
        isFirst = first;
    }

    private void commit(int maxVersions, long versionTTL) throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        globalDictStore.commit(getWorkingDir(), maxVersions, versionTTL);
    }

    private static NGlobalDictStore getResourceStore(String baseDir) throws IOException {
        return new NGlobalDictHDFSStore(baseDir);
    }
}
