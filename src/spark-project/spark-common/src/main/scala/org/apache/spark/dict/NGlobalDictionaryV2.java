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
import java.util.Arrays;

import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.nacos.common.JustForTest;

import lombok.val;

public class NGlobalDictionaryV2 implements Serializable {

    public static final String SEPARATOR = "_0_DOT_0_";

    // The latest dictionary file is read by default if no version is specified
    public static final long NO_VERSION_SPECIFIED = -1L;

    protected static final Logger logger = LoggerFactory.getLogger(NGlobalDictionaryV2.class);
    private NGlobalDictMetaInfo metadata;

    private String baseDir;
    private String project;
    private String sourceTable;
    private String sourceColumn;

    // Default is -1L, which means that no version is specified.
    // Dict data is read from the latest version each time
    private final long buildVersion;
    private boolean isFirst = true;

    public NGlobalDictionaryV2(String dictParams, long buildVersion) throws IOException {
        val dictInfo = dictParams.split(SEPARATOR);
        this.project = dictInfo[0];
        this.sourceTable = dictInfo[1];
        this.sourceColumn = dictInfo[2];
        this.baseDir = dictInfo[3];
        this.baseDir = baseDir + getResourceDir();
        this.buildVersion = buildVersion;
        this.metadata = getMetaInfo();
        if (metadata != null) {
            isFirst = false;
        }
    }

    // Note: You do not need to specify a build version
    //       if you do not need to encode flat tables or
    //       if you only need to read the latest dictionary data
    public NGlobalDictionaryV2(String project, String sourceTable, String sourceColumn, String baseDir)
            throws IOException {
        this(project,sourceTable,sourceColumn,baseDir, NO_VERSION_SPECIFIED);
    }

    public NGlobalDictionaryV2(String project, String sourceTable, String sourceColumn, String baseDir, long buildVersion)
            throws IOException {
        this.project = project;
        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.baseDir = baseDir + getResourceDir();
        this.buildVersion = buildVersion;
        this.metadata = getMetaInfo();
        if (metadata != null) {
            isFirst = false;
        }
    }

    public String getResourceDir() {
        return "/" + project + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT + "/" + sourceTable + "/" + sourceColumn + "/";
    }

    private String getWorkingDir() {
        return getResourceStore(baseDir).getWorkingDir(this.buildVersion);
    }

    public NBucketDictionary loadBucketDictionary(int bucketId) throws IOException {
        return loadBucketDictionary(bucketId, false);
    }

    // Only for S3 test !
    @JustForTest
    public NBucketDictionary loadBucketDictionaryForTestS3(int bucketId) throws IOException {
        if (null == metadata) {
            metadata = getMetaInfoForTestS3();
        }
        return new NBucketDictionary(baseDir, getWorkingDir(), bucketId, metadata, false, this.buildVersion, true);
    }

    public NBucketDictionary loadBucketDictionary(int bucketId, boolean isForColumnEncoding) throws IOException {
        return loadBucketDictionary(bucketId, isForColumnEncoding, this.buildVersion);
    }

    public NBucketDictionary loadBucketDictionary(int bucketId, boolean isForColumnEncoding, long buildVersion) throws IOException {
        if (null == metadata) {
            metadata = getMetaInfo();
        }
        return new NBucketDictionary(baseDir, getWorkingDir(), bucketId, metadata, isForColumnEncoding, buildVersion);
    }

    public NBucketDictionary createNewBucketDictionary() {
        return new NBucketDictionary(getWorkingDir());
    }

    public void prepareWrite() throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        globalDictStore.prepareForWrite(getWorkingDir());
    }

    // Only for S3 test !
    @JustForTest
    public void prepareWriteS3() throws IOException {
        NGlobalDictStore globalDictStore = new NGlobalDictS3Store(baseDir);
        globalDictStore.prepareForWrite(globalDictStore.getWorkingDir(this.buildVersion));
    }

    // Only for S3 test !
    @JustForTest
    public void writeMetaDictForTestS3(int bucketSize, int maxVersions, long versionTTL) throws IOException {
        NGlobalDictStore globalDictStore = new NGlobalDictS3Store(baseDir);
        String workingDir = globalDictStore.getWorkingDir(this.buildVersion);
        globalDictStore.writeMetaInfo(bucketSize, workingDir);
        globalDictStore.commit(workingDir, maxVersions, versionTTL, this.buildVersion);
    }

    public void writeMetaDict(int bucketSize, int maxVersions, long versionTTL) throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        globalDictStore.writeMetaInfo(bucketSize, getWorkingDir());
        commit(maxVersions, versionTTL);
    }


    private NGlobalDictMetaInfo getMetaInfo(NGlobalDictStore globalDictStore) throws IOException {
        NGlobalDictMetaInfo nGlobalDictMetaInfo;
        Long[] versions = globalDictStore.listAllVersions();
        logger.info("getMetaInfo versions.length is {}", versions.length);
        if (versions.length == 0) {
            return null;
        } else if (this.buildVersion == NO_VERSION_SPECIFIED || !Arrays.asList(versions).contains(buildVersion)) {
            logger.info("Initializes dict metainfo with the latest version:{}", versions[versions.length - 1]);
            nGlobalDictMetaInfo = globalDictStore.getMetaInfo(versions[versions.length - 1]);
        } else {
            logger.info("Initializes dict metainfo with the specified version:{}", this.buildVersion);
            nGlobalDictMetaInfo = globalDictStore.getMetaInfo(this.buildVersion);
        }
        logger.info("getMetaInfo metadata is null : [{}]", nGlobalDictMetaInfo == null);
        return nGlobalDictMetaInfo;
    }

    // Only for S3 test !
    @JustForTest
    public NGlobalDictMetaInfo getMetaInfoForTestS3() throws IOException {
        NGlobalDictStore globalDictStore = new NGlobalDictS3Store(baseDir);
        return getMetaInfo(globalDictStore);
    }

    public NGlobalDictMetaInfo getMetaInfo() throws IOException {
        NGlobalDictStore globalDictStore = getResourceStore(baseDir);
        return getMetaInfo(globalDictStore);
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
        globalDictStore.commit(getWorkingDir(), maxVersions, versionTTL, this.buildVersion);
    }

    private NGlobalDictStore getResourceStore(String baseDir) {
        return NGlobalDictStoreFactory.getResourceStore(baseDir);
    }
}
