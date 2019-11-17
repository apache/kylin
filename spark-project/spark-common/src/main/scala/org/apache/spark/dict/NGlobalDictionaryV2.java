/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.spark.dict;

import java.io.IOException;
import java.io.Serializable;

import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class NGlobalDictionaryV2 implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(NGlobalDictionaryV2.class);

    private final static String WORKING_DIR = "working";

    public static final String SEPARATOR = "_0_DOT_0_";

    private NGlobalDictMetaInfo metadata;

    private String baseDir;
    private String project;
    private String sourceTable;
    private String sourceColumn;
    private boolean isFirst = true;

    public String getResourceDir() {
        return "/" + project + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT + "/" + sourceTable + "/" + sourceColumn + "/";
    }

    private String getWorkingDir() {
        return baseDir + WORKING_DIR;
    }

    public NGlobalDictionaryV2(String project, String sourceTable, String sourceColumn, String baseDir)
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

    public NGlobalDictionaryV2(String dictParams) throws IOException {
        val dictInfo = dictParams.split(SEPARATOR);
        this.project = dictInfo[0];
        this.sourceTable = dictInfo[1];
        this.sourceColumn = dictInfo[2];
        this.baseDir = dictInfo[3];
        this.baseDir = baseDir + getResourceDir();
        this.metadata = getMetaInfo();
        if (metadata != null) {
            isFirst = false;
        }
    }

    public NBucketDictionary loadBucketDictionary(int bucketId) throws IOException {
        if (null == metadata) {
            metadata = getMetaInfo();
        }
        return new NBucketDictionary(baseDir, getWorkingDir(), bucketId, metadata);
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
