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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

public class NBucketDictionary {

    protected static final Logger logger = LoggerFactory.getLogger(NGlobalDictionaryV2.class);

    private String workingDir;

    private int bucketId;

    private Object2LongMap<String> absoluteDictMap;
    // Relative dictionary needs to calculate dictionary code according to NGlobalDictMetaInfo's bucketOffsets
    private Object2LongMap<String> relativeDictMap;

    NBucketDictionary(String baseDir, String workingDir, int bucketId, NGlobalDictMetaInfo metainfo)
            throws IOException {
        this.workingDir = workingDir;
        this.bucketId = bucketId;
        final NGlobalDictStore globalDictStore = new NGlobalDictHDFSStore(baseDir);
        Long[] versions = globalDictStore.listAllVersions();
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
        relativeDictMap.put(value, relativeDictMap.size() + 1l);
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
        NGlobalDictStore globalDictStore = new NGlobalDictHDFSStore(workingDir);
        globalDictStore.writeBucketPrevDict(workingDir, bucketId, absoluteDictMap);
    }

    private void writeBucketCurrDict(int bucketId) throws IOException {
        if (relativeDictMap.isEmpty())
            return;
        NGlobalDictStore globalDictStore = new NGlobalDictHDFSStore(workingDir);
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
