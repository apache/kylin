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
import java.nio.charset.Charset;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

public class NGlobalDictHDFSStore implements NGlobalDictStore {

    protected static final String VERSION_PREFIX = "version_";
    protected static final String DICT_METADATA_NAME = "meta";
    protected static final String DICT_CURR_PREFIX = "CURR_";
    protected static final String DICT_PREV_PREFIX = "PREV_";
    protected static final String WORKING_DIR = "working";
    static final Logger logger = LoggerFactory.getLogger(NGlobalDictHDFSStore.class);
    protected final Path basePath;
    protected final FileSystem fileSystem;
    protected final String baseDir;

    public NGlobalDictHDFSStore(String baseDir) throws IOException {
        this.baseDir = baseDir;
        this.basePath = new Path(baseDir);
        this.fileSystem = basePath.getFileSystem(new Configuration());
    }

    @Override
    public void prepareForWrite(String workingDir) throws IOException {
        if (!fileSystem.exists(basePath)) {
            logger.info("Global dict store at {} doesn't exist, create a new one", basePath);
            fileSystem.mkdirs(basePath);
        }

        logger.trace("Prepare to write Global dict store at {}", workingDir);
        Path working = new Path(workingDir);

        if (fileSystem.exists(working)) {
            fileSystem.delete(working, true);
            logger.trace("Working directory {} exits, delete it first", working);
        }

        fileSystem.mkdirs(working);
    }

    @Override
    public Long[] listAllVersions() throws IOException {
        if (!fileSystem.exists(basePath)) {
            return new Long[0];
        }

        FileStatus[] versionDirs = fileSystem.listStatus(basePath, path -> path.getName().startsWith(VERSION_PREFIX));
        TreeSet<Long> versions = new TreeSet<>();
        for (FileStatus versionDir : versionDirs) {
            Path path = versionDir.getPath();
            versions.add(Long.parseLong(path.getName().substring(VERSION_PREFIX.length())));
        }
        return versions.toArray(new Long[versions.size()]);
    }

    @Override
    public Path getVersionDir(long version) {
        return new Path(basePath, VERSION_PREFIX + version);
    }

    @Override
    public NGlobalDictMetaInfo getMetaInfo(long version) throws IOException {
        Path versionDir = getVersionDir(version);
        FileStatus[] metaFiles = fileSystem.listStatus(versionDir,
                path -> path.getName().startsWith(DICT_METADATA_NAME));

        if (metaFiles.length == 0) {
            logger.info("because metaFiles.length is 0, metaInfo is null");
            return null;
        }

        String metaFile = metaFiles[0].getPath().getName();
        Path metaPath = new Path(versionDir, metaFile);
        if (!fileSystem.exists(metaPath)) {
            logger.info("because metaPath[{}] is not exists, metaInfo is null", metaPath);
            return null;
        }

        NGlobalDictMetaInfo metaInfo;

        try (FSDataInputStream is = fileSystem.open(metaPath)) {
            int bucketSize = is.readInt();
            long[] bucketOffsets = new long[bucketSize];
            long[] bucketCount = new long[bucketSize];
            long dictCount = is.readLong();
            for (int i = 0; i < bucketSize; i++) {
                bucketOffsets[i] = is.readLong();
            }
            for (int i = 0; i < bucketSize; i++) {
                bucketCount[i] = is.readLong();
            }
            metaInfo = new NGlobalDictMetaInfo(bucketSize, bucketOffsets, dictCount, bucketCount);
        }

        return metaInfo;
    }

    @Override
    public Object2LongMap<String> getBucketDict(long version, NGlobalDictMetaInfo metaInfo, int bucketId)
            throws IOException {
        Object2LongMap<String> object2IntMap = new Object2LongOpenHashMap<>();
        Path versionDir = getVersionDir(version);
        FileStatus[] bucketFiles = fileSystem.listStatus(versionDir, path -> path.getName().endsWith("_" + bucketId));

        for (FileStatus file : bucketFiles) {
            if (file.getPath().getName().startsWith(DICT_CURR_PREFIX)) {
                object2IntMap.putAll(getBucketDict(file.getPath(), metaInfo.getOffset(bucketId)));
            }
            if (file.getPath().getName().startsWith(DICT_PREV_PREFIX)) {
                object2IntMap.putAll(getBucketDict(file.getPath(), 0));
            }
        }

        return object2IntMap;
    }

    private Object2LongMap<String> getBucketDict(Path dictPath, long offset) throws IOException {
        Object2LongMap<String> object2IntMap = new Object2LongOpenHashMap<>();
        try (FSDataInputStream is = fileSystem.open(dictPath)) {
            int elementCnt = is.readInt();
            for (int i = 0; i < elementCnt; i++) {
                long value = is.readLong();
                int bytesLength = is.readInt();
                byte[] bytes = new byte[bytesLength];
                IOUtils.readFully(is, bytes, 0, bytes.length);
                object2IntMap.put(new String(bytes, Charset.defaultCharset()), value + offset);
            }
        }

        return object2IntMap;
    }

    @Override
    public void writeBucketCurrDict(String workingPath, int bucketId, Object2LongMap<String> openHashMap)
            throws IOException {
        Path dictPath = new Path(workingPath, DICT_CURR_PREFIX + bucketId);
        writeBucketDict(dictPath, openHashMap);
    }

    @Override
    public void writeBucketPrevDict(String workingPath, int bucketId, Object2LongMap<String> openHashMap)
            throws IOException {
        Path dictPath = new Path(workingPath, DICT_PREV_PREFIX + bucketId);
        writeBucketDict(dictPath, openHashMap);
    }

    private void writeBucketDict(Path dictPath, Object2LongMap<String> openHashMap) throws IOException {
        if (fileSystem.exists(dictPath)) {
            fileSystem.delete(dictPath, true);
        }
        logger.info("Write dict path: {}", dictPath);
        try (FSDataOutputStream dos = fileSystem.create(dictPath)) {
            dos.writeInt(openHashMap.size());
            for (Object2LongMap.Entry<String> entry : openHashMap.object2LongEntrySet()) {
                dos.writeLong(entry.getLongValue());
                byte[] bytes = entry.getKey().getBytes(Charset.defaultCharset());
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
            dos.flush();
        }

        logger.info("Write dict path: {} , dict num: {} success", dictPath, openHashMap.size());
    }

    public void writeMetaInfo(int bucketSize, String workingPath) throws IOException {
        Path metaPath = new Path(workingPath, DICT_METADATA_NAME);
        if (fileSystem.exists(metaPath)) {
            fileSystem.delete(metaPath, true);
        }
        logger.info("Write dict meta path: {}", metaPath);

        Path workPath = new Path(workingPath);
        FileStatus[] dictPrevFiles = fileSystem.listStatus(workPath,
                path -> StringUtils.contains(path.getName(), DICT_PREV_PREFIX));
        FileStatus[] dictCurrFiles = fileSystem.listStatus(workPath,
                path -> StringUtils.contains(path.getName(), DICT_CURR_PREFIX));

        long prevDictCount = 0;
        long[] bucketCnts = new long[bucketSize];
        long[] bucketOffsets = new long[bucketSize];

        for (FileStatus fileStatus : dictPrevFiles) {
            try (FSDataInputStream is = fileSystem.open(fileStatus.getPath())) {
                String bucketId = fileStatus.getPath().getName().replaceAll(DICT_PREV_PREFIX, "");
                int cnt = is.readInt();
                prevDictCount = prevDictCount + cnt;
                bucketCnts[Integer.parseInt(bucketId)] = cnt;
            }
        }

        /**
         * MetaInfo file structure
         *     NGlobalDictMetaInfo: [bucketSize][dictCount][bucketOffsets...][bucketCount...]
         *         bucketSize: The number of buckets used by the dictionary
         *         dictCount: The total number of dictionaries in all buckets
         *         bucketOffsets: Offset of the curr dictionary in each bucket
         *         bucketCount: Number of dictionaries per bucket
         */
        try (FSDataOutputStream dos = fileSystem.create(metaPath)) {
            // #1 Total number of buckets written
            dos.writeInt(bucketSize);

            int currDictCnt = 0;
            for (FileStatus fileStatus : dictCurrFiles) {
                try (FSDataInputStream is = fileSystem.open(fileStatus.getPath())) {
                    String bucketId = fileStatus.getPath().getName().replaceAll(DICT_CURR_PREFIX, "");
                    int cnt = is.readInt();
                    int bucket = Integer.parseInt(bucketId);
                    bucketCnts[bucket] = bucketCnts[bucket] + cnt;
                    bucketOffsets[bucket] = cnt;
                    currDictCnt = currDictCnt + cnt;
                }
            }

            // #2 Write the total number of all bucket dicts
            dos.writeLong(prevDictCount + currDictCnt);

            // #3 Write the bucketOffsets of each bucket, mainly used to calculate the curr dictionary file
            for (long offset : bucketOffsets) {
                dos.writeLong(prevDictCount);
                prevDictCount = prevDictCount + offset;
            }

            // #4 Write the size of each bucket
            for (long cnt : bucketCnts) {
                dos.writeLong(cnt);
            }
            dos.flush();
        }
    }

    @Override
    public void commit(String workingDir, int maxVersions, long versionTTL) throws IOException {
        Path workingPath = new Path(workingDir);
        // copy working dir to newVersion dir
        Path newVersionPath = new Path(basePath, VERSION_PREFIX + System.currentTimeMillis());
        fileSystem.rename(workingPath, newVersionPath);
        logger.info("Commit from {} to {}", workingPath, newVersionPath);
        cleanUp(maxVersions, versionTTL);
    }

    @Override
    public String getWorkingDir() {
        return baseDir + WORKING_DIR;
    }

    // Check versions count, delete expired versions
    protected void cleanUp(int maxVersions, long versionTTL) throws IOException {
        long timestamp = System.currentTimeMillis();
        Long[] versions = listAllVersions();
        for (int i = 0; i < versions.length - maxVersions; i++) {
            if (versions[i] + versionTTL < timestamp) {
                fileSystem.delete(getVersionDir(versions[i]), true);
            }
        }
    }
}
