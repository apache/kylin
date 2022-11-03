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
package io.kyligence.kap.cache.fs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.sparkproject.guava.hash.Hashing.sha256;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import alluxio.client.file.CacheContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.conf.AlluxioConfiguration;
import alluxio.hadoop.AlluxioHdfsInputStream;
import alluxio.hadoop.HadoopFileOpener;
import alluxio.hadoop.HadoopUtils;
import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;
import alluxio.wire.FileInfo;
import io.kyligence.kap.cache.utils.ReflectionUtil;

public abstract class AbstractCacheFileSystem extends FilterFileSystem {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCacheFileSystem.class);

    protected URI uri;
    protected String originalScheme;
    protected int bufferSize = 4096;
    protected boolean useLocalCache = false;
    protected boolean useFileStatusCache = false;
    protected boolean useLegacyFileInputStream = false;
    protected boolean useBufferFileInputStream = false;
    protected HadoopFileOpener mHadoopFileOpener;
    protected LocalCacheFileInStream.FileInStreamOpener mAlluxioFileOpener;
    protected CacheManager mCacheManager;
    protected AlluxioConfiguration mAlluxioConf;

    protected LoadingCache<Path, FileStatus> fileStatusCache;

    protected static final Map<String, String> schemeClassMap = Stream
            .of(new AbstractMap.SimpleImmutableEntry<>("file", "org.apache.hadoop.fs.LocalFileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("viewfs", "org.apache.hadoop.fs.viewfs.ViewFileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("s3", "org.apache.hadoop.fs.s3.S3FileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("s3n", "org.apache.hadoop.fs.s3native.NativeS3FileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("hdfs", "org.apache.hadoop.hdfs.DistributedFileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("wasb", "org.apache.hadoop.fs.azure.NativeAzureFileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("wasbs",
                            "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure"),
                    new AbstractMap.SimpleImmutableEntry<>("alluxio", "alluxio.hadoop.FileSystem"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    /**
     * Create internal FileSystem
     */
    protected static FileSystem createInternalFS(URI uri, Configuration conf) throws IOException {
        if (!schemeClassMap.containsKey(uri.getScheme())) {
            throw new IOException("No FileSystem for scheme: " + uri.getScheme());
        }
        FileSystem fs;
        try {
            Class<? extends FileSystem> clazz = (Class<? extends FileSystem>) conf
                    .getClassByName(schemeClassMap.get(uri.getScheme()));
            fs = ReflectionUtils.newInstance(clazz, conf);
            fs.initialize(uri, conf);
            logger.info("Create filesystem {} for scheme {} .", schemeClassMap.get(uri.getScheme()), uri.getScheme());
        } catch (ClassNotFoundException e) {
            throw new IOException("Can not found FileSystem Clazz for scheme: " + uri.getScheme());
        }
        return fs;
    }

    protected void createLocalCacheManager(Configuration conf) throws IOException {
        mHadoopFileOpener = uriStatus -> this.fs.open(new Path(uriStatus.getPath()));
        mAlluxioFileOpener = status -> new AlluxioHdfsInputStream(mHadoopFileOpener.open(status));

        mAlluxioConf = HadoopUtils.toAlluxioConf(conf);
        // Handle metrics
        Properties metricsProperties = new Properties();
        for (Map.Entry<String, String> entry : conf) {
            metricsProperties.setProperty(entry.getKey(), entry.getValue());
        }
        MetricsSystem.startSinksFromConfig(new MetricsConfig(metricsProperties));
        mCacheManager = CacheManager.Factory.get(mAlluxioConf);
        if (mCacheManager == null) {
            throw new IOException("CacheManager is null !");
        }
    }

    @Override
    public synchronized void initialize(URI name, Configuration conf) throws IOException {
        this.originalScheme = name.getScheme();
        // create internal FileSystem according to the scheme
        this.fs = createInternalFS(name, conf);
        this.statistics = (FileSystem.Statistics) ReflectionUtil.getFieldValue(this.fs, "statistics");
        if (null == this.statistics) {
            logger.info("======= original statistics is null.");
        } else {
            logger.info("======= original statistics is {} {}.", this.statistics.getScheme(), this.statistics);
        }
        super.initialize(name, conf);
        this.setConf(conf);
        logger.info("======= current statistics is {} {}.", this.statistics.getScheme(), this.statistics);

        this.bufferSize = conf.getInt(CacheFileSystemConstants.PARAMS_KEY_IO_FILE_BUFFER_SIZE,
                CacheFileSystemConstants.PARAMS_KEY_IO_FILE_BUFFER_SIZE_DEFAULT_VALUE);
        // when scheme is jfs, use the cache by jfs itself
        this.useLocalCache = conf.getBoolean(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE,
                CacheFileSystemConstants.PARAMS_KEY_USE_CACHE_DEFAULT_VALUE);

        this.useFileStatusCache = conf.getBoolean(CacheFileSystemConstants.PARAMS_KEY_USE_FILE_STATUS_CACHE,
                CacheFileSystemConstants.PARAMS_KEY_USE_FILE_STATUS_CACHE_DEFAULT_VALUE);

        this.useLegacyFileInputStream = conf.getBoolean(CacheFileSystemConstants.PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM,
                CacheFileSystemConstants.PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM_DEFAULT_VALUE);

        this.useBufferFileInputStream = conf.getBoolean(CacheFileSystemConstants.PARAMS_KEY_USE_BUFFER_FILE_INPUTSTREAM,
                CacheFileSystemConstants.PARAMS_KEY_USE_BUFFER_FILE_INPUTSTREAM_DEFAULT_VALUE);

        // create FileStatus cache
        long fileStatusTTL = conf.getLong(CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_TTL,
                CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_TTL_DEFAULT_VALUE);
        long fileStatusMaxSize = conf.getLong(CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_MAX_SIZE,
                CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_MAX_SIZE_DEFAULT_VALUE);
        CacheLoader<Path, FileStatus> fileStatusCacheLoader = new CacheLoader<Path, FileStatus>() {
            @Override
            public FileStatus load(Path path) throws Exception {
                return getFileStatusForCache(path);
            }
        };
        this.fileStatusCache = CacheBuilder.newBuilder().maximumSize(fileStatusMaxSize)
                .expireAfterAccess(fileStatusTTL, TimeUnit.SECONDS).recordStats().build(fileStatusCacheLoader);

        // create LocalCacheFileSystem if it needs
        if (this.isUseLocalCache()) {
            this.createLocalCacheManager(conf);
            logger.info("Create LocalCacheFileSystem successfully .");
        }
    }

    protected FileStatus getFileStatusForCache(Path path) throws IOException {
        return this.fs.getFileStatus(path);
    }

    @Override
    public String getScheme() {
        return this.originalScheme;
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return open(f, bufferSize);
    }

    /**
     * Check whether it needs to cache data on the current executor
     */
    public abstract boolean isUseLocalCacheForTargetExecs();

    /**
     * Wrap FileStatus to Alluxio FileInfo
     */
    public FileInfo wrapFileInfo(FileStatus fileStatus) {
        return (new FileInfo().setLength(fileStatus.getLen()).setPath(fileStatus.getPath().toString())
                .setFolder(fileStatus.isDirectory()).setBlockSizeBytes(fileStatus.getBlockSize())
                .setLastModificationTimeMs(fileStatus.getModificationTime())
                .setLastAccessTimeMs(fileStatus.getAccessTime()).setOwner(fileStatus.getOwner())
                .setGroup(fileStatus.getGroup()));
    }

    private int checkBufferSize(int size) {
        if (size < this.bufferSize) {
            size = this.bufferSize;
        }

        return size;
    }

    @Override
    public FSDataInputStream open(Path p, int bufferSize) throws IOException {
        return this.open(p, bufferSize, this.isUseLocalCacheForTargetExecs());
    }

    public FSDataInputStream open(Path p, int bufferSize, boolean useLocalCacheForExec) throws IOException {
        Path f = this.fs.makeQualified(p);

        if (this.isUseLocalCache() && this.mCacheManager != null && useLocalCacheForExec) {
            FileStatus fileStatus = this.getFileStatus(f);
            FileInfo fileInfo = wrapFileInfo(fileStatus);
            // FilePath is a unique identifier for a file, however it can be a long string
            // hence using md5 hash of the file path as the identifier in the cache.
            CacheContext context = CacheContext.defaults()
                    .setCacheIdentifier(sha256().hashString(fileStatus.getPath().toString(), UTF_8).toString());
            URIStatus status = new URIStatus(fileInfo, context);

            if (this.useLegacyFileInputStream) {
                logger.info("Use local cache Legacy FileSystem to open file {} .", f);
                return new FSDataInputStream(new AlluxioHdfsFileInputStream(
                        new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager, mAlluxioConf),
                        statistics));
            } else if (this.useBufferFileInputStream) {
                logger.info("Use local cache Buffer FileSystem to open file {} .", f);
                return new FSDataInputStream(new CacheFileInputStream(f,
                        new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager, mAlluxioConf), null,
                        statistics, checkBufferSize(bufferSize)));
            }
        }
        logger.info("Use original FileSystem to open file {} .", f);
        return super.open(f, bufferSize);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        if (isUseFileStatusCache()) {
            this.statistics.incrementReadOps(1);
            long start = System.currentTimeMillis();
            FileStatus fileStatus;
            Path p = this.fs.makeQualified(f);
            try {
                fileStatus = this.fileStatusCache.get(p);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof FileNotFoundException) {
                    logger.info("Get file status from cache error: {}", p);
                    throw new FileNotFoundException("File does not exist: " + p);
                }
                return null;
            }
            logger.info("Get file status {} from cache took: {}", f, (System.currentTimeMillis() - start));
            return fileStatus;
        } else {
            return super.getFileStatus(f);
        }
    }

    public boolean isUseLocalCache() {
        return useLocalCache;
    }

    public boolean isUseFileStatusCache() {
        return useFileStatusCache;
    }
}