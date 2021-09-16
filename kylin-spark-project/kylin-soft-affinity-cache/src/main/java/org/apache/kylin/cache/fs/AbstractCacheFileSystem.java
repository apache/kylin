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

package org.apache.kylin.cache.fs;

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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.cache.utils.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractCacheFileSystem extends FilterFileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCacheFileSystem.class);

    //private static final DirectBufferPool bufferPool = new DirectBufferPool();

    protected URI uri;
    protected String originalScheme;
    protected int bufferSize = 4096;
    protected boolean useLocalCache = false;
    protected boolean useLegacyFileInputStream = false;
    protected HadoopFileOpener mHadoopFileOpener;
    protected LocalCacheFileInStream.FileInStreamOpener mAlluxioFileOpener;
    protected CacheManager mCacheManager;
    protected AlluxioConfiguration mAlluxioConf;

    protected LoadingCache<Path, FileStatus> fileStatusCache;

    // put("s3", "com.amazon.ws.emr.hadoop.fs.EmrFileSystem");
    // put("s3n", "com.amazon.ws.emr.hadoop.fs.EmrFileSystem");
    // put("s3bfs", "org.apache.hadoop.fs.s3.S3FileSystem");
    protected static final Map<String, String> schemeClassMap = new HashMap<String, String>() {
        {
            put("file", "org.apache.hadoop.fs.LocalFileSystem");
            put("viewfs", "org.apache.hadoop.fs.viewfs.ViewFileSystem");
            put("s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            put("s3", "org.apache.hadoop.fs.s3.S3FileSystem");
            put("s3n", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
            put("hdfs", "org.apache.hadoop.hdfs.DistributedFileSystem");
            put("wasb", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
            put("wasbs", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure");
            put("jfs", "io.juicefs.JuiceFileSystem");
            put("alluxio", "alluxio.hadoop.FileSystem");
        }
    };

    /**
     * Create internal FileSystem
     */
    protected static FileSystem createInternalFS(URI uri, Configuration conf)
            throws IOException {
        if (!schemeClassMap.containsKey(uri.getScheme())) {
            throw new IOException("No FileSystem for scheme: " + uri.getScheme());
        }
        FileSystem fs = null;
        try {
            Class<? extends FileSystem> clazz =
                    (Class<? extends FileSystem>) conf.getClassByName(
                            schemeClassMap.get(uri.getScheme()));
            fs = ReflectionUtils.newInstance(clazz, conf);
            fs.initialize(uri, conf);
            LOG.info("Create filesystem {} for scheme {} .",
                    schemeClassMap.get(uri.getScheme()), uri.getScheme());
        } catch (ClassNotFoundException e) {
            throw new IOException("Can not found FileSystem Clazz for scheme: " + uri.getScheme());
        }
        return fs;
    }

    protected void createLocalCacheManager(URI name, Configuration conf) throws IOException{
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
        this.statistics = (FileSystem.Statistics) ReflectionUtil.getFieldValue(this.fs,
                "statistics");
        if (null == this.statistics) {
            LOG.info("======= original statistics is null.");
        } else {
            LOG.info("======= original statistics is {} {}.", this.statistics.getScheme(),
                    this.statistics.toString());
        }
        super.initialize(name, conf);
        this.setConf(conf);
        LOG.info("======= current statistics is {} {}.", this.statistics.getScheme(),
                this.statistics.toString());

        this.bufferSize = conf.getInt(CacheFileSystemConstants.PARAMS_KEY_IO_FILE_BUFFER_SIZE,
                CacheFileSystemConstants.PARAMS_KEY_IO_FILE_BUFFER_SIZE_DEFAULT_VALUE);
        // when scheme is jfs, use the cache by jfs itself
        this.useLocalCache = conf.getBoolean(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE,
                CacheFileSystemConstants.PARAMS_KEY_USE_CACHE_DEFAULT_VALUE)
                && !this.originalScheme.equals(CacheFileSystemConstants.JUICEFS_SCHEME);

        this.useLegacyFileInputStream = conf.getBoolean(
                CacheFileSystemConstants.PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM,
                CacheFileSystemConstants.PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM_DEFAULT_VALUE);

        // create FileStatus cache
        long fileStatusTTL =
                conf.getLong(CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_TTL,
                        CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_TTL_DEFAULT_VALUE);
        long fileStatusMaxSize =
                conf.getLong(CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_MAX_SIZE,
                     CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_MAX_SIZE_DEFAULT_VALUE);
        CacheLoader<Path, FileStatus> fileStatusCacheLoader = new CacheLoader<Path, FileStatus>() {
            @Override
            public FileStatus load(Path path) throws Exception {
                return getFileStatusForCache(path);
            }
        };
        this.fileStatusCache =
                CacheBuilder.newBuilder()
                        .maximumSize(fileStatusMaxSize)
                        .expireAfterAccess(fileStatusTTL, TimeUnit.SECONDS)
                        .recordStats()
                        .build(fileStatusCacheLoader);

        // create LocalCacheFileSystem if needs
        if (this.isUseLocalCache()) {
            // Todo: Can set local cache dir here for the current executor
            this.createLocalCacheManager(this.getUri(), conf);
            LOG.info("Create LocalCacheFileSystem successfully .");
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
     * Check whether needs to cache data on the current executor
     */
    public abstract boolean isUseLocalCacheForTargetExecs();

    /**
     * Wrap FileStatus to Alluxio FileInfo
     */
    public FileInfo wrapFileInfo(FileStatus fileStatus) {
        return (new FileInfo()
                .setLength(fileStatus.getLen())
                .setPath(fileStatus.getPath().toString())
                .setFolder(fileStatus.isDirectory())
                .setBlockSizeBytes(fileStatus.getBlockSize())
                .setLastModificationTimeMs(fileStatus.getModificationTime())
                .setLastAccessTimeMs(fileStatus.getAccessTime())
                .setOwner(fileStatus.getOwner())
                .setGroup(fileStatus.getGroup()));
    }

    private int checkBufferSize(int size) {
        if (size < this.bufferSize) {
            size = this.bufferSize;
        }
        //  int numWords = (size + 7) / 8;
        //  int alignedSize = numWords * 8;
        //  assert (alignedSize >= size);
        //  return alignedSize;
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
            CacheContext context = CacheContext.defaults().setCacheIdentifier(
                    md5().hashString(fileStatus.getPath().toString(), UTF_8).toString());
            URIStatus status = new URIStatus(fileInfo, context);
            LOG.info("Use local cache FileSystem to open file {} .", f);
            if (this.useLegacyFileInputStream) {
                return new FSDataInputStream(new AlluxioHdfsFileInputStream(
                        new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager,
                                mAlluxioConf), statistics));
            }
            return new FSDataInputStream(new CacheFileInputStream(f,
                    new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager,
                            mAlluxioConf),
                    null, statistics, checkBufferSize(bufferSize)));
        }
        LOG.info("Use original FileSystem to open file {} .", f);
        return super.open(f, bufferSize);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        this.statistics.incrementReadOps(1);
        long start = System.currentTimeMillis();
        FileStatus fileStatus = null;
        Path p = this.fs.makeQualified(f);
        try {
            fileStatus = this.fileStatusCache.get(p);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof FileNotFoundException)
                throw new FileNotFoundException("File does not exist: " + p);
            LOG.error("Get file status from cache error: " + p, e);
            return fileStatus;
        }
        LOG.info("Get file status {} from cache took: {}", f,
                (System.currentTimeMillis() - start));
        return fileStatus;
    }

    public CacheManager getmCacheManager() {
        return mCacheManager;
    }

    public void setmCacheManager(CacheManager mCacheManager) {
        this.mCacheManager = mCacheManager;
    }

    public AlluxioConfiguration getmAlluxioConf() {
        return mAlluxioConf;
    }

    public void setmAlluxioConf(AlluxioConfiguration mAlluxioConf) {
        this.mAlluxioConf = mAlluxioConf;
    }

    public boolean isUseLocalCache() {
        return useLocalCache;
    }

    public void setUseLocalCache(boolean useLocalCache) {
        this.useLocalCache = useLocalCache;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean isUseLegacyFileInputStream() {
        return useLegacyFileInputStream;
    }

    public void setUseLegacyFileInputStream(boolean useLegacyFileInputStream) {
        this.useLegacyFileInputStream = useLegacyFileInputStream;
    }

    public LoadingCache<Path, FileStatus> getFileStatusCache() {
        return fileStatusCache;
    }
}
