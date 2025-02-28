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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.sparkproject.guava.hash.Hashing.sha256;

import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.cache.utils.Hadoop3CompaUtil;
import org.apache.kylin.cache.utils.ReflectionUtil;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.fileseg.FileSegments;
import org.apache.kylin.fileseg.FileSegments.SourceFilePredicate;
import org.apache.kylin.fileseg.FileSegmentsDetector;

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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractCacheFileSystem extends FilterFileSystem {

    protected URI uri;
    protected String originalScheme;
    protected int bufferSize = 4096;
    protected boolean useLocalCache = false;
    protected boolean useFileStatusCache = false;
    protected boolean useLegacyFileInputStream = false;
    protected boolean useBufferFileInputStream = false;
    protected HadoopFileOpener mHadoopFileOpener;
    protected LocalCacheFileInStream.FileInStreamOpener mAlluxioFileOpener;
    protected AlluxioConfiguration mAlluxioConf;
    protected ManagerOfCacheFileContent mCacheManager;
    protected ManagerOfCacheFileStatus mStatusCache;
    protected Map<Path, Long> lastFetchTimeOfPaths = new ConcurrentHashMap<>();

    protected static final Map<String, String> schemeClassMap = Stream
            .of(new AbstractMap.SimpleImmutableEntry<>("file", "org.apache.hadoop.fs.LocalFileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("viewfs", "org.apache.hadoop.fs.viewfs.ViewFileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
                    new AbstractMap.SimpleImmutableEntry<>("s3", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
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
            @SuppressWarnings("unchecked")
            Class<? extends FileSystem> clazz = (Class<? extends FileSystem>) conf
                    .getClassByName(schemeClassMap.get(uri.getScheme()));
            fs = ReflectionUtils.newInstance(clazz, conf);
            fs.initialize(uri, conf);
            log.info("Create filesystem {} for scheme {}", schemeClassMap.get(uri.getScheme()), uri.getScheme());
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
        mCacheManager = new ManagerOfCacheFileContent(CacheManager.Factory.get(mAlluxioConf));
    }

    @Override
    public synchronized void initialize(URI name, Configuration conf) throws IOException {
        this.originalScheme = name.getScheme();
        // create internal FileSystem according to the scheme
        this.fs = createInternalFS(name, conf);
        this.statistics = (FileSystem.Statistics) ReflectionUtil.getFieldValue(this.fs, "statistics");
        if (null == this.statistics) {
            log.info("======= original statistics is null.");
        } else {
            log.info("======= original statistics is {} {}.", this.statistics.getScheme(), this.statistics);
        }
        super.initialize(name, conf);
        this.setConf(conf);
        log.info("======= current statistics is {} {}.", this.statistics.getScheme(), this.statistics);

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
        if (this.useFileStatusCache) {
            long fileStatusTTL = conf.getLong(CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_TTL,
                    CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_TTL_DEFAULT_VALUE);
            long fileStatusMaxSize = conf.getLong(CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_MAX_SIZE,
                    CacheFileSystemConstants.PARAMS_KEY_FILE_STATUS_CACHE_MAX_SIZE_DEFAULT_VALUE);
            this.mStatusCache = new ManagerOfCacheFileStatus(fileStatusTTL, fileStatusMaxSize, p -> {
                try {
                    return fs.getFileStatus(p);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }, p -> {
                try {
                    List<LocatedFileStatus> ret = Hadoop3CompaUtil.RemoteIterators.toList(fs.listLocatedStatus(p));
                    return ret.toArray(new LocatedFileStatus[0]);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }

        // create LocalCacheFileSystem if it needs
        if (this.useLocalCache) {
            this.createLocalCacheManager(conf);
            log.info("Create LocalCacheFileSystem successfully .");
        }
    }

    @Override
    public String getScheme() {
        return this.originalScheme;
    }

    /**
     * Check whether it needs to cache data on the current executor
     */
    protected abstract boolean isUseLocalCacheForCurrentExecutor();

    protected abstract long getAcceptCacheTime();

    /**
     * Wrap FileStatus to Alluxio FileInfo
     */
    private FileInfo wrapFileInfo(FileStatus fileStatus) {
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
    public FSDataInputStream open(Path f) throws IOException {
        return open(f, bufferSize);
    }

    @Override
    public FSDataInputStream open(Path p, int bufferSize) throws IOException {
        return this.open(p, bufferSize, this.isUseLocalCacheForCurrentExecutor());
    }

    public FSDataInputStream open(Path p, int bufferSize, boolean useLocalCacheForExec) throws IOException {
        // quick return if cache is not enabled
        boolean enabled = (this.useLocalCache && this.mCacheManager != null && useLocalCacheForExec);
        if (!enabled) {
            log.debug("Use original FileSystem to open file {} .", p);
            return fs.open(p, bufferSize);
        }

        Path f = this.fs.makeQualified(p);
        FileStatus fileStatus = this.getFileStatus(f);
        FileInfo fileInfo = wrapFileInfo(fileStatus);

        // FilePath is a unique identifier for a file, however it can be a long string
        // hence using md5 hash of the file path as the identifier in the cache.
        String fileHashFrom = String.valueOf(fileStatus.getPath()) + fileStatus.getLen()
                + fileStatus.getModificationTime();
        String fileId = sha256().hashString(fileHashFrom, UTF_8).toString();
        CacheContext context = CacheContext.defaults().setCacheIdentifier(fileId);
        URIStatus status = new URIStatus(fileInfo, context);
        int cachedCount = mCacheManager.countCachedPages(fileId);

        if (this.useLegacyFileInputStream) {
            log.debug("Local cache (Legacy) opens file ({} pages cached) {} .", cachedCount, f);
            return new FSDataInputStream(new AlluxioHdfsFileInputStream(
                    new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager, mAlluxioConf), statistics));
        } else if (this.useBufferFileInputStream) {
            log.debug("Local cache (Direct-Buffer) opens file ({} pages cached) {} .", cachedCount, f);
            return new FSDataInputStream(new CacheFileInputStream(f,
                    new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager, mAlluxioConf), null,
                    statistics, checkBufferSize(bufferSize)));
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public FileStatus getFileStatus(Path p) throws IOException {
        if (!this.useFileStatusCache) {
            StopWatch w = StopWatch.createStarted();
            try {
                return fs.getFileStatus(p);
            } finally {
                log.warn("Slow fs operation took {} ms: {}({})", w.getTime(), "getFileStatus", p);
            }
        }

        p = this.fs.makeQualified(p);
        this.statistics.incrementReadOps(1);

        mStatusCache.ensureCacheCreateTime(p, getAcceptCacheTime());
        return mStatusCache.getFileStatus(p);
    }

    @Override
    public FileStatus[] listStatus(Path p) throws IOException {
        return _listLocatedStatus(p);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path p) throws IOException {
        return Hadoop3CompaUtil.RemoteIterators.remoteIteratorFromIterator(filteredStatusStream(p).iterator());
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path p) throws IOException {
        return Hadoop3CompaUtil.RemoteIterators.remoteIteratorFromIterator(filteredStatusStream(p) //
                .map(FileStatus.class::cast).iterator());
    }

    private Stream<LocatedFileStatus> filteredStatusStream(Path p) throws IOException {
        LocatedFileStatus[] ret = _listLocatedStatus(p);

        Optional<SourceFilePredicate> filter = FileSegments.getFileSegFilterLocally(p.toString());
        if (filter.isPresent()) {
            return Arrays.stream(ret).filter(f -> filter.get().checkFile(f));
        } else {
            return Arrays.stream(ret);
        }

    }

    private LocatedFileStatus[] _listLocatedStatus(Path p) throws IOException {
        LocatedFileStatus[] result;

        if (this.useFileStatusCache) {
            p = this.fs.makeQualified(p);
            this.statistics.incrementReadOps(1);

            long requiredCacheTime = getAcceptCacheTime();

            // path is going to be fetched by the required cached time,
            // use this info to update QueryContext.dataFetchTime
            updateDataFetchTimeInContext(p, requiredCacheTime);

            mStatusCache.ensureCacheCreateTime(p, requiredCacheTime);
            result = mStatusCache.listChildren(p);

        } else {
            StopWatch w = StopWatch.createStarted();
            try {
                List<LocatedFileStatus> list = Hadoop3CompaUtil.RemoteIterators.toList(fs.listLocatedStatus(p));
                result = list.toArray(new LocatedFileStatus[0]);
            } finally {
                log.warn("Slow fs operation took {} ms: {}({})", w.getTime(), "listLocatedStatus", p);
            }
        }

        FileSegmentsDetector.onObserveFileSystemListFileStatus(p, result);

        return result;
    }

    private void updateDataFetchTimeInContext(Path p, long requiredCacheTime) {
        QueryContext context = QueryContext.current();
        if (context == null)
            return; // not in KE process, thus has no query context

        long lastFetchTime = lastFetchTimeOfPaths.getOrDefault(p, 0L);
        lastFetchTime = Long.max(lastFetchTime, requiredCacheTime);

        if (lastFetchTime == 0) { // accessing the path for the very first time?
            lastFetchTime = System.currentTimeMillis();
        }

        // path is going to be fetched by the required cached time, 
        // use this info to update QueryContext.dataFetchTime
        context.getMetrics().addDataFetchTime(lastFetchTime);
        lastFetchTimeOfPaths.put(p, lastFetchTime);
    }

    public long countCachedFilePages() {
        return mCacheManager.countTotalCachedPages();
    }

    public long countCachedFileStatus() {
        return mStatusCache.size();
    }

    public long countCachedFileStatusEvictions() {
        return mStatusCache.countEvictions();
    }
}
