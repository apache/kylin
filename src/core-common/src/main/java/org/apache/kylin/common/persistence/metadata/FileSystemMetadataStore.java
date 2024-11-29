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
package org.apache.kylin.common.persistence.metadata;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.kylin.common.constant.Constants.CORE_META_DIR;
import static org.apache.kylin.common.persistence.metadata.FileSystemFilterFactory.MATCH_ALL_EVAL;
import static org.apache.kylin.common.persistence.metadata.FileSystemFilterFactory.convertConditionsToFilter;
import static org.apache.kylin.common.util.HadoopUtil.FILE_PREFIX;
import static org.apache.kylin.common.util.MetadataChecker.verifyNonMetadataFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.SnapshotRawResource;
import org.apache.kylin.common.persistence.VersionedRawResource;
import org.apache.kylin.common.persistence.lock.LockTimeoutException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.common.util.FileSystemUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.guava30.shaded.common.util.concurrent.Uninterruptibles;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileSystemMetadataStore extends MetadataStore {

    public static final String HDFS_SCHEME = "hdfs";
    public static final String FILE_SCHEME = "file";
    private static final String COMPRESSED_FILE = "metadata.zip";
    public static final String JSON_SUFFIX = ".json";
    private static final int DEFAULT_FILE_NUMBER = 10000;
    private static final ThreadLocal<Set<ReentrantLock>> OWNED_LOCKS = ThreadLocal.withInitial(HashSet::new);
    private static final ConcurrentHashMap<String, ReentrantLock> LOCK_MAP = new ConcurrentHashMap<>();
    @VisibleForTesting
    protected static volatile ExecutorService fileSystemMetadataExecutor = null;

    @Getter
    protected Path rootPath;
    @Getter
    protected FileSystem fs;

    @Getter
    public enum Type {
        DIR, ZIP
    }

    @VisibleForTesting
    protected final Type type;

    private final CompressHandlerInterface compressHandlerInterface;

    public FileSystemMetadataStore(KylinConfig kylinConfig) throws IOException {
        // The file system metadata store will use NoopAuditLogStore by default
        super(kylinConfig);
        try {
            val storageUrl = kylinConfig.getMetadataUrl();
            val scheme = storageUrl.getScheme();
            Preconditions.checkState(HDFS_SCHEME.equals(scheme) || FILE_SCHEME.equals(scheme),
                    "FileSystemMetadataStore only support hdfs or file scheme");
            type = storageUrl.getParameter("zip") != null ? Type.ZIP : Type.DIR;
            compressHandlerInterface = storageUrl.getParameter("snapshot") != null ? new SnapShotCompressHandler()
                    : new CompressHandler();
            String pathStr = scheme.equals(HDFS_SCHEME) ? storageUrl.getParameter("path")
                    : kylinConfig.getMetadataUrl().getIdentifier();

            initWithMetadataPath(pathStr, scheme, kylinConfig);

            if (fs instanceof RawLocalFileSystem || fs instanceof LocalFileSystem) {
                // set this as false to avoid writing crc file when migrate or dump metadata.
                fs.setWriteChecksum(false);
                // set this as true to avoid building job failed in UT.
                fs.setVerifyChecksum(true);
            }

            if (!fs.exists(rootPath)) {
                Path p = rootPath;
                if (type == Type.ZIP && rootPath.toString().endsWith(".zip")) {
                    p = rootPath.getParent();
                }
                log.warn("Path not exist in FileSystem, create it: {}", p.toString());
                fs.mkdirs(p);
            }

            if (fileSystemMetadataExecutor == null && kylinConfig.isConcurrencyProcessMetadataEnabled()) {
                synchronized (FileSystemMetadataStore.class) {
                    if (fileSystemMetadataExecutor == null) {
                        fileSystemMetadataExecutor = new ThreadPoolExecutor(
                                kylinConfig.getConcurrencyProcessMetadataThreadNumber(),
                                kylinConfig.getConcurrencyProcessMetadataThreadNumber(), 300L, TimeUnit.SECONDS,
                                new LinkedBlockingQueue<>(), new DaemonThreadFactory("fileSystemMetadataExecutor"));
                    }
                }
            }
            auditLogStore = new MemoryAuditLogStore(kylinConfig);
            log.info("The FileSystem location is {}, hdfs root path : {}", fs.getUri().toString(), rootPath.toString());
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new KylinRuntimeException(e);
        }
    }

    private void initWithMetadataPath(String pathStr, String scheme, KylinConfig kylinConfig) throws IOException {
        if (pathStr == null) {
            assert !scheme.equals(FILE_SCHEME) : "When scheme is file, the pathStr shouldn't be null";
            pathStr = HadoopUtil.getBackupFolder(kylinConfig);
            fs = HadoopUtil.getWorkingFileSystem();
            createMetaFolderIfNeed(new Path(pathStr));
            Path tmpRootPath = Stream.of(FileSystemUtil.listStatus(fs, new Path(pathStr)))
                    .filter(fileStatus -> fileStatus.getPath().getName().endsWith("_backup"))
                    .max(Comparator.comparing(FileStatus::getModificationTime)).map(FileStatus::getPath)
                    .orElse(new Path(pathStr + "/backup_0/"));
            createMetaFolderIfNeed(tmpRootPath);
            rootPath = checkCoreMetaDir(tmpRootPath);
        } else {
            if (scheme.equals(FILE_SCHEME) && !pathStr.startsWith(FILE_SCHEME)) {
                pathStr = FILE_PREFIX + new File(pathStr).getAbsolutePath();
            }
            Path tempPath = new Path(pathStr);
            if (tempPath.toUri().getScheme() != null) {
                fs = HadoopUtil.getWorkingFileSystem(tempPath);
                rootPath = tempPath;
            } else {
                fs = HadoopUtil.getWorkingFileSystem();
                rootPath = fs.makeQualified(tempPath);
            }
        }
    }

    // make compatible with old version KE, 'core_meta' directory does not exist in old version KE
    private Path checkCoreMetaDir(Path tmpRootPath) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(tmpRootPath);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory() && CORE_META_DIR.equals(fileStatus.getPath().getName())) {
                return fileStatus.getPath();
            }
        }
        return tmpRootPath;
    }

    /**
     * Get resource from HDFS
     * @param type The type of the resource
     * @param filter The RawResource filter condition
     * @param needLock File system does not support lock
     * @param needContent Whether to load the content of the resource
     * @return The list of the RawResource
     */
    @SuppressWarnings("unchecked")
    public <T extends RawResource> List<T> get(MetadataType type, RawResourceFilter filter, boolean needLock,
            boolean needContent) {
        Preconditions.checkArgument(type != MetadataType.ALL,
                "Fetching all metadata in the transaction is not allowed.");
        List<T> resList;
        val context = convertConditionsToFilter(filter, type);
        Class<T> resourceClass = (Class<T>) type.getResourceClass();
        if (needLock) {
            if (context.isWholePath()) {
                lockResource(context.getResPath());
            } else {
                lockResource(type.name());
            }
        }
        try {
            if (this.type == Type.DIR) {
                resList = getFromDir(context, needContent, resourceClass);
            } else {
                resList = getFromZip(context, needContent, resourceClass);
            }
        } catch (IOException e) {
            throw new KylinRuntimeException("get resource fail", e);
        }
        return resList;
    }

    private <T extends RawResource> List<T> getFromDir(FileSystemFilterFactory.FilterContext context,
            boolean needContent, Class<T> resourceClass) throws IOException {
        List<T> resList = new ArrayList<>();
        String resPath = context.getResPath();
        RawResourceFilter jsonFilter = context.getRawResourceFilter();
        Path p = getRealFileSystemPath(resPath);
        if (context.isWholePath()) {
            if (fs.exists(p) && fs.isFile(p)) {
                T rawResource = getRawResource(needContent, fs.getFileStatus(p), resourceClass, jsonFilter);
                if (rawResource != null)
                    resList.add(rawResource);
            }
        } else {
            // With complex query, need to traverse files by filter `jsonFilter`
            if (fs.exists(p) && fs.isDirectory(p)) {
                val stream = context.getRegex() == null ? Arrays.stream(fs.listStatus(p))
                        : Arrays.stream(fs.globStatus(new Path(rootPath, resPath + "/" + MATCH_ALL_EVAL),
                                path -> path.getName().matches(context.getRegex())));
                stream.forEach(path -> {
                    T rawResource = getRawResource(needContent, path, resourceClass, jsonFilter);
                    if (rawResource != null)
                        resList.add(rawResource);
                });
            }
        }
        return resList;
    }

    private <T extends RawResource> List<T> getFromZip(FileSystemFilterFactory.FilterContext context,
            boolean needContent, Class<T> resourceClass) {
        List<T> resList = new ArrayList<>();
        String resPath = context.getResPath();
        RawResourceFilter jsonFilter = context.getRawResourceFilter();
        if (context.isWholePath()) {
            val rawResource = resourceClass.cast(getCompressedFiles().get(resPath));
            if (rawResource != null && jsonFilter.isMatch(rawResource)) {
                if (!needContent) {
                    rawResource.setContent(null);
                }
                resList.add(rawResource);
            }
        } else {
            // With complex query, need to traverse files by filter `jsonFilter`
            getCompressedFiles().entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(context.getResPath()) && (context.getRegex() == null
                            || entry.getKey().matches(context.getResPath() + "/" + context.getRegex())))
                    .forEach(entry -> {
                        val rawResource = resourceClass.cast(entry.getValue());
                        if (jsonFilter.isMatch(rawResource)) {
                            if (!needContent) {
                                rawResource.setContent(null);
                            }
                            resList.add(rawResource);
                        }
                    });
        }
        return resList;
    }

    private <T extends RawResource> T getRawResource(boolean needContent, FileStatus fileStatus, Class<T> resourceClass,
            RawResourceFilter jsonFilter) {
        if (fileStatus.getLen() == 0) {
            log.warn("Zero length file: " + fileStatus.getPath().toString());
        }

        String filePath = fileStatus.getPath().toString().replace(rootPath.toString(), "");
        if (verifyNonMetadataFile(filePath)) {
            return null;
        }

        if (filePath.split("/", 2).length != 2) {
            throw new IllegalStateException("Can not get file path: " + filePath + ".");
        }
        String fileName = splitFilePath(filePath).getValue();
        String metaKey = fileName.substring(0, fileName.length() - 5);

        T rawResource = openFileWithRetry(fileStatus.getPath(), resourceClass, needContent, 3);
        long ts = fileStatus.getModificationTime();
        rawResource.setTs(ts);
        rawResource.setMetaKey(metaKey);
        rawResource.setMvcc(0);
        return jsonFilter.isMatch(rawResource) ? rawResource : null;
    }

    private <T extends RawResource> T openFileWithRetry(Path filePath, Class<T> resourceClass, boolean needContent,
            int retryCnt) {
        T rawResource;
        byte[] byteArray = null;
        try (val in = fs.open(filePath)) {
            byteArray = IOUtils.toByteArray(in);
            rawResource = JsonUtil.readValue(byteArray, resourceClass);
        } catch (Exception e) {
            log.warn("Failed to load resource from file: " + filePath);
            if (byteArray != null && byteArray.length == 0 && retryCnt > 0) {
                // Maybe this file is modified by others now, retry it.
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new KylinRuntimeException(ex);
                }
                return openFileWithRetry(filePath, resourceClass, needContent, retryCnt - 1);
            }
            try {
                rawResource = resourceClass.newInstance();
            } catch (Exception ex) {
                throw new KylinRuntimeException(ex);
            }
        }
        if (needContent) {
            rawResource.setContent(byteArray);
        }
        return rawResource;
    }

    public KylinConfig getKylinConfigFromFile() {
        Path confPath = new Path(rootPath, "kylin.properties");
        try {
            if (!fs.exists(confPath)) {
                throw new KylinRuntimeException("kylin.properties not exist under: " + rootPath);
            }
            try (val in = fs.open(confPath)) {

                return KylinConfig.createKylinConfig(KylinConfig.streamToProps(in));
            }
        } catch (IOException e) {
            throw new KylinRuntimeException("Read kylin.properties failed from file system.");
        }
    }

    private void dumpKylinConfigToFile(KylinConfig conf) {
        Path confPath = new Path(rootPath, "kylin.properties");
        try (FSDataOutputStream out = fs.create(confPath, true)) {
            conf.exportToProperties().store(out, confPath.toString());
        } catch (Exception e) {
            throw new KylinRuntimeException("Dump resource fail", e);
        }
    }

    @Override
    public int save(MetadataType type, final RawResource raw) {
        val resPath = raw.generateFilePath();
        val p = getRealFileSystemPath(resPath);
        try {
            createMetaFolderIfNeed(p.getParent());
            val bytes = raw.getContent();
            if (bytes == null) {
                return fs.delete(p, true) ? 1 : 0;
            }

            val bs = ByteSource.wrap(raw.getContent());
            try (FSDataOutputStream out = createFileWithDefaultPermission(p)) {
                IOUtils.copy(bs.openStream(), out);
                out.hflush();
                fs.setTimes(p, raw.getTs(), -1);
            }

            val fileStatus = fs.getFileStatus(p);
            if (fileStatus.getLen() == 0) {
                throw new KylinRuntimeException(
                        "Put resource fail : " + resPath + ", because resource file is Zero length");
            }
            if (bs.size() != fileStatus.getLen()) {
                throw new KylinRuntimeException(
                        "Put resource fail : " + resPath + ", because resource file length not equal with ByteSource");
            }
        } catch (IOException e) {
            throw new KylinRuntimeException(e);
        }
        return 1;
    }

    private FSDataOutputStream createFileWithDefaultPermission(Path f) throws IOException {
        return fs.create(f, null, true, fs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
                fs.getDefaultReplication(f), fs.getDefaultBlockSize(f), null);
    }

    @Override
    public NavigableSet<String> listAll() {
        try {
            if (compressedFilesContains("/")) {
                return Sets.newTreeSet(getAllFilePathFromCompressedFiles());
            }
            Path p = this.rootPath;
            if (!fs.exists(p) || !fs.isDirectory(p)) {
                log.warn("path {} does not exist in HDFS", p);
                return new TreeSet<>();
            }
            // if you set kylin.env.engine-write-fs, the schema may be inconsistent.
            val replacedPath = Path.getPathWithoutSchemeAndAuthority(rootPath);
            val replacedValue = fs.makeQualified(replacedPath).toString();
            return getAllFilePath(p, fs).parallelStream().map(path -> {
                String replaced = path.toString().replace(replacedValue + "/", "");
                return pathWithoutJsonSuffix(replaced);
            }).collect(Collectors.toCollection(TreeSet::new));
        } catch (IOException e) {
            Throwables.throwIfUnchecked(e);
            throw new KylinRuntimeException(e);
        }
    }

    protected RawResource loadOne(FileStatus status, Path parentPath)
            throws IOException, InstantiationException, IllegalAccessException {
        Path p = status.getPath();
        if (status.getLen() == 0) {
            log.warn("Zero length file: " + p.toString());
        }

        // if you set kylin.env.engine-write-fs, the schema may be inconsistent.
        val replacedPath = Path.getPathWithoutSchemeAndAuthority(parentPath);
        val replacedValue = fs.makeQualified(replacedPath).toString();
        String replaced = p.toString().replace(replacedValue, "");
        if (verifyNonMetadataFile(replaced)) {
            return null;
        }

        val srcPair = splitFilePath(replaced);
        RawResource res;
        byte[] byteArray;
        val resourceClass = srcPair.getKey().getResourceClass();
        long ts = status.getModificationTime();
        try (val in = fs.open(p)) {
            String metaKey = srcPair.getValue().substring(0, srcPair.getValue().length() - 5);
            byteArray = IOUtils.toByteArray(in);
            try {
                res = JsonUtil.readValue(byteArray, resourceClass);
            } catch (Exception e) {
                log.warn("Failed to load resource from file: " + p + ". This json file is broken!");
                res = resourceClass.newInstance();
            }
            if (res.getMetaKey() == null) {
                res.setMetaKey(metaKey);
            }
            res.setContent(byteArray);
            res.setMvcc(0);
            res.setTs(ts);
            return res;
        }
    }

    @Override
    public void dump(ResourceStore store) throws IOException, InterruptedException, ExecutionException {
        val resources = store.listResourcesRecursively(MetadataType.ALL.name());
        if (Type.ZIP.name().equals(this.type.name())) {
            dumpToZip(store, resources, new Path(this.rootPath, COMPRESSED_FILE));
        } else {
            dumpToFile(store, resources);
        }
    }

    /**
     * Dump metadata compressed file to FileSystem
     * @param store
     * @param resources
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void dump(ResourceStore store, Collection<String> resources) throws IOException, InterruptedException {
        val compressedFile = new Path(this.rootPath, COMPRESSED_FILE);
        dumpToZip(store, resources, compressedFile);
    }

    public void dumpToFile(ResourceStore resourceStore, Collection<String> resources)
            throws ExecutionException, InterruptedException {
        dumpKylinConfigToFile(resourceStore.getConfig());

        if (resources == null || resources.isEmpty()) {
            log.info("there is no resources to dump, please check.");
            return;
        }
        if (fileSystemMetadataExecutor == null || resources.size() < DEFAULT_FILE_NUMBER) {
            try {
                super.dump(resourceStore, resources);
            } catch (Exception e) {
                throw new KylinRuntimeException(e);
            }
        } else {
            List<String> batchResources = new ArrayList<>(DEFAULT_FILE_NUMBER);
            List<Future<Boolean>> futures = Lists.newArrayList();

            for (String resPath : resources) {
                batchResources.add(resPath);
                if (batchResources.size() >= DEFAULT_FILE_NUMBER) {
                    futures.addAll(batchInsertByResources(batchResources, resourceStore));
                    batchResources = new ArrayList<>(DEFAULT_FILE_NUMBER);
                }
            } // end for resources

            if (!batchResources.isEmpty()) {
                futures.addAll(batchInsertByResources(batchResources, resourceStore));
            }

            if (!futures.isEmpty()) {
                for (Future<Boolean> task : futures) {
                    task.get();
                }
            }
        } // end else
    }

    /**
     * Dump metadata compressed file to FileSystem
     *
     * @param store          Current ResourceStore
     * @param resources      The resources to be dumped
     * @param compressedFile The path of the compressed file
     * @throws IOException
     * @throws InterruptedException
     */
    public void dumpToZip(ResourceStore store, Collection<String> resources, Path compressedFile)
            throws IOException, InterruptedException {
        if (resources != null && !resources.isEmpty()) {
            try (FSDataOutputStream out = fs.create(compressedFile, true);
                    ZipOutputStream zipOut = new ZipOutputStream(new CheckedOutputStream(out, new CRC32()))) {
                for (String resPath : resources) {
                    val raw = store.getResource(resPath);
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    if (raw == null) {
                        continue;
                    }
                    compress(zipOut, raw, resPath);
                }
            } catch (IOException e) {
                throw new IOException("Put compressed resource fail", e);
            }
        } else {
            log.info("there is no resources, please check.");
        }
    }

    private List<Future<Boolean>> batchInsertByResources(List<String> batchResources, ResourceStore resourceStore) {
        List<Future<Boolean>> taskList = Lists.newArrayList();
        final List<String> innerBatchResources = batchResources;
        taskList.add(fileSystemMetadataExecutor.submit(() -> {
            for (String resource : innerBatchResources) {
                val raw = resourceStore.getResource(resource);
                save(raw.getMetaType(), raw);
            }
            innerBatchResources.clear();
            return true;
        }));
        return taskList;
    }

    @Override
    public MemoryMetaData reloadAll() throws IOException {
        val compressedFile = getRealFileSystemPath(COMPRESSED_FILE);
        if (!fs.exists(compressedFile) || !fs.isFile(compressedFile)) {
            return getAllFile(rootPath);
        }
        log.info("reloadAll from metadata.zip");
        MemoryMetaData data = MemoryMetaData.createEmpty();
        getCompressedFiles().forEach((resPath, raw) -> data.put(raw.getMetaType(), new VersionedRawResource(raw)));
        return data;
    }

    private void compress(ZipOutputStream out, RawResource raw, String resPath) throws IOException {
        ZipEntry entry = new ZipEntry(resPath + JSON_SUFFIX);
        entry.setTime(raw.getTs());
        out.putNextEntry(entry);
        compressHandlerInterface.write(out, raw);
    }

    public static Pair<MetadataType, String> splitFilePath(String resourcePath) {
        if ("/".equals(resourcePath)) {
            return new Pair<>(MetadataType.ALL, null);
        } else if (resourcePath.startsWith("/") && resourcePath.length() > 1) {
            resourcePath = resourcePath.substring(1);
        }
        String[] split = resourcePath.split("/", 2);
        if (split.length < 2) {
            throw new KylinRuntimeException("resourcePath is invalid: " + resourcePath);
        }
        String typeStr = split[0].toUpperCase(Locale.ROOT);
        return new Pair<>(MetadataType.create(typeStr), split[1]);
    }

    @VisibleForTesting
    protected Path getRealFileSystemPath(String resourcePath) {
        if (resourcePath.equals("/")) {
            return this.rootPath;
        } else if (resourcePath.endsWith(".zip")) {
            return new Path(this.rootPath, resourcePath);
        } else if (resourcePath.startsWith("/") && resourcePath.length() > 1) {
            resourcePath = resourcePath.substring(1);
        }
        return new Path(this.rootPath, resourcePath);
    }

    public static TreeSet<Path> getAllFilePath(Path filePath, FileSystem fs) {
        try {
            TreeSet<Path> fileList = Sets.newTreeSet();
            Arrays.stream(fs.listStatus(filePath)).forEach(status -> getAllFilePath(status.getPath(), fs, fileList));
            return fileList;
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new KylinRuntimeException(e);
        }
    }

    MemoryMetaData getAllFile(Path filePath) {
        Date startTime = new Date();
        MemoryMetaData data = MemoryMetaData.createEmpty();

        // Extract duplicate code, load data to MemoryMetaData through FileStatus
        BiConsumer<FileStatus, MemoryMetaData> loadMetadataProcess = (innerStat, dataHelper) -> {
            RawResource innerRaw;
            try {
                innerRaw = loadOne(innerStat, filePath);
                if (innerRaw != null) {
                    dataHelper.put(innerRaw.getMetaType(), new VersionedRawResource(innerRaw));
                }
            } catch (IOException | InstantiationException | IllegalAccessException e) {
                Throwables.throwIfUnchecked(e);
            }
        };

        try {
            FileStatus[] fileStatuses = fs.listStatus(filePath);
            log.info("getAllFile from {} started", filePath);
            List<Future<?>> futures = new ArrayList<>();
            for (FileStatus childStatus : fileStatuses) {
                getAndPutAllFileRecursion(childStatus, fs, data, futures, loadMetadataProcess);
            }

            // wait for fileSystemMetadataExecutor to finish
            for (Future<?> future : futures) {
                Uninterruptibles.getUninterruptibly(future);
            }

            log.info("getAllFile cost {} ms", new Date().getTime() - startTime.getTime());
            return data;
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new KylinRuntimeException(e);
        }
    }

    private static void getAllFilePath(Path filePath, FileSystem fs, TreeSet<Path> fileList) {
        try {
            FileStatus[] files = fs.listStatus(filePath);
            for (FileStatus file : files) {
                if (file.isDirectory()) {
                    getAllFilePath(file.getPath(), fs, fileList);
                } else {
                    fileList.add(file.getPath());
                }
            }
        } catch (IOException e) {
            Throwables.throwIfUnchecked(e);
        }
    }

    public void getAndPutAllFileRecursion(FileStatus status, FileSystem fs, MemoryMetaData data, List<Future<?>> futures,
            BiConsumer<FileStatus, MemoryMetaData> process) {
        try {
            for (FileStatus childStatus : fs.listStatus(status.getPath())) {
                if (childStatus.isDirectory()) {
                    getAndPutAllFileRecursion(childStatus, fs, data, futures, process);
                } else {
                    if (fileSystemMetadataExecutor != null) {
                        futures.add(fileSystemMetadataExecutor.submit(() -> process.accept(childStatus, data)));
                    } else {
                        process.accept(childStatus, data);
                    }

                }
            }
        } catch (IOException e) {
            Throwables.throwIfUnchecked(e);
        }
    }

    private List<String> getAllFilePathFromCompressedFiles() {
        return getCompressedFiles().keySet().stream().map(FileSystemMetadataStore::pathWithoutJsonSuffix)
                .collect(Collectors.toList());
    }

    private void createMetaFolderIfNeed(Path metaDirName) {
        //create hdfs meta path
        try {
            if (!fs.exists(metaDirName)) {
                fs.mkdirs(metaDirName);
            }
        } catch (IOException e) {
            Throwables.throwIfUnchecked(e);
        }
    }

    @Getter(lazy = true)
    private final Map<String, RawResource> compressedFiles = getFilesFromCompressedFile();

    private Map<String, RawResource> getFilesFromCompressedFile() {
        val compressedFile = getRealFileSystemPath(COMPRESSED_FILE);
        return getFilesFromCompressedFile(compressedFile, compressHandlerInterface, fs);
    }

    public static Map<String, RawResource> getFilesFromCompressedFile(Path compressedFile,
            CompressHandlerInterface handler, FileSystem fs) {
        try {
            if (fs == null || !fs.exists(compressedFile) || !fs.isFile(compressedFile)) {
                return Maps.newHashMap();
            }
        } catch (IOException e) {
            log.warn("Check file failed. ", e);
            return Maps.newHashMap();
        }
        try {
            return getFilesFromCompressedFileByStream(fs.open(compressedFile), handler);
        } catch (IOException e) {
            log.warn("Get file from compressed file error", e);
            return Maps.newHashMap();
        }
    }

    public static Map<String, RawResource> getFilesFromCompressedFileByStream(InputStream stream,
            CompressHandlerInterface handler) {
        val res = Maps.<String, RawResource> newHashMap();
        Preconditions.checkNotNull(handler, "compress handler should not be null!");
        try (InputStream in = stream; ZipInputStream zipIn = new ZipInputStream(in)) {
            ZipEntry zipEntry;
            while ((zipEntry = zipIn.getNextEntry()) != null) {
                String path = zipEntry.getName();
                if (path.startsWith("/")) {
                    path = path.substring(1);
                }
                val raw = handler.read(zipIn, path, zipEntry.getTime(), splitFilePath(path).getKey());
                if (raw != null) {
                    res.put(path, raw);
                }
            }
            return res;
        } catch (Exception e) {
            log.warn("get file from compressed file error", e);
        }
        return Maps.newHashMap();
    }

    private boolean compressedFilesContains(String path) {
        if (File.separator.equals(path)) {
            return !getCompressedFiles().isEmpty();
        }
        return getCompressedFiles().keySet().stream()
                .anyMatch(file -> file.startsWith(path + "/") || file.equals(path));
    }

    private static String pathWithoutJsonSuffix(String path) {
        if (path.endsWith(JSON_SUFFIX)) {
            path = path.substring(0, path.length() - JSON_SUFFIX.length());
        }
        return path;
    }

    @Override
    public TransactionStatus getTransaction() throws TransactionException {
        return new SimpleTransactionStatus();
    }

    @Override
    public void commit(TransactionStatus status) throws TransactionException {
        releaseOwnedLocks();
    }

    @Override
    public void rollback(TransactionStatus status) throws TransactionException {
        KylinConfig conf = KylinConfig.readSystemKylinConfig();
        InMemResourceStore mem = (InMemResourceStore) ResourceStore.getKylinMetaStore(conf);
        Set<String> changesResource = UnitOfWork.get().getCopyForWriteItems();
        for (String resPath : changesResource) {
            RawResource raw = mem.getResource(resPath);
            Pair<MetadataType, String> typeAndKey = MetadataType.splitKeyWithType(resPath);
            if (raw == null) {
                raw = RawResource.constructResource(typeAndKey.getFirst(), null, 0, -1, typeAndKey.getSecond());
            }
            save(typeAndKey.getFirst(), raw);
            log.info("Rollback metadata for FileSystemMetadataStore: {}", resPath);
        }
        releaseOwnedLocks();
    }

    private void lockResource(String lockPath) {
        ReentrantLock lock = LOCK_MAP.computeIfAbsent(lockPath, k -> new ReentrantLock());
        log.debug("LOCK: try to lock path {}", lockPath);
        if (OWNED_LOCKS.get().contains(lock)) {
            log.debug("LOCK: already locked {}", lockPath);
        } else {
            try {
                if (!lock.tryLock(1, TimeUnit.MINUTES)) {
                    log.debug("LOCK: failed to lock for " + lockPath);
                    throw new LockTimeoutException(lockPath);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KylinRuntimeException(e);
            }
            log.debug("LOCK: locked {}", lockPath);
            OWNED_LOCKS.get().add(lock);
        }
    }

    private void releaseOwnedLocks() {
        OWNED_LOCKS.get().forEach(ReentrantLock::unlock);
        log.debug("LOCK: release lock count {}", OWNED_LOCKS.get().size());
        OWNED_LOCKS.get().clear();
    }

    public interface CompressHandlerInterface {
        <T extends RawResource> T read(InputStream in, String resPath, long time, MetadataType type) throws IOException;

        void write(OutputStream out, RawResource raw) throws IOException;
    }

    @VisibleForTesting
    public static class CompressHandler implements CompressHandlerInterface {
        @Override
        @SuppressWarnings("unchecked")
        public <T extends RawResource> T read(InputStream in, String resPath, long time, MetadataType type)
                throws IOException {
            val raw = IOUtils.toByteArray(in);
            String name = resPath.substring(resPath.lastIndexOf("/") + 1);
            String metaKey = pathWithoutJsonSuffix(name);
            return (T) RawResource.constructResource(type, ByteSource.wrap(raw), time, 0, metaKey);
        }

        @Override
        public void write(OutputStream out, RawResource raw) throws IOException {
            try (InputStream inputStream = ByteSource.wrap(raw.getContent()).openStream()) {
                IOUtils.copy(inputStream, out);
            }
        }
    }

    public static class SnapShotCompressHandler implements CompressHandlerInterface {
        @Override
        @SuppressWarnings("unchecked")
        public <T extends RawResource> T read(InputStream in, String resPath, long time, MetadataType type)
                throws IOException {
            val snap = JsonUtil.readValue(IOUtils.toByteArray(in), SnapshotRawResource.class);
            String name = resPath.substring(resPath.lastIndexOf("/") + 1);
            return (T) RawResource.constructResource(type, snap.getByteSource(), snap.getTimestamp(), snap.getMvcc(),
                    pathWithoutJsonSuffix(name));
        }

        @Override
        public void write(OutputStream out, RawResource raw) throws IOException {
            val snapshotRawResource = new SnapshotRawResource(raw);
            out.write(JsonUtil.writeValueAsIndentBytes(snapshotRawResource));
        }
    }
}
