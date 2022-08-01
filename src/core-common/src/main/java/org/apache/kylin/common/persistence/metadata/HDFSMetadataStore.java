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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.SnapshotRawResource;
import org.apache.kylin.common.persistence.VersionedRawResource;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.guava20.shaded.common.base.Throwables;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HDFSMetadataStore extends MetadataStore {

    public static final String HDFS_SCHEME = "hdfs";
    private static final String COMPRESSED_FILE = "metadata.zip";

    private final Path rootPath;
    private final FileSystem fs;

    private enum Type {
        DIR, ZIP
    }

    private final Type type;

    private final CompressHandlerInterface compressHandlerInterface;

    public HDFSMetadataStore(KylinConfig kylinConfig) throws IOException {
        super(kylinConfig);
        try {
            val storageUrl = kylinConfig.getMetadataUrl();
            Preconditions.checkState(HDFS_SCHEME.equals(storageUrl.getScheme()));
            type = storageUrl.getParameter("zip") != null ? Type.ZIP : Type.DIR;
            compressHandlerInterface = storageUrl.getParameter("snapshot") != null ? new SnapShotCompressHandler()
                    : new CompressHandler();
            String path = storageUrl.getParameter("path");
            if (path == null) {
                path = HadoopUtil.getBackupFolder(kylinConfig);
                fs = HadoopUtil.getWorkingFileSystem();
                if (!fs.exists(new Path(path))) {
                    fs.mkdirs(new Path(path));
                }
                rootPath = Stream.of(fs.listStatus(new Path(path)))
                        .max(Comparator.comparing(FileStatus::getModificationTime)).map(FileStatus::getPath)
                        .orElse(new Path(path + "/backup_0/"));
                if (!fs.exists(rootPath)) {
                    fs.mkdirs(rootPath);
                }
            } else {
                Path tempPath = new Path(path);
                if (tempPath.toUri().getScheme() != null) {
                    fs = HadoopUtil.getWorkingFileSystem(tempPath);
                    rootPath = tempPath;
                } else {
                    fs = HadoopUtil.getWorkingFileSystem();
                    rootPath = fs.makeQualified(tempPath);
                }
            }

            if (!fs.exists(rootPath)) {
                log.warn("Path not exist in HDFS, create it: {}", path);
                createMetaFolder(rootPath);
            }

            log.info("The FileSystem location is {}, hdfs root path : {}", fs.getUri().toString(), rootPath.toString());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void save(String resPath, ByteSource bs, long ts, long mvcc, String unitPath, long epochId)
            throws Exception {
        log.trace("res path : {}", resPath);
        Path p = getRealHDFSPath(resPath);
        if (bs == null) {
            fs.delete(p, true);
            return;
        }
        FSDataOutputStream out = null;
        try {
            out = fs.create(p, true);
            IOUtils.copy(bs.openStream(), out);
            fs.setTimes(p, ts, -1);
        } catch (Exception e) {
            throw new IOException("Put resource fail", e);
        } finally {
            IOUtils.closeQuietly(out);
        }
        final FileStatus fileStatus = fs.getFileStatus(p);
        if (bs.size() != fileStatus.getLen()) {
            throw new IOException(
                    "Put resource fail : " + resPath + ", because resource file length not equal with ByteSource");
        }
        if (fileStatus.getLen() == 0) {
            throw new IOException("Put resource fail : " + resPath + ", because resource file is Zero length");
        }
    }

    @Override
    public void move(String srcPath, String destPath) throws Exception {
        log.trace("res path : {}", srcPath);
        Path srcHDFSPath = getRealHDFSPath(srcPath);
        if (!fs.exists(srcHDFSPath)) {
            return;
        }
        Path destHDFSPath = getRealHDFSPath(destPath);
        FSDataOutputStream out = null;
        try {
            fs.rename(srcHDFSPath, destHDFSPath);
        } catch (Exception e) {
            throw new IOException("rename resource fail", e);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    @Override
    public NavigableSet<String> list(String resPath) {
        try {
            if (compressedFilesContains(resPath)) {
                return Sets.newTreeSet(getAllFilePathFromCompressedFiles(resPath));
            }
            Path p = getRealHDFSPath(resPath);
            if (!fs.exists(p) || !fs.isDirectory(p)) {
                log.warn("path {} does not exist in HDFS", resPath);
                return new TreeSet<>();
            }
            TreeSet<String> r;

            r = getAllFilePath(p);
            return r.isEmpty() ? new TreeSet<>() : r;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public RawResource load(String resPath) throws IOException {
        if (getCompressedFiles().containsKey(resPath)) {
            return getCompressedFiles().get(resPath);
        }
        Path p = getRealHDFSPath(resPath);
        if (fs.exists(p) && fs.isFile(p)) {
            if (fs.getFileStatus(p).getLen() == 0) {
                log.warn("Zero length file: " + p.toString());
            }
            try (val in = fs.open(p)) {
                val bs = ByteSource.wrap(IOUtils.toByteArray(in));
                long t = fs.getFileStatus(p).getModificationTime();
                return new RawResource(resPath, bs, t, 0);
            }
        } else {
            throw new IOException("path " + p + " not found");
        }
    }

    @Override
    public void dump(ResourceStore store, String rootPath) throws Exception {
        if (type == Type.DIR) {
            super.dump(store, rootPath);
            return;
        }
        val resources = store.listResourcesRecursively(rootPath);
        if (resources == null || resources.isEmpty()) {
            log.info("there is no resources in rootPath ({}),please check the rootPath.", rootPath);
            return;
        }
        dump(store, resources);
    }

    @Override
    public void dump(ResourceStore store, Collection<String> resources) throws Exception {
        val compressedFile = new Path(this.rootPath, COMPRESSED_FILE);
        try (FSDataOutputStream out = fs.create(compressedFile, true);
                ZipOutputStream zipOut = new ZipOutputStream(new CheckedOutputStream(out, new CRC32()))) {
            for (String resPath : resources) {
                val raw = store.getResource(resPath);
                compress(zipOut, raw);
            }
        } catch (Exception e) {
            throw new IOException("Put compressed resource fail", e);
        }
    }

    @Override
    public MemoryMetaData reloadAll() throws IOException {
        val compressedFile = getRealHDFSPath(COMPRESSED_FILE);
        if (!fs.exists(compressedFile) || !fs.isFile(compressedFile)) {
            return super.reloadAll();
        }
        log.info("reloadAll from metadata.zip");
        MemoryMetaData data = MemoryMetaData.createEmpty();
        getCompressedFiles().forEach((name, raw) -> data.put(name, new VersionedRawResource(raw)));
        return data;
    }

    private void compress(ZipOutputStream out, RawResource raw) throws IOException {
        ZipEntry entry = new ZipEntry(raw.getResPath());
        entry.setTime(raw.getTimestamp());
        out.putNextEntry(entry);
        compressHandlerInterface.write(out, raw);
    }

    private Path getRealHDFSPath(String resourcePath) {
        if (resourcePath.equals("/"))
            return this.rootPath;
        if (resourcePath.startsWith("/") && resourcePath.length() > 1)
            resourcePath = resourcePath.substring(1);
        return new Path(this.rootPath, resourcePath);
    }

    TreeSet<String> getAllFilePath(Path filePath) {
        try {
            TreeSet<String> fileList = new TreeSet<>();
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(filePath, true);
            // if you set kylin.env.engine-write-fs, the schema may be inconsistent.
            Path replacedPath = Path.getPathWithoutSchemeAndAuthority(filePath);
            String replacedValue = fs.makeQualified(replacedPath).toString();
            while (it.hasNext()) {
                fileList.add(it.next().getPath().toString().replace(replacedValue, ""));
            }
            return fileList;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private List<String> getAllFilePathFromCompressedFiles(String path) {
        if (File.separator.equals(path)) {
            return Lists.newArrayList(getCompressedFiles().keySet());
        }
        return getCompressedFiles().keySet().stream()
                .filter(file -> file.startsWith(path + File.separator) || file.equals(path))
                .map(file -> file.substring(path.length())).collect(Collectors.toList());
    }

    private void createMetaFolder(Path metaDirName) {
        //create hdfs meta path
        try {
            if (!fs.exists(metaDirName)) {
                fs.mkdirs(metaDirName);
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Getter(lazy = true)
    private final Map<String, RawResource> compressedFiles = getFilesFromCompressedFile();

    private Map<String, RawResource> getFilesFromCompressedFile() {
        Preconditions.checkNotNull(compressHandlerInterface, "compress handler should not be null!");
        val res = Maps.<String, RawResource> newHashMap();
        val compressedFile = getRealHDFSPath(COMPRESSED_FILE);
        try {
            if (!fs.exists(compressedFile) || !fs.isFile(compressedFile)) {
                return Maps.newHashMap();
            }
        } catch (IOException ignored) {
        }
        try (FSDataInputStream in = fs.open(compressedFile); ZipInputStream zipIn = new ZipInputStream(in);) {
            ZipEntry zipEntry = null;
            while ((zipEntry = zipIn.getNextEntry()) != null) {
                if (!zipEntry.getName().startsWith("/")) {
                    continue;
                }
                long t = zipEntry.getTime();
                val raw = compressHandlerInterface.read(zipIn, zipEntry.getName(), t);
                res.put(zipEntry.getName(), raw);
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

    private interface CompressHandlerInterface {
        RawResource read(InputStream in, String resPath, long time) throws IOException;

        void write(OutputStream out, RawResource raw) throws IOException;
    }

    private static class CompressHandler implements CompressHandlerInterface {
        @Override
        public RawResource read(InputStream in, String resPath, long time) throws IOException {
            val bs = ByteSource.wrap(IOUtils.toByteArray(in));
            return new RawResource(resPath, bs, time, 0);
        }

        @Override
        public void write(OutputStream out, RawResource raw) throws IOException {
            IOUtils.copy(raw.getByteSource().openStream(), out);
        }
    }

    private static class SnapShotCompressHandler implements CompressHandlerInterface {
        @Override
        public RawResource read(InputStream in, String resPath, long time) throws IOException {
            val snap = JsonUtil.readValue(IOUtils.toByteArray(in), SnapshotRawResource.class);
            return new RawResource(resPath, snap.getByteSource(), snap.getTimestamp(), snap.getMvcc());
        }

        @Override
        public void write(OutputStream out, RawResource raw) throws IOException {
            val snapshotRawResource = new SnapshotRawResource(raw);
            out.write(JsonUtil.writeValueAsIndentBytes(snapshotRawResource));
        }
    }
}
