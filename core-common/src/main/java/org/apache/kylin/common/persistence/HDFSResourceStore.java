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

package org.apache.kylin.common.persistence;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class HDFSResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(HDFSResourceStore.class);

    private Path hdfsMetaPath;

    private FileSystem fs;

    public static final String HDFS_SCHEME = "hdfs";

    public HDFSResourceStore(KylinConfig kylinConfig) throws Exception {
        this(kylinConfig, kylinConfig.getMetadataUrl());
    }

    public HDFSResourceStore(KylinConfig kylinConfig, StorageURL metadataUrl) throws Exception {
        super(kylinConfig);
        Preconditions.checkState(HDFS_SCHEME.equals(metadataUrl.getScheme()));

        String path = metadataUrl.getParameter("path");
        if (path == null) {
            // missing path is not expected, but don't fail it
            path = kylinConfig.getHdfsWorkingDirectory(null) + "tmp_metadata";
            logger.warn("Missing path, fall back to {}. ", path);
        }

        fs = HadoopUtil.getFileSystem(path);
        Path metadataPath = new Path(path);
        if (!fs.exists(metadataPath)) {
            logger.warn("Path not exist in HDFS, create it: {}. ", path);
            createMetaFolder(metadataPath);
        }

        hdfsMetaPath = metadataPath;
        logger.info("hdfs meta path : {}", hdfsMetaPath);

    }

    private void createMetaFolder(Path metaDirName) throws Exception {
        //create hdfs meta path
        if (!fs.exists(metaDirName)) {
            fs.mkdirs(metaDirName);
        }

        logger.info("hdfs meta path created: {}", metaDirName);
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        return listResourcesImpl(folderPath, false);
    }

    @Override
    protected NavigableSet<String> listResourcesRecursivelyImpl(String folderPath) throws IOException {
        return listResourcesImpl(folderPath, true);
    }

    private NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) throws IOException {
        Path p = getRealHDFSPath(folderPath);
        String prefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        if (!fs.exists(p) || !fs.isDirectory(p)) {
            return null;
        }
        TreeSet<String> r;

        if (recursive) {
            r = getAllFilePath(p, prefix);
        } else {
            r = getFilePath(p, prefix);
        }
        return r.isEmpty() ? null : r;
    }

    private TreeSet<String> getFilePath(Path p, String resPathPrefix) throws IOException {
        TreeSet<String> fileList = new TreeSet<>();
        for (FileStatus fileStat : fs.listStatus(p)) {
            fileList.add(resPathPrefix + fileStat.getPath().getName());
        }
        return fileList;
    }

    TreeSet<String> getAllFilePath(Path filePath, String resPathPrefix) throws IOException {
        String fsPathPrefix = filePath.toUri().getPath();

        TreeSet<String> fileList = new TreeSet<>();
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(filePath, true);
        while (it.hasNext()) {
            String path = it.next().getPath().toUri().getPath();
            if (!path.startsWith(fsPathPrefix))
                throw new IllegalStateException("File path " + path + " is supposed to start with " + fsPathPrefix);

            String resPath = resPathPrefix + path.substring(fsPathPrefix.length() + 1);
            fileList.add(resPath);
        }
        return fileList;
    }

    @Override
    protected void visitFolderImpl(String folderPath, boolean recursive, VisitFilter filter, boolean loadContent,
            Visitor visitor) throws IOException {
        Path p = getRealHDFSPath(folderPath);
        if (!fs.exists(p) || !fs.isDirectory(p)) {
            return;
        }

        String fsPathPrefix = p.toUri().getPath();
        String resPathPrefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";

        RemoteIterator<LocatedFileStatus> it = fs.listFiles(p, recursive);
        while (it.hasNext()) {
            LocatedFileStatus status = it.next();
            if (status.isDirectory())
                continue;

            String path = status.getPath().toUri().getPath();
            if (!path.startsWith(fsPathPrefix))
                throw new IllegalStateException("File path " + path + " is supposed to start with " + fsPathPrefix);

            String resPath = resPathPrefix + path.substring(fsPathPrefix.length() + 1);

            if (filter.matches(resPath, status.getModificationTime())) {
                RawResource raw;
                if (loadContent)
                    raw = new RawResource(resPath, status.getModificationTime(), fs.open(status.getPath()));
                else
                    raw = new RawResource(resPath, status.getModificationTime());

                try {
                    visitor.visit(raw);
                } finally {
                    raw.close();
                }
            }
        }
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        Path p = getRealHDFSPath(resPath);
        return fs.exists(p) && fs.isFile(p);
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        Path p = getRealHDFSPath(resPath);
        if (fs.exists(p) && fs.isFile(p)) {
            FileStatus fileStatus = fs.getFileStatus(p);
            if (fileStatus.getLen() == 0) {
                logger.warn("Zero length file: {}. ", p);
            }
            FSDataInputStream in = fs.open(p);
            long ts = fileStatus.getModificationTime();
            return new RawResource(resPath, ts, in);
        } else {
            return null;
        }
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        Path p = getRealHDFSPath(resPath);
        if (!fs.exists(p) || !fs.isFile(p)) {
            return 0;
        }
        try {
            return fs.getFileStatus(p).getModificationTime();
        } catch (Exception e) {
            throw new IOException("Put resource fail", e);
        }

    }

    @Override
    protected void putResourceImpl(String resPath, ContentWriter content, long ts) throws IOException {
        logger.trace("res path : {}. ", resPath);
        Path p = getRealHDFSPath(resPath);
        logger.trace("put resource : {}. ", p.toUri());
        FSDataOutputStream out = null;
        try {
            out = fs.create(p, true);
            content.write(out);
        } catch (Exception e) {
            throw new IOException("Put resource fail", e);
        } finally {
            IOUtils.closeQuietly(out);
            fs.setTimes(p, ts, -1);
        }
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, WriteConflictException {
        Path p = getRealHDFSPath(resPath);
        if (!fs.exists(p)) {
            if (oldTS != 0) {
                throw new IllegalStateException(
                        "For not exist file. OldTS have to be 0. but Actual oldTS is : " + oldTS);
            }

        } else {
            long realLastModify = getResourceTimestamp(resPath);
            if (realLastModify != oldTS) {
                throw new WriteConflictException("Overwriting conflict " + resPath + ", expect old TS " + oldTS
                        + ", but found " + realLastModify);
            }
        }
        putResourceImpl(resPath, ContentWriter.create(content), newTS);
        return newTS;
    }

    @Override
    protected void updateTimestampImpl(String resPath, long timestamp) throws IOException {
        try {
            Path p = getRealHDFSPath(resPath);
            if (fs.exists(p)) {
                fs.setTimes(p, timestamp, -1);
            }
        } catch (Exception e) {
            throw new IOException("Update resource timestamp fail", e);
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        try {
            Path p = getRealHDFSPath(resPath);
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
        } catch (Exception e) {
            throw new IOException("Delete resource fail", e);
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath, long timestamp) throws IOException {
        try {
            Path p = getRealHDFSPath(resPath);
            if (fs.exists(p)) {
                long origLastModified = fs.getFileStatus(p).getModificationTime();
                if (checkTimeStampBeforeDelete(origLastModified, timestamp)) {
                    fs.delete(p, true);
                } else {
                    throw new IOException("Resource " + resPath + " timestamp not match, [originLastModified: "
                            + origLastModified + ", timestampToDelete: " + timestamp + "]");
                }

            }
        } catch (Exception e) {
            throw new IOException("Delete resource fail", e);
        }
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return getRealHDFSPath(resPath).toString();
    }

    private Path getRealHDFSPath(String resourcePath) {
        if (resourcePath.equals("/"))
            return this.hdfsMetaPath;
        if (resourcePath.startsWith("/") && resourcePath.length() > 1)
            resourcePath = resourcePath.substring(1, resourcePath.length());
        return new Path(this.hdfsMetaPath, resourcePath);
    }
}
