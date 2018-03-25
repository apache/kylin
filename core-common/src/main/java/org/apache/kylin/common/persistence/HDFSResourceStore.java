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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class HDFSResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(HDFSResourceStore.class);

    private Path hdfsMetaPath;

    private FileSystem fs;

    private static final String HDFS_SCHEME = "hdfs";

    public HDFSResourceStore(KylinConfig kylinConfig) throws Exception {
        this(kylinConfig, kylinConfig.getMetadataUrl());
    }
    
    public HDFSResourceStore(KylinConfig kylinConfig, StorageURL metadataUrl) throws Exception {
        super(kylinConfig);
        Preconditions.checkState(HDFS_SCHEME.equals(metadataUrl.getScheme()));
        
        String path = metadataUrl.getParameter("path");
        if (path == null) {
            // missing path is not expected, but don't fail it
            path = kylinConfig.getHdfsWorkingDirectory() + "tmp_metadata";
            logger.warn("Missing path, fall back to " + path);
        }
        
        fs = HadoopUtil.getFileSystem(path);
        Path metadataPath = new Path(path);
        if (fs.exists(metadataPath) == false) {
            logger.warn("Path not exist in HDFS, create it: " + path);
            createMetaFolder(metadataPath);
        }

        hdfsMetaPath = metadataPath;
        logger.info("hdfs meta path : " + hdfsMetaPath.toString());

    }

    private void createMetaFolder(Path metaDirName) throws Exception {
        //create hdfs meta path
        if (!fs.exists(metaDirName)) {
            fs.mkdirs(metaDirName);
        }

        logger.info("hdfs meta path created: " + metaDirName.toString());
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) throws IOException {
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

    private TreeSet<String> getFilePath(Path p, String prefix) throws IOException {
        TreeSet<String> fileList = new TreeSet<>();
        for (FileStatus fileStat : fs.listStatus(p)) {
            fileList.add(prefix + fileStat.getPath().getName());
        }
        return fileList;
    }

    TreeSet<String> getAllFilePath(Path filePath, String prefix) throws IOException {
        TreeSet<String> fileList = new TreeSet<>();
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(filePath, true);
        while (it.hasNext()) {
            String[] path = it.next().getPath().toString().split(prefix, 2);
            fileList.add(prefix + path[1]);
        }
        return fileList;
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        Path p = getRealHDFSPath(resPath);
        return fs.exists(p) && fs.isFile(p);
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException {
        NavigableSet<String> resources = listResources(folderPath);
        if (resources == null)
            return Collections.emptyList();
        List<RawResource> result = Lists.newArrayListWithCapacity(resources.size());
        try {
            for (String res : resources) {
                long ts = getResourceTimestampImpl(res);
                if (timeStart <= ts && ts < timeEndExclusive) {
                    RawResource resource = getResourceImpl(res);
                    if (resource != null) // can be null if is a sub-folder
                        result.add(resource);
                }
            }
        } catch (IOException ex) {
            for (RawResource rawResource : result) {
                IOUtils.closeQuietly(rawResource.inputStream);
            }
            throw ex;
        }
        return result;
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        Path p = getRealHDFSPath(resPath);
        if (fs.exists(p) && fs.isFile(p)) {
            if (fs.getFileStatus(p).getLen() == 0) {
                logger.warn("Zero length file: " + p.toString());
            }
            FSDataInputStream in = fs.open(p);
            long t = in.readLong();
            return new RawResource(in, t);
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
        FSDataInputStream in = null;
        try {
            in = fs.open(p);
            long t = in.readLong();
            return t;
        } catch (Exception e) {
            throw new IOException("Put resource fail", e);
        } finally {
            IOUtils.closeQuietly(in);
        }

    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        logger.trace("res path : " + resPath);
        Path p = getRealHDFSPath(resPath);
        logger.trace("put resource : " + p.toUri());
        FSDataOutputStream out = null;
        try {
            out = fs.create(p, true);
            out.writeLong(ts);
            IOUtils.copy(content, out);

        } catch (Exception e) {
            throw new IOException("Put resource fail", e);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, WriteConflictException {
        Path p = getRealHDFSPath(resPath);
        if (!fs.exists(p)) {
            if (oldTS != 0) {
                throw new IllegalStateException("For not exist file. OldTS have to be 0. but Actual oldTS is : " + oldTS);
            }

        } else {
            long realLastModify = getResourceTimestamp(resPath);
            if (realLastModify != oldTS) {
                throw new WriteConflictException("Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but found " + realLastModify);
            }
        }
        putResourceImpl(resPath, new ByteArrayInputStream(content), newTS);
        return newTS;
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
