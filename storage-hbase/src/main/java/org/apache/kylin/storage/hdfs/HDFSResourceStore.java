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

package org.apache.kylin.storage.hdfs;

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
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HDFSResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(HDFSResourceStore.class);

    private Path hdfsMetaPath;

    private FileSystem fs;

    public HDFSResourceStore(KylinConfig kylinConfig) throws Exception {
        super(kylinConfig);
        String metadataUrl = kylinConfig.getMetadataUrl();
        int cut = metadataUrl.indexOf('@');
        if (cut < 0) {
            throw new IOException("kylin.metadata.url not recognized for HDFSResourceStore: " + metadataUrl);
        }
        String suffix = metadataUrl.substring(cut + 1);
        if (!suffix.equals("hdfs"))
            throw new IOException("kylin.metadata.url not recognized for HDFSResourceStore:" + suffix);

        String path = metadataUrl.substring(0, cut);
        fs = HadoopUtil.getFileSystem(path);
        Path metadataPath = new Path(path);
        if (fs.exists(metadataPath) == false) {
            logger.warn("Path not exist in HDFS, create it: " + path);
            createMetaFolder(metadataPath, kylinConfig);
        }

        hdfsMetaPath = metadataPath;
        logger.info("hdfs meta path : " + hdfsMetaPath.toString());

    }

    private void createMetaFolder(Path metaDirName, KylinConfig kylinConfig) throws Exception {
        //create hdfs meta path
        if (!fs.exists(metaDirName)) {
            fs.mkdirs(metaDirName);
        }

        logger.info("hdfs meta path created: " + metaDirName.toString());
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        Path p = getRealHDFSPath(folderPath);
        if (!fs.exists(p) || !fs.isDirectory(p)) {
            return null;
        }
        TreeSet<String> r = new TreeSet<>();
        FileStatus[] statuses = fs.listStatus(p);
        String prefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        for (FileStatus status : statuses) {
            r.add(prefix + status.getPath().getName());
        }
        return r;
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
        logger.info("res path : " + resPath);
        Path p = getRealHDFSPath(resPath);
        logger.info("put resource : " + p.toUri());
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
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException {
        Path p = getRealHDFSPath(resPath);
        if (!fs.exists(p)) {
            if (oldTS != 0) {
                throw new IllegalStateException("For not exist file. OldTS have to be 0. but Actual oldTS is : " + oldTS);
            }

        } else {
            long realLastModify = getResourceTimestamp(resPath);
            if (realLastModify != oldTS) {
                throw new IllegalStateException("Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but found " + realLastModify);
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

    private static String getRelativePath(Path hdfsPath) {
        String path = hdfsPath.toString();
        int index = path.indexOf("://");
        if (index > 0) {
            path = path.substring(index + 3);
        }

        if (path.startsWith("/") == false) {
            if (path.indexOf("/") > 0) {
                path = path.substring(path.indexOf("/"));
            } else {
                path = "/" + path;
            }
        }
        return path;
    }

}
