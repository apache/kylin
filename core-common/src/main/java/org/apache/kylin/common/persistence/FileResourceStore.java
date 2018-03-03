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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class FileResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(FileResourceStore.class);

    File root;

    public FileResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
        root = new File(getPath(kylinConfig)).getAbsoluteFile();
        if (root.exists() == false)
            throw new IllegalArgumentException(
                    "File not exist by '" + kylinConfig.getMetadataUrl() + "': " + root.getAbsolutePath());
    }

    protected String getPath(KylinConfig kylinConfig) {
        return kylinConfig.getMetadataUrl().getIdentifier();
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) throws IOException {
        synchronized (FileResourceStore.class) {
            TreeSet<String> r = new TreeSet<>();
            File file = file(folderPath);
            String[] names = file.list();
            // not a directory
            if (names == null)
                // fixme should return empty set, like HBase implement.
                return null;
            String prefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
            if (recursive) {
                Collection<File> files = FileUtils.listFiles(file, null, true);
                for (File f : files) {
                    String path = f.getAbsolutePath();
                    String[] split = path.split(prefix);
                    Preconditions.checkArgument(split.length == 2);
                    r.add(prefix + split[1]);
                }
            } else {
                for (String n : names) {
                    r.add(prefix + n);
                }
            }
            return r;
        }
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        synchronized (FileResourceStore.class) {
            File f = file(resPath);
            return f.exists() && f.isFile(); // directory is not considered a resource
        }
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive)
            throws IOException {
        synchronized (FileResourceStore.class) {

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
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        synchronized (FileResourceStore.class) {

            File f = file(resPath);
            if (f.exists() && f.isFile()) {
                if (f.length() == 0) {
                    logger.warn("Zero length file: " + f.getAbsolutePath());
                }

                FileInputStream resource = new FileInputStream(f);
                ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
                IOUtils.copy(resource, baos);
                IOUtils.closeQuietly(resource);
                byte[] data = baos.toByteArray();

                return new RawResource(new ByteArrayInputStream(data), f.lastModified());
            } else {
                return null;
            }
        }
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        synchronized (FileResourceStore.class) {

            File f = file(resPath);
            if (f.exists() && f.isFile())
                return f.lastModified();
            else
                return 0;
        }
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        synchronized (FileResourceStore.class) {

            File f = file(resPath);
            f.getParentFile().mkdirs();
            FileOutputStream out = new FileOutputStream(f);
            try {
                IOUtils.copy(content, out);
            } finally {
                IOUtils.closeQuietly(out);
            }

            f.setLastModified(ts);
        }
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, IllegalStateException {
        synchronized (FileResourceStore.class) {

            File f = file(resPath);
            if ((f.exists() && f.lastModified() != oldTS) || (f.exists() == false && oldTS != 0))
                throw new IllegalStateException("Overwriting conflict " + resPath + ", expect old TS " + oldTS
                        + ", but found " + f.lastModified());

            putResourceImpl(resPath, new ByteArrayInputStream(content), newTS);

            // some FS lose precision on given time stamp
            return f.lastModified();
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        synchronized (FileResourceStore.class) {

            File f = file(resPath);
            f.delete();
        }
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        synchronized (FileResourceStore.class) {
            return file(resPath).toString();
        }
    }

    private File file(String resPath) {
        if (resPath.equals("/"))
            return root;
        else
            return new File(root, resPath);
    }

    @Override
    public String toString() {
        return root.getAbsolutePath();
    }
}
