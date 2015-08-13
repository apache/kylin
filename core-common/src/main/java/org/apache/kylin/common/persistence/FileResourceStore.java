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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;

public class FileResourceStore extends ResourceStore {

    File root;

    public FileResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
        root = new File(kylinConfig.getMetadataUrl()).getAbsoluteFile();
        if (root.exists() == false)
            throw new IllegalArgumentException("File not exist by '" + kylinConfig.getMetadataUrl() + "': " + root.getAbsolutePath());
    }

    @Override
    protected ArrayList<String> listResourcesImpl(String resPath) throws IOException {
        String[] names = file(resPath).list();
        if (names == null) // not a directory
            return null;

        ArrayList<String> r = new ArrayList<String>(names.length);
        String prefix = resPath.endsWith("/") ? resPath : resPath + "/";
        for (String n : names) {
            r.add(prefix + n);
        }
        return r;
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        File f = file(resPath);
        return f.exists() && f.isFile(); // directory is not considered a
                                         // resource
    }

    @Override
    protected List<RawResource> getAllResources(String rangeStart, String rangeEnd) throws IOException {
        List<RawResource> result = Lists.newArrayList();
        try {
            String commonPrefix = StringUtils.getCommonPrefix(rangeEnd, rangeStart);
            commonPrefix = commonPrefix.substring(0, commonPrefix.lastIndexOf("/") + 1);
            final ArrayList<String> resources = listResourcesImpl(commonPrefix);
            for (String resource : resources) {
                if (resource.compareTo(rangeStart) >= 0 && resource.compareTo(rangeEnd) <= 0) {
                    if (existsImpl(resource)) {
                        result.add(new RawResource(getResourceImpl(resource), getResourceTimestampImpl(resource)));
                    }
                }
            }
            return result;
        } catch (IOException ex) {
            for (RawResource rawResource : result) {
                IOUtils.closeQuietly(rawResource.resource);
            }
            throw ex;
        } catch (Exception ex) {
            throw new UnsupportedOperationException(ex);
        }
    }

    @Override
    protected InputStream getResourceImpl(String resPath) throws IOException {
        File f = file(resPath);
        if (f.exists() && f.isFile())
            return new FileInputStream(file(resPath));
        else
            return null;
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        File f = file(resPath);
        return f.lastModified();
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
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

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException {
        File f = file(resPath);
        if ((f.exists() && f.lastModified() != oldTS) || (f.exists() == false && oldTS != 0))
            throw new IllegalStateException("Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but found " + f.lastModified());

        putResourceImpl(resPath, new ByteArrayInputStream(content), newTS);

        // some FS lose precision on given time stamp
        return getResourceTimestamp(resPath);
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        File f = file(resPath);
        f.delete();
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return file(resPath).toString();
    }

    private File file(String resPath) {
        if (resPath.equals("/"))
            return root;
        else
            return new File(root, resPath);
    }

}
