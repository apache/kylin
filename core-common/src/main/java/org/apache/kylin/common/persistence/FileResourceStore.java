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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(FileResourceStore.class);

    File root;

    int failPutResourceCountDown = Integer.MAX_VALUE;
    int failVisitFolderCountDown = Integer.MAX_VALUE;

    public FileResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
        root = new File(getPath(kylinConfig)).getAbsoluteFile();
        if (!root.exists())
            throw new IllegalArgumentException(
                    "File not exist by '" + kylinConfig.getMetadataUrl() + "': " + root.getAbsolutePath());
    }

    protected String getPath(KylinConfig kylinConfig) {
        return kylinConfig.getMetadataUrl().getIdentifier();
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        File f = file(resPath);
        return f.exists() && f.isFile(); // directory is not considered a resource
    }

    @Override
    protected void visitFolderImpl(String folderPath, boolean recursive, VisitFilter filter, boolean loadContent,
            Visitor visitor) throws IOException {
        if (--failVisitFolderCountDown == 0)
            throw new IOException("for test");

        File file = file(folderPath);
        if (!file.exists() || !file.isDirectory())
            return;

        String prefix = fixWinPath(file);
        Collection<File> files = FileUtils.listFiles(file, null, recursive);

        for (File f : files) {

            String path = fixWinPath(f);
            if (!path.startsWith(prefix))
                throw new IllegalStateException("File path " + path + " is supposed to start with " + prefix);

            String resPath = folderPath.equals("/") ? path.substring(prefix.length())
                    : folderPath + path.substring(prefix.length());

            if (filter.matches(resPath, f.lastModified())) {
                RawResource raw = loadContent ? new RawResource(resPath, f.lastModified(), new FileInputStream(f))
                        : new RawResource(resPath, f.lastModified());
                try {
                    visitor.visit(raw);
                } finally {
                    raw.close();
                }
            }
        }
    }

    private String fixWinPath(File file) {
        String path = file.getAbsolutePath();
        if (path.length() > 2 && path.charAt(1) == ':' && path.charAt(2) == '\\')
            path = path.replace('\\', '/');
        return path;
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {

        File f = file(resPath);
        if (f.exists() && f.isFile()) {
            if (f.length() == 0) {
                logger.warn("Zero length file: {}. ", f.getAbsolutePath());
            }

            return new RawResource(resPath, f.lastModified(), new FileInputStream(f));
        } else {
            return null;
        }
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {

        File f = file(resPath);
        if (f.exists() && f.isFile())
            return f.lastModified();
        else
            return 0;
    }

    @Override
    protected void putResourceImpl(String resPath, ContentWriter content, long ts) throws IOException {

        if (--failPutResourceCountDown == 0)
            throw new IOException("for test");

        File tmp = File.createTempFile("kylin-fileresource-", ".tmp");
        try {

            try (FileOutputStream out = new FileOutputStream(tmp); DataOutputStream dout = new DataOutputStream(out)) {
                content.write(dout);
                dout.flush();
            }

            File f = file(resPath);
            f.getParentFile().mkdirs();

            if (!tmp.renameTo(f)) {
                f.delete();
                for (int i = 0; f.exists() && i < 3; i++) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    f.delete();
                }

                FileUtils.moveFile(tmp, f);
            }

            f.setLastModified(ts);

        } finally {
            if (tmp.exists())
                FileUtils.forceDelete(tmp);
        }
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, WriteConflictException {

        File f = file(resPath);
        if ((f.exists() && f.lastModified() != oldTS) || (f.exists() == false && oldTS != 0))
            throw new WriteConflictException(
                    "Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but found " + f.lastModified());

        putResourceImpl(resPath, ContentWriter.create(content), newTS);

        return f.lastModified();
    }

    @Override
    protected void updateTimestampImpl(String resPath, long timestamp) throws IOException {
        File f = file(resPath);
        if (f.exists()) {
            // note file timestamp may lose precision for last two digits of timestamp
            boolean success = f.setLastModified(timestamp);
            if (!success) {
                throw new IOException(
                        "Update resource timestamp failed, resPath:" + resPath + ", timestamp: " + timestamp);
            }
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {

        File f = file(resPath);
        try {
            if (f.exists())
                FileUtils.forceDelete(f);
        } catch (FileNotFoundException e) {
            // FileNotFoundException is not a problem in case of delete
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath, long timestamp) throws IOException {
        File f = file(resPath);
        try {
            if (f.exists()) {
                long origLastModified = getResourceTimestampImpl(resPath);
                if (checkTimeStampBeforeDelete(origLastModified, timestamp)) {
                    FileUtils.forceDelete(f);
                } else {
                    throw new IOException("Resource " + resPath + " timestamp not match, [originLastModified: "
                            + origLastModified + ", timestampToDelete: " + timestamp + "]");
                }
            }
        } catch (FileNotFoundException e) {
            // FileNotFoundException is not a problem in case of delete
        }
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

    @Override
    public String toString() {
        return root.getAbsolutePath();
    }
}
