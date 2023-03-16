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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileMetadataStore extends MetadataStore {

    private final File root;

    public FileMetadataStore(KylinConfig kylinConfig) throws Exception {
        super(kylinConfig);
        root = new File(kylinConfig.getMetadataUrl().getIdentifier()).getAbsoluteFile();
        epochStore = FileEpochStore.getEpochStore(kylinConfig);
    }

    @Override
    protected void save(String path, ByteSource bs, long ts, long mvcc, String unitPath, long epochId)
            throws Exception {
        File f = file(path);
        f.getParentFile().mkdirs();
        if (bs == null) {
            FileUtils.deleteQuietly(f);
            return;
        }
        try (FileOutputStream out = new FileOutputStream(f)) {
            IOUtils.copy(bs.openStream(), out);
        }

        if (!f.setLastModified(ts)) {
            log.info("{} modified time change failed", f);
        }
    }

    @Override
    public void move(String srcPath, String destPath) throws Exception {
        File srcFilePath = file(srcPath);
        if (!srcFilePath.exists()) {
            return;
        }

        File destFilePath = file(destPath);
        File parentFile = destFilePath.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }

        boolean renameResult = srcFilePath.renameTo(destFilePath);
        if (!renameResult) {
            throw new RuntimeException(
                    String.format(Locale.ROOT, "update path %s %s failed", srcFilePath, destFilePath));
        }
    }

    @Override
    public NavigableSet<String> list(String subPath) {
        TreeSet<String> result = Sets.newTreeSet();
        val scanFolder = new File(root, subPath);
        if (!scanFolder.exists()) {
            return result;
        }
        val files = FileUtils.listFiles(scanFolder, null, true);
        for (File file : files) {
            result.add(file.getPath().replace(scanFolder.getPath(), ""));
        }
        return result;
    }

    @Override
    public RawResource load(String path) throws IOException {
        val f = new File(root, path);
        val resPath = f.getPath().replace(root.getPath(), "");
        try (FileInputStream in = new FileInputStream(f)) {
            val bs = ByteSource.wrap(IOUtils.toByteArray(in));
            return new RawResource(resPath, bs, f.lastModified(), 0);
        }
    }

    private File file(String resPath) {
        if (resPath.equals("/"))
            return root;
        else
            return new File(root, resPath);
    }

}
