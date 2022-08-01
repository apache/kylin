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
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.FilePathUtil;
import org.apache.kylin.common.persistence.metadata.MetadataStore.MemoryMetaData;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemResourceStore.class);

    @Getter
    private volatile ConcurrentSkipListMap<String, VersionedRawResource> data;

    public InMemResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
        if (kylinConfig.isMetadataKeyCaseInSensitiveEnabled()) {
            data = new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);
        } else {
            data = new ConcurrentSkipListMap<>();
        }
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) {
        //a folder named /folder with a resource named /folderxx
        folderPath = FilePathUtil.completeFolderPathWithSlash(folderPath);

        val subset = data.subMap(folderPath, folderPath + Character.MAX_VALUE);

        TreeSet<String> ret = new TreeSet<>();
        String finalFolderPath = folderPath;
        subset.keySet().stream().map(x -> mapToFolder(x, recursive, finalFolderPath)).forEach(ret::add);

        // return null to indicate not a folder
        return ret.isEmpty() ? null : ret;
    }

    static String mapToFolder(String path, boolean recursive, String folderPath) {
        if (recursive)
            return path;

        int index = path.indexOf("/", folderPath.length());
        if (index >= 0)
            return path.substring(0, index);
        else
            return path;
    }

    @Override
    protected boolean existsImpl(String resPath) {
        return getResourceImpl(resPath) != null;
    }

    @Override
    protected RawResource getResourceImpl(String resPath) {
        VersionedRawResource orDefault = data.getOrDefault(resPath, null);
        if (orDefault == null) {
            return null;
        }
        return orDefault.getRawResource();
    }

    protected void putTomb(String resPath) {
        data.put(resPath, TombVersionedRawResource.getINSTANCE());
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long oldMvcc) {
        return checkAndPutResource(resPath, byteSource, System.currentTimeMillis(), oldMvcc);
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long timeStamp, long oldMvcc) {
        checkEnv();
        if (!data.containsKey(resPath) || data.get(resPath) == TombVersionedRawResource.getINSTANCE()) {
            if (oldMvcc != -1) {
                throw new IllegalStateException(
                        "Trying to update a non-exist meta entry: " + resPath + ", with mvcc: " + oldMvcc);
            }
            synchronized (data) {
                if (!data.containsKey(resPath) || data.get(resPath) == TombVersionedRawResource.getINSTANCE()) {
                    RawResource rawResource = new RawResource(resPath, byteSource, timeStamp, oldMvcc + 1);
                    data.put(resPath, new VersionedRawResource(rawResource));
                    return rawResource;
                }
            }
        }
        VersionedRawResource versionedRawResource = data.get(resPath);
        RawResource r = new RawResource(resPath, byteSource, timeStamp, oldMvcc + 1);
        try {
            versionedRawResource.update(r);
        } catch (VersionConflictException e) {
            logger.info("current RS: {}", this.toString());
            throw e;
        }

        return r;
    }

    protected long getResourceMvcc(String resPath) {
        if (!data.containsKey(resPath)) {
            return -1;
        }

        if (data.get(resPath) == TombVersionedRawResource.getINSTANCE()) {
            //getResourceMvcc is only called on underlying
            throw new IllegalStateException();
        }

        VersionedRawResource versionedRawResource = data.get(resPath);
        return versionedRawResource.getMvcc();
    }

    @Override
    protected void deleteResourceImpl(String resPath) {
        checkEnv();
        this.data.remove(resPath);
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return toString() + ":" + resPath;
    }

    @Override
    public String toString() {
        return "<in memory metastore@" + System.identityHashCode(this) + ":kylin config@"
                + System.identityHashCode(kylinConfig.base()) + ">";
    }

    @Override
    public void putResourceWithoutCheck(String resPath, ByteSource bs, long timeStamp, long newMvcc) {
        synchronized (data) {
            if (kylinConfig.isJobNode() && data.containsKey(resPath)
                    && data.get(resPath) != TombVersionedRawResource.getINSTANCE()) {
                throw new IllegalStateException(
                        "resource " + resPath + " already exists, use check and put api instead");
            }
            RawResource rawResource = new RawResource(resPath, bs, timeStamp, newMvcc);
            data.put(resPath, new VersionedRawResource(rawResource));
        }
    }

    @Override
    public void reload() throws IOException {
        MemoryMetaData metaData = metadataStore.reloadAll();
        data = metaData.getData();
        if (metaData.containOffset()) {
            offset = metaData.getOffset();
        }
    }

    private void checkEnv() {
        // UT env or replay thread can ignore transactional lock
        if (!kylinConfig.isSystemConfig() || kylinConfig.isUTEnv() || UnitOfWork.isReplaying()
                || kylinConfig.getStreamingChangeMeta()) {
            return;
        }
        Preconditions.checkState(!UnitOfWork.isReadonly(),
                "cannot update or delete resource in a readonly transaction");
        throw new IllegalStateException("cannot update or delete resource");
    }

}
