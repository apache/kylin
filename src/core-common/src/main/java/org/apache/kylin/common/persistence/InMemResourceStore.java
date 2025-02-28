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

import static org.apache.kylin.common.persistence.MetadataType.ALL_TYPE_STR;
import static org.apache.kylin.common.persistence.MetadataType.CASE_INSENSITIVE_METADATA;
import static org.apache.kylin.common.persistence.MetadataType.NEED_CACHED_METADATA;
import static org.apache.kylin.common.persistence.MetadataType.mergeKeyWithType;
import static org.apache.kylin.common.persistence.MetadataType.splitKeyWithType;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.MetadataStore.MemoryMetaData;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemResourceStore.class);

    @Getter
    private final Map<MetadataType, Map<String, VersionedRawResource>> data;

    public InMemResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
        data = new ConcurrentHashMap<>();
        NEED_CACHED_METADATA.forEach(type -> data.put(type,
                CASE_INSENSITIVE_METADATA.contains(type) ? new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER)
                        : new ConcurrentHashMap<>()));
    }

    /**
     *
     * @param folderPath, the table type name, such as PROJECT, or ALL
     * @return metadata list under the special table, or all metadata in every table if the folderPath is ALL.
     */
    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, RawResourceFilter filter, boolean recursive) {
        if (!ALL_TYPE_STR.contains(folderPath)) {
            throw new IllegalArgumentException(
                    "listResourcesImpl params must be a metadata type name, but got: " + folderPath);
        }

        TreeSet<String> ret = new TreeSet<>();
        MetadataType type = MetadataType.valueOf(folderPath);
        if (type == MetadataType.ALL) {
            if (recursive) {
                NEED_CACHED_METADATA.forEach(t -> ret.addAll(listResourcesImpl(t.name(), filter, false)));
            } else {
                NEED_CACHED_METADATA.forEach(t -> ret.add(t.name()));
            }
        } else {
            data.get(type).entrySet().stream().filter(entity -> filter.isMatch(entity.getValue().getRawResource()))
                    .forEach(entity -> ret.add(mergeKeyWithType(entity.getKey(), type)));
        }
        return ret;
    }

    @Override
    protected boolean existsImpl(String resPath) {
        return getResourceImpl(resPath, false) != null;
    }

    @Override
    public int batchLock(MetadataType type, RawResourceFilter filter) {
        // Do nothing for inMemResourceStore.
        return 0;
    }

    @Override
    protected RawResource getResourceImpl(String resPath, boolean needLock) {
        Pair<MetadataType, String> metaKeyAndType = splitKeyWithType(resPath);
        if (!NEED_CACHED_METADATA.contains(metaKeyAndType.getFirst())) {
            return null;
        }
        VersionedRawResource orDefault = data.get(metaKeyAndType.getFirst()).getOrDefault(metaKeyAndType.getSecond(),
                null);
        if (orDefault == null) {
            return null;
        }
        return orDefault.getRawResource();
    }

    protected void putTomb(String resPath) {
        Pair<MetadataType, String> metaKeyAndType = splitKeyWithType(resPath);
        data.get(metaKeyAndType.getFirst()).put(metaKeyAndType.getSecond(), TombVersionedRawResource.getINSTANCE());
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long oldMvcc) {
        return checkAndPutResource(resPath, byteSource, System.currentTimeMillis(), oldMvcc);
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long timeStamp, long oldMvcc) {
        checkEnv();
        Pair<MetadataType, String> meteKeyAndType = splitKeyWithType(resPath);
        MetadataType type = meteKeyAndType.getFirst();
        String metaKey = meteKeyAndType.getSecond();
        RawResource r = RawResource.constructResource(type, byteSource);
        r.setMetaKey(metaKey);
        r.setTs(timeStamp);
        r.setMvcc(oldMvcc + 1);

        if (!data.get(type).containsKey(metaKey)) {
            if (oldMvcc != -1) {
                throw new IllegalStateException(
                        "Trying to update a non-exist meta entry: " + resPath + ", with mvcc: " + oldMvcc);
            }
            synchronized (data) {
                if (!data.get(type).containsKey(metaKey)) {
                    data.get(type).put(metaKey, new VersionedRawResource(r));
                    return r;
                }
            }
        }
        VersionedRawResource versionedRawResource = data.get(type).get(metaKey);
        try {
            versionedRawResource.update(r);
        } catch (VersionConflictException e) {
            logger.info("current RS: {}", this);
            throw e;
        }

        return r;
    }

    protected long getResourceMvcc(String resPath) {
        Pair<MetadataType, String> meteKeyAndType = splitKeyWithType(resPath);
        MetadataType type = meteKeyAndType.getFirst();
        String metaKey = meteKeyAndType.getSecond();
        if (!data.get(type).containsKey(metaKey)) {
            return -1;
        }

        if (data.get(type).get(metaKey) == TombVersionedRawResource.getINSTANCE()) {
            //getResourceMvcc is only called on underlying
            throw new IllegalStateException();
        }

        VersionedRawResource versionedRawResource = data.get(type).get(metaKey);
        return versionedRawResource.getMvcc();
    }

    @Override
    protected void deleteResourceImpl(String resPath) {
        checkEnv();
        Pair<MetadataType, String> meteKeyAndType = splitKeyWithType(resPath);
        this.data.get(meteKeyAndType.getFirst()).remove(meteKeyAndType.getSecond());
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
    public void putResourceWithoutCheck(String resPath, ByteSource bs, long timeStamp, long newMvcc, boolean force) {
        Pair<MetadataType, String> meteKeyAndType = splitKeyWithType(resPath);
        MetadataType type = meteKeyAndType.getFirst();
        String metaKey = meteKeyAndType.getSecond();
        synchronized (data) {
            if (!force && kylinConfig.isJobNode() && data.get(type).containsKey(metaKey)) {
                throw new IllegalStateException(
                        "resource " + resPath + " already exists, use check and put api instead");
            }
            RawResource rawResource = RawResource.constructResource(type, bs);
            rawResource.setMvcc(newMvcc);
            rawResource.setTs(timeStamp);
            rawResource.setMetaKey(metaKey);
            data.get(type).put(metaKey, new VersionedRawResource(rawResource));
        }
    }

    @Override
    public void reload() throws IOException {
        resetData(metadataStore.reloadAll());
    }

    public void resetData(MemoryMetaData metaData) {
        val replaceData = metaData.getData();
        NEED_CACHED_METADATA.forEach(type -> data.replace(type, replaceData.get(type)));
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
