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

import static org.apache.kylin.common.persistence.MetadataType.splitKeyWithType;
import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.META_KEY_PROPERTIES_NAME;

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.JdbcMetadataStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;

import lombok.Getter;
import lombok.val;

/**
 * Read and write metadata directly via the metadata store.
 */
public class TransparentResourceStore extends ResourceStore {

    @Getter
    private final List<RawResource> resources;

    private final InMemResourceStore underlying;

    private final InMemResourceStore overlay;
    
    private final boolean needToComputeRawDiff;

    public TransparentResourceStore(InMemResourceStore underlying, KylinConfig kylinConfig) {
        super(kylinConfig);
        this.underlying = underlying;
        this.overlay = new InMemResourceStore(kylinConfig);
        this.metadataStore = underlying.getMetadataStore();
        this.resources = Lists.newArrayList();
        this.needToComputeRawDiff = kylinConfig.isAuditLogJsonPatchEnabled()
                && (metadataStore instanceof JdbcMetadataStore)
                && (kylinConfig.isUTEnv() || !UnitOfWork.get().isSkipAuditLog());
    }

    @Override
    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, RawResourceFilter filter, boolean recursive) {
        NavigableSet<String> fromUnderlying = underlying.listResourcesImpl(folderPath, filter, recursive);
        NavigableSet<String> fromOverlay = overlay.listResourcesImpl(folderPath, filter, recursive);
        TreeSet<String> ret = new TreeSet<>();
        if (fromUnderlying != null)
            ret.addAll(fromUnderlying);
        if (fromOverlay != null)
            ret.addAll(fromOverlay);
        if (!folderPath.equals(MetadataType.ALL.name()) || recursive) {
            ret.removeIf(key -> overlay.getResourceImpl(key, false) == TombRawResource.getINSTANCE());
        }

        return ret;
    }

    @Override
    protected boolean existsImpl(String resPath) {
        RawResource overlayResource = overlay.getResourceImpl(resPath, false);
        if (overlayResource != null) {
            return overlayResource != TombRawResource.getINSTANCE();
        }

        return underlying.exists(resPath);
    }

    public int batchLock(MetadataType type, RawResourceFilter filter) {
        return getMetadataStore().get(type, filter, true, false).size();
    }

    @Override
    protected RawResource getResourceImpl(String resPath, boolean needLock) {
        return getResourceImpl(resPath, needLock, true);
    }

    private RawResource getResourceImpl(String resPath, boolean needLock, boolean needContent) {
        if (!needLock) {
            return getResourceFromCache(resPath);
        }
        Pair<MetadataType, String> metaKeyAndType = splitKeyWithType(resPath);
        MetadataType type = metaKeyAndType.getFirst();
        String metaKey = metaKeyAndType.getSecond();
        RawResourceFilter filter = RawResourceFilter.equalFilter(META_KEY_PROPERTIES_NAME, metaKey);
        val raw = getMetadataStore().get(type, filter, needLock, needContent);
        RawResource rawResource = raw.size() == 1 ? raw.get(0) : null;
        updateOverlay(resPath, rawResource);
        return rawResource;
    }
    
    private void updateOverlay(String resPath, RawResource resource) {
        if (resource == null) {
            overlay.putTomb(resPath);
        } else {
            overlay.putResourceWithoutCheck(resPath, resource.getByteSource(), resource.getTs(), resource.getMvcc(),
                    true);
        }
    }

    private RawResource getResourceFromCache(String resPath) {
        val r = overlay.getResourceImpl(resPath, false);
        if (r != null) {
            return r == TombRawResource.getINSTANCE() ? null //deleted
                    : r; // updated
        }

        return underlying.getResourceImpl(resPath, false);
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long oldMvcc) {
        return checkAndPutResource(resPath, byteSource, System.currentTimeMillis(), oldMvcc);
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long timeStamp, long oldMvcc) {
        Pair<MetadataType, String> metaKeyAndType = splitKeyWithType(resPath);
        MetadataType type = metaKeyAndType.getFirst();
        RawResource raw = RawResource.constructResource(type, byteSource);
        assert raw != null;
        raw.setMvcc(oldMvcc + 1);
        raw.setTs(timeStamp);
        raw.setMetaKey(metaKeyAndType.getSecond());
        int affect = getMetadataStore().save(type, raw);
        if (affect == 1) {
            // If the same metadata is written multiple times in the transaction, the last write metadata will be
            // retained in underlying for generating json patch
            if (needToComputeRawDiff) {
                RawResource before = getResource(resPath);
                raw.fillContentDiffFromRaw(before);
            }
            updateOverlay(resPath, raw);
            resources.add(raw);
            return raw;
        } else {
            throw new IllegalStateException("Update metadata failed.");
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) {
        Pair<MetadataType, String> metaKeyAndType = splitKeyWithType(resPath);
        MetadataType type = metaKeyAndType.getFirst();
        String metaKey = metaKeyAndType.getSecond();
        RawResource raw = RawResource.constructResource(type, null);
        raw.setMetaKey(metaKey);
        getMetadataStore().save(type, raw);
        updateOverlay(resPath, null);
        resources.add(new TombRawResource(raw.getMetaKey(), raw.getMetaType()));
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return toString() + ":" + resPath;
    }

    @Override
    public void reload() {
        throw new NotImplementedException("ThreadViewResourceStore doesn't support reload");
    }

    @Override
    public String toString() {
        return "<thread view metastore@" + System.identityHashCode(this) + ":KylinConfig@"
                + System.identityHashCode(kylinConfig.base()) + ">";
    }

}
