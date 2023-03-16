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

import java.util.List;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.FilePathUtil;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import lombok.Getter;
import lombok.val;

/**
 * for meta mutation threads, it needs an exclusive view of the meta store,
 * which is underlying meta store + mutations by itself
 */
public class ThreadViewResourceStore extends ResourceStore {

    private InMemResourceStore underlying;

    @Getter
    private InMemResourceStore overlay;

    @Getter
    private List<RawResource> resources;

    public ThreadViewResourceStore(InMemResourceStore underlying, KylinConfig kylinConfig) {
        super(kylinConfig);
        this.underlying = underlying;
        this.overlay = new InMemResourceStore(kylinConfig);
        resources = Lists.newArrayList();
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) {
        //a folder named /folder with a resource named /folderxx
        folderPath = FilePathUtil.completeFolderPathWithSlash(folderPath);

        NavigableSet<String> fromUnderlying = underlying.listResourcesImpl(folderPath, true);
        NavigableSet<String> fromOverlay = overlay.listResourcesImpl(folderPath, true);
        TreeSet<String> ret = new TreeSet<>();
        if (fromUnderlying != null)
            ret.addAll(fromUnderlying);
        if (fromOverlay != null)
            ret.addAll(fromOverlay);
        ret.removeIf(key -> overlay.getResourceImpl(key) == TombRawResource.getINSTANCE());

        String finalFolderPath = folderPath;

        if (ret.isEmpty()) {
            return null;
        } else if (recursive) {
            return ret;
        } else {
            TreeSet<String> ret2 = new TreeSet<>();

            ret.stream().map(x -> {
                if (recursive) {
                    return x;
                }
                return InMemResourceStore.mapToFolder(x, recursive, finalFolderPath);
            }).forEach(ret2::add);
            return ret2;
        }

    }

    @Override
    protected boolean existsImpl(String resPath) {
        RawResource overlayResource = overlay.getResourceImpl(resPath);
        if (overlayResource != null) {
            return overlayResource != TombRawResource.getINSTANCE();
        }

        return underlying.exists(resPath);
    }

    @Override
    protected RawResource getResourceImpl(String resPath) {
        val r = overlay.getResourceImpl(resPath);
        if (r != null) {
            return r == TombRawResource.getINSTANCE() ? null //deleted
                    : r; // updated
        }

        return underlying.getResourceImpl(resPath);

    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long oldMvcc) {
        return checkAndPutResource(resPath, byteSource, System.currentTimeMillis(), oldMvcc);
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long timeStamp, long oldMvcc) {
        RawResource r = overlay.getResourceImpl(resPath);

        if (r == null) {
            //TODO: should check meta's write footprint
            long resourceMvcc = underlying.getResourceMvcc(resPath);
            Preconditions.checkState(resourceMvcc == oldMvcc, "Resource mvcc not equals old mvcc", resourceMvcc,
                    oldMvcc);
            overlay.putResourceWithoutCheck(resPath, byteSource, timeStamp, oldMvcc + 1);
        } else {
            if (!KylinConfig.getInstanceFromEnv().isUTEnv() && r instanceof TombRawResource) {
                throw new IllegalStateException(String.format(Locale.ROOT,
                        "It's not allowed to create the same metadata in path {%s} after deleting it in one transaction",
                        resPath));
            }
            overlay.checkAndPutResource(resPath, byteSource, timeStamp, oldMvcc);
        }

        val raw = overlay.getResourceImpl(resPath);
        resources.add(raw);
        return raw;
    }

    @Override
    protected void deleteResourceImpl(String resPath) {
        overlay.putTomb(resPath);
        resources.add(new TombRawResource(resPath));
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
