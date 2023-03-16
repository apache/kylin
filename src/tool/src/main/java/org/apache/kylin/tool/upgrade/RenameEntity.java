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

package org.apache.kylin.tool.upgrade;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;

import org.apache.kylin.guava30.shaded.common.base.Throwables;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;

class RenameEntity {

    private static final ResourceStore resourceStore;

    static {
        resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
    }

    private String originName;
    private String destName;
    private RawResource rs;

    /**
     * update path
     *
     * @param originName
     * @param destName
     */
    public RenameEntity(String originName, String destName) {
        this.originName = originName;
        this.destName = destName;
    }

    /**
     * update path
     *
     * @param originName
     * @param destName
     */
    public RenameEntity(String originName, String destName, RawResource rs) {
        this.originName = originName;
        this.destName = destName;
        this.rs = rs;
    }

    /**
     * update path and content
     *
     * @param originName
     * @param destName
     * @param entity
     * @param clazz
     */
    public RenameEntity(String originName, String destName, RootPersistentEntity entity, Class clazz) {
        this.originName = originName;
        this.destName = destName;

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        try {
            new JsonSerializer<>(clazz).serialize(entity, dout);
            dout.close();
            buf.close();
        } catch (IOException e) {
            Throwables.propagate(e);
        }

        ByteSource byteSource = ByteSource.wrap(buf.toByteArray());

        this.rs = new RawResource(destName, byteSource, System.currentTimeMillis(), entity.getMvcc());
    }

    public RawResource getRs() {
        if (rs == null && StringUtils.isNotEmpty(originName)) {
            RawResource resource = resourceStore.getResource(originName);
            if (resource != null) {
                this.rs = new RawResource(destName, resource.getByteSource(), System.currentTimeMillis(),
                        resource.getMvcc());
            }
        }
        return rs;
    }

    public String getOriginName() {
        return originName;
    }

    public String getDestName() {
        return destName;
    }

    public void updateMetadata() throws Exception {
        MetadataStore metadataStore = resourceStore.getMetadataStore();
        RawResource rawResource = this.getRs();
        if (rawResource != null) {
            metadataStore.move(this.getOriginName(), this.getDestName());
            metadataStore.deleteResource(this.getOriginName(), null, UnitOfWork.DEFAULT_EPOCH_ID);
            metadataStore.putResource(rawResource, null, UnitOfWork.DEFAULT_EPOCH_ID);
        }
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s -> %s", originName, destName);
    }
}
