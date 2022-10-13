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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileEpochStore extends EpochStore {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final File root;

    public static EpochStore getEpochStore(KylinConfig config) {
        return Singletons.getInstance(FileEpochStore.class, clz -> new FileEpochStore(config));
    }

    private FileEpochStore(KylinConfig kylinConfig) {
        root = Paths
                .get(Paths.get(kylinConfig.getMetadataUrlPrefix()).getParent().toFile().getAbsolutePath(), EPOCH_SUFFIX)
                .toFile().getAbsoluteFile();
    }

    @Override
    public void update(Epoch epoch) {
        insert(epoch);
    }

    @Override
    public void insert(Epoch epoch) {
        if (Objects.isNull(epoch)) {
            return;
        }
        if (!root.exists()) {
            root.mkdirs();
        }
        try {
            epoch.setMvcc(epoch.getMvcc() + 1);
            objectMapper.writeValue(new File(root, epoch.getEpochTarget()), epoch);
        } catch (IOException e) {
            log.warn("Save or update epoch {} failed", epoch, e);
        }
    }

    @Override
    public void updateBatch(List<Epoch> epochs) {
        if (CollectionUtils.isEmpty(epochs)) {
            return;
        }
        epochs.forEach(this::update);
    }

    @Override
    public void insertBatch(List<Epoch> epochs) {
        if (CollectionUtils.isEmpty(epochs)) {
            return;
        }
        epochs.forEach(this::insert);
    }

    @Override
    public Epoch getEpoch(String epochTarget) {
        File file = new File(root, epochTarget);
        if (file.exists()) {
            try {
                return objectMapper.readValue(file, Epoch.class);
            } catch (IOException e) {
                log.warn("Get epoch {} failed", epochTarget, e);
            }
        }
        return null;
    }

    @Override
    public List<Epoch> list() {
        List<Epoch> results = new ArrayList<>();

        File[] files = root.listFiles();
        if (files != null) {
            for (File file : files) {
                try {
                    results.add(objectMapper.readValue(file, Epoch.class));
                } catch (IOException e) {
                    log.warn("Get epoch from file {} failed", file.getAbsolutePath(), e);
                }
            }
        }
        return results;
    }

    @Override
    public void delete(String epochTarget) {
        File file = new File(root, epochTarget);
        if (file.exists()) {
            try {
                Files.delete(file.toPath());
            } catch (IOException e) {
                log.warn("Delete epoch {} failed", epochTarget);
            }
        }
    }

    @Override
    public void createIfNotExist() throws Exception {
        if (!root.exists()) {
            root.mkdirs();
        }
    }

    /**
     * file store don't support transaction, so execute directly
     * @param callback
     * @param <T>
     * @return
     */
    @Override
    public <T> T executeWithTransaction(Callback<T> callback) {
        try {
            return callback.handle();
        } catch (Exception e) {
            log.warn("execute failed in call back", e);
        }

        return null;
    }

    @Override
    public <T> T executeWithTransaction(Callback<T> callback, int timeout) {
        return executeWithTransaction(callback);
    }
}
