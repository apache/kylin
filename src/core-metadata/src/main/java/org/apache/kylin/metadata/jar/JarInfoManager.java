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

package org.apache.kylin.metadata.jar;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_ALREADY_EXISTS_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_NOT_EXISTS_JAR;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JarInfoManager {

    private KylinConfig kylinConfig;
    private CachedCrudAssist<JarInfo> crud;
    private final String project;

    public static JarInfoManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, JarInfoManager.class);
    }

    static JarInfoManager newInstance(KylinConfig kylinConfig, String project) {
        return new JarInfoManager(kylinConfig, project);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    private JarInfoManager(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;
        this.project = project;
        this.crud = new CachedCrudAssist<JarInfo>(getStore(), MetadataType.JAR_INFO, project, JarInfo.class) {
            @Override
            protected JarInfo initEntityAfterReload(JarInfo entity, String resourceName) {
                return entity;
            }
        };
        crud.reloadAll();
    }

    public JarInfo getJarInfo(JarTypeEnum jarTypeEnum, String jarName) {
        if (StringUtils.isEmpty(jarName) ) {
            return null;
        }
        return crud.get(JarInfo.concatResourceName(project, jarTypeEnum, jarName));
    }

    private JarInfo copyForWrite(JarInfo jarInfo) {
        return crud.copyForWrite(jarInfo);
    }

    public JarInfo createJarInfo(JarInfo jarInfo) {
        if (Objects.isNull(jarInfo) || StringUtils.isEmpty(jarInfo.resourceName())) {
            throw new IllegalArgumentException("jar info is null or resourceName is null");
        }
        JarInfo copy = copyForWrite(jarInfo);
        if (crud.contains(copy.resourceName())) {
            throw new KylinException(CUSTOM_PARSER_ALREADY_EXISTS_JAR, copy.getJarName());
        }
        copy.updateRandomUuid();
        return crud.save(copy);
    }

    public JarInfo updateJarInfo(JarTypeEnum jarTypeEnum, String jarName, JarInfoUpdater updater) {
        JarInfo cached = getJarInfo(jarTypeEnum, jarName);
        if (cached == null) {
            throw new KylinException(CUSTOM_PARSER_NOT_EXISTS_JAR, jarName);
        }
        JarInfo copy = copyForWrite(cached);
        updater.modify(copy);
        return crud.save(copy);
    }

    public JarInfo removeJarInfo(JarTypeEnum jarTypeEnum, String jarName) {
        JarInfo jarInfo = getJarInfo(jarTypeEnum, jarName);
        if (Objects.isNull(jarInfo)) {
            throw new KylinException(CUSTOM_PARSER_NOT_EXISTS_JAR, jarName);
        }
        crud.delete(jarInfo);
        return jarInfo;
    }

    public List<JarInfo> listJarInfo() {
        return new ArrayList<>(crud.listAll());
    }

    public List<JarInfo> listJarInfoByType(JarTypeEnum jarTypeEnum) {
        return listJarInfo().stream().filter(jarInfo -> jarTypeEnum == jarInfo.getJarType())
                .collect(Collectors.toList());
    }

    public interface JarInfoUpdater {
        void modify(JarInfo copyForWrite);
    }
}
