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

package org.apache.kylin.metadata.streaming;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_ALREADY_EXISTS_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_NOT_EXISTS_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_TABLES_USE_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_TABLES_USE_PARSER;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataParserManager {

    private String project;
    private KylinConfig kylinConfig;
    private CachedCrudAssist<DataParserInfo> crud;

    private static final String DEFAULT_JAR_NAME = "default";
    private static final String DEFAULT_PARSER_NAME = "org.apache.kylin.parser.TimedJsonStreamParser";

    public static DataParserManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, DataParserManager.class);
    }

    static DataParserManager newInstance(KylinConfig kylinConfig, String project) {
        return new DataParserManager(kylinConfig, project);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    private DataParserManager(KylinConfig kylinConfig, String project) {
        this.project = project;
        this.kylinConfig = kylinConfig;
        this.crud = new CachedCrudAssist<DataParserInfo>(getStore(), MetadataType.DATA_PARSER, project, DataParserInfo.class) {
            @Override
            protected DataParserInfo initEntityAfterReload(DataParserInfo entity, String resourceName) {
                return entity;
            }
        };
        crud.reloadAll();
    }

    public void initDefault() {
        if (isInitialized()) {
            return;
        }
        createDataParserInfo(new DataParserInfo(project, DEFAULT_PARSER_NAME, DEFAULT_JAR_NAME));
    }

    public boolean isInitialized() {
        return crud.contains(DataParserInfo.generateResourcePath(project, DEFAULT_PARSER_NAME));
    }

    public DataParserInfo getDataParserInfo(String className) {
        if (StringUtils.isEmpty(className)) {
            return null;
        }
        return crud.get(DataParserInfo.generateResourcePath(project, className));
    }

    public DataParserInfo createDataParserInfo(DataParserInfo parserInfo) {
        if (parserInfo == null || StringUtils.isEmpty(parserInfo.getClassName())) {
            throw new IllegalArgumentException("data parser info is null or class name is null");
        }
        DataParserInfo copy = copyForWrite(parserInfo);
        if (crud.contains(copy.resourceName())) {
            throw new KylinException(CUSTOM_PARSER_ALREADY_EXISTS_PARSER, copy.getClassName());
        }
        copy.updateRandomUuid();
        return crud.save(copy);
    }

    public DataParserInfo updateDataParserInfo(DataParserInfo parserInfo) {
        DataParserInfo copy = copyForWrite(parserInfo);
        if (!crud.contains(copy.resourceName())) {
            throw new KylinException(CUSTOM_PARSER_NOT_EXISTS_PARSER, copy.getClassName());
        }
        parserInfo.copyPropertiesTo(copy);
        return crud.save(copy);
    }

    /**
     * Delete a single parser, and throw an exception if a table is being referenced
     * For logical deletion, you can actually force load through reflection. Unless unregisterjar
     */
    public DataParserInfo removeParser(String className) {
        if (StringUtils.equals(className, DEFAULT_PARSER_NAME)) {
            throw new KylinException(CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER);
        }
        DataParserInfo dataParserInfo = getDataParserInfo(className);
        if (Objects.isNull(dataParserInfo)) {
            // throw exception when delete not exists parser
            throw new KylinException(CUSTOM_PARSER_NOT_EXISTS_PARSER, className);
        }
        if (!dataParserInfo.getStreamingTables().isEmpty()) {
            // There is a table reference, which cannot be deleted
            throw new KylinException(CUSTOM_PARSER_TABLES_USE_PARSER,
                    StringUtils.join(dataParserInfo.getStreamingTables(), ", "));
        }
        crud.delete(dataParserInfo);
        log.info("Removing DataParserClass '{}' success", className);
        return dataParserInfo;
    }

    public void removeJar(String jarName) {
        if (StringUtils.equals(jarName, DEFAULT_JAR_NAME)) {
            throw new KylinException(CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER);
        }
        // Check whether all parsers under jar are referenced by table
        if (isJarParserUsed(jarName)) {
            // There is a table reference, which cannot be deleted
            throw new KylinException(CUSTOM_PARSER_TABLES_USE_JAR,
                    StringUtils.join(getJarParserUsedTables(jarName), ","));
        }
        log.info("start to remove jar [{}]", jarName);
        List<DataParserInfo> dataParserWithJar = getDataParserByJar(jarName);
        dataParserWithJar.forEach(dataParserInfo -> removeParser(dataParserInfo.getClassName()));
    }

    public DataParserInfo removeUsingTable(String table, String className) {
        DataParserInfo dataParserInfo = getDataParserInfo(className);
        if (Objects.isNull(dataParserInfo)) {
            throw new KylinException(CUSTOM_PARSER_NOT_EXISTS_PARSER, className);
        }
        DataParserInfo copy = copyForWrite(dataParserInfo);
        copy.getStreamingTables().remove(table);
        log.info("class [{}], remove using table [{}]", className, table);
        return updateDataParserInfo(copy);
    }

    public List<DataParserInfo> listDataParserInfo() {
        return Lists.newArrayList(crud.listAll());
    }

    public List<DataParserInfo> getDataParserByJar(String jarName) {
        return listDataParserInfo().stream()
                .filter(info -> StringUtils.equals(info.getJarName(), jarName))
                .collect(Collectors.toList());
    }

    public boolean isJarParserUsed(String jarName) {
        return listDataParserInfo().stream()
                .filter(info -> StringUtils.equals(info.getJarName(), jarName))
                .anyMatch(info -> !info.getStreamingTables().isEmpty());
    }

    public List<String> getJarParserUsedTables(String jarName) {
        List<String> tableList = Lists.newArrayList();
        listDataParserInfo().stream()
                .filter(info -> StringUtils.equals(info.getJarName(), jarName))//
                .map(DataParserInfo::getStreamingTables)//
                .filter(streamingTables -> !streamingTables.isEmpty())//
                .forEach(tableList::addAll);
        return tableList;
    }

    public boolean jarHasParser(String jarName) {
        return listDataParserInfo().stream()//
                .map(DataParserInfo::getJarName)//
                .anyMatch(name -> StringUtils.equals(name, jarName));
    }

    public DataParserInfo copyForWrite(DataParserInfo dataParserInfo) {
        return crud.copyForWrite(dataParserInfo);
    }
}
