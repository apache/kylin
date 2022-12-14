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

package org.apache.kylin.rest.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool to generate database and tables from metadata backups of customer
 */
public class CreateTableFromJson {

    private static final String QUOTE = "`";
    private static final Map<String, String> TYPE_MAP = Maps.newHashMap();

    private static final Logger logger = LoggerFactory.getLogger(CreateTableFromJson.class);

    static {
        TYPE_MAP.put("integer", "int");
        TYPE_MAP.put("long", "bigint");
    }

    public static void main(String[] args) throws IOException {

        String pathDir = args[0];
        Map<String, List<String>> map = createDbAndTables(pathDir);

        map.forEach((k, v) -> {
            logger.info(k);
            v.forEach(logger::info);
        });

        logger.info("\n\n\n\n\n");
    }

    // the path is /{metadata_backup_path}/{project_name}/table/
    private static Map<String, List<String>> createDbAndTables(String pathDir) throws IOException {
        Map<String, List<String>> map = Maps.newHashMap();
        File file = new File(pathDir).getAbsoluteFile();
        File[] files = file.listFiles();

        for (File f : Objects.requireNonNull(files)) {
            final TableDesc tableDesc = JsonUtil.readValue(f, TableDesc.class);
            List<String> columnNameTypeList = Lists.newArrayList();
            for (ColumnDesc column : tableDesc.getColumns()) {
                String name = column.getName();
                String type = convert(column.getDatatype());
                columnNameTypeList.add(String.format(Locale.ROOT, "%s %s", quote(name), type));
            }

            String databaseSql = String.format(Locale.ROOT, "create database %s;%nuse %s;",
                    quote(tableDesc.getDatabase()), quote(tableDesc.getDatabase()));
            map.putIfAbsent(databaseSql, Lists.newArrayList());
            String tableSql = createTableSql(tableDesc.getName(), columnNameTypeList);
            map.get(databaseSql).add(tableSql);
        }
        return map;
    }

    private static String quote(String identifier) {
        return QUOTE + identifier + QUOTE;
    }

    private static String createTableSql(String table, List<String> columns) {
        return String.format(Locale.ROOT, "create table %s(%s);", quote(table), String.join(", ", columns));
    }

    private static String convert(String oriType) {
        return TYPE_MAP.getOrDefault(oriType.toLowerCase(Locale.ROOT), oriType.toLowerCase(Locale.ROOT));
    }
}
