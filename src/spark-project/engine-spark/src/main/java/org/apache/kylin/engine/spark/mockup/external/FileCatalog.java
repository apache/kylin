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
package org.apache.kylin.engine.spark.mockup.external;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.externalCatalog.api.ApiException;
import org.apache.kylin.externalCatalog.api.catalog.Database;
import org.apache.kylin.externalCatalog.api.catalog.FieldSchema;
import org.apache.kylin.externalCatalog.api.catalog.IExternalCatalog;
import org.apache.kylin.externalCatalog.api.catalog.Partition;
import org.apache.kylin.externalCatalog.api.catalog.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

/**
 * A simple (ephemeral) implementation of the external catalog.
 *
 * This is a dummy implementation that does not require setting up external systems.
 * It is intended for testing or exploration purposes only and should not be used
 * in production.
 */

@Slf4j
public class FileCatalog implements IExternalCatalog {

    private static final SecureRandom RNG = new SecureRandom();
    // Database name -> description
    private Map<String, DatabaseDesc> catalog = Maps.newHashMap();

    //TODO: Refactor.
    // The plugin should load configuration by itself, the user can only assume ${KYLIN_HOME} is set
    public FileCatalog(Configuration hadoopConfig) {
        init(hadoopConfig);
    }

    static Path toAbsolutePath(FileSystem fileSystem, Path path) throws IOException {

        if (path.isAbsolute())
            return path;

        try {
            return fileSystem.getFileStatus(path).getPath();
        } catch (FileNotFoundException ignored) {
            log.warn("{} does not exist! create empty table", path.toUri().toString());
        }
        return path;
    }

    private void init(Configuration hadoopConfig) {
        try {
            FileSystem fileSystem;
            KylinConfigExt ext = KylinConfigExt.createInstance(KylinConfig.getInstanceFromEnv(), Maps.newHashMap());

            Preconditions.checkArgument(!Strings.isNullOrEmpty(ext.getExternalCatalogClass()));
            String meta = ext.getOptional("kylin.NSparkDataSource.data.dir", null);
            String fs = ext.getOptional("kylin.NSparkDataSource.data.fs", null);
            if (fs == null) {
                fileSystem = FileSystem.get(hadoopConfig);
            } else {
                URI uri = URI.create(hadoopConfig.get("fs.defaultFS." + fs));
                fileSystem = FileSystem.get(uri, hadoopConfig);
            }
            readDatabases(fileSystem, meta).forEach(
                    (database, tables) -> initDatabase(fileSystem, meta, database, Lists.newArrayList(tables)));
        } catch (Exception e) {
            log.error("Failed to initialize FileCatalog", e);
            throw new IllegalStateException(e);
        }
    }

    private Map<String, Set<String>> readDatabases(FileSystem fileSystem, String meta) throws IOException {
        Path tableDescPath = new Path(meta + "/tableDesc");
        Map<String, Set<String>> databases = Maps.newHashMap();
        Preconditions.checkArgument(fileSystem.exists(tableDescPath), "%s doesn't exists",
                tableDescPath.toUri().getRawPath());
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(tableDescPath, false);
        while (it.hasNext()) {
            LocatedFileStatus fs = it.next();
            if (fs.isDirectory())
                continue;
            String fileName = fs.getPath().getName();
            String[] strings = fileName.split("\\.");
            if (strings.length == 3 && "json".equals(strings[2])) {
                String database = strings[0];
                String table = strings[1];
                if (databases.containsKey(database)) {
                    databases.get(database).add(table);
                } else {
                    databases.put(database, Sets.newHashSet(table));
                }
            }
        }
        databases.put("FILECATALOGUT", Sets.newHashSet());
        return databases;
    }

    private void initDatabase(FileSystem fileSystem, String meta, String database, List<String> tables) {

        String dbUpperCaseName = database.toUpperCase(Locale.ROOT);

        if (!catalog.containsKey(dbUpperCaseName)) {
            Database newDb = new Database(dbUpperCaseName, "", "", Maps.newHashMap());
            catalog.put(database.toUpperCase(Locale.ROOT), new DatabaseDesc(newDb));
        }

        try {
            for (String table : tables) {

                String tableUpperCaseName = table.toUpperCase(Locale.ROOT);
                if (catalog.get(dbUpperCaseName).tables.containsKey(tableUpperCaseName))
                    continue;

                Path tableDescPath = new Path(
                        String.format(Locale.ROOT, "%s/tableDesc/%s.%s.json", meta, database, table));
                TableDesc tableDesc = JsonUtil.readValue(fileSystem.open(tableDescPath), TableDesc.class);

                Table tableObj = new Table(tableUpperCaseName, dbUpperCaseName);
                List<FieldSchema> schemas = Arrays.stream(tableDesc.getColumns())
                        .map(columnDesc -> new FieldSchema(columnDesc.getName(), columnDesc.getDatatype(), ""))
                        .collect(Collectors.toList());

                Path path = toAbsolutePath(fileSystem,
                        new Path(String.format(Locale.ROOT, "%s/%s.csv", meta, tableDesc.getIdentity())));
                if (tableUpperCaseName.equals("SUPPLIER")) {
                    path = toAbsolutePath(fileSystem,
                            new Path(String.format(Locale.ROOT, "%s/%s", meta, tableDesc.getIdentity())));
                    tableObj.setPartitionColumnNames(Arrays.asList(new FieldSchema("S_NATION", "varchar(10)", ""),
                            new FieldSchema("S_CITY", "varchar(10)", "")));
                    tableObj.setFields(schemas.stream()
                            .filter(i -> !i.getName().equals("S_CITY") && !i.getName().equals("S_NATION"))
                            .collect(Collectors.toList()));
                }

                tableObj.setFields(schemas);
                tableObj.getSd().setPath(path.toUri().toString());

                catalog.get(dbUpperCaseName).tables.put(tableUpperCaseName, tableObj);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void requireDbExists(String db) throws ApiException {
        trySleepQuietly("requireDbExists");
        if (!catalog.containsKey(db.toUpperCase(Locale.ROOT))) {
            throw new ApiException(); // Error NoSuchDatabaseException
        }
    }

    private void requireTableExists(String db, String table) throws ApiException {
        trySleepQuietly("requireTableExists");
        requireDbExists(db);
        if (!catalog.get(db.toUpperCase(Locale.ROOT)).tables.containsKey(table.toUpperCase(Locale.ROOT))) {
            throw new ApiException(); // Error NoSuchTableException
        }
    }

    public List<String> getDatabases(String databasePattern) throws ApiException {
        //TODO: support pattern
        return new ArrayList<>(catalog.keySet());
    }

    @Override
    public Database getDatabase(String databaseName) throws ApiException {
        trySleepQuietly("getDatabase");
        DatabaseDesc desc = catalog.get(databaseName.toUpperCase(Locale.ROOT));
        if (desc == null)
            return null;
        return desc.db;
    }

    @Override
    public Table getTable(String dbName, String tableName, boolean throwException) throws ApiException {
        try {
            trySleepQuietly("getTable");
            requireTableExists(dbName, tableName);
            return catalog.get(dbName.toUpperCase(Locale.ROOT)).tables.get(tableName.toUpperCase(Locale.ROOT));
        } catch (ApiException e) {
            if (throwException) {
                throw e;
            }
        }
        return null;
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws ApiException {
        trySleepQuietly("getTables");
        requireDbExists(dbName);
        //TODO: support pattern
        return new ArrayList<>(catalog.get(dbName.toUpperCase(Locale.ROOT)).tables.keySet());
    }

    @Override
    public Dataset<Row> getTableData(SparkSession session, String dbName, String tableName, boolean throwException)
            throws ApiException {
        trySleepQuietly("getTableData");
        Table table = getTable(dbName, tableName, throwException);
        if (table == null || table.getSd().getPath() == null)
            return null;
        String schema = table.getFields().stream().map(s -> s.getName() + " " + s.getType())
                .collect(Collectors.joining(","));
        return session.read().schema(schema).csv(table.getSd().getPath());
    }

    private void trySleepQuietly(String method) {
        KylinConfigExt ext = KylinConfigExt.createInstance(KylinConfig.getInstanceFromEnv(), Maps.newHashMap());
        int sleepInterval = (int) TimeUtil
                .timeStringAs(ext.getOptional("kylin.external.catalog.mockup.sleep-interval", "0s"), TimeUnit.SECONDS);
        if (sleepInterval != 0) {
            int sleepTime = RNG.nextInt(sleepInterval);
            log.debug("Sleep {} s for {}", sleepTime, method);
            try {
                Thread.sleep(sleepTime * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public List<Partition> listPartitions(String dbName, String tablePattern) throws ApiException {
        if (tablePattern.equals("supplier")) {
            return Arrays.asList(new Partition(ImmutableMap.of("S_CITY", "shanghai")),
                    new Partition(ImmutableMap.of("S_CITY", "beijing")),
                    new Partition(ImmutableMap.of("S_CITY", "shenzhen")));
        } else {
            return Arrays.asList();
        }
    }

    private static class DatabaseDesc {
        Database db;
        Map<String, Table> tables = new HashMap<>();

        public DatabaseDesc(Database db) {
            this.db = db;
        }
    }
}
