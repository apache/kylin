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

package org.apache.kylin.storage.hbase.steps;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HBaseResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(HBaseResourceStore.class);

    private static final String DEFAULT_TABLE_NAME = "kylin_metadata";
    private static final String FAMILY = "f";
    private static final byte[] B_FAMILY = Bytes.toBytes(FAMILY);
    private static final String COLUMN = "c";
    private static final byte[] B_COLUMN = Bytes.toBytes(COLUMN);
    private static final String COLUMN_TS = "t";
    private static final byte[] B_COLUMN_TS = Bytes.toBytes(COLUMN_TS);

    private static final Map<String, String> TABLE_SUFFIX_MAP = new LinkedHashMap<String, String>();

    static {
        TABLE_SUFFIX_MAP.put(CUBE_RESOURCE_ROOT + "/", "_cube");
        TABLE_SUFFIX_MAP.put(DICT_RESOURCE_ROOT + "/", "_dict");
        TABLE_SUFFIX_MAP.put("/invertedindex/", "_invertedindex");
        TABLE_SUFFIX_MAP.put(JOB_PATH_ROOT + "/", "_job");
        TABLE_SUFFIX_MAP.put(JOB_OUTPUT_PATH_ROOT + "/", "_job_output");
        TABLE_SUFFIX_MAP.put(PROJECT_RESOURCE_ROOT + "/", "_proj");
        TABLE_SUFFIX_MAP.put(SNAPSHOT_RESOURCE_ROOT + "/", "_table_snapshot");
        TABLE_SUFFIX_MAP.put("", ""); // DEFAULT CASE
    }

    final String tableNameBase;
    final String hbaseUrl;

    //    final Map<String, String> tableNameMap; // path prefix ==> HBase table name

    private HConnection getConnection() throws IOException {
        return HBaseConnection.get(hbaseUrl);
    }

    public HBaseResourceStore(KylinConfig kylinConfig) throws IOException {
        super(kylinConfig);

        String metadataUrl = kylinConfig.getMetadataUrl();
        // split TABLE@HBASE_URL
        int cut = metadataUrl.indexOf('@');
        tableNameBase = cut < 0 ? DEFAULT_TABLE_NAME : metadataUrl.substring(0, cut);
        hbaseUrl = cut < 0 ? metadataUrl : metadataUrl.substring(cut + 1);

        createHTableIfNeeded(getAllInOneTableName());

        //        tableNameMap = new LinkedHashMap<String, String>();
        //        for (Entry<String, String> entry : TABLE_SUFFIX_MAP.entrySet()) {
        //            String pathPrefix = entry.getKey();
        //            String tableName = tableNameBase + entry.getValue();
        //            tableNameMap.put(pathPrefix, tableName);
        //            createHTableIfNeeded(tableName);
        //        }

    }

    private void createHTableIfNeeded(String tableName) throws IOException {
        HBaseConnection.createHTableIfNeeded(getConnection(), tableName, FAMILY);
    }

    private String getAllInOneTableName() {
        return tableNameBase;
    }

    @Override
    protected ArrayList<String> listResourcesImpl(String resPath) throws IOException {
        assert resPath.startsWith("/");
        String lookForPrefix = resPath.endsWith("/") ? resPath : resPath + "/";
        byte[] startRow = Bytes.toBytes(lookForPrefix);
        byte[] endRow = Bytes.toBytes(lookForPrefix);
        endRow[endRow.length - 1]++;

        ArrayList<String> result = new ArrayList<String>();

        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        Scan scan = new Scan(startRow, endRow);
        scan.setFilter(new KeyOnlyFilter());
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                String path = Bytes.toString(r.getRow());
                assert path.startsWith(lookForPrefix);
                int cut = path.indexOf('/', lookForPrefix.length());
                String child = cut < 0 ? path : path.substring(0, cut);
                if (result.contains(child) == false)
                    result.add(child);
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
        // return null to indicate not a folder
        return result.isEmpty() ? null : result;
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        Result r = getByScan(resPath, null, null);
        return r != null;
    }

    @Override
    protected List<RawResource> getAllResources(String rangeStart, String rangeEnd) throws IOException {
        byte[] startRow = Bytes.toBytes(rangeStart);
        byte[] endRow = plusZero(Bytes.toBytes(rangeEnd));

        Scan scan = new Scan(startRow, endRow);
        scan.addColumn(B_FAMILY, B_COLUMN_TS);
        scan.addColumn(B_FAMILY, B_COLUMN);

        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        List<RawResource> result = Lists.newArrayList();
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                result.add(new RawResource(getInputStream(Bytes.toString(r.getRow()), r), getTimestamp(r)));
            }
        } catch (IOException e) {
            for (RawResource rawResource : result) {
                IOUtils.closeQuietly(rawResource.resource);
            }
            throw e;
        } finally {
            IOUtils.closeQuietly(table);
        }
        return result;
    }

    private InputStream getInputStream(String resPath, Result r) throws IOException {
        if (r == null) {
            return null;
        }
        byte[] value = r.getValue(B_FAMILY, B_COLUMN);
        if (value.length == 0) {
            Path redirectPath = bigCellHDFSPath(resPath);
            Configuration hconf = HadoopUtil.getCurrentConfiguration();
            FileSystem fileSystem = FileSystem.get(hconf);

            return fileSystem.open(redirectPath);
        } else {
            return new ByteArrayInputStream(value);
        }
    }

    private long getTimestamp(Result r) {
        if (r == null) {
            return 0;
        } else {
            return Bytes.toLong(r.getValue(B_FAMILY, B_COLUMN_TS));
        }
    }

    @Override
    protected InputStream getResourceImpl(String resPath) throws IOException {
        Result r = getByScan(resPath, B_FAMILY, B_COLUMN);
        return getInputStream(resPath, r);
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        Result r = getByScan(resPath, B_FAMILY, B_COLUMN_TS);
        return getTimestamp(r);
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        IOUtils.copy(content, bout);
        bout.close();

        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        try {
            byte[] row = Bytes.toBytes(resPath);
            Put put = buildPut(resPath, ts, row, bout.toByteArray(), table);

            table.put(put);
            table.flushCommits();
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException {
        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        try {
            byte[] row = Bytes.toBytes(resPath);
            byte[] bOldTS = oldTS == 0 ? null : Bytes.toBytes(oldTS);
            Put put = buildPut(resPath, newTS, row, content, table);

            boolean ok = table.checkAndPut(row, B_FAMILY, B_COLUMN_TS, bOldTS, put);
            logger.info("Update row " + resPath + " from oldTs: " + oldTS + ", to newTs: " + newTS + ", operation result: " + ok);
            if (!ok) {
                long real = getResourceTimestamp(resPath);
                throw new IllegalStateException("Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but it is " + real);
            }

            table.flushCommits();

            return newTS;
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        try {
            Delete del = new Delete(Bytes.toBytes(resPath));
            table.delete(del);
            table.flushCommits();
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return getAllInOneTableName() + "(key='" + resPath + "')@" + kylinConfig.getMetadataUrl();
    }

    private Result getByScan(String path, byte[] family, byte[] column) throws IOException {
        byte[] startRow = Bytes.toBytes(path);
        byte[] endRow = plusZero(startRow);

        Scan scan = new Scan(startRow, endRow);
        if (family == null || column == null) {
            scan.setFilter(new KeyOnlyFilter());
        } else {
            scan.addColumn(family, column);
        }

        HTableInterface table = getConnection().getTable(getAllInOneTableName());
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result result = null;
            for (Result r : scanner) {
                result = r;
            }
            return result == null || result.isEmpty() ? null : result;
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    private byte[] plusZero(byte[] startRow) {
        byte[] endRow = Arrays.copyOf(startRow, startRow.length + 1);
        endRow[endRow.length - 1] = 0;
        return endRow;
    }

    private Path writeLargeCellToHdfs(String resPath, byte[] largeColumn, HTableInterface table) throws IOException {
        Path redirectPath = bigCellHDFSPath(resPath);
        Configuration hconf = HadoopUtil.getCurrentConfiguration();
        FileSystem fileSystem = FileSystem.get(hconf);

        if (fileSystem.exists(redirectPath)) {
            fileSystem.delete(redirectPath, true);
        }

        FSDataOutputStream out = fileSystem.create(redirectPath);

        try {
            out.write(largeColumn);
        } finally {
            IOUtils.closeQuietly(out);
        }

        return redirectPath;
    }

    public Path bigCellHDFSPath(String resPath) {
        String hdfsWorkingDirectory = this.kylinConfig.getHdfsWorkingDirectory();
        Path redirectPath = new Path(hdfsWorkingDirectory, "resources" + resPath);
        return redirectPath;
    }

    private Put buildPut(String resPath, long ts, byte[] row, byte[] content, HTableInterface table) throws IOException {
        int kvSizeLimit = this.kylinConfig.getHBaseKeyValueSize();
        if (content.length > kvSizeLimit) {
            writeLargeCellToHdfs(resPath, content, table);
            content = BytesUtil.EMPTY_BYTE_ARRAY;
        }

        Put put = new Put(row);
        put.add(B_FAMILY, B_COLUMN, content);
        put.add(B_FAMILY, B_COLUMN_TS, Bytes.toBytes(ts));

        return put;
    }
}
