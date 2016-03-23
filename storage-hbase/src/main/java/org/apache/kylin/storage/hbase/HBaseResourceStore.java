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

package org.apache.kylin.storage.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HBaseResourceStore extends ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(HBaseResourceStore.class);

    private static final String DEFAULT_TABLE_NAME = "kylin_default_instance";
    private static final String FAMILY = "f";
    private static final byte[] B_FAMILY = Bytes.toBytes(FAMILY);
    private static final String COLUMN = "c";
    private static final byte[] B_COLUMN = Bytes.toBytes(COLUMN);
    private static final String COLUMN_TS = "t";
    private static final byte[] B_COLUMN_TS = Bytes.toBytes(COLUMN_TS);

    final String tableNameBase;
    final String hbaseUrl;

    Connection getConnection() throws IOException {
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
    }

    private void createHTableIfNeeded(String tableName) throws IOException {
        HBaseConnection.createHTableIfNeeded(getConnection(), tableName, FAMILY);
    }

    private String getAllInOneTableName() {
        return tableNameBase;
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        Result r = getFromHTable(resPath, false, false);
        return r != null;
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        final TreeSet<String> result = new TreeSet<>();

        visitFolder(folderPath, new KeyOnlyFilter(), new FolderVisitor() {
            @Override
            public void visit(String childPath, String fullPath, Result hbaseResult) {
                result.add(childPath);
            }
        });
        // return null to indicate not a folder
        return result.isEmpty() ? null : result;
    }

    private void visitFolder(String folderPath, Filter filter, FolderVisitor visitor) throws IOException {
        assert folderPath.startsWith("/");
        String lookForPrefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        byte[] startRow = Bytes.toBytes(lookForPrefix);
        byte[] endRow = Bytes.toBytes(lookForPrefix);
        endRow[endRow.length - 1]++;

        Table table = getConnection().getTable(TableName.valueOf(getAllInOneTableName()));
        Scan scan = new Scan(startRow, endRow);
        if ((filter != null && filter instanceof KeyOnlyFilter) == false) {
            scan.addColumn(B_FAMILY, B_COLUMN_TS);
            scan.addColumn(B_FAMILY, B_COLUMN);
        }
        if (filter != null) {
            scan.setFilter(filter);
        }

        tuneScanParameters(scan);

        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                String path = Bytes.toString(r.getRow());
                assert path.startsWith(lookForPrefix);
                int cut = path.indexOf('/', lookForPrefix.length());
                String child = cut < 0 ? path : path.substring(0, cut);
                visitor.visit(child, path, r);
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    private void tuneScanParameters(Scan scan) {
        // divide by 10 as some resource like dictionary or snapshot can be very large
        // scan.setCaching(kylinConfig.getHBaseScanCacheRows() / 10);
        scan.setCaching(kylinConfig.getHBaseScanCacheRows());

        scan.setMaxResultSize(kylinConfig.getHBaseScanMaxResultSize());
        scan.setCacheBlocks(true);
    }

    interface FolderVisitor {
        void visit(String childPath, String fullPath, Result hbaseResult) throws IOException;
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException {
        FilterList filter = generateTimeFilterList(timeStart, timeEndExclusive);
        final List<RawResource> result = Lists.newArrayList();
        try {
            visitFolder(folderPath, filter, new FolderVisitor() {
                @Override
                public void visit(String childPath, String fullPath, Result hbaseResult) throws IOException {
                    // is a direct child (not grand child)?
                    if (childPath.equals(fullPath))
                        result.add(new RawResource(getInputStream(childPath, hbaseResult), getTimestamp(hbaseResult)));
                }
            });
        } catch (IOException e) {
            for (RawResource rawResource : result) {
                IOUtils.closeQuietly(rawResource.inputStream);
            }
            throw e;
        }
        return result;
    }

    private FilterList generateTimeFilterList(long timeStart, long timeEndExclusive) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (timeStart != Long.MIN_VALUE) {
            SingleColumnValueFilter timeStartFilter = new SingleColumnValueFilter(B_FAMILY, B_COLUMN_TS, CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(timeStart));
            filterList.addFilter(timeStartFilter);
        }
        if (timeEndExclusive != Long.MAX_VALUE) {
            SingleColumnValueFilter timeEndFilter = new SingleColumnValueFilter(B_FAMILY, B_COLUMN_TS, CompareFilter.CompareOp.LESS, Bytes.toBytes(timeEndExclusive));
            filterList.addFilter(timeEndFilter);
        }
        return filterList.getFilters().size() == 0 ? null : filterList;
    }

    private InputStream getInputStream(String resPath, Result r) throws IOException {
        if (r == null) {
            return null;
        }
        byte[] value = r.getValue(B_FAMILY, B_COLUMN);
        if (value.length == 0) {
            Path redirectPath = bigCellHDFSPath(resPath);
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();

            return fileSystem.open(redirectPath);
        } else {
            return new ByteArrayInputStream(value);
        }
    }

    private long getTimestamp(Result r) {
        if (r == null || r.getValue(B_FAMILY, B_COLUMN_TS) == null) {
            return 0;
        } else {
            return Bytes.toLong(r.getValue(B_FAMILY, B_COLUMN_TS));
        }
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        Result r = getFromHTable(resPath, true, true);
        if (r == null)
            return null;
        else
            return new RawResource(getInputStream(resPath, r), getTimestamp(r));
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        return getTimestamp(getFromHTable(resPath, false, true));
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        IOUtils.copy(content, bout);
        bout.close();

        Table table = getConnection().getTable(TableName.valueOf(getAllInOneTableName()));
        try {
            byte[] row = Bytes.toBytes(resPath);
            Put put = buildPut(resPath, ts, row, bout.toByteArray(), table);

            table.put(put);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException {
        Table table = getConnection().getTable(TableName.valueOf(getAllInOneTableName()));
        try {
            byte[] row = Bytes.toBytes(resPath);
            byte[] bOldTS = oldTS == 0 ? null : Bytes.toBytes(oldTS);
            Put put = buildPut(resPath, newTS, row, content, table);

            boolean ok = table.checkAndPut(row, B_FAMILY, B_COLUMN_TS, bOldTS, put);
            logger.trace("Update row " + resPath + " from oldTs: " + oldTS + ", to newTs: " + newTS + ", operation result: " + ok);
            if (!ok) {
                long real = getResourceTimestampImpl(resPath);
                throw new IllegalStateException("Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but it is " + real);
            }

            return newTS;
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(getAllInOneTableName()));
        try {
            boolean hdfsResourceExist = false;
            Result result = internalGetFromHTable(table, resPath, true, false);
            if (result != null) {
                byte[] value = result.getValue(B_FAMILY, B_COLUMN);
                if (value != null && value.length == 0) {
                    hdfsResourceExist = true;
                }
            }

            Delete del = new Delete(Bytes.toBytes(resPath));
            table.delete(del);

            if (hdfsResourceExist) { // remove hdfs cell value
                Path redirectPath = bigCellHDFSPath(resPath);
                FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();

                if (fileSystem.exists(redirectPath)) {
                    fileSystem.delete(redirectPath, true);
                }
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return getAllInOneTableName() + "(key='" + resPath + "')@" + kylinConfig.getMetadataUrl();
    }

    private Result getFromHTable(String path, boolean fetchContent, boolean fetchTimestamp) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(getAllInOneTableName()));
        try {
            return internalGetFromHTable(table, path, fetchContent, fetchTimestamp);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    private Result internalGetFromHTable(Table table, String path, boolean fetchContent, boolean fetchTimestamp) throws IOException {
        byte[] rowkey = Bytes.toBytes(path);

        Get get = new Get(rowkey);

        if (!fetchContent && !fetchTimestamp) {
            get.setCheckExistenceOnly(true);
        } else {
            if (fetchContent)
                get.addColumn(B_FAMILY, B_COLUMN);
            if (fetchTimestamp)
                get.addColumn(B_FAMILY, B_COLUMN_TS);
        }

        Result result = table.get(get);
        boolean exists = result != null && (!result.isEmpty() || (result.getExists() != null && result.getExists()));
        return exists ? result : null;
    }

    private Path writeLargeCellToHdfs(String resPath, byte[] largeColumn, Table table) throws IOException {
        Path redirectPath = bigCellHDFSPath(resPath);
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();

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

    private Put buildPut(String resPath, long ts, byte[] row, byte[] content, Table table) throws IOException {
        int kvSizeLimit = Integer.parseInt(getConnection().getConfiguration().get("hbase.client.keyvalue.maxsize", "10485760"));
        if (content.length > kvSizeLimit) {
            writeLargeCellToHdfs(resPath, content, table);
            content = BytesUtil.EMPTY_BYTE_ARRAY;
        }

        Put put = new Put(row);
        put.addColumn(B_FAMILY, B_COLUMN, content);
        put.addColumn(B_FAMILY, B_COLUMN_TS, Bytes.toBytes(ts));

        return put;
    }
}
