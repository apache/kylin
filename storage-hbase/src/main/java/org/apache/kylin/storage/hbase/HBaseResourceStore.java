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
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ContentWriter;
import org.apache.kylin.common.persistence.PushdownResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class HBaseResourceStore extends PushdownResourceStore {

    private static Logger logger = LoggerFactory.getLogger(HBaseResourceStore.class);

    private static final String FAMILY = "f";

    private static final byte[] B_FAMILY = Bytes.toBytes(FAMILY);

    private static final String COLUMN = "c";

    private static final byte[] B_COLUMN = Bytes.toBytes(COLUMN);

    private static final String COLUMN_TS = "t";

    private static final byte[] B_COLUMN_TS = Bytes.toBytes(COLUMN_TS);

    final String tableName;
    final StorageURL metadataUrl;
    final int kvSizeLimit;

    public HBaseResourceStore(KylinConfig kylinConfig) throws IOException {
        super(kylinConfig);
        metadataUrl = buildMetadataUrl(kylinConfig);
        tableName = metadataUrl.getIdentifier();
        createHTableIfNeeded(tableName);

        int kvSizeLimitActual = Integer
                .parseInt(getConnection().getConfiguration().get("hbase.client.keyvalue.maxsize", "10485760"));
        kvSizeLimit = kvSizeLimitActual > 10485760 ? kvSizeLimitActual : 10485760;
        logger.debug("hbase.client.keyvalue.maxsize is {}, kvSizeLimit is set to {}", kvSizeLimitActual, kvSizeLimit);
    }

    Connection getConnection() throws IOException {
        return HBaseConnection.get(metadataUrl);
    }

    private StorageURL buildMetadataUrl(KylinConfig kylinConfig) throws IOException {
        StorageURL url = kylinConfig.getMetadataUrl();
        if (!url.getScheme().equals("hbase"))
            throw new IOException("Cannot create HBaseResourceStore. Url not match. Url: " + url);

        // control timeout for prompt error report
        Map<String, String> newParams = new LinkedHashMap<>();
        newParams.put("hbase.client.scanner.timeout.period", kylinConfig.getHbaseClientScannerTimeoutPeriod());
        newParams.put("hbase.rpc.timeout", kylinConfig.getHbaseRpcTimeout());
        newParams.put("hbase.client.retries.number", kylinConfig.getHbaseClientRetriesNumber());
        newParams.putAll(url.getAllParameters());

        return url.copy(newParams);
    }

    private void createHTableIfNeeded(String tableName) throws IOException {
        HBaseConnection.createHTableIfNeeded(getConnection(), tableName, FAMILY);
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        Result r = getFromHTable(resPath, false, false);
        return r != null;
    }

    /* override get meta store uuid method for backward compatibility */
    @Override
    protected String createMetaStoreUUID() throws IOException {
        try (final Admin hbaseAdmin = HBaseConnection.get(metadataUrl).getAdmin()) {
            final String metaStoreName = metadataUrl.getIdentifier();
            final HTableDescriptor desc = hbaseAdmin.getTableDescriptor(TableName.valueOf(metaStoreName));
            String uuid = desc.getValue(HBaseConnection.HTABLE_UUID_TAG);
            if (uuid != null)
                return uuid;
            return UUID.randomUUID().toString();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String getMetaStoreUUID() throws IOException {
        if (!exists(ResourceStore.METASTORE_UUID_TAG)) {
            checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(createMetaStoreUUID()), 0,
                    StringEntity.serializer);
        }
        StringEntity entity = getResource(ResourceStore.METASTORE_UUID_TAG, StringEntity.serializer);
        return entity.toString();
    }

    @Override
    protected void visitFolderImpl(String folderPath, final boolean recursive, VisitFilter filter,
            final boolean loadContent, final Visitor visitor) throws IOException {

        visitFolder(folderPath, filter, loadContent, new FolderVisitor() {
            @Override
            public void visit(String childPath, String fullPath, Result hbaseResult) throws IOException {
                // is a direct child (not grand child)?
                boolean isDirectChild = childPath.equals(fullPath);

                if (isDirectChild || recursive) {
                    RawResource resource = rawResource(fullPath, hbaseResult, loadContent);
                    try {
                        visitor.visit(resource);
                    } finally {
                        resource.close();
                    }
                }
            }
        });
    }

    private void visitFolder(String folderPath, VisitFilter filter, boolean loadContent, FolderVisitor visitor)
            throws IOException {
        assert folderPath.startsWith("/");

        String folderPrefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        String lookForPrefix = folderPrefix;
        if (filter.hasPathPrefixFilter()) {
            Preconditions.checkArgument(filter.pathPrefix.startsWith(folderPrefix));
            lookForPrefix = filter.pathPrefix;
        }

        byte[] startRow = Bytes.toBytes(lookForPrefix);
        byte[] endRow = Bytes.toBytes(lookForPrefix);
        endRow[endRow.length - 1]++;

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(startRow, endRow);
        scan.addColumn(B_FAMILY, B_COLUMN_TS);
        if (loadContent) {
            scan.addColumn(B_FAMILY, B_COLUMN);
        }
        FilterList timeFilter = generateTimeFilterList(filter);
        if (timeFilter != null) {
            scan.setFilter(timeFilter);
        }

        tuneScanParameters(scan);

        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                String path = Bytes.toString(r.getRow());
                assert path.startsWith(lookForPrefix);
                int cut = path.indexOf('/', folderPrefix.length());
                String directChild = cut < 0 ? path : path.substring(0, cut);
                visitor.visit(directChild, path, r);
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    private void tuneScanParameters(Scan scan) {
        scan.setCaching(kylinConfig.getHBaseScanCacheRows());

        scan.setMaxResultSize(kylinConfig.getHBaseScanMaxResultSize());
        scan.setCacheBlocks(true);
    }

    interface FolderVisitor {
        void visit(String childPath, String fullPath, Result hbaseResult) throws IOException;
    }

    private RawResource rawResource(String path, Result hbaseResult, boolean loadContent) {
        long lastModified = getTimestamp(hbaseResult);
        if (loadContent) {
            try {
                return new RawResource(path, lastModified, getInputStream(path, hbaseResult));
            } catch (IOException ex) {
                return new RawResource(path, lastModified, ex); // let the caller handle broken content
            }
        } else {
            return new RawResource(path, lastModified);
        }
    }

    private FilterList generateTimeFilterList(VisitFilter visitFilter) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (visitFilter.lastModStart >= 0) { // NOTE: Negative value does not work in its binary form
            SingleColumnValueFilter timeStartFilter = new SingleColumnValueFilter(B_FAMILY, B_COLUMN_TS,
                    CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(visitFilter.lastModStart));
            filterList.addFilter(timeStartFilter);
        }
        if (visitFilter.lastModEndExclusive != Long.MAX_VALUE) {
            SingleColumnValueFilter timeEndFilter = new SingleColumnValueFilter(B_FAMILY, B_COLUMN_TS,
                    CompareFilter.CompareOp.LESS, Bytes.toBytes(visitFilter.lastModEndExclusive));
            filterList.addFilter(timeEndFilter);
        }
        return filterList.getFilters().isEmpty() ? null : filterList;
    }

    private InputStream getInputStream(String resPath, Result r) throws IOException {
        if (r == null) {
            return null;
        }
        byte[] value = r.getValue(B_FAMILY, B_COLUMN);
        if (value.length == 0) {
            return openPushdown(resPath);
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
        else {
            return rawResource(resPath, r, true);
        }
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        return getTimestamp(getFromHTable(resPath, false, true));
    }

    @Override
    protected void putSmallResource(String resPath, ContentWriter content, long ts) throws IOException {
        byte[] row = Bytes.toBytes(resPath);
        byte[] bytes = content.extractAllBytes();

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        RollbackablePushdown pushdown = null;
        try {
            if (bytes.length > kvSizeLimit) {
                pushdown = writePushdown(resPath, ContentWriter.create(bytes));
                bytes = BytesUtil.EMPTY_BYTE_ARRAY;
            }

            Put put = new Put(row);
            put.addColumn(B_FAMILY, B_COLUMN, bytes);
            put.addColumn(B_FAMILY, B_COLUMN_TS, Bytes.toBytes(ts));

            table.put(put);

        } catch (Exception ex) {
            if (pushdown != null)
                pushdown.rollback();
            throw ex;
        } finally {
            if (pushdown != null)
                pushdown.close();
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, IllegalStateException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        RollbackablePushdown pushdown = null;
        try {
            byte[] row = Bytes.toBytes(resPath);
            byte[] bOldTS = oldTS == 0 ? null : Bytes.toBytes(oldTS);

            if (content.length > kvSizeLimit) {
                logger.info("Length of content exceeds the limit of {} bytes, push down {} to HDFS", kvSizeLimit, resPath);
                pushdown = writePushdown(resPath, ContentWriter.create(content));
                content = BytesUtil.EMPTY_BYTE_ARRAY;
            }

            Put put = new Put(row);
            put.addColumn(B_FAMILY, B_COLUMN, content);
            put.addColumn(B_FAMILY, B_COLUMN_TS, Bytes.toBytes(newTS));

            boolean ok = table.checkAndPut(row, B_FAMILY, B_COLUMN_TS, bOldTS, put);
            logger.trace("Update row {} from oldTs: {}, to newTs: {}, operation result: {}", resPath, oldTS, newTS, ok);
            if (!ok) {
                long real = getResourceTimestampImpl(resPath);
                throw new WriteConflictException(
                        "Overwriting conflict " + resPath +
                                ", expect old TS " + oldTS +
                                ", but it is " + real +
                                ", the expected new TS: " + newTS);
            }

            return newTS;

        } catch (Exception ex) {
            if (pushdown != null)
                pushdown.rollback();
            throw ex;
        } finally {
            if (pushdown != null)
                pushdown.close();
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected void updateTimestampImpl(String resPath, long timestamp) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        try {
            boolean hdfsResourceExist = isHdfsResourceExist(table, resPath);
            long oldTS = getResourceLastModified(table, resPath);

            byte[] bOldTS = oldTS == 0 ? null : Bytes.toBytes(oldTS);
            byte[] row = Bytes.toBytes(resPath);
            Put put = new Put(row);
            put.addColumn(B_FAMILY, B_COLUMN_TS, Bytes.toBytes(timestamp));

            boolean ok = table.checkAndPut(row, B_FAMILY, B_COLUMN_TS, bOldTS, put);
            logger.trace("Update row {} from oldTs: {}, to newTs: {}, operation result: {}", resPath, oldTS, timestamp,
                    ok);
            if (!ok) {
                long real = getResourceTimestampImpl(resPath);
                throw new WriteConflictException(
                        "Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but it is " + real);
            }

            if (hdfsResourceExist) { // update timestamp in hdfs
                updateTimestampPushdown(resPath, timestamp);
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        try {
            boolean hdfsResourceExist = isHdfsResourceExist(table, resPath);

            Delete del = new Delete(Bytes.toBytes(resPath));
            table.delete(del);

            if (hdfsResourceExist) { // remove hdfs cell value
                deletePushdown(resPath);
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath, long timestamp) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        try {
            boolean hdfsResourceExist = isHdfsResourceExist(table, resPath);
            long origLastModified = getResourceLastModified(table, resPath);
            if (checkTimeStampBeforeDelete(origLastModified, timestamp)) {
                Delete del = new Delete(Bytes.toBytes(resPath));
                table.delete(del);

                if (hdfsResourceExist) { // remove hdfs cell value
                    deletePushdown(resPath);
                }
            } else {
                throw new IOException("Resource " + resPath + " timestamp not match, [originLastModified: "
                        + origLastModified + ", timestampToDelete: " + timestamp + "]");
            }

        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    // to avoid get Table twice time to improve delete performance
    private long getResourceLastModified(Table table, String resPath) throws IOException {
        return getTimestamp(internalGetFromHTable(table, resPath, false, true));
    }

    private boolean isHdfsResourceExist(Table table, String resPath) throws IOException {
        boolean hdfsResourceExist = false;
        Result result = internalGetFromHTable(table, resPath, true, false);
        if (result != null) {
            byte[] contentVal = result.getValue(B_FAMILY, B_COLUMN);
            if (contentVal != null && contentVal.length == 0) {
                hdfsResourceExist = true;
            }
        }

        return hdfsResourceExist;
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return tableName + "(key='" + resPath + "')@" + kylinConfig.getMetadataUrl();
    }

    private Result getFromHTable(String path, boolean fetchContent, boolean fetchTimestamp) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        try {
            return internalGetFromHTable(table, path, fetchContent, fetchTimestamp);
        } finally {
            IOUtils.closeQuietly(table);
        }

    }

    private Result internalGetFromHTable(Table table, String path, boolean fetchContent, boolean fetchTimestamp)
            throws IOException {
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

    @Override
    protected String pushdownRootPath() {
        String hdfsWorkingDirectory = this.kylinConfig.getHdfsWorkingDirectory(null);
        if (hdfsWorkingDirectory.endsWith("/"))
            return hdfsWorkingDirectory + "resources";
        else
            return hdfsWorkingDirectory + "/" + "resources";
    }

    @Override
    protected FileSystem pushdownFS() {
        return HadoopUtil.getFileSystem(new Path("/"), HBaseConnection.getCurrentHBaseConfiguration());
    }

    // visible for test
    Path bigCellHDFSPath(String resPath) {
        return super.pushdownPath(resPath);
    }

    @Override
    protected boolean isUnreachableException(Throwable ex) {
        return (super.isUnreachableException(ex) || ex instanceof SocketTimeoutException
                || ex instanceof ConnectException || ex instanceof RetriesExhaustedException);
    }

    @Override
    public String toString() {
        return tableName + "@hbase";
    }
}
