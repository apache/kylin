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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

public class JDBCResourceStore extends ResourceStore {

    private static final String JDBC_SCHEME = "jdbc";

    private String[] tablesName = new String[2];

    private JDBCResourceDAO resourceDAO;

    public JDBCResourceStore(KylinConfig kylinConfig) throws SQLException {
        super(kylinConfig);
        StorageURL metadataUrl = kylinConfig.getMetadataUrl();
        checkScheme(metadataUrl);
        tablesName[0] = metadataUrl.getIdentifier();
        tablesName[1] = metadataUrl.getIdentifier() + "1";
        this.resourceDAO = new JDBCResourceDAO(kylinConfig, tablesName);
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        try {
            return resourceDAO.existResource(resPath);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        return getResourceImpl(resPath, false);
    }

    protected RawResource getResourceImpl(String resPath, final boolean isAllowBroken) throws IOException {
        try {
            JDBCResource resource = resourceDAO.getResource(resPath, true, true, isAllowBroken);
            if (resource != null)
                return new RawResource(resource.getContent(), resource.getTimestamp());
            else
                return null;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        try {
            JDBCResource resource = resourceDAO.getResource(resPath, false, true);
            if (resource != null) {
                return resource.getTimestamp();
            } else {
                return 0L;
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, boolean recursive) throws IOException {
        try {
            final TreeSet<String> result = resourceDAO.listAllResource(makeFolderPath(folderPath), recursive);
            return result.isEmpty() ? null : result;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive)
            throws IOException {
        return getAllResourcesImpl(folderPath, timeStart, timeEndExclusive, false);
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive,
                                                    final boolean isAllowBroken) throws IOException {
        final List<RawResource> result = Lists.newArrayList();
        try {
            List<JDBCResource> allResource = resourceDAO.getAllResource(makeFolderPath(folderPath), timeStart,
                    timeEndExclusive, isAllowBroken);
            for (JDBCResource resource : allResource) {
                result.add(new RawResource(resource.getContent(), resource.getTimestamp()));
            }
            return result;
        } catch (SQLException e) {
            for (RawResource rawResource : result) {
                IOUtils.closeQuietly(rawResource.inputStream);
            }
            throw new IOException(e);
        }
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        try {
            JDBCResource resource = new JDBCResource(resPath, ts, content);
            resourceDAO.putResource(resource);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, WriteConflictException {
        try {
            resourceDAO.checkAndPutResource(resPath, content, oldTS, newTS);
            return newTS;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        try {
            resourceDAO.deleteResource(resPath);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return tablesName + "(key='" + resPath + "')@" + kylinConfig.getMetadataUrl();
    }

    private String makeFolderPath(String folderPath) {
        Preconditions.checkState(folderPath.startsWith("/"));
        String lookForPrefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        return lookForPrefix;
    }

    protected JDBCResourceDAO getResourceDAO() {
        return resourceDAO;
    }

    public long getQueriedSqlNum() {
        return resourceDAO.getQueriedSqlNum();
    }

    public static void checkScheme(StorageURL url) {
        Preconditions.checkState(JDBC_SCHEME.equals(url.getScheme()));
    }
}