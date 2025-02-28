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

package org.apache.kylin.rest.source;

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CHECK_KERBEROS;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.code.ErrorCodeSystem.QUERY_NODE_API_INVALID;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.response.NHiveTableNameResponse;
import org.apache.kylin.rest.security.KerberosLoginManager;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.SparderEnv;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataSourceState implements Runnable {

    private static final String JDBC_SOURCE_KEY_PREFIX = "project#";
    private static final String SOURCE_KEY_PREFIX = "ugi#";

    private final StopWatch sw;
    private final Map<String, NHiveSourceInfo> cache;
    private final Map<String, Boolean> runningStateMap;
    private final Map<String, Long> lastLoadTimeMap;
    private ISourceMetadataExplorer explore;

    //only jdbc source use project name as key
    private static final Set<Integer> USE_PROJECT_AS_KEY_SOURCE_TYPE = Sets.newHashSet(8);

    public static DataSourceState getInstance() {
        return Singletons.getInstance(DataSourceState.class);
    }

    private DataSourceState() {
        setExplore(SourceFactory.getSparkSource().getSourceMetadataExplorer());
        this.cache = Maps.newConcurrentMap();
        this.runningStateMap = Maps.newConcurrentMap();
        this.lastLoadTimeMap = Maps.newConcurrentMap();
        this.sw = StopWatch.create();
    }

    @Override
    public void run() {
        try {
            int waitSeconds = 0;
            while (!SparderEnv.isSparkAvailable() && KylinConfig.getInstanceFromEnv().getKerberosProjectLevelEnable()) {
                if (waitSeconds >= KylinConfig.getInstanceFromEnv().getLoadHiveTableWaitSparderSeconds()) {
                    log.warn("Skip wait sparder start, wait seconds :{}", waitSeconds);
                    return;
                }
                startSparder();
                log.info("Wait sparder start");
                Integer intervals = KylinConfig.getInstanceFromEnv().getLoadHiveTableWaitSparderIntervals();
                TimeUnit.SECONDS.sleep(intervals);
                waitSeconds = waitSeconds + intervals;
            }
            loadAllSourceInfoToCache();
        } catch (InterruptedException ex) {
            log.info("thread interrupted while wait sparder start");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Scheduling refresh of hive table name cache failed", e);
        } finally {
            sw.reset();
        }
    }

    private void startSparder() {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            SparderEnv.init();
        }
    }

    /**
     * load all source info to cache
     */
    public void loadAllSourceInfoToCache() throws IOException {
        sw.start();
        checkIsAllNode();
        log.info("start load all table name to cache");

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        KerberosLoginManager kerberosManager = KerberosLoginManager.getInstance();

        Map<String, Pair<ProjectInstance, UserGroupInformation>> ugiMap = Maps.newLinkedHashMap();
        ugiMap.put(SOURCE_KEY_PREFIX + UserGroupInformation.getLoginUser().getUserName(),
                Pair.newPair(null, UserGroupInformation.getLoginUser()));

        projectManager.listAllProjects().stream().filter(p -> StringUtils.isNotBlank(p.getPrincipal()))
                .forEach(projectInstance -> {
                    try {
                        UserGroupInformation projectUgi = kerberosManager.getProjectUGI(projectInstance.getName());
                        ugiMap.put(getCacheKeyByProject(projectInstance), Pair.newPair(projectInstance, projectUgi));
                    } catch (Exception e) {
                        log.error("The kerberos information of the project {} is incorrect.",
                                projectInstance.getName());
                    }
                });

        ugiMap.forEach((cacheKey, pair) -> {
            ProjectInstance projectInstance = pair.getFirst();
            UserGroupInformation projectUgi = pair.getSecond();
            runningStateMap.put(cacheKey, true);
            List<String> tableFilterList = getHiveFilterList(projectInstance);
            NHiveSourceInfo sourceInfo = fetchUgiSourceInfo(projectUgi, tableFilterList);
            putCache(cacheKey, sourceInfo, tableFilterList);
            runningStateMap.put(cacheKey, false);
            lastLoadTimeMap.put(cacheKey, System.currentTimeMillis());
        });

        sw.stop();
        log.info("Load hive table name successful within {} second", sw.getTime(TimeUnit.SECONDS));
    }

    public NHiveTableNameResponse loadAllSourceInfoToCacheForced(String project, boolean force) {
        log.info("Load hive tables immediately {}, force: {}", project, force);
        checkIsAllNode();
        checkKerberosInfo(project);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        NHiveTableNameResponse response = new NHiveTableNameResponse();
        String cacheKey = getCacheKeyByProject(projectInstance);
        runningStateMap.putIfAbsent(cacheKey, false);
        lastLoadTimeMap.putIfAbsent(cacheKey, 0L);

        if (!force) {
            response.setIsRunning(runningStateMap.get(cacheKey));
            response.setTime(System.currentTimeMillis() - lastLoadTimeMap.get(cacheKey));
            return response;
        }
        setExplore(SourceFactory.getSource(projectInstance).getSourceMetadataExplorer());

        KerberosLoginManager kerberosManager = KerberosLoginManager.getInstance();
        UserGroupInformation projectUGI = kerberosManager.getProjectUGI(project);
        runningStateMap.put(cacheKey, true);
        List<String> tableFilterList = getHiveFilterList(projectInstance);
        NHiveSourceInfo sourceInfo = fetchUgiSourceInfo(projectUGI, tableFilterList);
        putCache(cacheKey, sourceInfo, tableFilterList);
        runningStateMap.put(cacheKey, false);
        response.setIsRunning(runningStateMap.get(cacheKey));
        response.setTime(0L);
        return response;
    }

    public synchronized List<String> getTables(String project, String db) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);

        List<String> result = Lists.newArrayList();
        NHiveSourceInfo sourceInfo = cache.get(getCacheKeyByProject(projectInstance));
        if (Objects.nonNull(sourceInfo) && Objects.nonNull(sourceInfo.getDatabaseInfo(db))) {
            result.addAll(sourceInfo.getDatabaseInfo(db));
        }
        return result;
    }

    public synchronized void putCache(String cacheKey, NHiveSourceInfo sourceInfo, List<String> tableFilterList) {
        if (CollectionUtils.isNotEmpty(tableFilterList)) {
            NHiveSourceInfo info = cache.get(cacheKey);
            if (!checkSourceInfoEmpty(sourceInfo) && !checkSourceInfoEmpty(info)) {
                info.getTables().keySet().forEach(db -> {
                    if (CollectionUtils.isEmpty(sourceInfo.getDatabaseInfo(db))) {
                        if (tableFilterList.contains(db)) {
                            return;
                        }
                        sourceInfo.putDatabaseInfo(db, info.getDatabaseInfo(db));
                    }
                });
            }
        }
        cache.put(cacheKey, sourceInfo);
    }

    private boolean checkSourceInfoEmpty(NHiveSourceInfo sourceInfo) {
        return sourceInfo == null || sourceInfo.getTables().isEmpty();
    }

    private String getCacheKeyByProject(ProjectInstance projectInstance) {
        String projectName = projectInstance.getName();
        if (USE_PROJECT_AS_KEY_SOURCE_TYPE.contains(projectInstance.getSourceType())) {
            return JDBC_SOURCE_KEY_PREFIX + projectName;
        } else {
            return SOURCE_KEY_PREFIX + KerberosLoginManager.getInstance().getProjectUGI(projectName).getUserName();
        }
    }

    public NHiveSourceInfo fetchUgiSourceInfo(UserGroupInformation ugi, List<String> filterList) {
        log.info("Load hive tables from ugi {}", ugi.getUserName());
        NHiveSourceInfo sourceInfo;
        if (UserGroupInformation.isSecurityEnabled()) {
            sourceInfo = ugi.doAs((PrivilegedAction<NHiveSourceInfo>) () -> fetchSourceInfo(filterList));
        } else {
            sourceInfo = fetchSourceInfo(filterList);
        }
        return sourceInfo;
    }

    private NHiveSourceInfo fetchSourceInfo(List<String> filterList) {
        NHiveSourceInfo sourceInfo = new NHiveSourceInfo();
        try {
            List<String> databaseList = explore.listDatabases().stream().map(StringUtils::toRootUpperCase)
                    .filter(database -> CollectionUtils.isEmpty(filterList) || filterList.contains(database))
                    .collect(Collectors.toList());
            Map<String, List<String>> dbTableList = listTables(databaseList);
            sourceInfo.setTables(dbTableList);
        } catch (Exception e) {
            log.error("Load hive tables error.", e);
        }
        return sourceInfo;
    }

    private Map<String, List<String>> listTables(List<String> databaseList) throws Exception {
        HashMap<String, List<String>> dbTableList = Maps.newHashMap();
        int databaseTotalSize = databaseList.size();

        for (String database : databaseList) {
            if (!explore.checkDatabaseAccess(database)) {
                continue;
            }
            List<String> tableList = explore.listTables(database).stream().map(StringUtils::toRootUpperCase)
                    .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(tableList)) {
                dbTableList.put(database, tableList);
            }
            int currDatabaseSize = dbTableList.keySet().size();
            if (currDatabaseSize % 20 == 0) {
                log.info("Foreach database curr pos {}, total num {}", currDatabaseSize, databaseTotalSize);
            }
        }
        return dbTableList;
    }

    private void checkKerberosInfo(String project) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        ProjectInstance projectInstance = projectManager.getProject(project);

        String principal = projectInstance.getPrincipal();
        String keytab = projectInstance.getKeytab();
        try {
            if (kylinConfig.getKerberosProjectLevelEnable() && !StringUtils.isAllBlank(principal, keytab)) {
                String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();

                String keyTabPath = new Path(kylinConfHome, principal.concat(KerberosLoginManager.KEYTAB_SUFFIX))
                        .toString();
                File keyTabFile = new File(keyTabPath);
                if (!keyTabFile.exists()) {
                    FileUtils.writeStringToFile(keyTabFile, keytab);
                }
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTabPath);
            }
        } catch (Exception e) {
            throw new KylinException(FAILED_CHECK_KERBEROS,
                    "The project " + project + " kerberos information has expired.");
        }
    }

    /**
     * get filter list from kylin.source.hive.databases
     */
    public List<String> getHiveFilterList(ProjectInstance projectInstance) {
        if (Objects.isNull(projectInstance)) {
            return Collections.emptyList();
        }
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String[] databases = projectInstance.getConfig().getHiveDatabases();
        if (databases.length == 0) {
            databases = config.getHiveDatabases();
        }
        return Arrays.stream(databases).map(str -> str.toUpperCase(Locale.ROOT)).collect(Collectors.toList());
    }

    private void checkIsAllNode() {
        if (!KylinConfig.getInstanceFromEnv().isJobNode() && !KylinConfig.getInstanceFromEnv().isMetadataNode()) {
            throw new KylinException(QUERY_NODE_API_INVALID);
        }
        if (!KylinConfig.getInstanceFromEnv().getLoadHiveTablenameEnabled()) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getInvalidLoadHiveTableName());
        }
    }

    private synchronized void setExplore(ISourceMetadataExplorer explore) {
        this.explore = explore;
    }

}
