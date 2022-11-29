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
package org.apache.kylin.rest.security;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_KERBEROS_FILE;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.io.File;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.FileUtils;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class KerberosLoginManager {

    public static final String KEYTAB_SUFFIX = ".keytab";
    public static final String TMP_KEYTAB_SUFFIX = "_tmp.keytab";
    private static final Logger logger = LoggerFactory.getLogger(KerberosLoginManager.class);

    public static KerberosLoginManager getInstance() {
        return Singletons.getInstance(KerberosLoginManager.class);
    }

    public UserGroupInformation getProjectUGI(String projectName) {
        val config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);
        val project = projectManager.getProject(projectName);
        val principal = project.getPrincipal();
        val keytab = project.getKeytab();
        UserGroupInformation ugi = null;
        try {
            if (project.isProjectKerberosEnabled()) {
                val keytabPath = wrapAndDownloadKeytab(projectName);
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
            } else {
                ugi = UserGroupInformation.getLoginUser();
            }
        } catch (Exception e) {
            try {
                ugi = UserGroupInformation.getLoginUser();
            } catch (Exception ex) {
                logger.error("Fetch login user error.", projectName, principal, ex);
            }
            throw new KylinException(INVALID_KERBEROS_FILE, MsgPicker.getMsg().getKerberosInfoError(), e);
        }

        return ugi;
    }

    private String wrapAndDownloadKeytab(String projectName) throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);
        val project = projectManager.getProject(projectName);
        val principal = project.getPrincipal();
        val keytab = project.getKeytab();
        String keytabPath = null;
        if (project.isProjectKerberosEnabled()) {
            String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();
            keytabPath = new Path(kylinConfHome, principal + KEYTAB_SUFFIX).toString();
            File kFile = new File(keytabPath);
            if (!kFile.exists()) {
                FileUtils.decoderBase64File(keytab, keytabPath);
            }
        }
        return keytabPath;
    }

    public void checkKerberosInfo(String principal, String keytab) {
        try {
            UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        } catch (Exception e) {
            throw new KylinException(INVALID_KERBEROS_FILE, MsgPicker.getMsg().getKerberosInfoError(), e);
        }
    }

    public void checkAndReplaceProjectKerberosInfo(String project, String principal) throws Exception {
        String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();
        String keytab = new Path(kylinConfHome, principal + TMP_KEYTAB_SUFFIX).toString();
        checkKerberosInfo(principal, keytab);

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        if (!checkExistsTablesAccess(ugi, project)) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getProjectHivePermissionError());
        }
    }

    @VisibleForTesting
    boolean checkExistsTablesAccess(UserGroupInformation ugi, String project) {
        val kapConfig = KapConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(kapConfig.getKylinConfig());
        return ugi.doAs((PrivilegedAction<Boolean>) () -> {
            ProjectInstance projectInstance = projectManager.getProject(project);
            val tables = projectInstance.getTables();
            AtomicBoolean accessible = new AtomicBoolean(true);
            val tableMap = tables.stream().map(tableName -> {
                NTableMetadataManager tableMetadataManager = NTableMetadataManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), project);
                return tableMetadataManager.getTableDesc(tableName);
            }).collect(Collectors.groupingBy(TableDesc::getSourceType));

            tableMap.forEach((sourceType, tableDescSet) -> {
                ISourceMetadataExplorer explorer = SourceFactory.getSource(sourceType, projectInstance.getConfig())
                        .getSourceMetadataExplorer();
                accessible.set(accessible.get() && explorer.checkTablesAccess(
                        tableDescSet.stream().map(tableDesc -> tableDesc.getIdentity()).collect(Collectors.toSet())));
            });
            return accessible.get();
        });
    }

}
