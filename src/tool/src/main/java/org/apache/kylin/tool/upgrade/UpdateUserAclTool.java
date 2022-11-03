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
package org.apache.kylin.tool.upgrade;

import static org.apache.kylin.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.naming.directory.SearchControls;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.upgrade.GlobalAclVersion;
import org.apache.kylin.metadata.upgrade.GlobalAclVersionManager;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.security.KylinPermissionGrantingStrategy;
import org.apache.kylin.rest.security.UserAcl;
import org.apache.kylin.rest.security.UserAclManager;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.tool.MaintainModeTool;
import org.apache.kylin.tool.util.LdapUtils;
import org.springframework.security.acls.domain.ConsoleAuditLogger;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateUserAclTool extends ExecutableApplication {
    public static final String UPGRADE = "upgrade";
    public static final String ROLLBACK = "rollback";

    private static final Option OPTION_SCRIPT = OptionBuilder.getInstance().hasArg(true)
            .withDescription("the identify of script").isRequired(true).withLongOpt("script").create("s");
    private static final Option OPTION_OLD_KYLIN_VERSION = OptionBuilder.getInstance().hasArg(true)
            .withDescription("old kylin version").isRequired(false).withLongOpt("old_kylin_version").create("v");
    private static final Option OPTION_KYLIN_HOME = OptionBuilder.getInstance().hasArg(true)
            .withDescription("kylin home").isRequired(true).withLongOpt("kylin_home").create("h");
    private static final Option OPTION_ROLLBACK = OptionBuilder.getInstance().hasArg(false)
            .withDescription("rollback the user acl").isRequired(false).withLongOpt(ROLLBACK).create("r");
    private static final Option OPTION_FORCE_UPGRADE = OptionBuilder.getInstance().hasArg(false)
            .withDescription("force upgrade").isRequired(false).withLongOpt("force").create("f");
    private static final Pattern VERSION_PATTERN = Pattern.compile("\\d+\\.\\d+.*");

    public static void main(String[] args) {
        try {
            UpdateUserAclTool tool = new UpdateUserAclTool();
            if (tool.matchUpgradeCondition(args)) {
                log.info("Start update user acl metadata.");
                tool.execute(args);
                log.info("Update user acl metadata successfully.");
            } else {
                log.info("Skip update the acl metadata.");
            }
        } catch (Throwable e) {
            log.error("Update user acl metadata failed.", e);
            systemExitWhenMainThread(1);
        }
        systemExitWhenMainThread(0);
    }

    public boolean matchUpgradeCondition(String[] args) {
        val config = KylinConfig.getInstanceFromEnv();
        if (config.isQueryNodeOnly()) {
            log.info("Only job/all node can update user acl.");
            return false;
        }
        OptionsHelper optionsHelper = convertToOptionsHelper(args);
        val scriptIdentify = optionsHelper.getOptionValue(OPTION_SCRIPT);
        log.info("scriptIdentify={}", scriptIdentify);
        if ("migrate".equalsIgnoreCase(scriptIdentify)) {
            val oldKylinVersion = optionsHelper.getOptionValue(OPTION_OLD_KYLIN_VERSION);
            val newKylinHome = optionsHelper.getOptionValue(OPTION_KYLIN_HOME);
            log.info("oldKylinVersion={}", oldKylinVersion);
            log.info("newKylinHome={}", newKylinHome);
            if (!VERSION_PATTERN.matcher(oldKylinVersion.trim()).matches()) {
                throw new IllegalArgumentException("oldKylinVersion=" + oldKylinVersion);
            }
            return (isDataPermissionSeparateVersion(newKylinHome) && compareVersion(oldKylinVersion, "4.2") >= 0
                    && compareVersion(oldKylinVersion, "4.5.19") < 0);
        }
        if (UPGRADE.equalsIgnoreCase(scriptIdentify)) {
            try {
                val oldKylinHome = optionsHelper.getOptionValue(OPTION_KYLIN_HOME);
                log.info("kylinHome={}", oldKylinHome);
                val versionFile = new File(oldKylinHome + "/VERSION");
                val version = parseVersion(FileUtils.readFileToString(versionFile, StandardCharsets.UTF_8));
                log.info("version={}", version);
                return (versionFile.exists() && compareVersion(version, "4.2") >= 0
                        && !isDataPermissionSeparateVersion(oldKylinHome));
            } catch (Exception e) {
                log.error("matchUpgradeCondition error", e);
                return false;
            }
        }
        throw new IllegalArgumentException("unknown script identify=" + scriptIdentify);
    }

    private boolean isDataPermissionSeparateVersion(String kylinHome) {
        try {
            String command = String.format(Locale.ROOT, "grep %s %s/server/jars/*.jar", UserAcl.class.getName(),
                    kylinHome);
            log.info("command = {}", command);
            CliCommandExecutor exec = new CliCommandExecutor();
            val result = exec.execute(command, null);
            return result.getCode() == 0;
        } catch (ShellException e) {
            log.warn("execute command failed.", e);
            return false;
        }
    }

    @Override
    protected Options getOptions() {
        val options = new Options();
        options.addOption(OPTION_SCRIPT);
        options.addOption(OPTION_OLD_KYLIN_VERSION);
        options.addOption(OPTION_KYLIN_HOME);
        options.addOption(OPTION_ROLLBACK);
        options.addOption(OPTION_FORCE_UPGRADE);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (isUpgraded() && !optionsHelper.hasOption(OPTION_ROLLBACK)
                && !optionsHelper.hasOption(OPTION_FORCE_UPGRADE)) {
            log.info("The acl related metadata have been upgraded.");
            return;
        }
        MaintainModeTool maintainModeTool = new MaintainModeTool("update user acl");
        try {
            maintainModeTool.init();
            maintainModeTool.markEpochs();

            if (optionsHelper.hasOption(OPTION_ROLLBACK)) {
                updateAdminUserAcl(ROLLBACK, getAdminUsers());
                updateProjectAcl(ROLLBACK);
                removeAclVersion();
            } else {
                updateAdminUserAcl(UPGRADE, getAdminUsers());
                updateProjectAcl(UPGRADE);
                addAclVersion();
            }
        } finally {
            maintainModeTool.releaseEpochs();
        }
    }

    public boolean isAdminUserUpgraded() {
        val userAclManager = UserAclManager.getInstance(KylinConfig.getInstanceFromEnv());
        return userAclManager.listAclUsernames().size() > 0;
    }

    public boolean isUpgraded() {
        val versionManager = GlobalAclVersionManager.getInstance(KylinConfig.getInstanceFromEnv());
        return versionManager.exists();
    }

    private Set<String> getAdminUsers() {
        val config = KylinConfig.getInstanceFromEnv();
        val profile = config.getSecurityProfile().toLowerCase(Locale.ROOT);
        final Set<String> adminUsers = new HashSet<>();
        switch (profile) {
        case "testing":
            adminUsers.addAll(getKylinAdminUsers());
            break;
        case "ldap":
            adminUsers.addAll(getLdapAdminUsers());
            break;
        case "custom":
            break;
        default:
            throw new IllegalArgumentException("Unsupported kylin.security.profile=" + profile);
        }
        return adminUsers;
    }

    public void updateAdminUserAcl(String operation, Set<String> adminUsers) {
        if (adminUsers.isEmpty()) {
            return;
        }
        log.info("start to {} query permission for system admin user.", operation);

        val defaultUserAcl = new UserAcl("");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val userAclManager = UserAclManager.getInstance(KylinConfig.getInstanceFromEnv());
            adminUsers.forEach(user -> {
                val userAcl = Optional.ofNullable(userAclManager.get(user)).orElse(defaultUserAcl);
                if (UPGRADE.equalsIgnoreCase(operation) && !userAcl.hasPermission(AclPermission.DATA_QUERY)) {
                    log.info("add query permission for sys admin user {}", user);
                    userAclManager.add(user);
                } else if (ROLLBACK.equalsIgnoreCase(operation) && userAcl.hasPermission(AclPermission.DATA_QUERY)) {
                    log.info("remove query permission for sys admin user {}", user);
                    userAclManager.delete(user);
                }
            });
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1);
        log.info("{} query permission for system admin user successfully.", StringUtils.capitalize(operation));
    }

    private static List<String> getKylinAdminUsers() {
        val userManager = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
        return userManager.list().stream()
                .filter(user -> user.getAuthorities().contains(new SimpleGrantedAuthority(Constant.ROLE_ADMIN)))
                .map(user -> user.getUsername()).collect(Collectors.toList());
    }

    public Set<String> getLdapAdminUsers() {
        val ldapTemplate = createLdapTemplate();
        val ldapUserDNs = LdapUtils.getAllGroupMembers(ldapTemplate,
                KylinConfig.getInstanceFromEnv().getLDAPAdminRole());
        val searchControls = new SearchControls();
        searchControls.setSearchScope(2);
        Map<String, String> dnMapperMap = LdapUtils.getAllValidUserDnMap(ldapTemplate, searchControls);
        val users = new HashSet<String>();
        for (String u : ldapUserDNs) {
            Optional.ofNullable(dnMapperMap.get(u)).ifPresent(users::add);
        }
        return users;
    }

    public static boolean isCustomProfile() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        return "custom".equals(kylinConfig.getSecurityProfile());
    }

    private SpringSecurityLdapTemplate createLdapTemplate() {
        val properties = KylinConfig.getInstanceFromEnv().exportToProperties();
        val contextSource = new DefaultSpringSecurityContextSource(
                properties.getProperty("kylin.security.ldap.connection-server"));
        contextSource.setUserDn(properties.getProperty("kylin.security.ldap.connection-username"));
        contextSource.setPassword(getPassword(properties));
        contextSource.afterPropertiesSet();
        return new SpringSecurityLdapTemplate(contextSource);
    }

    public String getPassword(Properties properties) {
        val password = properties.getProperty("kylin.security.ldap.connection-password");
        return EncryptUtil.decrypt(password);
    }

    public void updateProjectAcl(String operation) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val aclManager = createAclManager();
            val aclRecordList = aclManager.listAll();

            aclRecordList.forEach(aclRecord -> {
                log.info("start to {} query permission for _global/acl/{}.", operation, aclRecord.getUuid());
                val entries = aclRecord.getEntries();
                val sidPermissionMap = new HashMap<Sid, Permission>();
                entries.forEach(ace -> {
                    val hasQueryPermission = AclPermissionUtil.hasQueryPermission(ace.getPermission());
                    if (UPGRADE.equalsIgnoreCase(operation) && !hasQueryPermission) {
                        sidPermissionMap.putIfAbsent(ace.getSid(),
                                AclPermissionUtil.addExtPermission(ace.getPermission(), AclPermission.DATA_QUERY));
                    } else if (ROLLBACK.equalsIgnoreCase(operation) && hasQueryPermission) {
                        sidPermissionMap.putIfAbsent(ace.getSid(),
                                AclPermissionUtil.convertToBasePermission(ace.getPermission()));
                    }
                });
                val mutableAclRecord = aclManager.readAcl(aclRecord.getDomainObjectInfo());
                aclManager.batchUpsertAce(mutableAclRecord, sidPermissionMap);
                log.info("{} query permission for _global/acl/{} successfully.", StringUtils.capitalize(operation),
                        aclRecord.getUuid());
            });
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    public void addAclVersion() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val versionManager = GlobalAclVersionManager.getInstance(KylinConfig.getInstanceFromEnv());
            GlobalAclVersion version = new GlobalAclVersion();
            version.setAclVersion(GlobalAclVersion.DATA_PERMISSION_SEPARATE);
            versionManager.save(version);
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    public void removeAclVersion() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val versionManager = GlobalAclVersionManager.getInstance(KylinConfig.getInstanceFromEnv());
            versionManager.delete();
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    private AclManager createAclManager() {
        val permissionFactory = new AclPermissionFactory();
        val permissionGrantingStrategy = new KylinPermissionGrantingStrategy(new ConsoleAuditLogger());
        val config = KylinConfig.getInstanceFromEnv();
        return new AclManager(config, permissionFactory, permissionGrantingStrategy);
    }

    public String parseVersion(String versionText) {
        int versionStartIdx = versionText.indexOf("Enterprise");
        int versionEndIdx = versionText.lastIndexOf("-") == -1 ? versionText.length() : versionText.lastIndexOf("-");
        return versionText.substring(versionStartIdx + 11, versionEndIdx).trim();
    }

    public int compareVersion(String version1, String version2) {
        if (StringUtils.isEmpty(version1)) {
            return -1;
        }
        if (StringUtils.isEmpty(version2)) {
            return 1;
        }
        String[] version1Array = version1.split("[._]");
        String[] version2Array = version2.split("[._]");
        int minLen = Math.min(version1Array.length, version2Array.length);
        long diff = 0;
        int index = 0;
        while (index < minLen
                && (diff = Long.parseLong(version1Array[index]) - Long.parseLong(version2Array[index])) == 0) {
            index++;
        }
        if (diff != 0) {
            return diff > 0 ? 1 : -1;
        }

        for (int i = index; i < version1Array.length; i++) {
            if (Long.parseLong(version1Array[i]) > 0) {
                return 1;
            }
        }

        for (int i = index; i < version2Array.length; i++) {
            if (Long.parseLong(version2Array[i]) > 0) {
                return -1;
            }
        }
        return 0;
    }
}