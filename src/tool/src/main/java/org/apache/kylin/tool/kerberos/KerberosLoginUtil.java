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
package org.apache.kylin.tool.kerberos;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.log4j.Logger;

import sun.security.krb5.internal.ktab.KeyTab;

public class KerberosLoginUtil {
    /**
     * java security login file path
     */
    public static final String JAVA_SECURITY_LOGIN_CONF_KEY = "java.security.auth.login.config";
    private static final Logger LOG = Logger.getLogger(KerberosLoginUtil.class);

    /**
     * line operator string
     */
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    /**
     * jaas file postfix
     */
    private static final String JAAS_POSTFIX = ".jaas.conf";

    /**
     * IBM jdk login module
     */
    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";

    /**
     * oracle jdk login module
     */
    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";
    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static final String LOGIN_FAILED_CAUSE_PSD_WRONG = "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to check";
    private static final String LOGIN_FAILED_CAUSE_TIME_WRONG = "(clock skew) time of local server and remote server not match, please check ntp to remote server";
    private static final String LOGIN_FAILED_CAUSE_AES256_WRONG = "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";
    private static final String LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG = "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in core-site.xml) value RULE:[1:$1] RULE:[2:$1]";
    private static final String LOGIN_FAILED_CAUSE_TIME_OUT = "(time out) can not connect to kdc server or there is fire wall in the network";
    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    public static synchronized void login(String userPrincipal, String userKeytabPath, String krb5ConfPath,
            Configuration conf) throws IOException {
        // 1.check input parameters
        if ((userPrincipal == null) || (userPrincipal.length() <= 0)) {
            LOG.error("input userPrincipal is invalid.");
            throw new IOException("input userPrincipal is invalid.");
        }

        if ((userKeytabPath == null) || (userKeytabPath.length() <= 0)) {
            LOG.error("input userKeytabPath is invalid.");
            throw new IOException("input userKeytabPath is invalid.");
        }

        if ((krb5ConfPath == null) || (krb5ConfPath.length() <= 0)) {
            LOG.error("input krb5ConfPath is invalid.");
            throw new IOException("input krb5ConfPath is invalid.");
        }

        if ((conf == null)) {
            LOG.error("input conf is invalid.");
            throw new IOException("input conf is invalid.");
        }

        // 2.check file exsits
        File userKeytabFile = new File(userKeytabPath);
        String userKeytabFilename = "userKeytabFile(" + userKeytabFile.getAbsolutePath() + ")";
        if (!userKeytabFile.exists()) {
            String message = userKeytabFilename + " does not exist.";
            LOG.error(message);
            throw new IOException(message);
        }
        if (!userKeytabFile.isFile()) {
            String message = userKeytabFilename + " is not a file.";
            LOG.error(message);
            throw new IOException(message);
        }

        File krb5ConfFile = new File(krb5ConfPath);
        String krb5ConfFilename = "krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ")";
        if (!krb5ConfFile.exists()) {
            String message = krb5ConfFilename + " does not exist.";
            LOG.error(message);
            throw new IOException(message);
        }
        if (!krb5ConfFile.isFile()) {
            String message = krb5ConfFilename + " is not a file.";
            LOG.error(message);
            throw new IOException(message);
        }

        // 3.set and check krb5config
        setKrb5Config(krb5ConfFile.getAbsolutePath());
        setConfiguration(conf);

        // 4.login and check for hadoop
        loginHadoop(userPrincipal, userKeytabFile.getAbsolutePath());
        LOG.info("Login fi success!!!!!!!!!!!!!!");
    }

    private static void setConfiguration(Configuration conf) {
        UserGroupInformation.setConfiguration(conf);
    }

    public static void setJaasFile(String principal, String keytabPath) throws IOException {
        String jaasPath = new File(System.getProperty("java.io.tmpdir")) + File.separator
                + System.getProperty("user.name") + JAAS_POSTFIX;

        // windows路径下分隔符替换
        jaasPath = jaasPath.replace("\\", "\\\\");
        keytabPath = keytabPath.replace("\\", "\\\\");
        // 删除jaas文件
        deleteJaasFile(jaasPath);
        writeJaasFile(jaasPath, principal, keytabPath);
        Unsafe.setProperty(JAVA_SECURITY_LOGIN_CONF_KEY, jaasPath);
    }

    private static void writeJaasFile(String jaasPath, String principal, String keytabPath) throws IOException {
        try (OutputStream os = Files.newOutputStream(Paths.get(jaasPath));
             BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(os, Charset.defaultCharset().name()))) {
            writer.write(getJaasConfContext(principal, keytabPath));
            writer.flush();
        } catch (IOException e) {
            throw new IOException("Failed to create jaas.conf File");
        }
    }

    private static void deleteJaasFile(String jaasPath) throws IOException {
        File jaasFile = new File(jaasPath);
        if (jaasFile.exists()) {
            Files.delete(jaasFile.toPath());
        }
    }

    private static String getJaasConfContext(String principal, String keytabPath) {
        KerberosLoginUtil.Module[] allModule = KerberosLoginUtil.Module.values();
        StringBuilder builder = new StringBuilder();
        for (KerberosLoginUtil.Module modlue : allModule) {
            builder.append(getModuleContext(principal, keytabPath, modlue));
        }
        return builder.toString();
    }

    private static String getModuleContext(String userPrincipal, String keyTabPath, KerberosLoginUtil.Module module) {
        StringBuilder builder = new StringBuilder();
        if (IS_IBM_JDK) {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(IBM_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("credsType=both").append(LINE_SEPARATOR);
            builder.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);
            builder.append("useKeytab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);
            builder.append("debug=true;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        } else {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("useKeyTab=true").append(LINE_SEPARATOR);
            builder.append("keyTab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);
            builder.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);
            builder.append("useTicketCache=false").append(LINE_SEPARATOR);
            builder.append("storeKey=true").append(LINE_SEPARATOR);
            builder.append("debug=true;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        }

        return builder.toString();
    }

    public static void setJaasConf(String loginContextName, String principal, String keytabFile) throws IOException {
        Preconditions.checkArgument((loginContextName == null) || (loginContextName.length() <= 0),
                "input loginContextName is invalid.");
        Preconditions.checkArgument((principal == null) || (principal.length() <= 0),
                "input principal is invalid.");
        Preconditions.checkArgument((keytabFile == null) || (keytabFile.length() <= 0),
                "input keytabFile is invalid.");

        File userKeytabFile = new File(keytabFile);
        if (!userKeytabFile.exists()) {
            LOG.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
        }

        javax.security.auth.login.Configuration.setConfiguration(
                new KerberosLoginUtil.JaasConfiguration(loginContextName, principal, userKeytabFile.getAbsolutePath()));

        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        if (!(conf instanceof KerberosLoginUtil.JaasConfiguration)) {
            LOG.error("javax.security.auth.login.Configuration is not JaasConfiguration.");
            throw new IOException("javax.security.auth.login.Configuration is not JaasConfiguration.");
        }

        AppConfigurationEntry[] entrys = conf.getAppConfigurationEntry(loginContextName);
        if (entrys == null) {
            LOG.error("javax.security.auth.login.Configuration has no AppConfigurationEntry named " + loginContextName
                    + ".");
            throw new IOException("javax.security.auth.login.Configuration has no AppConfigurationEntry named "
                    + loginContextName + ".");
        }

        boolean checkPrincipal = false;
        boolean checkKeytab = false;
        for (int i = 0; i < entrys.length; i++) {
            if (entrys[i].getOptions().get("principal").equals(principal)) {
                checkPrincipal = true;
            }

            if (IS_IBM_JDK) {
                if (entrys[i].getOptions().get("useKeytab").equals(keytabFile)) {
                    checkKeytab = true;
                }
            } else {
                if (entrys[i].getOptions().get("keyTab").equals(keytabFile)) {
                    checkKeytab = true;
                }
            }

        }

        if (!checkPrincipal) {
            LOG.error("AppConfigurationEntry named " + loginContextName + " does not have principal value of "
                    + principal + ".");
            throw new IOException("AppConfigurationEntry named " + loginContextName
                    + " does not have principal value of " + principal + ".");
        }

        if (!checkKeytab) {
            LOG.error("AppConfigurationEntry named " + loginContextName + " does not have keyTab value of " + keytabFile
                    + ".");
            throw new IOException("AppConfigurationEntry named " + loginContextName + " does not have keyTab value of "
                    + keytabFile + ".");
        }

    }

    public static void setZookeeperServerPrincipal(String zkServerPrincipal) throws IOException {
        setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, zkServerPrincipal);
    }
    public static void setKrb5Config(String krb5ConfFile) throws IOException {
        setZookeeperServerPrincipal(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfFile);
    }


    public static void setZookeeperServerPrincipal(String zkServerPrincipalKey, String zkServerPrincipal)
            throws IOException {
        Unsafe.setProperty(zkServerPrincipalKey, zkServerPrincipal);
        String ret = System.getProperty(zkServerPrincipalKey);
        if (ret == null) {
            String message = zkServerPrincipalKey + " is null.";
            LOG.error(message);
            throw new IOException(message);
        }
        if (!ret.equals(zkServerPrincipal)) {
            String message = zkServerPrincipalKey + " is " + ret + " is not " + zkServerPrincipal + ".";
            LOG.error(message);
            throw new IOException(message);
        }
    }

    private static void loginHadoop(String principal, String keytabFile) throws IOException {
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        } catch (IOException e) {
            LOG.error("login failed with " + principal + " and " + keytabFile + ".");
            LOG.error("perhaps cause 1 is " + LOGIN_FAILED_CAUSE_PSD_WRONG + ".");
            LOG.error("perhaps cause 2 is " + LOGIN_FAILED_CAUSE_TIME_WRONG + ".");
            LOG.error("perhaps cause 3 is " + LOGIN_FAILED_CAUSE_AES256_WRONG + ".");
            LOG.error("perhaps cause 4 is " + LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG + ".");
            LOG.error("perhaps cause 5 is " + LOGIN_FAILED_CAUSE_TIME_OUT + ".");

            throw e;
        }
    }

    public static boolean checkKeyTabIsValid(String path) {
        return KeyTab.getInstance(new File(path)).isValid();
    }

    public static boolean checkKeyTabIsExist(String path) {
        return !KeyTab.getInstance(new File(path)).isMissing();
    }

    public enum Module {
        STORM("StormClient"), KAFKA("KafkaClient"), ZOOKEEPER("Client");

        private String name;

        private Module(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * copy from hbase zkutil 0.94&0.98 A JAAS configuration that defines the login modules that we want to use for
     * login.
     */
    private static class JaasConfiguration extends javax.security.auth.login.Configuration {
        private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<>();
        private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap<>();
        private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN = new AppConfigurationEntry(
                KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                KEYTAB_KERBEROS_OPTIONS);
        private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF = new AppConfigurationEntry[] {
                KEYTAB_KERBEROS_LOGIN };

        static {
            String jaasEnvVar = System.getenv("HBASE_JAAS_DEBUG");
            if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
                BASIC_JAAS_OPTIONS.put("debug", "true");
            }
        }

        static {
            if (IS_IBM_JDK) {
                KEYTAB_KERBEROS_OPTIONS.put("credsType", "both");
            } else {
                KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
                KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", "false");
                KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
                KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
            }

            KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
        }

        private final String loginContextName;
        private final boolean useTicketCache;
        private final String keytabFile;
        private final String principal;
        private javax.security.auth.login.Configuration baseConfig;

        public JaasConfiguration(String loginContextName, String principal, String keytabFile) {
            this(loginContextName, principal, keytabFile, keytabFile == null || keytabFile.length() == 0);
        }

        private JaasConfiguration(String loginContextName, String principal, String keytabFile,
                boolean useTicketCache) {
            try {
                this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
            } catch (SecurityException e) {
                this.baseConfig = null;
            }
            this.loginContextName = loginContextName;
            this.useTicketCache = useTicketCache;
            this.keytabFile = keytabFile;
            this.principal = principal;

            initKerberosOption();
            LOG.info("JaasConfiguration loginContextName=" + loginContextName + " principal=" + principal
                    + " useTicketCache=" + useTicketCache + " keytabFile=" + keytabFile);
        }

        private void initKerberosOption() {
            if (!useTicketCache) {
                if (IS_IBM_JDK) {
                    KEYTAB_KERBEROS_OPTIONS.put("useKeytab", keytabFile);
                } else {
                    KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
                    KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
                    KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", "false");
                }
            }
            KEYTAB_KERBEROS_OPTIONS.put("principal", principal);
        }

        public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
            if (loginContextName.equals(appName)) {
                return KEYTAB_KERBEROS_CONF;
            }
            if (baseConfig != null)
                return baseConfig.getAppConfigurationEntry(appName);
            return (null);
        }
    }

}
