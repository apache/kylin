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

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

/**
 * KE-Process kerberos long-running scenario.
 * @deprecated since 'ke-4.5.18.0'
 * This implementation is no longer acceptable to fulfill in long-running scenario.
 * Use {@link DelegationTokenManager} instead.
 */
@Deprecated
public class KerberosLoginTask {

    private static final Logger logger = LoggerFactory.getLogger(KerberosLoginTask.class);

    private static final Configuration KRB_CONF = new Configuration();

    private KapConfig kapConfig;

    public void execute() {
        kapConfig = KapConfig.getInstanceFromEnv();

        if (kapConfig.isKerberosEnabled()) {
            Preconditions.checkState(KerberosLoginUtil.checkKeyTabIsExist(kapConfig.getKerberosKeytabPath()),
                    "The key tab is not exist : " + kapConfig.getKerberosKeytabPath());
            Preconditions.checkState(KerberosLoginUtil.checkKeyTabIsValid(kapConfig.getKerberosKeytabPath()),
                    "The key tab is invalid : " + kapConfig.getKerberosKeytabPath());
            try {
                reInitTGT();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void reInitTGT() throws IOException {

        // init kerberos ticket first
        renewKerberosTicketQuietly();

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        sleepQuietly(kapConfig.getKerberosTicketRefreshInterval() * 60 * 1000);
                        renewKerberosTicketQuietly();
                    }
                } catch (Exception e) {
                    logger.error("unexpected exception", e);
                }
            }
        });
        t.setDaemon(true);
        t.setName("TGT Reinit for " + UserGroupInformation.getLoginUser().getUserName());
        t.start();

        Thread kerberosMonitor = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        lookKerberosTicketQuietly();
                        sleepQuietly(kapConfig.getKerberosMonitorInterval() * 60 * 1000);
                    }
                } catch (Exception e) {
                    logger.error("unexpected exception", e);
                }
            }
        });
        kerberosMonitor.setDaemon(true);
        kerberosMonitor.setName("Kerberos monitor for " + UserGroupInformation.getLoginUser().getUserName());
        kerberosMonitor.start();

    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            logger.warn("sleep interrupted", e);
        }
    }

    private void renewKerberosTicketQuietly() {
        try {
            logger.info("kinit -kt " + kapConfig.getKerberosKeytabPath() + " " + kapConfig.getKerberosPrincipal());
            Shell.execCommand("kinit", "-kt", kapConfig.getKerberosKeytabPath(), kapConfig.getKerberosPrincipal());
            logger.info("Login " + kapConfig.getKerberosPrincipal() + " from keytab: "
                    + kapConfig.getKerberosKeytabPath() + ".");
            if (kapConfig.getKerberosPlatform().equals("Standard")) {
                loginStandardKerberos();
            } else if (kapConfig.getKerberosPlatform().equals(KapConfig.FI_PLATFORM) || kapConfig.getKerberosPlatform().equals(KapConfig.TDH_PLATFORM)) {
                loginNonStandardKerberos();
            }
        } catch (Exception e) {
            logger.error("Error renew kerberos ticket", e);
        }
    }

    private void loginNonStandardKerberos() throws IOException {
        String zkServerPrincipal = kapConfig.getKerberosZKPrincipal();
        if (Boolean.TRUE.equals(kapConfig.getPlatformZKEnable())) {
            Unsafe.setProperty("zookeeper.sasl.client", "true");
        }
        String jaasFilePath = kapConfig.getKerberosJaasConfPath();
        Unsafe.setProperty("java.security.auth.login.config", jaasFilePath);
        Unsafe.setProperty("java.security.krb5.conf", kapConfig.getKerberosKrb5ConfPath());

        KerberosLoginUtil.setJaasConf("Client", kapConfig.getKerberosPrincipal(), kapConfig.getKerberosKeytabPath());
        if (Boolean.TRUE.equals(kapConfig.getPlatformZKEnable())) {
            KerberosLoginUtil.setZookeeperServerPrincipal(zkServerPrincipal);
        }

        KerberosLoginUtil.login(kapConfig.getKerberosPrincipal(), kapConfig.getKerberosKeytabPath(),
                kapConfig.getKerberosKrb5ConfPath(), KRB_CONF);
    }

    private void loginStandardKerberos() throws IOException {
        UserGroupInformation.loginUserFromKeytab(kapConfig.getKerberosPrincipal(), kapConfig.getKerberosKeytabPath());
        logger.info("Login kerberos success.");
    }

    private void lookKerberosTicketQuietly() {
        try {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            logger.info("current user :" + currentUser);
            Credentials credentials = currentUser.getCredentials();
            logger.info("Current user has " + credentials.getAllTokens().size() + " token.");
            Collection<Token<? extends TokenIdentifier>> allTokens = credentials.getAllTokens();
            for (Token token : allTokens) {
                TokenIdentifier tokenIdentifier = token.decodeIdentifier();
                logger.info(tokenIdentifier.toString());
            }
            if (!allTokens.isEmpty()) {
                logger.info("Current user should have 0 token but there are non-zero. ReLogin current user: "
                        + currentUser.getUserName());
                renewKerberosTicketQuietly();
            }
        } catch (Exception e) {
            logger.error("Error showing kerberos tokens", e);
        }
    }
}
