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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.utils.ThreadUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

public class DelegationTokenManager {

    private String principal;
    private String keytab;
    private ScheduledExecutorService renewalExecutor;

    private final KapConfig kapConf;

    private static final String CONTEXT_NAME = "Client";
    private static final Configuration CONFIGURATION = new Configuration();
    private static final Logger logger = LoggerFactory.getLogger(DelegationTokenManager.class);

    public DelegationTokenManager() {
        this(KapConfig.getInstanceFromEnv());
    }

    public DelegationTokenManager(KapConfig kapConf) {
        this.kapConf = kapConf;
    }

    public void start() {
        if (Boolean.FALSE.equals(kapConf.isKerberosEnabled())) {
            logger.info("Kerberos is not enabled.");
            return;
        }

        principal = kapConf.getKerberosPrincipal();
        keytab = kapConf.getKerberosKeytabPath();

        preCheck();
        renewalExecutor = //
                ThreadUtils.newDaemonSingleThreadScheduledExecutor("Kylin Credential Renewal Thread");
        // invoke from external?
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

        // Login first before everything.
        tryLogin();

        // Schedule TGT renewal.
        // Update credentials of FileSystem cached UGIs.
        scheduleTGTRenewal();

        // Schedule TGT cache renewal for ZK.
        // ZK read tgt from external cache: KRB5CCNAME, not ugi credentials.
        scheduleTGTCacheRenewal();
    }

    private void tryLogin() {
        try {
            doLogin();
        } catch (IOException ioe) {
            long retryInterval = kapConf.getKerberosTGTRetryInterval();
            logger.error("Failed to login kerberos from principal: {}, keytab: {}," //
                    + " will try again in {} minutes." //
                    + " If this happens too often tasks will fail.", principal, keytab, retryInterval, ioe);
            renewalExecutor.schedule(this::tryLogin, Math.max(0, retryInterval), TimeUnit.MINUTES);
        }
    }

    private void scheduleTGTRenewal() {
        final Runnable tgtRenewalTask = () -> {
            try {
                updateCredentials();
            } catch (Exception e) {
                logger.error("Failed to update UGI credentials.", e);
            }
        };

        long renewalInternal = kapConf.getKerberosTGTRenewalInterval();
        renewalExecutor.scheduleWithFixedDelay(tgtRenewalTask, renewalInternal, renewalInternal, TimeUnit.MINUTES);
    }

    // We wouldn't do UGI#loginUserFromKeytab again and again.
    private void scheduleTGTCacheRenewal() {
        final Runnable tgtCacheRenewalTask = () -> {
            try {
                doRenewTGTCache();
            } catch (IOException ioe) {
                logger.error("Failed to renew kerberos tgt cache at KRB5CCNAME.", ioe);
            }
        };
        long renewalInternal = kapConf.getKerberosTicketRefreshInterval();
        renewalExecutor.scheduleWithFixedDelay(tgtCacheRenewalTask, //
                renewalInternal, renewalInternal, TimeUnit.MINUTES);
    }

    private void updateCredentials() throws IOException, NoSuchFieldException {
        final UserGroupInformation current = UserGroupInformation.getCurrentUser();
        current.checkTGTAndReloginFromKeytab();

        Object fsCache = getFileSystemCache();
        if (Objects.isNull(fsCache)) {
            return;
        }

        // Extract old UGIs from cacheKeys.
        final Collection<Object> cacheKeys = getCacheKeys(fsCache);
        // The latest credentials.
        final Credentials creds = current.getCredentials();
        // The latest public tokens.
        final Collection<Token<? extends TokenIdentifier>> tokens = getTokens(creds);
        // Backport the latest credentials to old UGIs.
        for (Object key : cacheKeys) {
            UserGroupInformation ugi = getUGI(key);
            if (updatable(ugi, current)) { // Avoid non-changeable updates.
                try {
                    // For potential private tokens.
                    updateTokens(ugi, tokens);
                } catch (Exception e) {
                    logger.debug("Failed to update private tokens, hadoop version not supported.", e);
                }

                // For public & tgt credentials.
                ugi.addCredentials(creds);
            }
        }
    }

    private Collection<Token<? extends TokenIdentifier>> getTokens(Credentials creds) {
        return creds.getAllTokens().stream() //
                .filter(t -> Objects.nonNull(t.getKind()) && Objects.nonNull(t.getService())) //
                .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableCollection));
    }

    private Object getFileSystemCache() throws NoSuchFieldException {
        Field cacheField = FileSystem.class.getDeclaredField("CACHE");
        ReflectionUtils.makeAccessible(cacheField);
        return ReflectionUtils.getField(cacheField, null);
    }

    private Collection<Object> getCacheKeys(Object fsCache) throws NoSuchFieldException {
        Field mapField = fsCache.getClass().getDeclaredField("map");
        ReflectionUtils.makeAccessible(mapField);
        Map<Object, Object> cacheMap = //
                (Map<Object, Object>) ReflectionUtils.getField(mapField, fsCache);
        if (Objects.isNull(cacheMap) || cacheMap.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableSet(cacheMap.keySet());
    }

    private UserGroupInformation getUGI(Object key) throws NoSuchFieldException {
        Field ugiField = key.getClass().getDeclaredField("ugi");
        ReflectionUtils.makeAccessible(ugiField);
        return (UserGroupInformation) ReflectionUtils.getField(ugiField, key);
    }

    private boolean updatable(UserGroupInformation ugi, UserGroupInformation current) {
        if (Objects.isNull(ugi) || Objects.equals(ugi, current) //
                || !Objects.equals(ugi.getUserName(), current.getUserName()) /* Project level kerberos separation. */) {
            return false;
        }

        // Caution: do not refer private-token here,
        // not all hadoop versions do have private-token.
        return getTokens(current.getCredentials()) // Public tokens.
                .stream().anyMatch(token -> //
                getDelegationTokenIdentifier(token).map(AbstractDelegationTokenIdentifier::getSequenceNumber)
                        .filter(dtSeq -> //
                        getTokens(ugi.getCredentials()) // Public tokens.
                                .stream()
                                .filter(otk -> Objects.equals(token.getKind(), otk.getKind())
                                        && Objects.equals(token.getService(), otk.getService()))
                                .anyMatch(otk -> getDelegationTokenIdentifier(otk)
                                        .map(AbstractDelegationTokenIdentifier::getSequenceNumber)
                                        .filter(odtSeq -> odtSeq < dtSeq).isPresent()))
                        .isPresent());
    }

    private Optional<AbstractDelegationTokenIdentifier> getDelegationTokenIdentifier(
            Token<? extends TokenIdentifier> token) {
        try {
            TokenIdentifier ti = token.decodeIdentifier();
            if (ti instanceof AbstractDelegationTokenIdentifier) {
                return Optional.of((AbstractDelegationTokenIdentifier) ti);
            }
        } catch (IOException e) {
            logger.debug("Failed to decode token {}", token, e);
        }
        return Optional.empty();
    }

    private void updateTokens(UserGroupInformation ugi, //
            Collection<Token<? extends TokenIdentifier>> tokens) throws NoSuchMethodException, NoSuchFieldException {
        Credentials creds = getCredentialsInternal(ugi);
        if (Objects.isNull(creds)) {
            return;
        }
        // Private tokens included.
        final Map<Text, Token<? extends TokenIdentifier>> oldInternalTokens = getTokenMapInternal(creds);
        tokens.forEach(token -> updateTokensInternal(token, ugi, oldInternalTokens));
    }

    private void updateTokensInternal(Token<? extends TokenIdentifier> token, // token backport to old UGI
            UserGroupInformation ugi, // old UGI
            Map<Text, Token<? extends TokenIdentifier>> oldInternalTokens /* old UGI tokens */) {

        getDelegationTokenIdentifier(token).map(AbstractDelegationTokenIdentifier::getSequenceNumber)
                .ifPresent(dtSeq -> oldInternalTokens.forEach((key, otk) -> {
                    if (!Objects.equals(token.getKind(), otk.getKind())) {
                        return;
                    }

                    getDelegationTokenIdentifier(otk).map(AbstractDelegationTokenIdentifier::getSequenceNumber)
                            .ifPresent(odtSeq -> {
                                if (odtSeq < dtSeq && isPrivateCloneOf(otk, token.getService())) {
                                    privateClone(token, otk.getService()).ifPresent(tk -> ugi.addToken(key, tk));
                                }
                            });
                }));
    }

    private boolean isPrivateCloneOf(Token<? extends TokenIdentifier> token, Text service) {
        try {
            Method privateMethod = token.getClass().getDeclaredMethod("isPrivateCloneOf");
            ReflectionUtils.makeAccessible(privateMethod);
            Object boolObj = ReflectionUtils.invokeMethod(privateMethod, token, service);
            if (Objects.isNull(boolObj)) {
                return false;
            }
            return (boolean) boolObj;
        } catch (NoSuchMethodException e) {
            logger.debug("Failed to get method 'isPrivateCloneOf', hadoop version not supported (since 2.8.2).");
        }
        return false;
    }

    private Optional<Token<? extends TokenIdentifier>> privateClone(Token<? extends TokenIdentifier> token,
            Text service) {
        try {
            Method cloneMethod = token.getClass().getDeclaredMethod("privateClone");
            ReflectionUtils.makeAccessible(cloneMethod);
            Object tkObj = ReflectionUtils.invokeMethod(cloneMethod, token, service);
            if (Objects.isNull(tkObj)) {
                return Optional.empty();
            }
            return Optional.of((Token<? extends TokenIdentifier>) tkObj);
        } catch (NoSuchMethodException e) {
            logger.debug("Failed to get method 'privateClone', hadoop version not supported (since 2.8.2).");
        }
        return Optional.empty();
    }

    private Credentials getCredentialsInternal(UserGroupInformation ugi) throws NoSuchMethodException {
        Method credsMethod = UserGroupInformation.class.getDeclaredMethod("getCredentialsInternal");
        ReflectionUtils.makeAccessible(credsMethod);
        return (Credentials) ReflectionUtils.invokeMethod(credsMethod, ugi);
    }

    private Map<Text, Token<? extends TokenIdentifier>> getTokenMapInternal(Credentials creds)
            throws NoSuchFieldException {
        Field mapFiled = Credentials.class.getDeclaredField("tokenMap");
        ReflectionUtils.makeAccessible(mapFiled);
        Map<Text, Token<? extends TokenIdentifier>> internalTokenMap = //
                (Map<Text, Token<? extends TokenIdentifier>>) ReflectionUtils.getField(mapFiled, creds);
        if (Objects.isNull(internalTokenMap)) {
            return Collections.emptyMap();
        }
        Map<Text, Token<? extends TokenIdentifier>> tokenMap = Maps.newHashMap(internalTokenMap);
        // Readable only.
        return Collections.unmodifiableMap(tokenMap);
    }

    private void doRenewTGTCache() throws IOException {
        // Prepare credential cache for external shell tools.
        logger.info("Prepare credential cache by 'kinit -kt {} {}' at KRB5CCNAME: {}", //
                keytab, principal, System.getenv("KRB5CCNAME"));
        Shell.execCommand("kinit", "-kt", keytab, principal);
    }

    // Copied from KerberosLoginTask
    private void doLogin() throws IOException {
        doRenewTGTCache();
        logger.info("Login kerberos from principal: {}, keytab: {}.", principal, keytab);
        final String platform = kapConf.getKerberosPlatform();
        switch (platform) {
        case "Standard":
            loginStandardPlatform();
            break;
        case KapConfig.FI_PLATFORM:
        case KapConfig.TDH_PLATFORM:
            loginNonStandardPlatform();
            break;
        default:
            throw new InvalidParameterException("Unknown platform: " + platform //
                    + ", please check 'kylin.kerberos.platform'.");
        }
    }

    // Copied from KerberosLoginTask
    // Mysterious settings, it is ancestral code.
    private void loginNonStandardPlatform() throws IOException {

        Unsafe.setProperty("zookeeper.sasl.client", "true");
        Unsafe.setProperty("java.security.auth.login.config", kapConf.getKerberosJaasConfPath());
        Unsafe.setProperty("java.security.krb5.conf", kapConf.getKerberosKrb5ConfPath());

        KerberosLoginUtil.setJaasConf(CONTEXT_NAME, principal, keytab);
        KerberosLoginUtil.setZookeeperServerPrincipal(kapConf.getKerberosZKPrincipal());

        KerberosLoginUtil.login(principal, keytab, kapConf.getKerberosKrb5ConfPath(), CONFIGURATION);
    }

    // Copied from KerberosLoginTask
    private void loginStandardPlatform() throws IOException {
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        logger.info("Login kerberos success.");
    }

    // Copied from KerberosLoginTask
    private void preCheck() {
        Preconditions.checkState(KerberosLoginUtil.checkKeyTabIsExist(keytab), //
                "The key tab is not exist : %s", keytab);
        Preconditions.checkState(KerberosLoginUtil.checkKeyTabIsValid(keytab), //
                "The key tab is invalid : %s", keytab);
    }

    private void stop() {
        if (renewalExecutor != null) {
            renewalExecutor.shutdownNow();
        }
    }

}
