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

package org.apache.kylin.engine.mr.common;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xduo
 * 
 */
public class DefaultX509TrustManager implements X509TrustManager {

    /** Log object for this class. */
    private static Logger logger = LoggerFactory.getLogger(DefaultX509TrustManager.class);
    private X509TrustManager standardTrustManager = null;

    /**
     * Constructor for DefaultX509TrustManager.
     * 
     */
    public DefaultX509TrustManager(KeyStore keystore) throws NoSuchAlgorithmException, KeyStoreException {
        super();

        TrustManagerFactory factory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(keystore);

        TrustManager[] trustmanagers = factory.getTrustManagers();

        if (trustmanagers.length == 0) {
            throw new NoSuchAlgorithmException("SunX509 trust manager not supported");
        }

        this.standardTrustManager = (X509TrustManager) trustmanagers[0];
    }

    public X509Certificate[] getAcceptedIssuers() {
        return this.standardTrustManager.getAcceptedIssuers();
    }

    public boolean isClientTrusted(X509Certificate[] certificates) {
        return true;
        // return this.standardTrustManager.isClientTrusted(certificates);
    }

    public boolean isServerTrusted(X509Certificate[] certificates) {
        if ((certificates != null) && logger.isDebugEnabled()) {
            logger.debug("Server certificate chain:");

            for (int i = 0; i < certificates.length; i++) {
                if (logger.isDebugEnabled()) {
                    logger.debug("X509Certificate[" + i + "]=" + certificates[i]);
                }
            }
        }

        if ((certificates != null) && (certificates.length == 1)) {
            X509Certificate certificate = certificates[0];

            try {
                certificate.checkValidity();
            } catch (CertificateException e) {
                logger.error(e.toString());

                return false;
            }

            return true;
        } else {
            return true;
            // return this.standardTrustManager.isServerTrusted(certificates);
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        // TODO Auto-generated method stub

    }

}
