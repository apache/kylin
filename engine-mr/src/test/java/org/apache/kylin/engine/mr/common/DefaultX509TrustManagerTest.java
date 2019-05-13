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

import org.junit.Test;

import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for class {@link DefaultX509TrustManager}.
 *
 * @see DefaultX509TrustManager
 *
 */
public class DefaultX509TrustManagerTest{

  @Test(expected = NullPointerException.class)
  public void testIsServerTrustedThrowsNullPointerException() throws KeyStoreException, NoSuchAlgorithmException {
      DefaultX509TrustManager defaultX509TrustManager = new DefaultX509TrustManager(null);

      defaultX509TrustManager.isServerTrusted(new X509Certificate[1]);
  }

  @Test
  public void testIsServerTrustedWithNull() throws KeyStoreException, NoSuchAlgorithmException {
      DefaultX509TrustManager defaultX509TrustManager = new DefaultX509TrustManager(null);

      assertTrue(defaultX509TrustManager.isServerTrusted(null));
  }

    @Test
    public void testGetAcceptedIssuersUsingNullKeyStore() throws KeyStoreException, NoSuchAlgorithmException {
        DefaultX509TrustManager defaultX509TrustManager = new DefaultX509TrustManager(null);
        X509Certificate[] x509CertificateArray = defaultX509TrustManager.getAcceptedIssuers();
    
        assertEquals(105, x509CertificateArray.length);
        assertTrue(defaultX509TrustManager.isServerTrusted(x509CertificateArray));
    }

}