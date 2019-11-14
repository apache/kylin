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
 *
 */

package org.apache.kylin.common.util;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class ToolUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ToolUtil.class);

    private ToolUtil() {
        throw new IllegalStateException("Class ToolUtil is an utility class !");
    }

    public static String getConfFolder() {
        final String CONF = "conf";
        String path = System.getProperty(KylinConfig.KYLIN_CONF);
        if (StringUtils.isNotEmpty(path)) {
            return path;
        }
        path = KylinConfig.getKylinHome();
        if (StringUtils.isNotEmpty(path)) {
            return path + File.separator + CONF;
        }
        throw new RuntimeException("Cannot find conf folder.");
    }

    public static String getMetaStoreId() throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        return store.getMetaStoreUUID();
    }

    public static String decideKylinMajorVersionFromCommitFile() {
        Map<String, String> majorVersionCommitMap = Maps.newHashMap();
        majorVersionCommitMap.put("1.3", "commit.sha1");
        majorVersionCommitMap.put("1.5", "commit_SHA1");
        for (Map.Entry<String, String> majorVersionEntry : majorVersionCommitMap.entrySet()) {
            if (new File(KylinConfig.getKylinHome(), majorVersionEntry.getValue()).exists()) {
                return majorVersionEntry.getKey();
            }
        }
        return null;
    }

    public static String getHostName() {
        String hostname = System.getenv("COMPUTERNAME");
        if (StringUtils.isEmpty(hostname)) {
            InetAddress address = null;
            try {
                address = InetAddress.getLocalHost();
                hostname = address.getHostName();
                if (StringUtils.isEmpty(hostname)) {
                    hostname = address.getHostAddress();
                }
            } catch (UnknownHostException uhe) {
                String host = uhe.getMessage(); // host = "hostname: hostname"
                if (host != null) {
                    int colon = host.indexOf(':');
                    if (colon > 0) {
                        return host.substring(0, colon);
                    }
                }
                hostname = "Unknown";
            }
        }
        return hostname;
    }

    public static InetAddress getFirstNonLoopbackAddress(boolean preferIpv4, boolean preferIPv6)
            throws SocketException {
        Enumeration en = NetworkInterface.getNetworkInterfaces();
        while (en.hasMoreElements()) {
            NetworkInterface element = (NetworkInterface) en.nextElement();
            for (Enumeration en2 = element.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr = (InetAddress) en2.nextElement();
                if (!addr.isLoopbackAddress()) {
                    if (addr instanceof Inet4Address) {
                        if (preferIPv6) {
                            continue;
                        }
                        return addr;
                    }
                    if (addr instanceof Inet6Address) {
                        if (preferIpv4) {
                            continue;
                        }
                        return addr;
                    }
                }
            }
        }
        return null;
    }

    public static InetAddress getFirstIPV4NonLoopBackAddress() throws SocketException {
        return getFirstNonLoopbackAddress(true, false);
    }

    public static InetAddress getFirstIPV6NonLoopBackAddress() throws SocketException {
        return getFirstNonLoopbackAddress(true, false);
    }

    public static String getListenPort() {
        MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
        String port = null;
        try {
            final QueryExp queryExp = Query.and(Query.eq(Query.attr("protocol"), Query.value("HTTP/1.1")),
                    Query.eq(Query.attr("scheme"), Query.value("http")));
            Set<ObjectName> objectNames = beanServer.queryNames(new ObjectName("*:type=Connector,*"), queryExp);
            ObjectName objectName = objectNames.iterator().next();
            port = objectName.getKeyProperty("port");
        } catch (Exception e) {
            LOG.error("GET Http Listen Port Error", e);
        }

        return port;
    }
}
