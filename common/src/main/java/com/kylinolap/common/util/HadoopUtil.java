/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.common.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopUtil {
    private static final Logger logger = LoggerFactory.getLogger(HadoopUtil.class);

    private static ThreadLocal<Configuration> hadoopConfig = new ThreadLocal<>();

    public static void setCurrentConfiguration(Configuration conf) {
        hadoopConfig.set(conf);
    }

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            hadoopConfig.set(new Configuration());
        }
        return hadoopConfig.get();
    }

    public static FileSystem getFileSystem(String path) throws IOException {
        return FileSystem.get(makeURI(path), getCurrentConfiguration());
    }

    public static URI makeURI(String filePath) {
        try {
            return new URI(filePath);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from URI: " + filePath, e);
        }
    }

    /**
     * e.g. "hbase:kylin-local.corp.ebay.com:2181:/hbase-unsecure"
     */
    public static Configuration newHBaseConfiguration(String url) {
        Configuration conf = HBaseConfiguration.create();
        if (StringUtils.isEmpty(url))
            return conf;

        // chop off "hbase:"
        if (url.startsWith("hbase:") == false)
            throw new IllegalArgumentException("hbase url must start with 'hbase:' -- " + url);

        url = StringUtils.substringAfter(url, "hbase:");
        if (StringUtils.isEmpty(url))
            return conf;

        // case of "hbase:domain.com:2181:/hbase-unsecure"
        Pattern urlPattern = Pattern.compile("([\\w\\d\\-.]+)[:](\\d+)(?:[:](.*))?");
        Matcher m = urlPattern.matcher(url);
        if (m.matches() == false)
            throw new IllegalArgumentException("HBase URL '" + url + "' is invalid, expected url is like '" + "hbase:domain.com:2181:/hbase-unsecure" + "'");

        logger.debug("Creating hbase conf by parsing -- " + url);

        String quorum = m.group(1);
        try {
            InetAddress.getByName(quorum);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Zookeeper quorum is invalid: " + quorum + "; urlString=" + url, e);
        }
        conf.set(HConstants.ZOOKEEPER_QUORUM, quorum);

        String port = m.group(2);
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);

        String znodePath = m.group(3) == null ? "" : m.group(3);
        if (StringUtils.isEmpty(znodePath) == false)
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znodePath);

        // reduce rpc retry
        conf.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
        conf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
        conf.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");
        // conf.set(ScannerCallable.LOG_SCANNER_ACTIVITY, "true");

        return conf;
    }

}
