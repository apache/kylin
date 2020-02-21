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

package org.apache.kylin.source.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.kylin.common.KylinConfig;

import java.io.IOException;
import java.lang.reflect.Method;

public class HiveMetaStoreClientFactory {

    /**
     * Get hivemetastoreclient. At present, it supports hivecatalog and glue catalog. When it is configured as hcatalog,
     * you can directly new hivemetastoreclient (hiveconf), which is more efficient.
     * But if you need to use hcatutil.gethivemetastoreclient (hiveconf) to configure gluecatalog,
     * you can get: com.amazon aws.glue.catalog.metastore.awsgluedatacataloghiveclientfactory according to the configuration file
     *
     * @param hiveConf
     * @return metaStoreClient
     * @throws MetaException
     * @throws IOException
     */
    public static IMetaStoreClient getHiveMetaStoreClient(HiveConf hiveConf) throws MetaException, IOException {
        IMetaStoreClient metaStoreClient = null;
        String hiveMetadataOption = KylinConfig.getInstanceFromEnv().getHiveMetaDataType();
        if ("hcatalog".equals(hiveMetadataOption)) {
            metaStoreClient = new HiveMetaStoreClient(hiveConf);
        } else if ("gluecatalog".equals(hiveMetadataOption)) {
            // getHiveMetastoreClient is not available in CDH profile
            try {
                Class<?> clazz = Class.forName("org.apache.hive.hcatalog.common.HCatUtil");
                Method getHiveMetastoreClientMethod = clazz.getDeclaredMethod("getHiveMetastoreClient", HiveConf.class);
                metaStoreClient = (IMetaStoreClient) getHiveMetastoreClientMethod.invoke(null, hiveConf);
            } catch (Exception exp) {
                throw new IllegalStateException("Unable to create MetaStoreClient for " + hiveMetadataOption, exp);
            }
        } else {
            throw new IllegalArgumentException(hiveMetadataOption + " is not a good option.");
        }
        return metaStoreClient;
    }
}
