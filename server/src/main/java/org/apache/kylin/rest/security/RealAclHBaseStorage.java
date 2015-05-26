package org.apache.kylin.rest.security;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.rest.service.AclService;
import org.apache.kylin.rest.service.UserService;
import org.h2.util.StringUtils;

import java.io.IOException;

/**
 * Created by Hongbin Ma(Binmahone) on 5/19/15.
 */
public class RealAclHBaseStorage implements AclHBaseStorage {

    private String hbaseUrl;
    private String aclTableName;
    private String userTableName;

    @Override
    public String prepareHBaseTable(Class clazz) throws IOException {
        String metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        int cut = metadataUrl.indexOf('@');
        String tableNameBase = cut < 0 ? DEFAULT_TABLE_PREFIX : metadataUrl.substring(0, cut);
        hbaseUrl = cut < 0 ? metadataUrl : metadataUrl.substring(cut + 1);

        if (clazz == AclService.class) {
            aclTableName = tableNameBase + ACL_TABLE_NAME;
            HBaseConnection.createHTableIfNeeded(hbaseUrl, aclTableName, ACL_INFO_FAMILY, ACL_ACES_FAMILY);
            return aclTableName;
        } else if (clazz == UserService.class) {
            userTableName = tableNameBase + USER_TABLE_NAME;
            HBaseConnection.createHTableIfNeeded(hbaseUrl, userTableName, USER_AUTHORITY_FAMILY);
            return userTableName;
        } else {
            throw new IllegalStateException("prepareHBaseTable for unknown class: " + clazz);
        }
    }

    @Override
    public HTableInterface getTable(String tableName) throws IOException {
        if (StringUtils.equals(tableName, aclTableName)) {
            return HBaseConnection.get(hbaseUrl).getTable(aclTableName);
        } else if (StringUtils.equals(tableName, userTableName)) {
            return HBaseConnection.get(hbaseUrl).getTable(userTableName);
        } else {
            throw new IllegalStateException("getTable failed" + tableName);
        }
    }
}
