package org.apache.kylin.rest.util;

import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * Created by Hongbin Ma(Binmahone) on 5/19/15.
 */
public interface AclHBaseStorage {
    String DEFAULT_TABLE_PREFIX = "kylin_metadata";

    String ACL_INFO_FAMILY = "i";
    String ACL_ACES_FAMILY = "a";
    String ACL_TABLE_NAME = "_acl";

    String USER_AUTHORITY_FAMILY = "a";
    String USER_TABLE_NAME = "_user";
    String USER_AUTHORITY_COLUMN = "c";

    String prepareHBaseTable(Class clazz) throws IOException;

    HTableInterface getTable(String tableName) throws IOException;

}
