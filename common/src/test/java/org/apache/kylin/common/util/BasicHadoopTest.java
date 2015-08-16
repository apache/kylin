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

package org.apache.kylin.common.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by honma on 11/11/14.
 *
 * development concept proving use
 */
@Ignore("convenient trial tool for dev")
public class BasicHadoopTest {

    @BeforeClass
    public static void setup() throws Exception {
        ClassUtil.addClasspath(new File("../examples/test_case_data/hadoop-site").getAbsolutePath());
    }

    @Test
    public void testCreateHtable() throws IOException {
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("testhbase"));
        tableDesc.setValue("KYLIN_HOST", "dev01");

        HColumnDescriptor cf = new HColumnDescriptor("f");
        cf.setMaxVersions(1);

        cf.setInMemory(true);
        cf.setBlocksize(4 * 1024 * 1024); // set to 4MB
        tableDesc.addFamily(cf);

        Admin admin = HBaseConnection.get().getAdmin();
        admin.createTable(tableDesc);
        admin.close();
    }

    @Test
    public void testRetriveHtableHost() throws IOException {
        Admin hbaseAdmin = HBaseConnection.get().getAdmin();
        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables();
        for (HTableDescriptor table : tableDescriptors) {
            String value = table.getValue("KYLIN_HOST");
            if (value != null) {
                System.out.println(table.getTableName());
                System.out.println("host is " + value);
                hbaseAdmin.disableTable(table.getTableName());
                table.setValue("KYLIN_HOST_ANOTHER", "dev02");
                hbaseAdmin.modifyTable(table.getTableName(), table);
                hbaseAdmin.enableTable(table.getTableName());
            }
        }
        hbaseAdmin.close();
    }
}
