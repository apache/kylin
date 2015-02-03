package org.apache.kylin.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by honma on 11/11/14.
 *
 * development concept proving use
 */
@Ignore("convenient trial tool for dev")
public class BasicHadoopTest {

    @BeforeClass
    public static void setup() throws Exception {
        ClasspathUtil.addClasspath(new File("../examples/test_case_data/hadoop-site").getAbsolutePath());
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

        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.createTable(tableDesc);
        admin.close();
    }

    @Test
    public void testRetriveHtableHost() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
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
