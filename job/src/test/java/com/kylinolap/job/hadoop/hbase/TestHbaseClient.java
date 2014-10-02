package com.kylinolap.job.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by hongbin on 5/15/14.
 */
public class TestHbaseClient {

    private static boolean reverse = false;

    public static void foo(int n, int k) {
        int t = k;
        if (n - k < k) {
            t = n - k;
            reverse = true;
        }
        boolean[] flags = new boolean[n];
        inner(flags, 0, t);
    }

    private static void print(boolean[] flags) {
        for (int i = 0; i < flags.length; i++) {
            if (!reverse) {
                if (flags[i])
                    System.out.print("0");
                else
                    System.out.print("1");
            } else {
                if (flags[i])
                    System.out.print("1");
                else
                    System.out.print("0");

            }
        }
        System.out.println();

    }

    private static void inner(boolean[] flags, int start, int remaining) {
        if (remaining <= 0) {
            print(flags);
            return;
        }

        if (flags.length - start < remaining) {
            return;
        }

        //write at flags[start]
        flags[start] = true;
        inner(flags, start + 1, remaining - 1);

        //not write at flags[start]
        flags[start] = false;
        inner(flags, start + 1, remaining);
    }

    public static void main(String[] args) throws IOException {
        foo(6, 5);
        foo(5, 2);
        foo(3, 0);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "yadesk00.corp.ebay.com");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        HTable table = new HTable(conf, "test1");
        Put put = new Put(Bytes.toBytes("row1"));

        put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));

        table.put(put);
    }
}
