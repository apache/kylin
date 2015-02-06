package org.apache.kylin.job.tools;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.BytesUtil;

import java.io.IOException;

/**
 * Created by Hongbin Ma(Binmahone) on 2/6/15.
 */
public class HBaseRowDigestTest {


    private static final byte[] CF = "f".getBytes();
    private static final byte[] QN = "c".getBytes();

    public static void main(String[] args) throws IOException {
        String hbaseUrl = "hbase"; // use hbase-site.xml on classpath
        HConnection conn = HBaseConnection.get(hbaseUrl);
        HTableInterface table = conn.getTable(args[0]);
        ResultScanner scanner = table.getScanner(CF, QN);
        StringBuilder sb = new StringBuilder();
        while (true) {
            Result r = scanner.next();
            if (r == null)
                break;

            byte[] row = r.getRow();
            byte[] value = r.getValue(CF, QN);

            sb.append(BytesUtil.toReadableText(row) + "\n");
            System.out.println(BytesUtil.toReadableText(value));
        }
    }
}
