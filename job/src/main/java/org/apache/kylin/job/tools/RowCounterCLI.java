package org.apache.kylin.job.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 */
public class RowCounterCLI {
    private static final Logger logger = LoggerFactory.getLogger(RowCounterCLI.class);

    public static void main(String[] args) throws IOException {

        if (args == null || args.length != 3) {
            System.out.println("Usage: hbase org.apache.hadoop.util.RunJar kylin-job-latest.jar org.apache.kylin.job.tools.RowCounterCLI [HTABLE_NAME] [STARTKEY] [ENDKEY]");
        }

        System.out.println(args[0]);
        String htableName = args[0];
        System.out.println(args[1]);
        byte[] startKey = BytesUtil.fromReadableText(args[1]);
        System.out.println(args[2]);
        byte[] endKey = BytesUtil.fromReadableText(args[2]);

        if (startKey == null) {
            System.out.println("startkey is null ");
        } else {
            System.out.println("startkey lenght: " + startKey.length);
        }

        System.out.println("start key in binary: " + Bytes.toStringBinary(startKey));
        System.out.println("end key in binary: " + Bytes.toStringBinary(endKey));

        Configuration conf = HBaseConfiguration.create();

        Scan scan = new Scan();
        scan.setCaching(1024);
        scan.setCacheBlocks(true);
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);

        logger.info("My Scan " + scan.toString());

        HConnection conn = HConnectionManager.createConnection(conf);
        HTableInterface tableInterface = conn.getTable(htableName);

        Iterator<Result> iterator = tableInterface.getScanner(scan).iterator();
        int counter = 0;
        while (iterator.hasNext()) {
            iterator.next();
            counter++;
            if (counter % 1000 == 1) {
                System.out.println("number of rows: " + counter);
            }
        }
        System.out.println("number of rows: " + counter);
        tableInterface.close();
        conn.close();
    }
}
