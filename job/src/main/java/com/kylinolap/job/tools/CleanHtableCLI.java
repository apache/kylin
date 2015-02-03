package com.kylinolap.job.tools;

import java.io.IOException;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * Created by honma on 11/11/14.
 */
@SuppressWarnings("static-access")
public class CleanHtableCLI extends AbstractHadoopJob {

    protected static final Logger log = LoggerFactory.getLogger(CleanHtableCLI.class);

    String tableName;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        try {

            clean();

            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    private void clean() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);

        for (HTableDescriptor descriptor : hbaseAdmin.listTables()) {
            String name = descriptor.getNameAsString().toLowerCase();
            if (name.startsWith("kylin") || name.startsWith("_kylin")) {
                String x = descriptor.getValue("KYLIN_HOST");
                System.out.println("table name " + descriptor.getNameAsString() + " host: " + x);
                System.out.println(descriptor);
                System.out.println();
            }
        }
        hbaseAdmin.close();
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CleanHtableCLI(), args);
        System.exit(exitCode);
    }
}
