package com.kylinolap.job.tools;

import com.kylinolap.job.hadoop.AbstractHadoopJob;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by honma on 11/11/14.
 */
@SuppressWarnings("static-access")
public class HtableAlterMetadataCLI extends AbstractHadoopJob {

    private static final Option OPTION_METADATA_KEY = OptionBuilder.withArgName("key").hasArg().isRequired(true).withDescription("The metadata key").create("key");
    private static final Option OPTION_METADATA_VALUE = OptionBuilder.withArgName("value").hasArg().isRequired(true).withDescription("The metadata value").create("value");

    protected static final Logger log = LoggerFactory.getLogger(HtableAlterMetadataCLI.class);

    String tableName;
    String metadataKey;
    String metadataValue;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        try {
            options.addOption(OPTION_HTABLE_NAME);
            options.addOption(OPTION_METADATA_KEY);
            options.addOption(OPTION_METADATA_VALUE);

            parseOptions(options, args);
            tableName = getOptionValue(OPTION_HTABLE_NAME);
            metadataKey = getOptionValue(OPTION_METADATA_KEY);
            metadataValue = getOptionValue(OPTION_METADATA_VALUE);

            alter();

            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        }
    }

    private void alter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
        HTableDescriptor table = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));

        hbaseAdmin.disableTable(table.getTableName());
        table.setValue(metadataKey, metadataValue);
        hbaseAdmin.modifyTable(table.getTableName(), table);
        hbaseAdmin.enableTable(table.getTableName());
        hbaseAdmin.close();
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HtableAlterMetadataCLI(), args);
        System.exit(exitCode);
    }
}
