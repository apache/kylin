package com.kylinolap.job.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.metadata.tool.HiveSourceTableMgmt;

/**
 * Created by honma on 9/4/14.
 * <p/>
 * This tool facilitates cube sampling by:
 * <p/>
 * 1. Run hive commands to sample the fact table, with sampling ratio configurable
 * <p/>
 * 2. Create a new cube named like CUBE_1_of_10000_sample into current env.
 * The fact table refers to the sampled fact table created in phrase 1.
 * NOTE: cube building require extra operations.
 */
public class CubeSamplingCLI {

    private static final Logger logger = LoggerFactory.getLogger(CubeSamplingCLI.class);
    private static final String HIVE_PARTITIONED_KEY = "partitioned";
    private static final String HIVE_PARTITIONED_COLUMNS_KEY = "partitionColumns";

    public static void main(String[] args) throws IOException, JobException {
        createSampleCube(args[0], Integer.parseInt(args[1]));
    }

    /**
     * @param cubeName
     * @param sampleRatio use 100 if you want to get a 1/100 sample
     */
    public static void createSampleCube(String cubeName, int sampleRatio) throws IOException, JobException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cube = cubeMgr.getCube(cubeName);
        String factTableName = cube.getDescriptor().getFactTable();

        //run hive query to sample the table
        String sampledFactTableName = sampleFactTable(factTableName, sampleRatio);

        //sync the table desc of the created sample table
        HiveSourceTableMgmt.reloadHiveTable(sampledFactTableName);

        //When integrating into REST server, create cube and desc for sample cube
    }

    private static String sampleFactTable(String factTableName, int sampleRatio) throws IOException,
            JobException {
        String sampleTableName = factTableName + "_sample_1_of_" + sampleRatio;

        //hive settings:
        String settingQueries =
                "set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.max.dynamic.partitions=10000;";

        //hive drop table query
        String dropQuery = "drop table if exists " + sampleTableName + ";";

        //hive create table query
        String createQuery = "create table " + sampleTableName + " like " + factTableName + ";";

        //hive insert query based on hive table attributes
        HiveSourceTableMgmt hstm = new HiveSourceTableMgmt();
        File dir = File.createTempFile("meta", null);
        dir.delete();
        dir.mkdir();//replace file with a folder
        //dir.deleteOnExit();
        logger.info("Extracting table " + factTableName + "'s metadata into " + dir.getAbsolutePath());
        hstm.extractTableDescWithTablePattern(factTableName, dir.getAbsolutePath());
        String factTableExdFilePath =
                dir.getAbsolutePath() + File.separator + HiveSourceTableMgmt.TABLE_EXD_FOLDER_NAME
                        + File.separator + factTableName.toUpperCase() + "."
                        + HiveSourceTableMgmt.OUTPUT_SURFIX;
        logger.info("Getting fact table's extend attributes from " + factTableExdFilePath);
        InputStream is = new FileInputStream(factTableExdFilePath);
        Map<String, String> attrs = JsonUtil.readValue(is, HashMap.class);
        is.close();
        String partitionClause = getPartitionClause(attrs);
        String insertQuery =
                " INSERT OVERWRITE TABLE " + sampleTableName + " " + partitionClause + "  SELECT * FROM "
                        + factTableName + " TABLESAMPLE(BUCKET 1 OUT OF " + sampleRatio + "  ON rand()) s;";

        String query = settingQueries + dropQuery + createQuery + insertQuery;
        logger.info("The query being submitted is: \r\n" + query);

        is = HiveSourceTableMgmt.executeHiveCommand(query);
        InputStreamReader reader = new InputStreamReader(is);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            logger.info(line);
        }
        logger.info("end of the hive output stream");
        is.close();

        return sampleTableName;
    }

    private static String getPartitionClause(Map<String, String> tableAttrs) {
        try {
            String partitioned = tableAttrs.get(HIVE_PARTITIONED_KEY);
            String partitionedCols = tableAttrs.get(HIVE_PARTITIONED_COLUMNS_KEY);
            logger.info("hive partitioned: " + partitioned);
            logger.info("hive partitioned columns: " + partitionedCols);

            if ("true".equalsIgnoreCase(partitioned) && !StringUtils.isEmpty(partitionedCols)) {
                StringBuffer sb = new StringBuffer();
                sb.append("partition(");
                Pattern pattern = Pattern.compile("struct partition_columns \\{(.*)\\}");
                Matcher matcher = pattern.matcher(partitionedCols);
                if (matcher.find()) {
                    String str = matcher.group(1).trim();
                    for (String pair : str.split(", ")) {
                        String[] tokens = pair.trim().split(" ");
                        if (tokens.length != 2) {
                            throw new IllegalStateException("Error parsing " + pair + " in "
                                    + partitionedCols);
                        }
                        sb.append(tokens[1] + ",");
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    sb.append(")");
                    return sb.toString();
                }
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
            return "";
        }

        return "";
    }
}
