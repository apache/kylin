/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.hadoop.cube;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.CliCommandExecutor;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.cube.CubeStatusEnum;

/**
 * @author ysong1
 */
public class StorageCleanupJob extends AbstractHadoopJob {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false).withDescription("Delete the unused storage").create("delete");
    @SuppressWarnings("static-access")
    private static final Option OPTION_UUID_LIST = OptionBuilder.withArgName("uuids").hasArgs(Option.UNLIMITED_VALUES).isRequired(false).withDescription("Remove storage with uuids in list,separated by ,").create("uuids");
    @SuppressWarnings("static-access")
    private static final Option OPTION_UUID_FILE = OptionBuilder.withArgName("file").hasArg().isRequired(false).withDescription("Remove storage with uuid in file").create("file");
    
    protected static final Logger log = LoggerFactory.getLogger(StorageCleanupJob.class);

    boolean delete = false;
    String uuids = "";
    String uuidFile = "";
    /*
     * (non-Javadoc)com.kylinolap.job.hadoop.cube.StorageCleanupJob

     * 
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        try {
            options.addOption(OPTION_DELETE);
            options.addOption(OPTION_UUID_LIST);
            options.addOption(OPTION_UUID_FILE);
            parseOptions(options, args);
            
            delete = Boolean.parseBoolean(getOptionValue(OPTION_DELETE));
            uuids = getOptionValue(OPTION_UUID_LIST);
            //Get the array of job uuids
            String[] uuidArr = getUuidArray();
            
            Configuration conf = HBaseConfiguration.create(getConf());
            
            if(uuidArr != null && uuidArr.length > 0){
                cleanUnusedHBaseTables(conf, uuidArr);
                cleanUnusedIntermediateHiveTable(conf, uuidArr);
                cleanUnusedHdfsFiles(conf, uuidArr);                
            }
            else if(uuids == null && uuidFile == null){
                cleanUnusedHBaseTables(conf);
                cleanUnusedIntermediateHiveTable(conf);
                cleanUnusedHdfsFiles(conf);      
            }
            
            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        }
    }
    
    private String[] getUuidArray() throws IOException{
        List<String> uuidList = new ArrayList<String>();
        
        if(uuids != null){
            String[] splittedUUids = uuids.split(",");
            Collections.addAll(uuidList, splittedUUids);
        }
        uuidFile = getOptionValue(OPTION_UUID_FILE);
        
        if(uuidFile != null){
            FileReader fileReader = new FileReader(uuidFile);
            BufferedReader buffReader = new BufferedReader(fileReader);
            String line = null;
            
            while((line = buffReader.readLine()) != null && line.length() > 10) {
                uuidList.add(line);
            }
            
            buffReader.close();
            fileReader.close();
        }   
        String[] uuidArr = (String[])(uuidList.toArray(new String[0]));
        return uuidArr;
    }
    
    private boolean isJobInUse(JobInstance job) {
        if (job.getStatus().equals(JobStatusEnum.NEW) || job.getStatus().equals(JobStatusEnum.PENDING) || job.getStatus().equals(JobStatusEnum.RUNNING) || job.getStatus().equals(JobStatusEnum.ERROR)) {
            return true;
        } else {
            return false;
        }
    }


    private void cleanUnusedHBaseTables(Configuration conf) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

        // get all kylin hbase tables
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
        String tableNamePrefix = CubeManager.getHBaseStorageLocationPrefix();
        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");
        List<String> allTablesNeedToBeDropped = new ArrayList<String>();
        for (HTableDescriptor desc : tableDescriptors) {
            String host = desc.getValue(CubeManager.getHtableMetadataKey());
            if (KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix().equalsIgnoreCase(host)) {
                //only take care htables that belongs to self
                allTablesNeedToBeDropped.add(desc.getTableName().getNameAsString());
            }
        }

        // remove every segment htable from drop list
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String tablename = seg.getStorageLocationIdentifier();
                allTablesNeedToBeDropped.remove(tablename);
                log.info("Remove table " + tablename + " from drop list, as the table belongs to cube " + cube.getName() + " with status " + cube.getStatus());
            }
        }

        if (delete == true) {
            // drop tables
            for (String htableName : allTablesNeedToBeDropped) {
                log.info("Deleting HBase table " + htableName);
                if (hbaseAdmin.tableExists(htableName)) {
                    hbaseAdmin.disableTable(htableName);
                    hbaseAdmin.deleteTable(htableName);
                    log.info("Deleted HBase table " + htableName);
                } else {
                    log.info("HBase table" + htableName + " does not exist");
                }
            }
        } else {
            System.out.println("--------------- Tables To Be Dropped ---------------");
            for (String htableName : allTablesNeedToBeDropped) {
                System.out.println(htableName);
            }
            System.out.println("----------------------------------------------------");
        }

        hbaseAdmin.close();
    }

    
    private void cleanUnusedHBaseTables(Configuration conf, String[] uuids) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
        List<String> allTablesNeedToBeDropped = new ArrayList<String>();
        List<String> uuidList = new ArrayList<String>();
        
        Collections.addAll(uuidList, uuids);
        // add hbase tables with uuid list if its cube status is DISABLED or DESCBROKEN
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            if(cube.getStatus() == CubeStatusEnum.DISABLED || cube.getStatus() == CubeStatusEnum.DESCBROKEN){
                for (CubeSegment seg : cube.getSegments()) {
                    String segUuid = seg.getUuid();
                    
                    if(uuidList.contains(segUuid)){
                        String tablename = seg.getStorageLocationIdentifier();
                        allTablesNeedToBeDropped.add(tablename);
                        uuidList.remove(segUuid);
                        log.info("Table " + tablename + " added to drop list, as the table belongs to cube " + cube.getName() + " with status " + cube.getStatus());
                    }
                }
            }
        }
        //Clean the expired Hbase tables after cube rebuilt
        if(uuidList.size() > 0){
            List<JobInstance> allJobs = JobDAO.getInstance(KylinConfig.getInstanceFromEnv()).listAllJobs();
            
            for (JobInstance jobInstance : allJobs) {          
                if (isJobInUse(jobInstance) == false && uuidList.contains(jobInstance.getUuid())){               
                    List<JobStep>  stepList = jobInstance.getSteps();
                    
                    for(JobStep jobStep : stepList){
                        if(jobStep.getName().equals("Create HTable")){
                            String stepCmd = jobStep.getExecCmd();
                            String tablename = "";
                            
                            if(stepCmd.length() > 16){
                                tablename = stepCmd.substring(stepCmd.length() -16, stepCmd.length());
                                allTablesNeedToBeDropped.add(tablename);
                                log.info("Table " + tablename + " added to drop list, as the table is expired now.");
                            }
                        }
                    }
                }
            }
        }
        if (delete == true) {
            // drop tables
            for (String htableName : allTablesNeedToBeDropped) {
                log.info("Deleting HBase table " + htableName);
                if (hbaseAdmin.tableExists(htableName)) {
                    hbaseAdmin.disableTable(htableName);
                    hbaseAdmin.deleteTable(htableName);
                    log.info("Deleted HBase table " + htableName);
                } else {
                    log.info("HBase table" + htableName + " does not exist");
                }
            }
        } else {
            System.out.println("--------------- Tables To Be Dropped ---------------");
            for (String htableName : allTablesNeedToBeDropped) {
                System.out.println(htableName);
            }
            System.out.println("----------------------------------------------------");
        }

        hbaseAdmin.close();
    }    
    
    private void cleanUnusedHdfsFiles(Configuration conf) throws IOException {
        JobEngineConfig engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

        FileSystem fs = FileSystem.get(conf);
        List<String> allHdfsPathsNeedToBeDeleted = new ArrayList<String>();
        // GlobFilter filter = new
        // GlobFilter(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
        // + "/kylin-.*");
        FileStatus[] fStatus = fs.listStatus(new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()));
        for (FileStatus status : fStatus) {
            String path = status.getPath().getName();
            // System.out.println(path);
            if (path.startsWith(JobInstance.JOB_WORKING_DIR_PREFIX)) {
                String kylinJobPath = engineConfig.getHdfsWorkingDirectory() + "/" + path;
                allHdfsPathsNeedToBeDeleted.add(kylinJobPath);
            }
        }

        List<JobInstance> allJobs = JobDAO.getInstance(KylinConfig.getInstanceFromEnv()).listAllJobs();
        for (JobInstance jobInstance : allJobs) {
            // only remove FINISHED and DISCARDED job intermediate files
            if (isJobInUse(jobInstance) == true) {
                String path = JobInstance.getJobWorkingDir(jobInstance, engineConfig);
                allHdfsPathsNeedToBeDeleted.remove(path);
                log.info("Remove " + path + " from deletion list, as the path belongs to job " + jobInstance.getUuid() + " with status " + jobInstance.getStatus());
            }
        }

        // remove every segment working dir from deletion list
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String jobUuid = seg.getLastBuildJobID();
                if (jobUuid != null && jobUuid.equals("") == false) {
                    String path = JobInstance.getJobWorkingDir(jobUuid, engineConfig.getHdfsWorkingDirectory());
                    allHdfsPathsNeedToBeDeleted.remove(path);
                    log.info("Remove " + path + " from deletion list, as the path belongs to segment " + seg + " of cube " + cube.getName());
                }
            }
        }

        if (delete == true) {
            // remove files
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                log.info("Deleting hdfs path " + hdfsPath);
                Path p = new Path(hdfsPath);
                if (fs.exists(p) == true) {
                    fs.delete(p, true);
                    log.info("Deleted hdfs path " + hdfsPath);
                } else {
                    log.info("Hdfs path " + hdfsPath + "does not exist");
                }
            }
        } else {
            System.out.println("--------------- HDFS Path To Be Deleted ---------------");
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                System.out.println(hdfsPath);
            }
            System.out.println("-------------------------------------------------------");
        }

    }
    
    private void cleanUnusedHdfsFiles(Configuration conf, String[] uuids) throws IOException {
        JobEngineConfig engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());        
        FileSystem fs = FileSystem.get(conf);
        List<String> allHdfsPathsNeedToBeDeleted = new ArrayList<String>();
        FileStatus[] fStatus = fs.listStatus(new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()));
        List<String> fileNameList = new ArrayList<String>();
        List<String> uuidList = new ArrayList<String>();
        Collections.addAll(uuidList, uuids);
        
        for(String uuid : uuids){
            fileNameList.add(JobInstance.JOB_WORKING_DIR_PREFIX + uuid);
        }
        
        for (FileStatus status : fStatus) {
            String path = status.getPath().getName();
            // System.out.println(path);
            if (fileNameList.contains(path)) {
                String kylinJobPath = engineConfig.getHdfsWorkingDirectory() + "/" + path;
                allHdfsPathsNeedToBeDeleted.add(kylinJobPath);
            }
        }
        
        
        //Remove files with cubes with status READY or BUILDING
        for (CubeInstance cube : cubeMgr.listAllCubes()) {
            for (CubeSegment seg : cube.getSegments()) {
                String uuid = seg.getUuid();
                
                if(uuidList.contains(uuid) && (cube.getStatus() == CubeStatusEnum.READY || cube.getStatus() == CubeStatusEnum.BUILDING)){
                    String path = engineConfig.getHdfsWorkingDirectory() + "/" + JobInstance.JOB_WORKING_DIR_PREFIX + uuid;
                    allHdfsPathsNeedToBeDeleted.remove(path);
                    log.info("Remove " + path + " from deletion list, as the path belongs to cube " + cube.getUuid() + " with status " + cube.getStatus());
                }
            }
        }

        if (delete == true) {
            // remove files
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                log.info("Deleting hdfs path " + hdfsPath);
                Path p = new Path(hdfsPath);
                if (fs.exists(p) == true) {
                    fs.delete(p, true);
                    log.info("Deleted hdfs path " + hdfsPath);
                } else {
                    log.info("Hdfs path " + hdfsPath + "does not exist");
                }
            }
        } else {
            System.out.println("--------------- HDFS Path To Be Deleted ---------------");
            for (String hdfsPath : allHdfsPathsNeedToBeDeleted) {
                System.out.println(hdfsPath);
            }
            System.out.println("-------------------------------------------------------");
        }

    }

    private void cleanUnusedIntermediateHiveTable(Configuration conf) throws IOException {
        StringBuilder cmd = new StringBuilder();
        cmd.append("hive -e \"");
        //cmd.append("use " + KylinConfig.getInstanceFromEnv().getHiveWorkingDB() + "; ");
        cmd.append("show tables " + "\'kylin_intermediate_*\'" + "; ");
        cmd.append("\"");
        
        CliCommandExecutor cmdExec = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        String output = cmdExec.execute(cmd.toString());  
        BufferedReader reader = new BufferedReader(new StringReader(output));
        String line = null;
        List<JobInstance> allJobs = JobDAO.getInstance(KylinConfig.getInstanceFromEnv()).listAllJobs();
        List<String> allHiveTablesNeedToBeDeleted = new ArrayList<String>();
        List<String> workingCubeList = new ArrayList<String>();
        
        for (JobInstance jobInstance : allJobs) {
            // only remove FINISHED and DISCARDED hive intermediate table
            if (isJobInUse(jobInstance) == true) {
                String relatedCube = jobInstance.getRelatedCube();
                workingCubeList.add(relatedCube);
            }
        }
        
        while ((line = reader.readLine()) != null) {
            if(line.startsWith("kylin_intermediate_")){
                boolean isNeedDel = true;
                
                for(String workingCube : workingCubeList){
                    if(line.contains(workingCube)){
                        isNeedDel = false;
                        break;
                    }
                }
                
                if(isNeedDel)
                    allHiveTablesNeedToBeDeleted.add(line);
            }
        }
        
        cmd.delete(0, cmd.length());
        cmd.append("hive -e \"");
        
        for(String delHive : allHiveTablesNeedToBeDeleted){
            cmd.append("drop table if exists " + delHive + "; ");
            log.info("Remove " + delHive + " from hive tables.");          
        }
        cmd.append("\"");
        output = cmdExec.execute(cmd.toString());
        
        if(reader != null)
            reader.close();
    }

    private void cleanUnusedIntermediateHiveTable(Configuration conf, String[] uuids) throws IOException {
        StringBuilder cmd = new StringBuilder();
        
        cmd.append("hive -e \"");
        //cmd.append("use " + KylinConfig.getInstanceFromEnv().getHiveWorkingDB() + "; ");
        cmd.append("show tables " + "\'kylin_intermediate_*\'" + "; ");
        cmd.append("\"");
        
        CliCommandExecutor cmdExec = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        String output = cmdExec.execute(cmd.toString());  
        BufferedReader reader = new BufferedReader(new StringReader(output));
        String line = null;
        List<JobInstance> allJobs = JobDAO.getInstance(KylinConfig.getInstanceFromEnv()).listAllJobs();
        List<String> allHiveTablesNeedToBeDeleted = new ArrayList<String>();
        List<String> workingCubeList = new ArrayList<String>();
        
        for (JobInstance jobInstance : allJobs) {
            // only remove FINISHED and DISCARDED hive intermediate table
            if (isJobInUse(jobInstance) == true) {
                String relatedCube = jobInstance.getRelatedCube();
                workingCubeList.add(relatedCube);
            }
        }
        
        while ((line = reader.readLine()) != null) {
            if(line.startsWith("kylin_intermediate_")){
                boolean isNeedDel = true;
                
                for(String workingCube : workingCubeList){
                    if(line.contains(workingCube)){
                        isNeedDel = false;
                        break;
                    }
                }
                
                if(isNeedDel){
                    isNeedDel = false;
                    
                    for(String uuid : uuids){
                        uuid = uuid.replace("-", "_");
                        
                        if(line.endsWith(uuid)){
                            isNeedDel = true;
                            break;
                        }
                    }
                }
                    
                if(isNeedDel)
                    allHiveTablesNeedToBeDeleted.add(line);
            }
        }
        
        cmd.delete(0, cmd.length());
        cmd.append("hive -e \"");
        
        for(String delHive : allHiveTablesNeedToBeDeleted){
            cmd.append("drop table if exists " + delHive + "; ");
            log.info("Remove " + delHive + " from hive tables.");          
        }
        cmd.append("\"");
        output = cmdExec.execute(cmd.toString());
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StorageCleanupJob(), args);
        System.exit(exitCode);
    }
}