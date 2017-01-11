package org.apache.kylin.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.engine.mr.HadoopUtil;

import java.io.IOException;

/**
 * Created by xiefan on 17-1-17.
 */
public class HDFSLockManager {

    private static final String LOCK_HOME = "LOCK_HOME";

    private Path lockPath;

    private FileSystem fs;

    public HDFSLockManager(String hdfsWorkingDir) throws IOException{
        this.lockPath = new Path(hdfsWorkingDir,LOCK_HOME);
        this.fs = HadoopUtil.getFileSystem(lockPath);
        if(!fs.exists(lockPath)){
            fs.create(lockPath);
        }
    }

    public HDFSLock getLock(String resourceFullPath) throws IOException,InterruptedException,IllegalStateException{
        HDFSLock lock = new HDFSLock(resourceFullPath);
        boolean success = lock.init(fs);
        if(success){
            return lock;
        }else{
            throw new IllegalStateException("Try get lock fail. Resourse path : " + resourceFullPath);
        }
    }

    public void releaseLock(HDFSLock lock) throws IOException,InterruptedException,IllegalStateException{
        boolean success = lock.release(fs);
        if(!success)
            throw new IllegalStateException("Release lock fail");
    }


}
