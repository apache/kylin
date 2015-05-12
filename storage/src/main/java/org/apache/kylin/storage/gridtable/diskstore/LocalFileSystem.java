package org.apache.kylin.storage.gridtable.diskstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by qianzhou on 5/4/15.
 */
class LocalFileSystem implements FileSystem {

    private static Logger logger = LoggerFactory.getLogger(LocalFileSystem.class);
    @Override
    public boolean checkExistence(String path) {
        return new File(path).exists();
    }

    @Override
    public boolean delete(String path) {
        return new File(path).delete();
    }

    @Override
    public boolean createDirectory(String path) {
        return new File(path).mkdirs();
    }

    @Override
    public boolean createFile(String path) {
        try {
            return new File(path).createNewFile();
        } catch (IOException e) {
            logger.warn("create file failed:" + path, e);
            return false;
        }
    }

    @Override
    public OutputStream getWriter(String path) {
        try {
            return new FileOutputStream(path);
        } catch (FileNotFoundException e) {
            //should not happen
            logger.error("path:" + path + " nout found");
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream getReader(String path) {
        try {
            return new FileInputStream(path);
        } catch (FileNotFoundException e) {
            //should not happen
            logger.error("path:" + path + " nout found");
            throw new RuntimeException(e);
        }
    }
}
