package org.apache.kylin.storage.gridtable.diskstore;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 */
class HadoopFileSystem implements FileSystem {

    private static final Logger logger = LoggerFactory.getLogger(HadoopFileSystem.class);

    final org.apache.hadoop.fs.FileSystem fileSystem;

    HadoopFileSystem() {
        try {
            fileSystem = org.apache.hadoop.fs.FileSystem.get(HadoopUtil.getCurrentConfiguration());
        } catch (IOException e) {
            logger.error("error construct HadoopFileSystem", e);
            throw new RuntimeException(e);
        }
    }
    @Override
    public boolean checkExistence(String path) {
        try {
            return fileSystem.exists(new Path(path));
        } catch (IOException e) {
            logger.error("error checkExistence, path:" + path, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean delete(String path) {
        try {
            return fileSystem.delete(new Path(path), true);
        } catch (IOException e) {
            logger.error("error delete, path:" + path, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteOnExit(String path) {
        try {
            fileSystem.deleteOnExit(new Path(path));
        } catch (IOException e) {
            logger.error("error deleteOnExit, path:" + path, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean createDirectory(String path) {
        try {
            return fileSystem.mkdirs(new Path(path));
        } catch (IOException e) {
            logger.error("error createDirectory, path:" + path, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean createFile(String path) {
        try {
            return fileSystem.createNewFile(new Path(path));
        } catch (IOException e) {
            logger.error("error createFile, path:" + path, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public OutputStream getWriter(String path) {
        try {
            return fileSystem.create(new Path(path));
        } catch (IOException e) {
            logger.error("error getWriter, path:" + path, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream getReader(String path) {
        try {
            return fileSystem.open(new Path(path));
        } catch (IOException e) {
            logger.error("error getReader, path:" + path, e);
            throw new RuntimeException(e);
        }
    }
}
