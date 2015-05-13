package org.apache.kylin.storage.gridtable.diskstore;

import java.io.InputStream;
import java.io.OutputStream;

/**
 */
interface FileSystem {

    boolean checkExistence(String path);

    boolean delete(String path);

    void deleteOnExit(String path);

    boolean createDirectory(String path);

    boolean createFile(String path);

    OutputStream getWriter(String path);

    InputStream getReader(String path);
}
