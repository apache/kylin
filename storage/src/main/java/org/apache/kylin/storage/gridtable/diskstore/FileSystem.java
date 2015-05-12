package org.apache.kylin.storage.gridtable.diskstore;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by qianzhou on 5/4/15.
 */
interface FileSystem {

    boolean checkExistence(String path);

    boolean delete(String path);

    boolean createDirectory(String path);

    boolean createFile(String path);

    OutputStream getWriter(String path);

    InputStream getReader(String path);
}
