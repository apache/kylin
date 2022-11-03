/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.cache.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheContext;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.client.file.cache.PageId;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.CheckAccessPOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.security.authorization.AclEntry;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

public class FileInputStreamTestHelper {

    public static final InstancedConfiguration sConf = new InstancedConfiguration(ConfigurationUtils.defaults());

    public static final int PAGE_SIZE = (int) sConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);

    private static URIStatus generateURIStatus(String path, long len) {
        FileInfo info = new FileInfo();
        info.setFileId(path.hashCode());
        info.setPath(path);
        info.setLength(len);
        return new URIStatus(info);
    }

    protected static class MultiReadByteArrayFileSystem extends ByteArrayFileSystem {
        MultiReadByteArrayFileSystem(Map<AlluxioURI, byte[]> files) {
            super(files);
        }

        @Override
        public FileInStream openFile(AlluxioURI path, OpenFilePOptions options) throws FileDoesNotExistException,
                OpenDirectoryException, FileIncompleteException, IOException, AlluxioException {
            return new MultiReadFileInStream(super.openFile(path, options));
        }

        @Override
        public FileInStream openFile(URIStatus status, OpenFilePOptions options) throws FileDoesNotExistException,
                OpenDirectoryException, FileIncompleteException, IOException, AlluxioException {
            return new MultiReadFileInStream(super.openFile(status, options));
        }
    }

    private static class MultiReadFileInStream extends FileInStream {
        private final FileInStream mIn;

        /**
         * Creates an FileInStream that may not serve read calls in a single call.
         *
         * @param in the backing FileInStream
         */
        public MultiReadFileInStream(FileInStream in) {
            mIn = in;
        }

        @Override
        public int read() throws IOException {
            return mIn.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int toRead = len > 1 ? ThreadLocalRandom.current().nextInt(1, len) : len;
            return mIn.read(b, off, toRead);
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            return mIn.read(buf);
        }

        @Override
        public long getPos() throws IOException {
            return mIn.getPos();
        }

        @Override
        public long remaining() {
            return mIn.remaining();
        }

        @Override
        public void seek(long pos) throws IOException {
            mIn.seek(pos);
        }

        @Override
        public int positionedRead(long position, byte[] buffer, int offset, int length) throws IOException {
            return mIn.positionedRead(position, buffer, offset, length);
        }
    }

    static LocalCacheFileInStream setupWithSingleFile(byte[] data, CacheManager manager) throws Exception {
        Map<AlluxioURI, byte[]> files = new HashMap<>();
        AlluxioURI testFilename = new AlluxioURI("/test");
        files.put(testFilename, data);

        ByteArrayFileSystem fs = new ByteArrayFileSystem(files);

        return new LocalCacheFileInStream(fs.getStatus(testFilename),
                (status) -> fs.openFile(status, OpenFilePOptions.getDefaultInstance()), manager, sConf);
    }

    protected static class ByteArrayFileSystem implements FileSystem {
        private final Map<AlluxioURI, byte[]> mFiles;

        ByteArrayFileSystem(Map<AlluxioURI, byte[]> files) {
            mFiles = files;
        }

        @Override
        public boolean isClosed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkAccess(AlluxioURI path, CheckAccessPOptions options)
                throws InvalidPathException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
                throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
                throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(AlluxioURI path, DeletePOptions options)
                throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(AlluxioURI path, ExistsPOptions options)
                throws InvalidPathException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void free(AlluxioURI path, FreePOptions options)
                throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
                throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public AlluxioConfiguration getConf() {
            return sConf;
        }

        @Override
        public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
                throws FileDoesNotExistException, IOException, AlluxioException {
            if (mFiles.containsKey(path)) {
                return generateURIStatus(path.getPath(), mFiles.get(path).length);
            } else {
                throw new FileDoesNotExistException(path);
            }
        }

        @Override
        public void iterateStatus(AlluxioURI path, ListStatusPOptions options, Consumer<? super URIStatus> action)
                throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
                throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void loadMetadata(AlluxioURI path, ListStatusPOptions options)
                throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
                throws IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateMount(AlluxioURI alluxioPath, MountPOptions options) throws IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileInStream openFile(AlluxioURI path, OpenFilePOptions options) throws FileDoesNotExistException,
                OpenDirectoryException, FileIncompleteException, IOException, AlluxioException {
            if (mFiles.containsKey(path)) {
                return new MockFileInStream(mFiles.get(path));
            } else {
                throw new FileDoesNotExistException(path);
            }
        }

        @Override
        public FileInStream openFile(URIStatus status, OpenFilePOptions options) throws FileDoesNotExistException,
                OpenDirectoryException, FileIncompleteException, IOException, AlluxioException {
            AlluxioURI path = new AlluxioURI(status.getPath());
            if (mFiles.containsKey(path)) {
                return new MockFileInStream(mFiles.get(path));
            } else {
                throw new FileDoesNotExistException(path);
            }
        }

        @Override
        public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
                throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
                throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public AlluxioURI reverseResolve(AlluxioURI ufsUri) throws IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries, SetAclPOptions options)
                throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startSync(AlluxioURI path) throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stopSync(AlluxioURI path) throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAttribute(AlluxioURI path, SetAttributePOptions options)
                throws FileDoesNotExistException, IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unmount(AlluxioURI path, UnmountPOptions options) throws IOException, AlluxioException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Implementation of cache manager that stores cached data in byte arrays in memory.
     */
    protected static class ByteArrayCacheManager implements CacheManager {
        private final Map<PageId, byte[]> mPages;

        /** Metrics for test validation. */
        long mPagesServed = 0;
        long mPagesCached = 0;

        ByteArrayCacheManager() {
            mPages = new HashMap<>();
        }

        @Override
        public boolean put(PageId pageId, byte[] page, CacheContext cacheContext) {
            mPages.put(pageId, page);
            mPagesCached++;
            return true;
        }

        @Override
        public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int offsetInBuffer,
                CacheContext cacheContext) {
            if (!mPages.containsKey(pageId)) {
                return 0;
            }
            mPagesServed++;
            System.arraycopy(mPages.get(pageId), pageOffset, buffer, offsetInBuffer, bytesToRead);
            return bytesToRead;
        }

        @Override
        public boolean delete(PageId pageId) {
            return mPages.remove(pageId) != null;
        }

        @Override
        public State state() {
            return State.READ_WRITE;
        }

        @Override
        public void close() throws Exception {
            // no-op
        }
    }
}
