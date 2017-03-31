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

package org.apache.kylin.storage.gtrecord;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.EmptyGTScanner;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStorage;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScannerWorker {

    private static final Logger logger = LoggerFactory.getLogger(ScannerWorker.class);
    private IGTScanner internal = null;

    public ScannerWorker(ISegment segment, Cuboid cuboid, GTScanRequest scanRequest, String gtStorage, StorageContext context) {
        if (scanRequest == null) {
            logger.info("Segment {} will be skipped", segment);
            internal = new EmptyGTScanner();
            return;
        }

        final GTInfo info = scanRequest.getInfo();

        try {
            IGTStorage rpc = (IGTStorage) Class.forName(gtStorage).getConstructor(ISegment.class, Cuboid.class, GTInfo.class, StorageContext.class).newInstance(segment, cuboid, info, context); // default behavior
            internal = rpc.getGTScanner(scanRequest);
        } catch (IOException | InstantiationException | InvocationTargetException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterator<GTRecord> iterator() {
        return internal.iterator();
    }

    public void close() throws IOException {
        internal.close();
    }
}
