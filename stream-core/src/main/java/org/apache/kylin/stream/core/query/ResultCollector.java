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

package org.apache.kylin.stream.core.query;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ResultCollector implements IStreamingSearchResult{
    private static Logger logger = LoggerFactory.getLogger(ResultCollector.class);

    protected List<IStreamingSearchResult> searchResults = Lists.newArrayList();
    private List<CloseListener> closeListeners = Lists.newArrayList();

    public void collectSearchResult(IStreamingSearchResult searchResult) {
        searchResults.add(searchResult);
    }

    public void addCloseListener(CloseListener listener) {
        closeListeners.add(listener);
    }

    @Override
    public void close() throws IOException{
        Exception oneException = null;
        for (CloseListener listener : closeListeners) {
            try {
                listener.onClose();
            } catch (Exception e) {
                logger.error("exception throws when on close is called", e);
            }
        }
        for (IStreamingSearchResult input : searchResults) {
            try {
                input.close();
            } catch (Exception e) {
                oneException = e;
            }
        }
        if (oneException != null) {
            if (oneException instanceof IOException) {
                throw (IOException)oneException;
            }
            throw new IOException(oneException);
        }
    }

    public void startRead(){
    }

    public void endRead(){
    }

    public interface CloseListener {
        void onClose();
    }
}
