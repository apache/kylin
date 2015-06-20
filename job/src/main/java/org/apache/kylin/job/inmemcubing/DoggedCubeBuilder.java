/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.kylin.job.inmemcubing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When base cuboid does not fit in memory, cut the input into multiple splits and merge the split outputs at last.
 */
public class DoggedCubeBuilder extends AbstractInMemCubeBuilder {

    private static Logger logger = LoggerFactory.getLogger(DoggedCubeBuilder.class);

    public DoggedCubeBuilder(CubeDesc cubeDesc, Map<TblColRef, Dictionary<?>> dictionaryMap) {
        super(cubeDesc, dictionaryMap);
    }

    @Override
    public void build(BlockingQueue<List<String>> input, ICuboidWriter output) throws IOException {
        new BuildOnce().build(input, output);
    }

    private class BuildOnce {

        public void build(BlockingQueue<List<String>> input, ICuboidWriter output) throws IOException {
            List<SplitThread> splits = new ArrayList<SplitThread>();
            Merger merger = new Merger();
            SplitThread last = null;
            boolean eof = false;

            while (!eof) {

                if (last != null && shouldCutSplit()) {
                    cutSplit(last);
                    last = null;
                }
                
                checkException(splits);
                
                if (last == null) {
                    last = new SplitThread(merger.newMergeSlot());
                    splits.add(last);
                    last.start();
                }
                
                eof = feedSomeInput(input, last, 1000);
            }
            
            merger.mergeAndOutput(splits, output);
            
            checkException(splits);
        }

        private void checkException(List<SplitThread> splits) throws IOException {
            for (int i = 0; i < splits.size(); i++) {
                SplitThread split = splits.get(i);
                if (split.exception != null)
                    abort(splits);
            }
        }

        private void abort(List<SplitThread> splits) throws IOException {
            for (SplitThread split : splits) {
                split.builder.abort();
            }
            
            ArrayList<Throwable> errors = new ArrayList<Throwable>();
            for (SplitThread split : splits) {
                try {
                    split.join();
                } catch (InterruptedException e) {
                    errors.add(e);
                }
                if (split.exception != null)
                    errors.add(split.exception);
            }
            
            if (errors.isEmpty()) {
                return;
            } else if (errors.size() == 1) {
                Throwable t = errors.get(0);
                if (t instanceof IOException)
                    throw (IOException) t;
                else
                    throw new IOException(t);
            } else {
                for (Throwable t : errors)
                    logger.error("Exception during in-mem cube build", t);
                throw new IOException(errors.size() + " exceptions during in-mem cube build, cause set to the first, check log for more", errors.get(0));
            }

        }

        private boolean feedSomeInput(BlockingQueue<List<String>> input, SplitThread split, int n) {
            try {
                int i = 0;
                while (i < n) {
                    List<String> record = input.take();
                    i++;
                    
                    while (split.inputQueue.offer(record, 1, TimeUnit.SECONDS) == false) {
                        if (split.exception != null)
                            return true; // got some error
                    }
                    
                    if (record == null || record.isEmpty()) {
                        return true;
                    }
                }
                return false;
                
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void cutSplit(SplitThread last) {
            try {
                // signal the end of input
                while (last.isAlive()) {
                    if (last.inputQueue.offer(Collections.<String> emptyList())) {
                        break;
                    }
                    Thread.sleep(1000);
                }
                
                // wait cuboid build done (but still pending output)
                while (last.isAlive()) {
                    if (last.builder.isAllCuboidDone()) {
                        break;
                    }
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private boolean shouldCutSplit() {
            return MemoryBudgetController.getSystemAvailMB() <= reserveMemoryMB;
        }
    }

    private class SplitThread extends Thread {
        final BlockingQueue<List<String>> inputQueue = new ArrayBlockingQueue<List<String>>(64);
        final BlockingQueue<Pair<Long, GTRecord>> outputQueue = new ArrayBlockingQueue<Pair<Long, GTRecord>>(64);
        final InMemCubeBuilder builder;
        final MergeSlot output;

        RuntimeException exception;

        public SplitThread(MergeSlot output) {
            this.builder = new InMemCubeBuilder(cubeDesc, dictionaryMap);
            this.builder.setConcurrentThreads(taskThreadCount);
            this.builder.setOutputOrder(true); // sort merge requires order
            this.builder.setReserveMemoryMB(reserveMemoryMB);
            
            this.output = output;
        }

        @Override
        public void run() {
            try {
                builder.build(inputQueue, output);
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    this.exception = (RuntimeException) e;
                else
                    this.exception = new RuntimeException(e);
            }
        }
    }
    
    private class Merger {
        
        public MergeSlot newMergeSlot() {
            return new MergeSlot();
        }

        public void mergeAndOutput(List<SplitThread> splits, ICuboidWriter output) {
            // TODO
        }
    }
    
    private static class MergeSlot implements ICuboidWriter {
        
        BlockingQueue<MergeSlot> queue = new ArrayBlockingQueue<MergeSlot>(1);
        long cuboidId;
        GTRecord record;
        
        @Override
        public void write(long cuboidId, GTRecord record) throws IOException {
            this.cuboidId = cuboidId;
            this.record = record;
        
            try {
                // deliver the record
                queue.put(this);
                
                // confirm merger consumed (took) the record
                queue.put(this);
                
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    };
}
