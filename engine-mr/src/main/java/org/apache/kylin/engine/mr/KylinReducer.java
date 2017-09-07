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

package org.apache.kylin.engine.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class KylinReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    private static final Logger logger = LoggerFactory.getLogger(KylinReducer.class);

    protected int reduceCounter = 0;

    protected void bindCurrentConfiguration(Configuration conf) {
        HadoopUtil.setCurrentConfiguration(conf);
    }

    @Override
    final protected void setup(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        try {
            logger.info("Do setup, available memory: {}m", MemoryBudgetController.getSystemAvailMB());
            doSetup(context);
        } catch (IOException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (InterruptedException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (RuntimeException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (Error ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        }
    }

    protected void doSetup(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        // NOTHING
    }

    @Override
    final public void reduce(KEYIN key, Iterable<VALUEIN> values,
            Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        try {
            if (reduceCounter++ % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
                logger.info("Accepting Reducer Key with ordinal: " + reduceCounter);
                logger.info("Do reduce, available memory: {}m", MemoryBudgetController.getSystemAvailMB());
            }

            doReduce(key, values, context);
        } catch (IOException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (InterruptedException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (RuntimeException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (Error ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        }
    }

    protected void doReduce(KEYIN key, Iterable<VALUEIN> values,
            Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }

    @Override
    final protected void cleanup(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        try {
            logger.info("Do cleanup, available memory: {}m", MemoryBudgetController.getSystemAvailMB());
            doCleanup(context);
            logger.info("Total rows: " + reduceCounter);
        } catch (IOException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (InterruptedException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (RuntimeException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (Error ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        }
    }

    protected void doCleanup(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
    }
}
