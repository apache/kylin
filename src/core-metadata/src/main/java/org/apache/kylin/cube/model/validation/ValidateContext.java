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

package org.apache.kylin.cube.model.validation;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Context. Supply all dependent objects for validator
 */
public class ValidateContext {
    private List<Result> results = new ArrayList<ValidateContext.Result>();

    public void addResult(ResultLevel level, String message) {
        results.add(new Result(level, message));
    }

    public void addResult(Result result) {
        results.add(result);
    }

    public class Result {
        private ResultLevel level;
        private String message;

        /**
         * @param level
         * @param message
         */
        public Result(ResultLevel level, String message) {
            this.level = level;
            this.message = message;
        }

        /**
         * @return the level
         */
        public ResultLevel getLevel() {
            return level;
        }

        /**
         * @return the message
         */
        public String getMessage() {
            return message;
        }
    }

    /**
     * Get validation result
     *
     * @return
     */
    public Result[] getResults() {
        Result[] rs = new Result[0];
        rs = results.toArray(rs);
        return rs;
    }

    /**
     *
     */
    public void print(PrintStream out) {
        if (results.isEmpty()) {
            out.println("The element is perfect.");
        }
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result result = it.next();
            out.println(result.level + " : " + result.message);
        }
    }

    /**
     * @return if there is not validation errors
     */
    public boolean ifPass() {
        return results.isEmpty();
    }

}
