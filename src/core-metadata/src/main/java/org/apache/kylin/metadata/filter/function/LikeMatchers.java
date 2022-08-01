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
package org.apache.kylin.metadata.filter.function;

import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

public class LikeMatchers {

    private final static String PERCENT_SIGN = "%";
    private final static String UNDERSCORE_SIGN = "_";

    public interface LikeMatcher {
        boolean matches(String input);
    }

    public static class DefaultLikeMatcher implements LikeMatcher {
        private Pattern p;

        public DefaultLikeMatcher(String patternStr, String escapeStr) {
            patternStr = patternStr.toLowerCase(Locale.ROOT);
            final String regex = Like.sqlToRegexLike(patternStr, escapeStr);
            p = Pattern.compile(regex);
        }

        private DefaultLikeMatcher(String patternStr) {
            this(patternStr, null);
        }

        @Override
        public boolean matches(String input) {
            return p.matcher(input).matches();
        }
    }

    // abc%, %abc, abc%def
    public static class OnePercentSignLikeMatcher implements LikeMatcher {

        enum SignPosition {
            LEFT, MIDDLE, RIGHT
        }

        private SignPosition signPosition;
        private String[] remaining;

        private OnePercentSignLikeMatcher(String patternStr) {
            if (patternStr.startsWith(PERCENT_SIGN)) {
                signPosition = SignPosition.LEFT;
            } else if (patternStr.endsWith(PERCENT_SIGN)) {
                signPosition = SignPosition.RIGHT;
            } else {
                signPosition = SignPosition.MIDDLE;
            }
            remaining = StringUtils.split(patternStr, PERCENT_SIGN);
        }

        @Override
        public boolean matches(String input) {
            if (input == null)
                return false;

            switch (signPosition) {
            case LEFT:
                return input.endsWith(remaining[0]);
            case RIGHT:
                return input.startsWith(remaining[0]);
            case MIDDLE:
                return input.startsWith(remaining[0]) && input.endsWith(remaining[1]);
            default:
                throw new IllegalStateException();
            }
        }
    }

    //only deal with %abc%
    public static class TwoPercentSignLikeMatcher implements LikeMatcher {
        private String[] remaining;

        private TwoPercentSignLikeMatcher(String patternStr) {
            remaining = StringUtils.split(patternStr, PERCENT_SIGN);
            Preconditions.checkState(remaining.length == 1);
        }

        @Override
        public boolean matches(String input) {
            return input.contains(remaining[0]);
        }
    }

    //only deal with %abc%def%
    public static class ThreePercentSignLikeMatcher implements LikeMatcher {
        private String[] remaining;

        private ThreePercentSignLikeMatcher(String patternStr) {
            remaining = StringUtils.split(patternStr, PERCENT_SIGN);
            Preconditions.checkState(remaining.length == 2);
        }

        @Override
        public boolean matches(String input) {
            int i = input.indexOf(remaining[0]);
            int j = input.lastIndexOf(remaining[1]);
            return (i != -1) && (j != -1) && (i <= j - remaining[0].length());
        }
    }

    public static LikeMatcher createMatcher(String patternStr) {
        if (patternStr == null) {
            throw new IllegalArgumentException("pattern is null");
        }

        if (patternStr.contains(UNDERSCORE_SIGN)) {
            return new DefaultLikeMatcher(patternStr);
        }

        int count = StringUtils.countMatches(patternStr, PERCENT_SIGN);
        if (count == 1) {
            return new OnePercentSignLikeMatcher(patternStr);
        } else if (count == 2 && patternStr.startsWith(PERCENT_SIGN) && patternStr.endsWith(PERCENT_SIGN)) {
            return new TwoPercentSignLikeMatcher(patternStr);
        } else if (count == 3 && patternStr.startsWith(PERCENT_SIGN) && patternStr.endsWith(PERCENT_SIGN)
                && !patternStr.contains(PERCENT_SIGN + PERCENT_SIGN)) {
            return new ThreePercentSignLikeMatcher(patternStr);
        } else {
            return new DefaultLikeMatcher(patternStr);
        }
    }
}
