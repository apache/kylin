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

package org.apache.kylin.common.persistence.metadata;

import static org.apache.kylin.common.persistence.metadata.FileSystemMetadataStore.JSON_SUFFIX;
import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.META_KEY_PROPERTIES_NAME;

import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileSystemFilterFactory {

    static final String MATCH_ALL_EVAL = "*";

    public static FilterContext convertConditionsToFilter(RawResourceFilter filter, MetadataType type) {
        FilterConversionContext context = new FilterConversionContext(type);
        if (filter != null) {
            for (RawResourceFilter.Condition condition : filter.getConditions()) {
                // 1.Fill the filter conditions
                processCondition(condition, context);
            }
        }

        // 2.Build the filter result
        return buildResult(context);
    }

    private static void processCondition(RawResourceFilter.Condition condition, FilterConversionContext context) {
        create(condition).process(context);
    }

    private static FilterContext buildResult(FilterConversionContext context) {
        val regex = context.buildPathFilterRegex();
        val resPath = context.buildResPath();
        return new FilterContext(resPath, regex, context.getJsonFilterConditions(), context.isWholePath());
    }

    @Data
    @AllArgsConstructor
    public static class FilterContext {
        private String resPath;
        private String regex;
        private RawResourceFilter rawResourceFilter;
        private boolean isWholePath;
    }

    private static ConditionProcessor create(RawResourceFilter.Condition condition) {
        return new SimpleConditionProcessor(condition);
    }

    private static class FilterConversionContext {
        private final MetadataType type;

        private boolean hasWholeKey = false;
        private String keyPath = null;
        @Getter
        private final RawResourceFilter jsonFilterConditions = new RawResourceFilter();
        // The index of pathFilterConditions is 0 for type, 1 for key
        private RawResourceFilter.Condition pathFilterConditions = null;

        public FilterConversionContext(MetadataType type) {
            this.type = type;
        }

        public void setKeyPath(String keyPath) {
            this.keyPath = keyPath;
            this.hasWholeKey = true;
        }

        public void addJsonFilterCondition(RawResourceFilter.Condition condition) {
            this.jsonFilterConditions.addConditions(condition);
        }

        public void setMetaKeyPathCondition(RawResourceFilter.Condition condition) {
            this.pathFilterConditions = condition;
        }

        public String buildResPath() {
            return isWholePath() ? type.name() + "/" + keyPath + JSON_SUFFIX : type.name();
        }

        public String buildPathFilterRegex() {
            if (isWholePath()) {
                return null;
            }
            return pathFilterConditions == null ? null : pathFilterConditions.getEval();
        }

        public boolean isWholePath() {
            return hasWholeKey;
        }
    }

    private interface ConditionProcessor {
        void validate(final RawResourceFilter.Condition condition);

        void process(final FilterConversionContext context);
    }

    private static class SimpleConditionProcessor implements ConditionProcessor {

        private final RawResourceFilter.Condition condition;

        public SimpleConditionProcessor(final RawResourceFilter.Condition condition) {
            validate(condition);
            this.condition = condition;
        }

        @Override
        public void validate(final RawResourceFilter.Condition condition) {
            if (condition == null || condition.getOp() == null || condition.getValues() == null) {
                throw new KylinRuntimeException(
                        "Can not convert condition to filter: " + (condition == null ? "null" : condition.getName()));
            }
        }

        @Override
        public void process(final FilterConversionContext context) {
            val isMetaKey = META_KEY_PROPERTIES_NAME.equals(condition.getName());
            switch (condition.getOp()) {
            case EQUAL:
                if (isMetaKey && !context.hasWholeKey) {
                    context.setKeyPath(condition.getValues().get(0).toString());
                } else if (!isMetaKey) {
                    context.addJsonFilterCondition(condition);
                } // If isMetaKey && context.hasWholeKey, it is a whole path, so ignore it
                break;
            case EQUAL_CASE_INSENSITIVE:
                if (isMetaKey && !context.hasWholeKey) {
                    condition.setEval("(?i)" + Pattern.quote(condition.getValues().get(0).toString() + JSON_SUFFIX));
                    context.setMetaKeyPathCondition(condition);
                } else if (!isMetaKey) {
                    context.addJsonFilterCondition(condition);
                }
                break;
            case LIKE_CASE_INSENSITIVE:
                if (isMetaKey && !context.hasWholeKey) {
                    condition.setEval(String.format(Locale.ROOT, "(?i).*%s.*",
                            Pattern.quote(condition.getValues().get(0).toString())));
                    context.setMetaKeyPathCondition(condition);
                } else if (!isMetaKey) {
                    context.addJsonFilterCondition(condition);
                }
                break;
            case IN:
                // If the in condition is empty, an empty string can skip all the files
                if (isMetaKey && !context.hasWholeKey) {
                    condition.setEval(condition.getValues().stream().map(x -> Pattern.quote(x + JSON_SUFFIX))
                            .collect(Collectors.joining("|")));
                    context.setMetaKeyPathCondition(condition);
                } else if (!isMetaKey) {
                    context.addJsonFilterCondition(condition);
                }
                break;
            case GT:
            case LT:
            case GE:
            case LE:
            default:
                context.addJsonFilterCondition(condition);
                break;
            }
        }
    }
}
