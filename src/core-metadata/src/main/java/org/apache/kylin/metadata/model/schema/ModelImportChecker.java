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

package org.apache.kylin.metadata.model.schema;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.schema.strategy.ComputedColumnStrategy;
import org.apache.kylin.metadata.model.schema.strategy.MultiplePartitionStrategy;
import org.apache.kylin.metadata.model.schema.strategy.OverWritableStrategy;
import org.apache.kylin.metadata.model.schema.strategy.SchemaChangeStrategy;
import org.apache.kylin.metadata.model.schema.strategy.TableColumnStrategy;
import org.apache.kylin.metadata.model.schema.strategy.TableStrategy;
import org.apache.kylin.metadata.model.schema.strategy.UnOverWritableStrategy;

import lombok.val;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ModelImportChecker {

    private static final List<SchemaChangeStrategy> strategies = Arrays.asList(new ComputedColumnStrategy(),
            new UnOverWritableStrategy(), new TableColumnStrategy(), new TableStrategy(), new OverWritableStrategy(),
            new MultiplePartitionStrategy());

    public static SchemaChangeCheckResult check(SchemaUtil.SchemaDifference difference,
            ImportModelContext importModelContext) {
        Set<String> importModels = NDataModelManager
                .getInstance(importModelContext.getImportKylinConfig(), importModelContext.getTargetProject())
                .listAllModelAlias().stream().map(model -> importModelContext.getNewModels().getOrDefault(model, model))
                .collect(Collectors.toSet());

        Set<String> originalModels = NDataModelManager
                .getInstance(KylinConfig.getInstanceFromEnv(), importModelContext.getTargetProject())
                .listAllModelAlias();

        val result = new SchemaChangeCheckResult();
        for (SchemaChangeStrategy strategy : strategies) {
            result.addMissingItems(strategy.missingItems(difference, importModels, originalModels));
            result.addNewItems(strategy.newItems(difference, importModels, originalModels));
            result.addReduceItems(strategy.reduceItems(difference, importModels, originalModels));
            result.addUpdateItems(strategy.updateItems(difference, importModels, originalModels));
            result.areEqual(strategy.areEqual(difference, importModels));
        }
        return result;
    }
}
