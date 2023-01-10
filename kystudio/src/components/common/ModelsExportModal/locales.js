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
export default {
  'en': {
    exportModel: 'Export Model',
    chooseModels: 'Select Model',
    emptyText: 'No results',
    placeholder: 'Search by model name',
    factTable: 'Fact Table',
    exportSuccess: 'The model metadata package(s) is being generated. Download will start automatically once ready.',
    exportFailed: 'Can\'t export models at the moment. Please try again.',
    export: 'Export',
    fetchModelsFailed: 'Can\'t fetch the model list.',
    exportAllTip: 'To ensure the file could be imported, please don\'t unzip the file or modify the contents.',
    exportOneModelTip: 'The model\'s metadata would be exported. It includes referenced tables, table relationships, partition columns, filter conditions, measures, dimensions, computed columns, and indexes.',
    exportOther: 'Select other contents to be exported',
    exportOtherTips: 'The selected content will be included when overwriting or adding new models during import.',
    override: 'Model rewrite settings',
    loading: 'Loading...',
    disabledOverrideTip: 'No overrides for the selected model(s)',
    exportBrokenModelCheckboxTip: 'Can\'t export model file at the moment as the model is BROKEN',
    subPartitionValues: 'Sub partition values',
    disabledMultPartitionTip: 'No subpartitions included in the selected model(s)'
  }
}
