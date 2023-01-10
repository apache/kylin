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
    addJoinCondition: 'Add Join Relationship',
    checkCompleteLink: 'connection info is incomplete',
    delConn: 'Delete connection',
    delConnTip: 'Are you sure you want to delete this connection?',
    noColumnFund: 'Column not found',
    tableJoin: 'Join Relationship for Tables',
    columnsJoin: 'Join Relationship for Columns',
    joinNotice: 'Join relationships must meet the following criteria. ',
    joinErrorNotice: 'The join condition can\'t be saved. Please modify and try again.',
    details: 'View Details',
    notice1: 'Can\'t define multiple join conditions for the same columns',
    notice2: 'Join relationship ≥ and < must be used in pairs, and same column must be joint in both conditions',
    notice3: 'Join relationship for columns should include at least one equal-join condition (=)',
    notice4: 'Two tables could only be joined by the same condition for one time',
    manyToOne: 'One-to-One or Many-to-One',
    oneToOne: 'One-to-One',
    oneToMany: 'One-to-Many',
    manyToMany: 'One-to-Many or Many-to-Many',
    tableRelationTips: 'When the relationship is one-to-many or many-to-many, the columns which are not defined as dimensions from the joined table {tableName} can\'t be queried.',
    tableRelation: 'Table Relationship',
    precomputeJoin: 'Precompute Join Relationships',
    precomputeJoinTip: '* If precomputing join relationships, the columns which are not defined as dimensions from the joined dimension table can’t be queried. <br/> * If not precomputing join relationships, the storage and operation cost would be reduced. However, the columns from the joined dimension table can\'t used for dimensions, measures, computed columns and indexes of the model. Query can be served by adding the join key to the aggregate group. ',
    disabledPrecomputeJoinTip: 'Precomputing the join relationships is not allowed. Because the joined dimension table is in the excluded rule of recommendation setting, precomputing may cause mistakes in query results. ',
    backEdit: 'Continue Editing',
    deletePrecomputeJoinDialogTips: 'If not precomputing the join relationships, the columns from the joined dimension table can’t be used for dimensions, measures, computed columns and indexes of the model. The following data will be deleted when the model is saved：',
    measureCollapse: 'Measures ({num})',
    dimensionCollapse: 'Dimensions ({num})',
    computedColumnCollapse: 'Computed Columns ({num})',
    indexesCollapse: 'Indexes ({num})',
    aggIndexes: 'Aggregate Indexes ({len})',
    tableIndexes: 'Table Indexes ({len})',
    noFactTableTips: 'Unable to add join relationship between dimension tables. Please add join relationship of fact table first.'
  }
}
