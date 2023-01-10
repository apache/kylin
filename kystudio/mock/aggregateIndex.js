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
let nodeTotalCount = 0

function getAggregateData () {
  const data =  {
    cuboidDesc: {
      id: Math.random() * 100000000,
      dimensions: [
        "KYLIN_ACCOUNT.ACCOUNT_ID",
        "KYLIN_SALES.SELLER_ID"
      ],
      size: Math.random() * 200000,
      range: {
        startDate: new Date('Thu Jan 1 2018 00:00:00 GMT+0800 (中国标准时间)').setDate(nodeTotalCount),
        endDate: new Date('Thu Jan 1 2018 00:00:00 GMT+0800 (中国标准时间)').setDate(nodeTotalCount + 1)
      },
      savedQueryCount: Math.random() * 100,
      measures: [],
      layouts: []
    },
    children: [],
    parent: []
  }
  nodeTotalCount++

  return data
}

function getAggregateTree () {
  const root = getAggregateData()
  getAggregateNode(root)
  return root
}

function getAggregateNode (parent) {
  const nodeCount = Math.round(Math.random() * 3)
  for (let i = 0; i < nodeCount; i++) {
    parent.children.push(getAggregateData())

    if (nodeTotalCount < 99) {
      getAggregateNode(parent.children[i])
    }
  }
}

export const aggregateTree = {
  "data": {
    "code": "000",
    "data": {
      "size": 120,
      "data": getAggregateTree()
    },
    "msg": ""
  }
}
