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
export const editTypes = {
  EDIT: 'edit'
}

export const fieldVisiableMaps = {
  [editTypes.EDIT]: []
}

export const titleMaps = {
  [editTypes.EDIT]: 'editAggregateGroup'
}

export function getPlaintDimensions (array = []) {
  const dimensions = []
  array.forEach(({ items = [] }) => {
    items.forEach(item => {
      dimensions.push(item)
    })
  })
  return dimensions
}

export function findIncludeDimension (includeOptEls, dimensionValueText) {
  for (let i = 0; i < includeOptEls.length; i++) {
    const includeOptEl = includeOptEls[i]
    const includeOptLabel = includeOptEl.querySelector('.el-select__tags-text').innerHTML

    if (includeOptLabel === dimensionValueText) {
      return includeOptEl
    }
  }
}
