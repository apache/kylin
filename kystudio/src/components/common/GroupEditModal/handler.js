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
export const fieldVisiableMaps = {
  'new': ['group_name'],
  'assign': ['users']
}

export const titleMaps = {
  'new': 'createGroup',
  'assign': 'assignUser'
}

export function getSubmitData (that) {
  const { editType, form } = that

  switch (editType) {
    case 'new':
      return {
        group_name: form.group_name
      }
    case 'assign':
      return {
        group_name: form.group_name,
        users: form.selected_users
      }
  }
}
