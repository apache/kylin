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
import { fromArrToObj } from '../../../util'

export const fieldVisiableMaps = {
  'new': ['name', 'type', 'description'],
  'edit': ['name', 'description', 'configuration']
}

export const titleMaps = {
  'new': 'addProject',
  'edit': 'project'
}

export const disabledProperties = [
  'kylin.source.default'
]

export function getSubmitData (that) {
  const { editType, form } = that

  switch (editType) {
    case 'new':
      return JSON.stringify({
        name: form.name,
        description: form.description,
        maintain_model_type: 'MANUAL_MAINTAIN',
        override_kylin_properties: fromArrToObj(form.properties)
      })
    case 'edit':
      const submitForm = JSON.parse(JSON.stringify(form))

      submitForm.override_kylin_properties = fromArrToObj(submitForm.properties)

      delete submitForm.properties

      return {
        name: submitForm.name,
        desc: JSON.stringify(submitForm)
      }
  }
}
