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
  'new': ['username', 'password', 'confirmPassword', 'admin'],
  'password': ['username', 'oldPassword', 'newPassword', 'confirmPassword'],
  'resetUserPassword': ['username', 'newPassword', 'confirmPassword'],
  'edit': ['username', 'admin'],
  'group': ['group']
}

export const titleMaps = {
  'new': 'addUser',
  'password': 'resetPassword',
  'resetUserPassword': 'resetPassword',
  'edit': 'editRole',
  'group': 'groupMembership'
}

export function getSubmitData (that) {
  const { editType, form, $route } = that

  switch (editType) {
    case 'new':
      return {
        name: form.username,
        detail: {
          username: form.username,
          password: form.password,
          disabled: form.disabled,
          authorities: ($route.params.groupName && !form.authorities.includes($route.params.groupName))
            ? [...form.authorities, $route.params.groupName]
            : form.authorities
        }
      }
    case 'password':
    case 'resetUserPassword':
      return {
        username: form.username,
        password: form.oldPassword,
        new_password: form.newPassword
      }
    case 'edit':
      return {
        uuid: form.uuid,
        username: form.username,
        default_password: form.default_password,
        disabled: form.disabled,
        authorities: form.authorities
      }
    case 'group':
      return {
        uuid: form.uuid,
        username: form.username,
        authorities: form.authorities
      }
  }
}
