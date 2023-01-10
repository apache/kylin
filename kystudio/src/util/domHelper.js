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
export const download = {
  post (url, data) {
    const $form = document.createElement('form')
    $form.style.display = 'none'
    $form.method = 'POST'
    $form.action = url
    $form.target = '_blank'
    $form.enctype = 'application/x-www-form-urlencoded'

    for (const [key, value] of Object.entries(data)) {
      if (value instanceof Array) {
        value.forEach((item, index) => {
          const $input = document.createElement('input')
          $input.name = `${key}[${index}]`
          $input.value = item
          $form.appendChild($input)
        })
      } else if (typeof value === 'object') {
        const $input = document.createElement('input')
        $input.name = key
        $input.value = JSON.stringify($input.value)
        $form.appendChild($input)
      } else {
        const $input = document.createElement('input')
        $input.name = key
        $input.value = value
        $form.appendChild($input)
      }
    }

    document.body.appendChild($form)
    $form.submit()

    setTimeout(() => {
      document.body.removeChild($form)
    })
  }
}
