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
/* eslint-disable no-extend-native */
if (!String.prototype.trimLeft) {
  String.prototype.trimLeft = function () {
    return this.trim(1)
  }
}

if (!String.prototype.trimRight) {
  String.prototype.trimRight = function () {
    return this.trim(2)
  }
}

// 兼容 ie 10 下的 uuid 库
function _randomUUID (min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min)
}

if (window.crypto && !window.crypto.getRandomValues) {
  window.crypto.getRandomValues = function (buf) {
    let min = 0
    let max = 255
    if (buf.length > 65536) {
      let e = new Error()
      e.code = 22
      e.message = `Failed to execute 'getRandomValues' : The ArrayBufferView's byte length (${buf.length}) exceeds the number of bytes of entropy available via this API (65536).`
      e.name = 'QuotaExceededError'
      throw e
    }
    if (buf instanceof Uint16Array) {
      max = 65535
    } else if (buf instanceof Uint32Array) {
      max = 4294967295
    }
    for (var element in buf) {
      buf[element] = _randomUUID(min, max)
    }
    return buf
  }
}
