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
