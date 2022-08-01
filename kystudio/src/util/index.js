import validate, * as validateTypes from './validate'
import * as dataGenerator from './dataGenerator'
import * as dataHelper from './dataHelper'
import autoLayout from './autoLayout'
import { handleSuccess } from './business'
// 获取部分对象字断
export function collectObject (obj, keys, needTransToString, ignoreNull) {
  let newObj = {}
  keys.forEach((k) => {
    if (ignoreNull && obj[k] || !ignoreNull) {
      newObj[k] = obj[k]
    }
  })
  if (needTransToString) {
    return JSON.stringify(newObj)
  }
  return newObj
}
export function fromObjToArr (obj) {
  let arr = []
  for (let key of Object.keys(obj)) {
    arr.push({
      key: key,
      value: obj[key]
    })
  }
  return arr
}

export function fromArrToObj (arr) {
  let obj = {}
  for (let item of arr) {
    obj[item.key] = item.value
  }
  return obj
}

export function sampleGuid () {
  let randomNumber = ('' + Math.random()).replace(/\./, '')
  return (new Date()).getTime() + '_' + randomNumber
}

var bailRE = /[^\w.$]/
// 解析对象路径属性
export function parsePath (path) {
  if (bailRE.test(path)) {
    return
  } else {
    var segments = path.split('.')
    return function (obj) {
      if (!path) {
        return obj
      }
      for (var i = 0; i < segments.length; i++) {
        if (!obj) { return }
        obj = obj[segments[i]]
      }
      return obj
    }
  }
}
// 科学计数法数字转换成可读数字
export function scToFloat (data) {
  var resultValue = ''
  if (data && data.indexOf('E') !== -1) {
    var regExp = new RegExp('^((\\d+.?\\d+)[Ee]{1}(\\d+))$', 'ig')
    var result = regExp.exec(data)
    var power = ''
    if (result !== null) {
      resultValue = result[2]
      power = result[3]
    }
    if (resultValue !== '' && power !== '') {
      var powVer = Math.pow(10, power)
      resultValue = (resultValue * powVer).toFixed(2)
      return resultValue
    }
  }
  return data
}
// 显示显示null
export function showNull (val) {
  if (val === null) {
    return 'null'
  }
  return val
}
// 将对象数组按照某一个key的值生成对象
export function groupData (data, groupName) {
  var len = data && data.length || 0
  var obj = {}
  for (var k = 0; k < len; k++) {
    obj[data[k][groupName]] = obj[data[k][groupName]] || []
    obj[data[k][groupName]].push(data[k])
  }
  return obj
}

export function countObjWithSomeKey (objectArr, key, equalVal) {
  let len = objectArr && objectArr.length || 0
  let count = 0
  for (var i = 0; i < len; i++) {
    var filterObj = objectArr[i]
    if (filterObj[key] === equalVal) {
      count++
    }
  }
  return count
}
// 从对象数组中找到某个符合key value 的对象的位置
export function indexOfObjWithSomeKey (objectArr, key, equalVal) {
  let len = objectArr && objectArr.length || 0
  for (var i = 0; i < len; i++) {
    var filterObj = objectArr[i]
    if (filterObj[key] === equalVal) {
      return i
    }
  }
  return -1
}
export function indexOfObjWithSomeKeys (objectArr, key, equalVal, key1, equalVal1) {
  for (var i = 0; i < objectArr.length; i++) {
    var filterObj = objectArr[i]
    if (filterObj[key] === equalVal && filterObj[key1] === equalVal1) {
      return i
    }
  }
  return -1
}
// 对象数组排序 （chrome 对象数组排序原生sort有bug）
export function objectArraySort (objArr, sequence, sortKey) {
  var objectArr = objectClone(objArr)
  var condition
  for (var i = 0; i < objectArr.length; i++) {
    for (var k = i + 1; k < objectArr.length; k++) {
      if (sequence) {
        condition = objectArr[i][sortKey] > objectArr[k][sortKey]
      } else {
        condition = objectArr[i][sortKey] <= objectArr[k][sortKey]
      }
      if (condition) {
        let temp = objectArr[i]
        objectArr[i] = objectArr[k]
        objectArr[k] = temp
      }
    }
  }
  return objectArr
}
// 拆分数组为子数组
/* arr:['a', 'b', 'c', 'd']
  result: [['a'], ['b'], ['c'], ['d']]
*/
export function split_array (arr, len) {
  var a_len = arr.length
  var result = []
  for (var i = 0; i < a_len; i += len) {
    result.push(arr.slice(i, i + len))
  }
  return result
}
/* 一个数组按照另一个数组里的排序进行重排
* args: arr1:['a', 'b', 'c', 'd']  arr2:['d','a']
* result ["d", "b", "c", "a"]
*/
export function arrSortByArr (arr1, arr2) {
  let pos = []
  arr2.forEach((s) => {
    let i = arr1.indexOf(s)
    if (i >= 0 && pos.indexOf(i) === -1) { // 当找到位置且没有重复的时候，记录位置
      pos.push(arr1.indexOf(s))
    }
  })
  pos.sort((a, b) => {
    return a - b
  })
  pos.forEach((p, i) => {
    arr1[p] = arr2[i]
  })
  return arr1
}
// 对象克隆
export function objectClone (obj) {
  if (typeof obj !== 'object') {
    return obj
  }
  var s = {}
  if (!obj) {
    return obj
  }
  if (obj.constructor === Array) {
    s = []
  }
  for (var i in obj) {
    s[i] = objectClone(obj[i])
  }
  return s
}
// 改变对象数组里对象的某个属性
export function changeObjectArrProperty (objectArr, key, val, newKey, newVal, _this) {
  var arr = objectArr
  let len = arr && arr.length || 0
  let setKey = ''
  let setVal = ''
  let vue = null
  if (key === '*') {
    setKey = val
    setVal = newKey
    vue = newVal
  } else {
    setKey = newKey
    setVal = newVal
    vue = _this
  }
  for (let i = 0; i < len; i++) {
    if (arr[i][key] === val || key === '*') {
      if (vue) {
        vue.$set(arr[i], setKey, setVal)
      } else {
        arr[i][setKey] = setVal
      }
    }
  }
}
// 获取对象数组对象属性符合条件的对象
export function filterObjectArray (objectArr, key, val) {
  objectArr = objectArr || []
  var resultArr = objectArr.filter((obj) => {
    return obj[key] === val
  })
  return resultArr
}

export function getNextOrPrevDate (days) {
  var dd = new Date()
  dd.setDate(dd.getDate() + days || 0)
  return dd.getTime()
}
export function isToday (ms) {
  var dt = new Date()
  var y = dt.getFullYear()
  var m = dt.getMonth()
  var d = dt.getDate()
  var ndt = new Date(ms)
  var ny = ndt.getFullYear()
  var nm = ndt.getMonth()
  var nd = ndt.getDate()
  if (y === ny && m === nm && d === nd) {
    return true
  }
  return false
}
// ms需要识别的时间戳，nowms当前的时间戳
export function isThisWeek (ms, nowms) {
  var oneDayTime = 1000 * 60 * 60 * 24
  let now = nowms || Date.now()
  var oldDays = parseInt(+ms / oneDayTime)
  var nowDays = parseInt(+now / oneDayTime)
  return parseInt((oldDays + 4) / 7) === parseInt((nowDays + 4) / 7)
}
export function isLastWeek (ms) {
  let lastWeekDateTimeStamp = getNextOrPrevDate(-7)
  return isThisWeek(ms, lastWeekDateTimeStamp)
}
function isValidDate (date) {
  return date instanceof Date && !isNaN(date.getTime())
}
// 时间转换工具
import moment from 'moment-timezone'
export function utcToConfigTimeZone (item, zone, formatSet) {
  var timezone = zone
  if (item === '' || item === null || item === undefined) {
    return ''
  }
  if (!isValidDate(new Date(item))) {
    return item
  }
  let momentObj = moment(item).tz(timezone)
  let offset = momentObj ? momentObj._offset / 60 : 0
  let timestr = momentObj ? momentObj.format('YYYY-MM-DD HH:mm:ss ') + 'GMT' + (offset >= 0 ? '+' + offset : offset) : ''
  return timestr
}
export function isIE () {
  if (!!window.ActiveXObject || 'ActiveXObject' in window) {
    return true
  }
  return false
}

export function isObject (obj) {
  return obj !== null && typeof obj === 'object'
}

export function looseEqual (a, b) {
  if (a === b) { return true }
  const isObjectA = isObject(a)
  const isObjectB = isObject(b)
  if (isObjectA && isObjectB) {
    try {
      const isArrayA = Array.isArray(a)
      const isArrayB = Array.isArray(b)
      if (isArrayA && isArrayB) {
        return a.length === b.length && a.every(function (e, i) {
          return looseEqual(e, b[i])
        })
      } else if (!isArrayA && !isArrayB) {
        const keysA = Object.keys(a)
        const keysB = Object.keys(b)
        return keysA.length === keysB.length && keysA.every(function (key) {
          return looseEqual(a[key], b[key])
        })
      } else {
        return false
      }
    } catch (e) {
      return false
    }
  } else if (!isObjectA && !isObjectB) {
    return String(a) === String(b)
  } else {
    return false
  }
}

export async function handleSuccessAsync (responses) {
  if (responses instanceof Array) {
    const results = []
    for (const response of responses) {
      results.push(await handleSuccessAsync(response))
    }
    return results
  } else {
    return new Promise((resolve, reject) => {
      handleSuccess(responses, resolve, reject)
    })
  }
}

// 获取url参数
export function getQueryString (name) {
  var reg = new RegExp('(^|&)' + name + '=([^&?]*)(&|$)', 'i') // 匹配目标参数
  var result = window.location.search.substr(1).match(reg) // 对querystring匹配目标参数
  if (result !== null) {
    return decodeURIComponent(result[2])
  } else {
    return null
  }
}

// 获取object full mapping
export function getFullMapping (mapping) {
  const fullMapping = { ...mapping }
  for (const [key, value] of Object.entries(mapping)) {
    fullMapping[value] = key
  }
  return fullMapping
}
export function cacheSessionStorage (name, val) {
  if (val) {
    sessionStorage.setItem(name, val)
  }
  return sessionStorage.getItem(name)
}
export function cacheLocalStorage (name, val) {
  if (val !== undefined) {
    localStorage.setItem(name, val)
  }
  return localStorage.getItem(name, val)
}
export function delayMs (ms) {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve()
    }, ms)
  })
}
// 过滤注入 （非严格过滤，谨慎使用）
export function filterInjectScript (str) {
  if (str) {
    str = str.replace(/<style[\s\S]*?<\/style>/ig, '') // 屏蔽样式
    str = str.replace(/<script[\s\S]*?<\/script>/ig, '') // 屏蔽脚本
    return str && str.replace(/</g, '&lt;').replace(/>/g, '&gt;') // 屏蔽其他尖括号
  }
  return ''
}
export function camelToUnderline (str) {
  var temp = str.replace(/[A-Z]/g, function (match) {
    return '_' + match.toLowerCase()
  })
  if (temp.slice(0, 1) === '_') { // 如果首字母是大写，执行replace时会多一个_，这里需要去掉
    temp = temp.slice(1)
  }
  return temp
}
// 递归数组打平
export function ArrayFlat (arr) {
  let flat = []
  if (!Array.isArray(arr)) return arr
  let breakUpArray = (_arr) => {
    _arr.forEach(item => {
      if (Array.isArray(item)) {
        breakUpArray(item)
      } else {
        flat.push(item)
      }
    })
  }
  breakUpArray(arr)
  return flat
}

// 数字划分千分位
export function sliceNumber (number, len) {
  if (!number || typeof number !== 'number') return 0
  let s = typeof len === 'number' ? len : 3
  let reg = new RegExp(`\\d{1,${s}}(?=(\\d{${s}})+$)`, 'g')
  return `${number}`.replace(reg, (v) => `${v},`)
}

// 复写 closest 方法兼容 IE
export function closestElm (element, parentElm) {
  if (!element || !parentElm) return
  let flag = false
  const getParent = (dom) => {
    if (!dom) return
    if (/^./.test(parentElm) && [...dom.classList].includes(parentElm.replace(/^./, ''))) {
      flag = true
    } else if (dom.getAttribute('id') && dom.getAttribute('id') === parentElm) {
      flag = true
    }
    if (dom.parentElement) {
      getParent(dom.parentElement)
    }
  }
  getParent(element)
  return flag
}

export { set, get, push } from './object'
export { handleError, handleSuccess, hasRole, hasPermission, kylinConfirm, transToGmtTime, transToServerGmtTime, isDatePartitionType, isTimePartitionType, isSubPartitionType, isStreamingPartitionType, transToUTCMs, getGmtDateFromUtcLike } from './business'
export { validate, validateTypes }
export { dataGenerator, autoLayout, dataHelper }
