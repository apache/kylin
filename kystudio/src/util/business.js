import { utcToConfigTimeZone } from './index'
import { permissionsMaps, DatePartitionRule, TimePartitionRule, SubPartitionRule, StreamingPartitionRule } from 'config/index'
import { MessageBox, Message } from 'kyligence-kylin-ui'
import moment from 'moment-timezone'

// 成功回调入口
export function handleSuccess (res, callback, errorcallback) {
  var responseData = res && res.data || null
  if (responseData && responseData.code === '000') {
    if (typeof callback === 'function') {
      callback(responseData.data, responseData.code, responseData.msg)
    }
  } else {
    callback(responseData && responseData.data || null, responseData && responseData.code || '000', responseData && responseData.msg || '')
  }
}
// 失败回调入口
export function handleError (res, errorcallback) {
  var responseData = res && res.data || null
  if (!res || res === 'cancel' || res === true || res === false) {
    return
  }
  // 服务器超时和无response的情况
  if (res.status === 504 || !res.status) {
    if (typeof errorcallback === 'function') {
      errorcallback(responseData, -1, res && res.status || -1, '')
      return
    }
    window.kylinVm.$store.state.config.errorMsgBox.isShow = true
    if (window.kylinVm.$store.state.config.platform === 'iframe') {
      window.kylinVm.$store.state.config.errorMsgBox.msg = res.message || window.kylinVm.$t('kylinLang.common.notConnectServerIframe')
    } else {
      window.kylinVm.$store.state.config.errorMsgBox.msg = res.message || (responseData && responseData.msg) || window.kylinVm.$t('kylinLang.common.notConnectServer')
    }
    window.kylinVm.$store.state.config.errorMsgBox.detail = responseData && responseData.stacktrace || res.stack || JSON.stringify(res)
  } else {
    var msg = responseData ? (responseData.msg || responseData.message || window.kylinVm.$t('kylinLang.common.unknownError')) : window.kylinVm.$t('kylinLang.common.unknownError')
    if (typeof errorcallback !== 'function') {
      window.kylinVm.$store.state.config.errorMsgBox.isShow = true
      window.kylinVm.$store.state.config.errorMsgBox.msg = msg
      window.kylinVm.$store.state.config.errorMsgBox.detail = responseData && responseData.stacktrace || JSON.stringify(res)
    } else {
      if (responseData && responseData.code) {
        errorcallback(responseData.data, responseData.code, res.status, responseData.msg)
      } else {
        errorcallback(responseData, -1, res && res.status || -1, res && res.msg || '')
      }
    }
  }
  window.kylinVm.$store.state.config.showLoadingBox = false
}
// 通用loading方法
export function loadingBox () {
  return {
    show () {
      window.kylinVm.$store.state.config.showLoadingBox = true
    },
    hide () {
      window.kylinVm.$store.state.config.showLoadingBox = false
    },
    status () {
      return window.kylinVm.$store.state.config.showLoadingBox
    }
  }
}
// 确认弹窗
export function kylinConfirm (content, para, title) {
  const dialogTitle = title || window.kylinVm.$t('kylinLang.common.tip')
  const dialogPara = para || {type: 'warning'}
  return MessageBox.confirm(content, dialogTitle, dialogPara)
}
export function kylinWarn (content, para) {
  const dialogPara = para || {
    type: 'warning',
    showCancelButton: false
  }
  return MessageBox.confirm(content, window.kylinVm.$t('kylinLang.common.tip'), dialogPara)
}
export function kylinMessage (content, para) {
  const messagePara = Object.assign({
    type: 'success',
    message: content,
    duration: 3000,
    showClose: false
  }, para)
  Message(messagePara)
}

// kylin配置中抓取属性值
export function getProperty (name, kylinConfig) {
  var result = (new RegExp(name + '=(.*?)\\n')).exec(kylinConfig)
  return result && result[1] || ''
}

// utc时间格式转换为本地时区的gmt格式
export function transToGmtTime (t, _vue) {
  let d = moment(new Date(t))
  if (d) {
    return d.format().replace(/T/, ' ').replace(/([+-])(\d+):\d+$/, ' GMT$1$2').replace(/0(\d)$/, '$1')
  }
  return ''
}

// utc时间格式转换为服务端时区的gmt格式
export function transToServerGmtTime (t, _vue) {
  var v = _vue || window.kylinVm
  if (v) {
    return utcToConfigTimeZone(t, v.$store.state.system.timeZone)
  }
}

// 测试当前用户在默认project下的权限
export function hasPermission (vue) {
  var curUser = vue.$store.state.user.currentUser
  var curUserAccess = vue.$store.state.user.currentUserAccess
  if (!curUser || !curUserAccess) {
    return null
  }
  var masks = []
  for (var i = 1; i < arguments.length; i++) {
    if (arguments[i]) {
      masks.push(permissionsMaps[arguments[i]])
    }
  }
  if (masks.indexOf(curUserAccess) >= 0) {
    return true
  }
  return false
}
// 监测当前用户在某个特定project下的权限
export function hasPermissionOfProjectAccess (vue, projectAccess) {
  var curUser = vue.$store.state.user.currentUser
  if (!curUser || !projectAccess) {
    return null
  }
  var masks = []
  for (var i = 2; i < arguments.length; i++) {
    if (arguments[i]) {
      masks.push(permissionsMaps[arguments[i]])
    }
  }
  if (masks.indexOf(projectAccess) >= 0) {
    return true
  }
  return false
}
// 检测当前用户是否有某种角色
export function hasRole (vue, roleName) {
  var haseRole = false
  var curUser = vue.$store.state.user.currentUser
  if (curUser && curUser.authorities) {
    curUser.authorities.forEach((auth, index) => {
      if (auth.authority === roleName) {
        haseRole = true
      }
    })
  }
  return haseRole
}

export function toDoubleNumber (n) {
  n = n > 9 ? n : '0' + n
  return n
}
export function transToUTCMs (ms, _vue) {
  let v = _vue || window.kylinVm
  let zone = v.$store.state.system.timeZone
  if (ms === undefined || !zone) { return ms }
  let date = new Date(ms)
  let y = date.getFullYear()
  let m = date.getMonth()
  let d = date.getDate()
  let h = date.getHours()
  let M = date.getMinutes()
  let s = date.getSeconds()
  let millis = date.getUTCMilliseconds()
  let timeFormat = '-'
  let utcTime = +Date.UTC(y, m, d, h, M, s, millis)
  let dateStr = y + timeFormat + toDoubleNumber(m + 1) + timeFormat + toDoubleNumber(d) + ' ' + toDoubleNumber(h) + ':' + toDoubleNumber(M) + ':' + toDoubleNumber(s)
  let momentObj = moment(dateStr).tz(zone)
  let offsetMinutes = momentObj._offset
  utcTime = utcTime - offsetMinutes * 60000
  return utcTime
}

export function isDatePartitionType (type) {
  return DatePartitionRule.some(rule => rule.test(type))
}

export function isTimePartitionType (type) {
  return TimePartitionRule.some(rule => rule.test(type))
}

export function isSubPartitionType (type) {
  return SubPartitionRule.some(rule => rule.test(type))
}

export function isStreamingPartitionType (type) {
  return StreamingPartitionRule.some(rule => rule.test(type))
}

export function getGmtDateFromUtcLike (value) {
  if (value !== undefined && value !== null) {
    return new Date(transToServerGmtTime(value).replace(/\s+GMT[+-]\d+$/, '').replace(/-/g, '/'))
  }
}

export function getStringLength (value) {
  let len = 0
  value.split('').forEach((v) => {
    if (/[\u4e00-\u9fa5]/.test(v)) {
      len += 2
    } else {
      len += 1
    }
  })
  return len
}

/**
 * 获取当前时间过去某个时间段的时间（此方法方便做时间范围的判断）
 * @param {dateTime} date 时间点
 * @param {number} m 相差的月份
 * @example 当前时间2020-2-28 00:00:00，往前推m = 1个月，得到2020-1-28 00:00:00
 */
export function getPrevTimeValue ({ date, m = 0 }) {
  let dt = new Date(date)
  let year = dt.getFullYear()
  let month = dt.getMonth() + 1
  const day = dt.getDate()
  const hour = dt.getHours()
  const minutes = dt.getMinutes()
  const seconds = dt.getSeconds()

  // 判断是否大于12，大于则减去相应的年份
  if (typeof m !== 'undefined' && m !== 0) {
    if (month - m <= 0) {
      let y = Math.ceil(m / 12)
      month = m > 12 ? month + (12 * y - m) : 12 - Math.abs(month - m)
      year -= y
    } else {
      month -= m
    }
  }

  return `${year}/${toDoubleNumber(month)}/${toDoubleNumber(day)} ${toDoubleNumber(hour)}:${toDoubleNumber(minutes)}:${toDoubleNumber(seconds)}`
}

// 跳转至job页面
export function jumpToJobs () {
  window.kylinVm.$router.push('/monitor/job')
}
