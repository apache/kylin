import Vue from 'vue'
export function stopPropagation (e) {
  e.stopPropagation()
  if (e.stopPropagation) {
    e.stopPropagation()
  }
  e.cancelBubble = true
}

/* istanbul ignore next */
export const on = (function () {
  if (!Vue.prototype.$isServer && document.addEventListener) {
    return function (element, event, handler) {
      if (element && event && handler) {
        element.addEventListener(event, handler, false)
      }
    }
  } else {
    return function (element, event, handler) {
      if (element && event && handler) {
        element.attachEvent('on' + event, handler)
      }
    }
  }
})()
