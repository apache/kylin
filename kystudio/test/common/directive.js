import $ from 'jquery'
import Scrollbar from 'smooth-scrollbar'
import store from '../../src/store'
import { stopPropagation, on } from 'util/event'
import commonTip from 'components/common/common_tip'

export function createDirectives (Vue) {
  const nodeList = []
  const ctx = '@@clickoutsideContext'

  let startClick
  let seed = 0
  // !Vue.prototype.$isServer && on(document, 'mousedown', e => (startClick = e))

  // !Vue.prototype.$isServer && on(document, 'mouseup', e => {
  //   nodeList.forEach(node => node[ctx].documentHandler(e, startClick))
  // })

  function createDocumentHandler (el, binding, vnode) {
    return function (mouseup = {}, mousedown = {}) {
      if (!vnode ||
        !vnode.context ||
        !mouseup.target ||
        !mousedown.target ||
        el.contains(mouseup.target) ||
        el.contains(mousedown.target) ||
        el === mouseup.target ||
        (vnode.context.popperElm &&
        (vnode.context.popperElm.contains(mouseup.target) ||
        vnode.context.popperElm.contains(mousedown.target)))) return

      if (binding.expression &&
        el[ctx].methodName &&
        vnode.context[el[ctx].methodName]) {
        vnode.context[el[ctx].methodName]()
      } else {
        el[ctx].bindingFn && el[ctx].bindingFn()
      }
    }
  }
  Vue.directive('number', {
    update: function (el, binding, vnode) {
      if (binding.value !== binding.oldValue) {
        setTimeout(() => {
          if (binding.value) {
            let newVal = ('' + binding.value).replace(/[^\d]/g, '')
            el.__vue__.$emit('input', +newVal || '0')
          }
        }, 0)
      }
    }
  })
  Vue.directive('number2', {
    update: function (el, binding, vnode) {
      if (binding.value !== binding.oldValue) {
        setTimeout(() => {
          if (binding.value) {
            let newVal = ('' + binding.value).replace(/[^\d]/g, '')
            el.__vue__.$emit('input', +newVal || '')
          }
        }, 0)
      }
    }
  })
  Vue.directive('number3', {
    update: function (el, binding, vnode) {
      if (binding.value !== binding.oldValue) {
        setTimeout(() => {
          const regexp = /^\d+(\.\d{1,2})?/g
          const regexp2 = /^\d+\.$/g
          if (binding.value) {
            const val = '' + binding.value
            const matchVal = val.match(regexp) && val.match(regexp)[0]
            const matchVal2 = val.match(regexp2) && val.match(regexp2)[0]
            const newVal = matchVal2 || matchVal
            el.__vue__.$emit('input', newVal || '')
          }
        }, 0)
      }
    }
  })
  Vue.directive('number4', {
    update: function (el, binding, vnode) {
      if (binding.value !== binding.oldValue) {
        setTimeout(() => {
          if (binding.value) {
            let newVal = ('' + binding.value).replace(/[^\d]|^0\d*/g, '')
            el.__vue__.$emit('input', +newVal || '1')
          }
        }, 0)
      }
    }
  })
  Vue.directive('clickoutside', {
    bind (el, binding, vnode) {
      nodeList.push(el)
      const id = seed++
      el[ctx] = {
        id,
        documentHandler: createDocumentHandler(el, binding, vnode),
        methodName: binding.expression,
        bindingFn: binding.value
      }
    },

    update (el, binding, vnode) {
      el[ctx].documentHandler = createDocumentHandler(el, binding, vnode)
      el[ctx].methodName = binding.expression
      el[ctx].bindingFn = binding.value
    },

    unbind (el) {
      let len = nodeList.length

      for (let i = 0; i < len; i++) {
        if (nodeList[i][ctx].id === el[ctx].id) {
          nodeList.splice(i, 1)
          break
        }
      }
      delete el[ctx]
    }
  })
  Vue.directive('timerHide', {
    inserted: function (el, binding) {
      let arg = binding.arg || 1
      let expression = binding.expression
      setTimeout(() => {
        el.style.display = 'none'
        if (typeof expression === 'function') {
          expression()
        }
      }, arg * 1000)
    }
  })
  Vue.directive('visible', {
    // 当绑定元素插入到 DOM 中。
    inserted: function (el, binding) {
      el.style.visibility = binding.value ? 'visible' : 'hidden'
    },
    update: function (el, binding) {
      el.style.visibility = binding.value ? 'visible' : 'hidden'
      if (el.tagName === 'INPUT') {
        el.focus()
        if (binding.value !== binding.oldValue) {
          el.select()
        }
      } else {
        for (var i = 0; i < el.childNodes.length; i++) {
          if (el.childNodes[i].tagName === 'INPUT') {
            el.childNodes[i].focus()
            if (binding.value !== binding.oldValue) {
              el.childNodes[i].select()
            }
          }
        }
      }
    }
  })
  // 只适用kyligence-kylin-ui里带focus method的组件和原生dom元素
  Vue.directive('focus', {
    inserted: function (el) {
      el.__vue__ && el.__vue__.focus ? el.__vue__.focus() : el.focus()
    },
    update: function (el, binding) {
      if (binding.value && !binding.oldValue) {
        el.__vue__  && el.__vue__.focus? el.__vue__.focus() : el.focus()
      }
    }
  })

  Vue.directive('scroll', {
    inserted: function (el, binding, vnode) {
      if (el) {
        let scrollbar = Scrollbar.init(el, {continuousScrolling: true})
        let needObserve = binding.modifiers.observe
        if (needObserve) {
          scrollbar.addListener((status) => {
            if (status.offset.y > status.limit.y - 10) {
              let scrollBottomFunc = vnode.data.on && vnode.data.on['scroll-bottom']
              scrollBottomFunc && scrollBottomFunc()
            }
          })
        }
      }
    },
    update: function (el, binding) {
      var isReactive = binding.modifiers.reactive
      if (isReactive) {
        Vue.nextTick(() => {
          let scrollBar = Scrollbar.get(el)
          scrollBar.update()
        })
      }
    }
  })
  // 禁止鼠标双击选中
  Vue.directive('unselect', {
    inserted: function (el, binding) {
      el.className += ' unselectable'
    }
  })

  Vue.directive('event-stop', {
    inserted: function (el, binding) {
      var eventName = binding.arg || 'mousedown'
      el['on' + eventName] = function (e) {
        stopPropagation(e)
      }
    }
  })

  Vue.directive('search-highlight', function (el, binding) {
    var searchKey = binding.value.hightlight.replace(/[?()]/g, (v) => `\\${v}`)
    var reg = new RegExp('(' + searchKey + ')', 'gi')
    var searchScope = binding.value.scope
    var direcDom = $(el)
    Vue.nextTick(() => {
      direcDom.find(searchScope).each(function () {
        var d = $(this)
        d.html(d.html().replace(/<\/?i.*?>/g, '').replace(new RegExp(reg), '<i class="keywords">$1</i>'))
      })
    })
  })

  var keyboardList = null
  var scrollInstance = null
  let st = null // 定时器全局变量
  let selectIndex = -1
  Vue.directive('keyborad-select', {
    unbind: function () {
      $(document).unbind('keydown')
      clearInterval(st)
    },
    componentUpdated: function (el, binding) {
      var searchScope = binding.value.scope
      keyboardList = $(el).find(searchScope)
      scrollInstance = Scrollbar.get(el)
      selectIndex = -1
    },
    inserted: function (el, binding) {
      selectIndex = -1
      var searchScope = binding.value.scope
      scrollInstance = Scrollbar.get(el)
      keyboardList = $(el).find(searchScope)
      selectList()
      let handleKey = (event) => {
        if (event.keyCode === 40 || event.keyCode === 39) {
          selectIndex = selectIndex + 1 >= keyboardList.length ? 0 : selectIndex + 1
          selectList(selectIndex)
        }
        if (event.keyCode === 37 || event.keyCode === 38) {
          selectIndex = selectIndex - 1 < 0 ? keyboardList.length - 1 : selectIndex - 1
          selectList(selectIndex)
        }
        if (event.keyCode === 13 && selectIndex >= 0) {
          keyboardList.eq(selectIndex).click()
        }
      }
      $(document).keydown((event) => {
        handleKey(event)
        clearInterval(st)
        st = setInterval(() => {
          handleKey(event)
        }, 300)
      })
      $(document).keyup(function (event) {
        clearInterval(st)
      })
      function selectList (i) {
        if (!keyboardList) {
          return
        }
        keyboardList.removeClass('active')
        if (i >= 0) {
          if (scrollInstance) {
            scrollInstance.scrollTo(100, 32 * i + 5, 400)
          }
          keyboardList.eq(i).addClass('active')
        }
      }
    }
  })
  Vue.directive('drag', {
    inserted: function (el, binding, vnode) {
      var oDiv = el
      var dragInfo = binding.value
      var limitObj = dragInfo.limit
      var changeOption = binding.modifiers.hasOwnProperty ? binding.modifiers : {}
      var boxDom = null
      var boxW = 0
      var boxH = 0
      var callback = binding.value && binding.value.sizeChangeCb
      let reverse = changeOption.hasOwnProperty('reverse')
      let reverseW = changeOption.hasOwnProperty('reverseW')
      let reverseH = changeOption.hasOwnProperty('reverseH')
      if (changeOption.hasOwnProperty('height') || changeOption.hasOwnProperty('width')) {
        el.className += ' ky-resize'
      } else {
        el.className += ' ky-move'
      }
      el.onselectstart = function () {
        return false
      }
      regainBox()
      // 盒子碰撞检测
      function checkBoxCollision (changeType, size, rectifyVal) {
        rectifyVal = rectifyVal || 0
        // 无自定义盒子和限制
        if (!dragInfo.box && !limitObj && dragInfo.allowOutOfView) {
          return true
        }
        if (limitObj) {
          let curCheckProp = limitObj[changeType]
          if (curCheckProp) {
            // 无自定义限制
            if (curCheckProp.length > 0 && undefined !== curCheckProp[0] && size < curCheckProp[0]) {
              dragInfo[changeType] = curCheckProp[0]
              return false
            }
            if (curCheckProp.length > 1 && undefined !== curCheckProp[1] && size > curCheckProp[1]) {
              dragInfo[changeType] = curCheckProp[1]
              return false
            }
          }
        }
        if (dragInfo.box && !dragInfo.allowOutOfView) {
          if (changeType === 'top') {
            if (size + dragInfo.height > boxH) {
              dragInfo.top = boxH - dragInfo.height > 0 ? boxH - dragInfo.height : 0
              return false
            }
            if (size < 0) {
              dragInfo.top = 0
              return false
            }
          }
          if (changeType === 'height') {
            if (dragInfo.top + rectifyVal < 0) {
              return false
            }
            if (rectifyVal + size + dragInfo.top > boxH) {
              dragInfo.height = boxH - dragInfo.top
              return false
            }
          }
          if (changeType === 'width') {
            if (!isNaN(dragInfo.left)) {
              if (dragInfo.left + rectifyVal < 0) {
                return false
              }
              if (rectifyVal + size + dragInfo.left > boxW) {
                dragInfo.left = boxW - dragInfo.width
                return false
              }
            }
            if (!isNaN(dragInfo.right)) {
              if (dragInfo.right + rectifyVal < 0) {
                return false
              }
              if (size + dragInfo.right - rectifyVal > boxW) {
                dragInfo.width = boxW - dragInfo.right
                return false
              }
            }
          }
          if (changeType === 'right' || changeType === 'left') {
            if (size + dragInfo.width > boxW) {
              dragInfo[changeType] = boxW - dragInfo.width > 0 ? boxW - dragInfo.width : 0
              return false
            }
            if (size < 0) {
              dragInfo[changeType] = 0
              return false
            }
          }
        }
        return true
      }
      function regainBox () {
        if (dragInfo.box) {
          boxDom = $(el).parents(dragInfo.box).eq(0)
          boxW = boxDom.width()
          boxH = boxDom.height()
        }
      }
      $(window).resize(() => {
        setTimeout(() => {
          regainBox()
          if (checkBoxCollision()) {
            return
          }
          if (!isNaN(dragInfo['right']) || dragInfo['right'] < 0) {
            if (dragInfo['right'] + dragInfo.width > boxW) {
              dragInfo['right'] = 0
            }
            if (dragInfo['right'] < 0) {
              dragInfo['right'] = boxW - dragInfo.width
            }
          }
          if (!isNaN(dragInfo['left']) && dragInfo['left'] + dragInfo.width > boxW) {
            if (dragInfo['left'] + dragInfo.width > boxW) {
              dragInfo['left'] = boxW - dragInfo.width
            }
            if (dragInfo['left'] < 0) {
              dragInfo['left'] = 0
            }
          }
          if (!isNaN(dragInfo['top']) && dragInfo['top'] + dragInfo.height > boxH) {
            if (dragInfo['top'] + dragInfo.height > boxH) {
              dragInfo['top'] = boxH - dragInfo.height
            }
            if (dragInfo['top'] < 0) {
              dragInfo['top'] = 0
            }
          }
          callback && callback(0, 0, boxW, boxH, dragInfo)
        }, 2)
      })
      oDiv.onmousedown = function (ev) {
        let zoom = el.getAttribute('data-zoom') || 10
        ev.stopPropagation()
        ev.preventDefault()
        var offsetX = ev.clientX
        var offsetY = ev.clientY
        regainBox()
        if (changeOption.hasOwnProperty('height') || changeOption.hasOwnProperty('width')) {
          el.className += ' ky-resize-ing'
        } else {
          el.className += ' ky-move-ing'
        }
        document.onmousemove = function (e) {
          ev.stopPropagation()
          ev.preventDefault()
          var x = e.clientX - offsetX
          var y = e.clientY - offsetY
          x /= zoom / 10
          y /= zoom / 10
          offsetX = e.clientX
          offsetY = e.clientY
          if (changeOption) {
            for (var i in changeOption) {
              if (i === 'top') {
                if (checkBoxCollision(i, dragInfo['top'] + y)) {
                  dragInfo['top'] += y
                }
              }
              if (i === 'right') {
                if (checkBoxCollision(i, dragInfo['right'] - x)) {
                  dragInfo['right'] -= x
                }
              }
              if (i === 'left') {
                if (checkBoxCollision(i, dragInfo['left'] + x)) {
                  dragInfo['left'] += x
                }
              }
              if (i === 'height') {
                if (reverse || reverseH) {
                  if (checkBoxCollision(i, dragInfo['height'] - y, y)) {
                    dragInfo['height'] -= y
                    dragInfo['top'] += y
                  }
                } else {
                  if (checkBoxCollision(i, dragInfo['height'] + y)) {
                    dragInfo['height'] += y
                  }
                }
              }
              if (i === 'width') {
                if (reverse || reverseW) {
                  let rectify = 0
                  if (!isNaN(dragInfo['left'])) {
                    rectify = x
                  }
                  if (checkBoxCollision(i, dragInfo['width'] - x, rectify)) {
                    dragInfo['width'] -= x
                    if (!isNaN(dragInfo['left'])) {
                      dragInfo['left'] += x
                    }
                  }
                } else {
                  let rectify = 0
                  if (dragInfo['right']) {
                    rectify = x
                  }
                  if (checkBoxCollision(i, dragInfo['width'] + x, rectify)) {
                    dragInfo['width'] += x
                    if (!isNaN(dragInfo['right'])) {
                      dragInfo['right'] -= x
                    }
                  }
                }
              }
            }
          }
          callback && callback(x, y, boxW, boxH, dragInfo)
        }
        document.onmouseup = function () {
          el.className = el.className.replace(/ky-[a-z]+-ing/g, '').replace(/\s+$/, '')
          document.onmousemove = null
          document.onmouseup = null
          $(window).unbind('resize')
        }
      }
    }
  })
  // 收集guide dom
  Vue.directive('guide', {
    // bind: function (el, binding, vnode) {
    //   console.log('----', vnode.key, vnode)
    //   // 设置alone参数 避免虚拟dom重用导致指令生命周期错误
    //   let keys = binding.modifiers
    //   if (binding.arg === 'alone') {
    //     vnode.key = Object.keys(keys).join('-')
    //   }
    // },
    update: function (el, binding, vnode) {
      let keys = binding.modifiers
      let storeGuide = store.state.system.guideConfig.targetList
      if (storeGuide) {
        for (let i in keys) {
          storeGuide[i] = el.__vue__ || el
        }
        if (store.state.system.guideConfig.globalMaskVisible && binding.value) {
          storeGuide[binding.value] = el.__vue__ || el
        }
      }
    },
    inserted: function (el, binding, vnode) {
      let keys = binding.modifiers
      let storeGuide = store.state.system.guideConfig.targetList
      if (storeGuide) {
        for (let i in keys) {
          storeGuide[i] = el.__vue__ || el
        }
        if (store.state.system.guideConfig.globalMaskVisible && binding.value) {
          storeGuide[binding.value] = el.__vue__ || el
        }
      }
    },
    unbind: function (el, binding) {
      let keys = binding.modifiers
      let storeGuide = store.state.system.guideConfig.targetList
      for (let i in keys) {
        delete storeGuide[i]
      }
    }
  })
  let keyCodeMap = {
    'esc': 27,
    'enter': 13
  }
  let timeTransit = null
  // 为非聚焦的元素绑定keyup监听
  Vue.directive('global-key-event', {
    inserted: function (el, binding) {
      let keys = binding.modifiers
      let keyCodes = []
      Object.keys(keys).forEach((key) => {
        keyCodeMap[key] && keyCodes.push(keyCodeMap[key])
      })
      document.onkeydown = (e) => {
        var key = (e || window.event).keyCode || e.which
        if (keyCodes.indexOf(key) >= 0) {
          if (key === 13 && 'debounce' in keys) {
            if (el.querySelector('input') && document.activeElement !== el.querySelector('input')) return
            clearTimeout(timeTransit)
            timeTransit = setTimeout(() => {
              typeof binding.value === 'function' && typeof e.target.value !== 'undefined' && binding.value.call(this, e.target.value)
            }, 500)
            return
          }
          typeof binding.value === 'function' && binding.value()
        }
      }
    },
    unbind: function (el, binding) {
      document.onkeydown = null
    }
  })

  // 增加滚动区域（上下左右）有滚动的情况下显示内阴影
  Vue.directive('scroll-shadow', {
    inserted: function (el, binding) {
      setTimeout(() => {
        const wrapper = el.querySelector('.el-table__body-wrapper')
        const tableDom = el.querySelector('.el-table__body-wrapper table')
        let isScrollY = false
        let isScrollX = false

        // 判断table是否拥有滚动条
        let fn = ({x = false, y = false}) => {
          const currentClientRect = el && el.getBoundingClientRect()
          const tableClientRect = tableDom && tableDom.getBoundingClientRect()
          isScrollX = wrapper && tableDom && tableClientRect.width > currentClientRect.width
          isScrollY = wrapper && tableDom && tableClientRect.height > currentClientRect.height

          if (isScrollX && !x) {
            let dom = document.createElement('div')
            dom.className = 'scroll-shadow-layout scrolling-x'
            el.appendChild(dom)
            wrapper.addEventListener('scroll', scrollEventX)
          }
          if (isScrollY && !y) {
            let dom = document.createElement('div')
            dom.className = 'scroll-shadow-layout scrolling-y'
            el.appendChild(dom)
            wrapper.addEventListener('scroll', scrollEventY)
          }
        }

        /**
         * 监听x轴滚动事件添加相应阴影区域样式
         * is-scrolling-left - 右侧阴影，is-scrolling-middle - 两侧阴影，is-scrolling-right - 左侧阴影
         */
        let scrollEventX = (e) => {
          let targetElementClass = e.target.classList.value || e.target.className
          const scrollLayout = el.getElementsByClassName('scroll-shadow-layout scrolling-x').length && el.getElementsByClassName('scroll-shadow-layout scrolling-x')[0]
          if (!scrollLayout) return
          if (targetElementClass.indexOf('is-scrolling-left') > -1) {
            scrollLayout.className = 'scroll-shadow-layout scrolling-x is-scrolling-left'
          } else if (targetElementClass.indexOf('is-scrolling-middle') > -1) {
            scrollLayout.className = 'scroll-shadow-layout scrolling-x is-scrolling-middle'
          } else {
            scrollLayout.className = 'scroll-shadow-layout scrolling-x is-scrolling-right'
          }
        }

        // 监听y轴滚动事件添加相应阴影区域
        let scrollEventY = (e) => {
          let scrollT = e.target.scrollTop
          let scrollH = e.target.scrollHeight
          let clientH = e.target.clientHeight
          let dom = el.getElementsByClassName('scroll-shadow-layout scrolling-y').length && el.getElementsByClassName('scroll-shadow-layout scrolling-y')[0]

          if (scrollT === 0) {
            dom.className = 'scroll-shadow-layout scrolling-y is-scrolling-top'
          } else if (scrollT > 0 && scrollT + clientH !== scrollH) {
            dom.className = 'scroll-shadow-layout scrolling-y is-scrolling-middle'
          } else if (scrollT + clientH === scrollH) {
            dom.className = 'scroll-shadow-layout scrolling-y is-scrolling-bottom'
          }
        }

        // 更改可视区域的大小，重新计算滚动属性
        window.onresize = function () {
          const resizeClientRect = el && el.getBoundingClientRect()
          const resizeTableClientRect = tableDom && tableDom.getBoundingClientRect()
          const scrollShadowDomX = el.getElementsByClassName('scroll-shadow-layout scrolling-x')
          const scrollShadowDomY = el.getElementsByClassName('scroll-shadow-layout scrolling-y')

          if (resizeTableClientRect.width - resizeClientRect.width < 2 && isScrollX) {
            isScrollX = false
            wrapper.removeEventListener('scroll', scrollEventX)
            el.removeChild(scrollShadowDomX.length && scrollShadowDomX[0])
          }
          if (resizeTableClientRect.height - resizeClientRect.height < 2 && isScrollY) {
            isScrollY = false
            wrapper.removeEventListener('scroll', scrollEventY)
            el.removeChild(scrollShadowDomY.length && scrollShadowDomY[0])
          }
          (!scrollShadowDomX.length || !scrollShadowDomY.length) && fn({x: scrollShadowDomX.length, y: scrollShadowDomY.length})
        }
        fn({})
      }, 500)
    },
    unbind: function () {
      window.onresize = null
    }
  })

  // 自定义增加tooltip指令，判断宽度（加偏移量）是否超过父盒子从而插入或移出tooltip
  let parentList = {}
  Vue.directive('custom-tooltip', {
    bind: (el) => {
      el.className += ' custom-tooltip-text'
      // el.style.cssText = 'white-space: nowrap'
    },
    inserted: (el, binding) => {
      if (!el || !el.parentElement) return
      let id = 'tooltip-' + new Date().getTime().toString(32)
      el.setAttribute('data-id', id)
      const nextElement = el.nextSibling
      const currentElWidth = getTextWidth(el, el.innerText)

      parentList[id] = {
        parent: el.parentElement,
        nextElement,
        textWidth: currentElWidth,
        binding,
        [`resizeFn-${id}`]: function () {
          const { parent } = parentList[id]
          if (!parent) return
          let textNode = parent.querySelector('.custom-tooltip-text')
          appendTipDom(textNode, getTextWidth(textNode, textNode.innerText), 'value' in binding && typeof binding.value.w === 'number' ? binding.value.w : 0)
        }
      }

      appendTipDom(el, currentElWidth, 'value' in binding && typeof binding.value.w === 'number' ? binding.value.w : 0)

      setTimeout(() => {
        // 当元素在table中，使用 MutationObserver 方法监听 table 宽度 style 的改变（这里监听的是 el-table__body dom 的宽度）
        if (binding.value.tableClassName) {
          let element = document.querySelector(`.${binding.value.tableClassName}`) && document.querySelector(`.${binding.value.tableClassName}`).querySelector('.el-table__body')
          let MutationObserver = window.MutationObserver || window.WebKitMutationObserver || window.MozMutationObserver
          if (parentList[id]) {
            let observer = new MutationObserver(parentList[id][`resizeFn-${id}`])
            parentList[id].observer = observer
            observer.observe(element, {
              attributes: true  // 监听 table 的 attributes 的改变
            })
          }
        } else {
          parentList[id] && parentList[id][`resizeFn-${id}`] && window.addEventListener('resize', parentList[id][`resizeFn-${id}`])
        }
      }, 500)
    },
    update: (el, binding, oldVnode) => {
      if (binding.value.text === binding.oldValue.text && binding.value.w === binding.oldValue.w) return
      let id = el.getAttribute('data-id')
      if (!id || !(id in parentList)) return
      let parent = parentList[id].parent
      let textNode = parent.querySelector('.custom-tooltip-text')
      parentList[id].binding = binding
      let currentWidth = parentList[id].textWidth = getTextWidth(textNode, binding.value.text)
      textNode.textContent = binding.value.text
      appendTipDom(textNode, currentWidth, 'value' in binding && typeof binding.value.w === 'number' ? binding.value.w : 0)
    },
    unbind: (el) => {
      let id = el.getAttribute('data-id')
      if (!id || !(id in parentList)) return
      !('observer' in parentList[id]) && `resizeFn-${id}` in parentList[id] && window.removeEventListener('resize', parentList[id][`resizeFn-${id}`])
      // 使用 MutationObserver 方法监听dom，需要用 disconnect 解绑
      'observer' in parentList[id] && parentList[id].observer.disconnect()
      delete parentList[id]
    }
  })

  // 自定义创建common-tip组件并挂载
  function createToolTipDom (el, binding, parentWidth) {
    const renderer = Vue.compile(el.outerHTML)
    let createCommonTip = (propsData) => {
      let Dom = Vue.extend(commonTip)
      return new Dom({
        propsData
      })
    }
    let t = createCommonTip({placement: 'top', effect: 'dark', value: true, content: binding.value.text})
    t.$slots.default = [t.$createElement(renderer)]
    t.$mount()
    t.$el.className = `${t.$el.className} custom-tooltip-layout ${binding.value.className || ''}`
    parentWidth && (t.$el.style.cssText = `width: ${parentWidth - binding.value.w || 0}px;`)
    return t
  }

  // 窗口resize以及更新数据时重新插入tooltip或去除tooltip
  function appendTipDom (el, currentW, w) {
    let id = el.getAttribute('data-id')
    if (!id || !(id in parentList)) return
    const { parent, nextElement, binding } = parentList[id]
    const parentW = parent && parent.offsetWidth
    let comp = createToolTipDom(el, binding, parentW)

    // 当前el超过父节点的宽度时插入tooltip否者移出
    if (currentW + w >= parentW) {
      if (!parent.querySelector('.tip_box')) {
        let _El = parent.querySelector('.custom-tooltip-text')
        nextElement ? parent.insertBefore(comp.$el, nextElement) : parent.appendChild(comp.$el)
        parent.removeChild(_El)
      } else {
        nextElement ? parent.insertBefore(comp.$el, nextElement) : parent.appendChild(comp.$el)
        parent.removeChild(parent.querySelector('.tip_box'))
      }
    } else {
      if (parent.querySelector('.tip_box')) {
        nextElement ? parent.insertBefore(el, nextElement) : parent.appendChild(el)
        parent.removeChild(parent.querySelector('.tip_box'))
      }
    }
  }

  // 获取文本的长度
  function getTextWidth (el, text) {
    const dom = document.createElement('div')
    const fontSize = window.getComputedStyle(el).fontSize || '14px'
    dom.innerText = text
    dom.style.cssText = `font-size: ${fontSize}; display: inline-block;`
    document.body.appendChild(dom)
    const width = dom.offsetWidth
    document.body.removeChild(dom)
    return width
  }

}
