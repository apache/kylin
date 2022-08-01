import * as types from '../store/types'
import { menusData } from '../config'
import store from '../store'
import ElementUI from 'kyligence-kylin-ui'
import { getAvailableOptions } from '../util/specParser'

export function bindRouterGuard (router) {
  // 捕获在异步组件加载时出现过期组件的情况（服务端替换部署包等）
  router.onError((error) => {
    const pattern = /Loading chunk (\d)+ failed/g
    const isChunkLoadFailed = error.message && error.message.match(pattern)
    if (isChunkLoadFailed) {
      location.reload()
    }
  })
  router.beforeEach(async (to, from, next) => {
    // 处理在模型添加的业务窗口刷新浏览器
    ElementUI.Message.closeAll() // 切换路由的时候关闭message
    store.state.config.showLoadingBox = false // 切换路由的时候关闭全局loading
    if (to.matched && to.matched.length) {
      store.state.config.layoutConfig.gloalProjectSelectShow = to.name !== 'Dashboard'
      // 确保在非点击菜单的路由跳转下还能够正确定位到指定的active name
      menusData.forEach((menu) => {
        if (to.name && menu.name.toLowerCase() === to.name.toLowerCase()) {
          store.state.config.routerConfig.currentPathName = menu.path
        } else if (menu.children) {
          menu.children.forEach((subMenu) => {
            if (to.name && subMenu.name.toLowerCase() === to.name.toLowerCase()) {
              store.state.config.routerConfig.currentPathName = subMenu.path
            }
          })
        }
      })
      let prepositionRequest = () => {
        Promise.all([
          store.dispatch(types.LOAD_AUTHENTICATION)
        ]).then(async () => {
          if (!store.state.system.authentication || !store.state.system.authentication.data) {
            router.replace('/access/login')
            return
          }
          store.commit(types.SAVE_CURRENT_LOGIN_USER, { user: store.state.system.authentication.data })
          Promise.all([
            store.dispatch(types.LOAD_ALL_PROJECT),
            store.dispatch(types.GET_INSTANCE_CONF),
            store.dispatch(types.GET_CONF) // 调用的action没有接收参数，因而这里把参数 selectedProject 去掉
          ]).then(() => {
            // 判断路由权限
            getRouteAuthority()
          })
        }, (res) => {
        })
      }

      // 判断用户是否有当前所要进入的页面权限
      let getRouteAuthority = (source) => {
        // 获取该用户所有有权限的菜单
        const defaultMenus = getAvailableOptions('menu', { groupRole: store.getters.userAuthorities, projectRole: store.state.user.currentUserAccess, menu: 'dashboard' })
        const adminMenus = getAvailableOptions('menu', { groupRole: store.getters.userAuthorities, projectRole: store.state.user.currentUserAccess, menu: 'project' })
        let availableMenus = [...defaultMenus, ...adminMenus]
        let auth = to.name && availableMenus.includes(to.name.toLowerCase())

        // 判断是否有路由权限，无权限或没有打开智能推荐跳至 /noAuthority 页面
        if (to.name && !['noauthority', 'refresh', '404'].includes(to.name.toLowerCase()) && !auth) {
          next({path: '/noauthority', query: {resouce: 'isNoAuthority'}})
        } else {
          next()
        }
      }

      // 如果是从登陆过来的，所有信息都要重新获取
      if (from.name === 'Login' && (to.name !== 'access' && to.name !== 'Login')) {
        prepositionRequest()
      } else if (from.name !== 'access' && from.name !== 'Login' && to.name !== 'access' && to.name !== 'Login') {
        // 如果是非登录页过来的，内页之间的路由跳转的话，就需要判断是否已经拿过权限
        if (!store.state.system.authentication && !store.state.system.serverConfig || (to.params.refresh || !from.name)) {
          prepositionRequest()
        } else {
          getRouteAuthority()
        }
      } else {
        const loginIn = localStorage.getItem('loginIn')
        if (from.name && to.name === 'Login') {
          if (loginIn && loginIn === 'true') {
            next(from.path)
            return
          } else {
            next()
            return
          }
        } else if (!from.name && to.name === 'Login' && loginIn && loginIn === 'true') {
          next('/query/insight')
          return
        }
        next()
      }
    } else {
      router.replace('/access/login')
    }
  })
  router.afterEach((to, from) => {
    var scrollBoxDom = document.getElementById('scrollContent')
    if (scrollBoxDom) {
      scrollBoxDom.scrollTop = 0
    }
    if (from.name && to.name && to.name === 'Login') {
      // 登出之后清除currentUser数据
      store.commit(types.RESET_CURRENT_USER)
      // 重置 project 权限
      store.commit(types.SAVE_CURRENT_USER_ACCESS, {access: 'DEFAULT'})
    }
    // 跳转路由的时候，关闭独立挂载的弹窗上的isShow的状态(暂只处理guide模式下)
    for (let i in store.state.modals) {
      if (store.state.modals[i]) {
        store.state.modals[i].isShow = false
        if (store.state.system.guideConfig.globalMaskVisible) {
          store.commit(i + '/RESET_MODAL_FORM')
        }
      }
    }
  })
  return router
}
