<template>
  <div class="full-layout" id="fullBox" :class="{fullLayout:isFullScreen,isModelList:($route.name === 'ModelList')}">
    <el-row class="panel" :class="{'brief_menu':briefMenuGet}">
      <el-col :span="24" class="panel-center">
        <aside class="left-menu">
          <img v-show="!briefMenuGet" src="../../assets/img/logo_kylin.png" class="logo" @click="goHome"/>
          <img v-show="briefMenuGet" src="../../assets/img/logo/logo_small_white.png" class="logo" @click="goHome"/>
          <el-menu background-color="#3c8dbc" sub-background-color="#3c8dbc" active-sub-background-color="#892e65" :default-active="defaultActive" id="menu-list" @select="handleselect" router :collapse="briefMenuGet">
            <template v-for="(item,index) in menus">
              <el-menu-item :index="item.path" v-if="!item.children && showMenuByRole(item.name)" :key="index">
                <el-icon :name="item.icon" type="mult"></el-icon><span
                slot="title" v-if="item.name === 'modelList'">{{$t('kylinLang.menu.modelList')}}</span><span
                slot="title" v-else>{{$t('kylinLang.menu.' + item.name)}}</span>
              </el-menu-item>
              <el-submenu :index="item.path" v-if="item.children && showMenuByRole(item.name)" :id="item.name" :key="index">
                <template slot="title">
                  <el-icon :name="item.icon" type="mult"></el-icon><span>{{$t('kylinLang.menu.' + item.name)}}</span>
                </template>
                <div v-for="child in item.children" :key="child.path" >
                  <el-menu-item :index="child.path" :class="{'ksd-pl-45': !briefMenuGet}" v-if="showMenuByRole(child.name)">
                    <span>{{$t('kylinLang.menu.' + child.name)}}</span>
                  </el-menu-item>
                </div>
              </el-submenu>
            </template>
          </el-menu>
          <div :class="['diagnostic-model', {'is-hold-menu': briefMenuGet}]" v-if="showMenuByRole('diagnostic')" @click="showDiagnosticDialog">
            <el-tooltip :content="$t('diagnosis')" effect="dark" placement="right" popper-class="diagnosis-tip" :disabled="!briefMenuGet">
              <span>
                <i class="el-icon-ksd-ostin_diagnose"/>
                <span class="text" v-if="!briefMenuGet">{{$t('diagnosis')}}</span>
              </span>
            </el-tooltip>
          </div>
        </aside>
        <div class="topbar">
          <div class="nav-icon">
            <common-tip :content="$t('holdNaviBar')" placement="bottom-start" v-if="!briefMenuGet">
              <el-button type="primary" text icon-button-mini icon="el-ksd-icon-nav_fold_22" @click="toggleLeftMenu"></el-button>
            </common-tip>
            <common-tip :content="$t('unholdNaviBar')" placement="bottom-start" v-else>
              <el-button type="primary" text icon-button-mini icon="el-ksd-icon-nav_unfold_22" @click="toggleLeftMenu"></el-button>
            </common-tip>
          </div>
          <template v-if="!isAdminView">
            <project_select v-on:changePro="changeProject" ref="projectSelect"></project_select>
            <common-tip :content="canAddProject ? $t('kylinLang.project.addProject') : $t('disableAddProject')" placement="bottom-start">
              <el-button class="add-project-btn" type="primary" text icon-button-mini icon="el-ksd-icon-add_22" @click="addProject" v-show="isAdmin" :disabled="!canAddProject">
              </el-button>
            </common-tip>
          </template>

          <ul class="top-ul ksd-fright">
            <li v-if="showMenuByRole('admin')" style="margin-right: 1px;">
              <el-tooltip :content="$t('kylinLang.menu.admin')" placement="bottom">
                <el-button
                  type="primary"
                  text
                  class="entry-admin"
                  icon-button
                  icon="el-ksd-icon-system_config_22"
                  :class="isAdminView ? 'active' : null"
                  @click="handleSwitchAdmin">
                </el-button>
              </el-tooltip>
            </li>
            <li style="margin-right: 1px;"><help></help></li>
            <!-- <li class="ksd-mr-10"><change_lang ref="changeLangCom"></change_lang></li> -->
            <li>
              <el-dropdown @command="handleCommand" class="user-msg-dropdown">
                <el-button type="primary" text iconr="el-ksd-icon-arrow_down_22"><span class="limit-user-name">{{currentUserInfo && currentUserInfo.username}}</span></el-button>
                <el-dropdown-menu slot="dropdown">
                  <div class="user-name">{{ currentUserInfo && currentUserInfo.username }}</div>
                  <el-dropdown-item :disabled="!isTestingSecurityProfile" command="setting">{{$t('kylinLang.common.changePassword')}}</el-dropdown-item>
                  <el-dropdown-item command="loginout">{{$t('kylinLang.common.logout')}}</el-dropdown-item>
                </el-dropdown-menu>
              </el-dropdown>
            </li>
          </ul>
        </div>
        <div class="panel-content" id="scrollBox">
          <div class="grid-content bg-purple-light" id="scrollContent">
            <el-col :span="24" class="main-content">
              <transition :name="isAnimation ? 'slide' : null" v-bind:css="isAnimation">
                <router-view v-on:addProject="addProject" v-if="isShowRouterView"></router-view>
                <div v-else class="blank-content">
                  <i class="el-icon-ksd-alert_1"></i>
                  <p class="text">
                    {{$t('noAuthorityText')}}
                  </p>
                </div>
              </transition>
            </el-col>
          </div>
        </div>
        <div class="global-mask" v-if="isGlobalMaskShow">
          <div class="background"></div>
          <div class="notify-contect font-medium">{{notifyContect}}</div>
        </div>
      </el-col>
    </el-row>

    <!-- 全局弹窗 带detail -->
    <kap-detail-dialog-modal></kap-detail-dialog-modal>
    <diagnostic v-if="showDiagnostic" @close="showDiagnostic = false"/>
    <!-- 模型引导 -->
    <GuideModal />
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { handleError, kylinConfirm, hasRole, hasPermission } from '../../util/business'
import { cacheSessionStorage, cacheLocalStorage, delayMs } from '../../util/index'
import { permissions, menusData, pageRefTags } from '../../config'
import { mapState, mapActions, mapMutations, mapGetters } from 'vuex'
import projectSelect from '../project/project_select'
// import changeLang from '../common/change_lang'
import help from '../common/help'
import KapDetailDialogModal from '../common/GlobalDialog/dialog/detail_dialog'
import Diagnostic from '../admin/Diagnostic/index'
import $ from 'jquery'
import ElementUI from 'kyligence-kylin-ui'
import GuideModal from '../studio/StudioModel/ModelList/GuideModal/GuideModal.vue'
let MessageBox = ElementUI.MessageBox
@Component({
  methods: {
    ...mapActions('ProjectEditModal', {
      callProjectEditModal: 'CALL_MODAL'
    }),
    ...mapActions({
      loginOut: 'LOGIN_OUT',
      saveProject: 'SAVE_PROJECT',
      getCurUserInfo: 'USER_AUTHENTICATION',
      resetPassword: 'RESET_PASSWORD',
      getUserAccess: 'USER_ACCESS'
    }),
    ...mapMutations({
      setCurUser: 'SAVE_CURRENT_LOGIN_USER',
      resetProjectState: 'RESET_PROJECT_STATE',
      toggleMenu: 'TOGGLE_MENU',
      cacheHistory: 'CACHE_HISTORY',
      saveTabs: 'SET_QUERY_TABS',
      resetSpeedInfo: 'CACHE_SPEED_INFO',
      setProject: 'SET_PROJECT'
    }),
    ...mapActions('UserEditModal', {
      callUserEditModal: 'CALL_MODAL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  components: {
    'project_select': projectSelect,
    // 'change_lang': changeLang,
    help,
    KapDetailDialogModal,
    Diagnostic,
    // Capacity,
    GuideModal
  },
  computed: {
    ...mapState({
      cachedHistory: state => state.config.cachedHistory,
      currentUser: state => state.user.currentUser,
      showRevertPasswordDialog: state => state.system.showRevertPasswordDialog
    }),
    ...mapGetters([
      'currentPathNameGet',
      'isFullScreen',
      'currentSelectedProject',
      'briefMenuGet',
      'currentProjectData',
      'availableMenus',
      'isOnlyQueryNode',
      'dashboardActions',
      'isTestingSecurityProfile'
    ]),
    canAddProject () {
      // 模型编辑页面的时候，新增项目的按钮不可点
      return this.$route.name !== 'ModelEdit'
    }
  },
  locales: {
    'en': {
      contactSales: 'Concat Sales',
      gotit: 'Got It',
      serviceOvertip1: 'The technical support service will expire in ',
      serviceOvertip2: ' days. After the expiration date, you couldn\'t use the support service and the ticket system. Please contact the sales to extend your service time if needed.',
      serviceEndDate: 'Service End Time: ',
      storageQuota: 'Storage Quota',
      settingTips: 'Configure',
      useageMana: 'Used Storage: ',
      trash: 'Low Usage Storage',
      tarshTips: 'Low usage storage refers to the obsolete files generated after the system has been running for a period of time. For more details, please refer to <a href="https://docs.kyligence.io/books/v4.5/en/Operation-and-Maintenance-Guide/junk_file_clean.en.html" target="_blank">user manual</a>.',
      clear: 'Clear',
      resetPassword: 'Reset Password',
      confirmLoginOut: 'Are you sure you want to log out?',
      validPeriod: 'Valid Period: ',
      applayLisence: 'Contact Sales',
      'continueUse': 'Got It',
      ignore: 'Ignore',
      apply: 'Apply',
      hello: 'Hi {user},',
      leaveAdmin: 'Going to leave the administration mode.',
      enterAdmin: 'Hi, welcome to the administration mode.',
      noProject: 'No project now. Please create one project via the top bar.',
      indexs: 'index group(s)',
      models: 'model(s)',
      holdNaviBar: 'Collapse',
      unholdNaviBar: 'Expand',
      diagnosis: 'Diagnosis',
      disableAddProject: 'Can not create project in edit mode',
      systemUprade: 'System is currently undergoing maintenance. Metadata related operations are temporarily unavailable.',
      onlyQueryNode: 'There\'s no active job node now. Metadata related operations are temporarily unavailable.',
      viewDetails: 'View details',
      quotaSetting: 'Storage Setting',
      noAuthorityText: 'No project found. Please contact your administrator to create a project.'
    }
  }
})
export default class LayoutLeftRightTop extends Vue {
  projectSaveLoading = false
  project = {}
  isEdit = false
  FormVisible = false
  manualClose = true
  currentPathName = 'DesignModel'
  currentPathNameParent = 'Model'
  defaultActive = ''
  currentUserInfo = {
    username: ''
  }
  form = {
    name: '',
    region: '',
    date1: '',
    date2: '',
    delivery: false,
    type: [],
    resource: '',
    desc: ''
  }
  menus = menusData
  resetPasswordFormVisible = false
  isAnimation = false
  isGlobalMaskShow = false
  showDiagnostic = false
  showChangePassword = false

  get isAdminView () {
    const adminRegex = /^\/admin/
    return adminRegex.test(this.$route.fullPath)
  }

  @Watch('$route.name')
  onRouterChange (newVal, val) {
    if (newVal !== val) {
      this.manualClose = true
    }
    setTimeout(() => {
      this.changeRouteEvent('route')
    }, 500)
  }
  setGlobalMask (notifyContect) {
    this.isGlobalMaskShow = true
    this.notifyContect = this.$t(notifyContect, this.currentUserInfo)
  }
  hideGlobalMask () {
    this.isGlobalMaskShow = false
    this.notifyContect = ''
  }
  async handleSwitchAdmin () {
    this.setGlobalMask(this.isAdminView ? 'leaveAdmin' : 'enterAdmin')
    await delayMs(1700)
    if (this.isAdminView) {
      const nextLocation = this.cachedHistory ? this.cachedHistory : '/query/insight'
      this.isAnimation = true
      this.$router.push(nextLocation)
      setTimeout(() => {
        this.isAnimation = false
      })
    } else {
      this.cacheHistory(this.$route.fullPath)
      this.isAnimation = true
      this.$router.push('/admin/project')
      setTimeout(() => {
        this.isAnimation = false
      })
    }
    this.hideGlobalMask()
  }
  created () {
    this.defaultActive = this.$route.path || '/query/insight'

    this.changeRouteEvent('created')
  }
  changeRouteEvent (from) {
    if (this.showRevertPasswordDialog === 'true' && 'defaultPassword' in this.currentUser && this.currentUser.defaultPassword) {
      this.showChangePassword = true
      this.callUserEditModal({ editType: 'password', showCloseBtn: false, showCancelBtn: false, userDetail: this.$store.state.user.currentUser }).then(() => {
        this.noProjectTips()
      })
    } else {
      this.noProjectTips()
    }
  }
  showMenuByRole (menuName) {
    let isShowSnapshot = true
    let isShowStreamingJob = true
    if (menuName === 'snapshot') {
      isShowSnapshot = this.$store.state.project.snapshot_manual_management_enabled
    }
    if (menuName === 'streamingjob') {
      isShowStreamingJob = this.$store.state.system.streamingEnabled === 'true'
    }
    return this.availableMenus.includes(menuName.toLowerCase()) && isShowSnapshot && isShowStreamingJob
  }
  defaultVal (obj) {
    if (!obj) {
      return 'N/A'
    } else {
      return obj
    }
  }
  _replaceRouter (currentPathName, currentPath) {
    this.$router.replace('/refresh')
    // 切换项目时重置speedInfo，避免前后两个项目混淆
    this.resetSpeedInfo({reachThreshold: false, queryCount: 0, modelCount: 0})
    this.$nextTick(() => {
      if (currentPathName === 'Job' && !this.hasPermissionWithoutQuery && !this.isAdmin) {
        this.$router.replace({name: 'Dashboard', params: { refresh: true }})
      } else if (currentPathName === 'ModelSubPartitionValues' || currentPathName === 'ModelDetails') {
        this.$router.replace({name: 'ModelList', params: { refresh: true }})
      } else {
        this.$router.replace({name: currentPathName, params: { refresh: true }})
        currentPath && (this.defaultActive = currentPath)
      }
    })
  }
  _isAjaxProjectAcess (allProject, curProjectName, currentPathName, currentPath) {
    let curProjectUserAccess = this.getUserAccess({project: curProjectName})
    Promise.all([curProjectUserAccess]).then((res) => {
      this._replaceRouter(currentPathName, currentPath)
    })
  }
  changeProject (val) {
    if (this.$store.state.system.isEditForm) {
      // agg group 弹框打开，有修改时取消确认
      this.callGlobalDetailDialog({
        msg: this.$t('kylinLang.common.unSavedTips'),
        title: this.$t('kylinLang.common.tip'),
        dialogType: 'warning',
        showDetailBtn: false,
        needConcelReject: true,
        hideBottomLine: true,
        wid: '420px',
        submitText: this.$t('kylinLang.common.discardChanges')
      }).then(() => {
        this.setProject(val)
        var currentPathName = this.$router.currentRoute.name
        var currentPath = this.$router.currentRoute.path
        this._isAjaxProjectAcess(this.$store.state.project.allProject, val, currentPathName, currentPath)
      }).catch(() => {
        this.$nextTick(() => {
          let preProject = this.$store.state.project.selected_project // 恢复上一次的project
          this.$refs['projectSelect'].selected_project = preProject
        })
      })
    } else {
      this.setProject(val)
      var currentPathName = this.$router.currentRoute.name
      var currentPath = this.$router.currentRoute.path
      this._isAjaxProjectAcess(this.$store.state.project.allProject, val, currentPathName, currentPath)
    }
  }
  async addProject () {
    try {
      const data = await this.callProjectEditModal({ editType: 'new' })
      if (data) {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        cacheSessionStorage('projectName', data.name)
        cacheLocalStorage('projectName', data.name)
        this.$store.state.project.selected_project = data.name
        this.FormVisible = false
        this.projectSaveLoading = false
        this._replaceRouter('Source', '/studio/source')
      }
    } catch (e) {
      handleError(e)
    }
  }
  Save () {
    this.$refs.projectForm.$emit('projectFormValid')
  }
  onSubmit () {
  }
  handleselect (a, b) {
    if (a !== '/project') {
      this.defaultActive = a
    }
  }
  toggleLeftMenu () {
    this.toggleMenu(!this.briefMenuGet)
  }
  logoutConfirm () {
    return kylinConfirm(this.$t('confirmLoginOut'), {
      type: 'warning',
      confirmButtonText: this.$t('kylinLang.common.logout')
    }, this.$t('kylinLang.common.logout'))
  }
  changePassword (userDetail) {
    this.callUserEditModal({ editType: 'password', userDetail })
  }
  loginOutFunc () {
    const handleLoginOut = () => {
      this.logoutConfirm().then(() => {
        this.loginOut().then(async (res) => {
          const { redirectUri } = res.data
          localStorage.setItem('buyit', false)
          localStorage.setItem('loginIn', false)
          this.removeAllPagerSizeCache()
          // reset 所有的project信息
          this.resetProjectState()
          this.resetQueryTabs()
          if (redirectUri) {
            window.location.href = redirectUri
          }
          if (this.$route.name === 'ModelEdit') {
            this.$router.push({name: 'Login', params: { ignoreIntercept: true }})
          } else {
            this.$router.push({name: 'Login'})
          }
        })
      })
    }
    if (this.$route.name === 'ModelEdit') {
      MessageBox.confirm(window.kylinVm.$t('kylinLang.common.willGo'), window.kylinVm.$t('kylinLang.common.notice'), {
        confirmButtonText: window.kylinVm.$t('kylinLang.common.logout'),
        cancelButtonText: window.kylinVm.$t('kylinLang.common.cancel'),
        type: 'warning'
      }).then(() => {
        handleLoginOut()
      })
    } else {
      handleLoginOut()
    }
  }
  removeAllPagerSizeCache () {
    for (let p in pageRefTags) {
      const pager = pageRefTags[p]
      if (localStorage.getItem(pager)) {
        localStorage.removeItem(pager)
      }
    }
  }
  resetQueryTabs () {
    this.saveTabs({tabs: null})
  }
  handleCommand (command) {
    if (command === 'loginout') {
      this.loginOutFunc()
    } else if (command === 'setting') {
      this.changePassword(this.currentUser)
    }
  }
  goToProjectList () {
    this.defaultActive = 'projectActive'
    this.$router.push({name: 'Project'})
  }
  goHome () {
    $('#menu-list').find('li').eq(0).click()
  }
  closeResetPassword () {
    this.$refs['resetPassword'].$refs['resetPasswordForm'].resetFields()
  }
  checkResetPasswordForm () {
    this.$refs['resetPassword'].$emit('resetPasswordFormValid')
  }
  resetPasswordValidSuccess (data) {
    let userPassword = {
      username: data.username,
      password: data.oldPassword,
      newPassword: data.password
    }
    this.resetPassword(userPassword).then((result) => {
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.updateSuccess')
      })
    }).catch((res) => {
      handleError(res)
    })
    this.resetPasswordFormVisible = false
  }
  resetProjectForm () {
    this.$refs['projectForm'].$refs['projectForm'].resetFields()
  }
  hasSomeProjectPermission () {
    return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask, permissions.OPERATION.mask)
  }
  hasAdminProjectPermission () {
    return hasPermission(this, permissions.ADMINISTRATION.mask)
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  get isShowRouterView () {
    return this.isAdmin || (!this.isAdmin && !this.highlightType)
  }
  get hasPermissionWithoutQuery () {
    return this.hasSomeProjectPermission()
  }
  get hasSomePermissionOfProject () {
    return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
  }
  get hasAdminPermissionOfProject () {
    return this.hasAdminProjectPermission()
  }
  get gloalProjectSelectShow () {
    return this.$store.state.config.layoutConfig.gloalProjectSelectShow
  }
  get currentPath () {
    var currentPathName = ''
    menusData.forEach((menu) => {
      if (this.defaultActive && menu.path.toLowerCase() === this.defaultActive.toLowerCase()) {
        currentPathName = menu.name
      }
    })
    if (this.defaultActive.toLowerCase() === '/messages') {
      currentPathName = 'messages'
    }
    return currentPathName || 'project'
  }
  get currentRouterNameArr () {
    const hashStr = this.$route.path
    const hashArr = hashStr.split('/').slice(1)
    return hashArr
  }
  get currentUser () {
    this.currentUserInfo = this.$store.state.user.currentUser
    let info = { ...this.currentUserInfo }
    info.password = ''
    info.confirmPassword = ''
    return info
  }
  get highlightType () {
    return !this.$store.state.project.selected_project && !this.$store.state.project.allProject.length ? 'primary' : ''
  }
  noProjectTips () {
    if (this.highlightType && this.currentUser && this.isAdmin) {
      MessageBox.alert(this.$t('noProject'), this.$t('kylinLang.common.notice'), {
        confirmButtonText: this.$t('kylinLang.common.ok'),
        type: 'info'
      })
    }
  }
  @Watch('currentPathNameGet')
  onCurrentPathNameGetChange (val) {
    this.defaultActive = val
  }
  showDiagnosticDialog () {
    this.showDiagnostic = true
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  .alter-block {
    position: fixed;
    top: 48px;
    width: 100%;
    z-index: 1;
  }
  .full-layout{
    .bread-box {
      padding:20px 0;
      background: #f9fafc;
      height: 54px;
    }
    &:not(.isModelList){
      .grid-content.bg-purple-light {
        .main-content {
          height: 100%;
        }
      }
    }
    .grid-content.bg-purple-light {
      overflow: auto;
      height: 100%;
      min-width: 960px;
      .main-content {
        box-sizing: border-box;
        background: white;
        position: relative;
        .blank-content {
            padding-top: 270px;
            color: @text-placeholder-color;
            width: 410px;
            margin: 0 auto;
            text-align: center;
            &.en {
              width: 380px;
            }
            i {
              font-size: 60px;
            }
            .text {
              font-size: 14px;
              font-weight: 400;
              line-height: 20px;
              margin-top: 15px;
              text-align: center;
            }
        }
      }
    }
    .brief_menu.panel .panel-center{
      .left-menu {
        width: 64px;
        text-align:center;
        li {
          text-align: center;
          span {
            display: none;
          }
        }
        .el-submenu.is-active{
          .el-submenu__title {
            background-color: @menu-active-bgcolor;
            i,span{
              color: @base-color-1;
            }
          }
        }
        .logo{
          width: 24px;
          height: 22px;
          margin: 17px 15px 12px 15px;
        }
      }
      .topbar .nav-icon {
        margin-left: 76px;
      }
      .panel-content{
        left: 64px;
      }
    }
    &:not(.isModelList){
      .panel {
        .panel-center {
          .panel-content {
            overflow-y: auto;
          }
        }
      }
    }
    .panel {
      position: absolute;
      top: 0px;
      bottom: 0px;
      width: 100%;

      .panel-center {
        position: absolute;
        top: 0px;
        bottom: 0px;
        overflow: hidden;

        .panel-content {
          position: absolute;
          right: 0px;
          top: 48px;
          bottom: 0;
          left: 184px;
          overflow-x: auto;
        }
        .left-menu {
          width: 184px;
          height: 100%;
          position: relative;
          z-index: 999;
          background-color: @ke-color-nav;
          .logo {
            width: 100%;
            vertical-align: middle;
            z-index:999;
            cursor: pointer;
          }
          .ky-line {
            background: #053C6C;
          }
        }
        .topbar{
          height: 47px;
          width: 100%;
          background: @ke-background-color-secondary;
          position: absolute;
          top:0;
          border-bottom: 1px solid @ke-border-divider-color;
          z-index: 100;
          .nav-icon {
            margin-left: 202px;
            margin-top: 10px;
            height: 14px;
            line-height: 14px;
            cursor: pointer;
            float: left;
          }
          .add-project-btn {
            margin: 10px 0 0 0;
          }
          .top-ul {
            font-size:0;
            margin-top: 7px;
            >li{
              vertical-align: middle;
              display: inline-block;
              margin-right: 24px;
              cursor: pointer;
              &.capacity-li {
                padding-right: 18px;
                margin-right: 10px;
                border-right: 1px solid @ke-border-secondary;
              }
              &:last-child {
                margin-right: 10px;
              }
            }
            .active-nodes {
              font-size: 14px;
              height: 22px;
              line-height: 22px;
              &:hover {
                color: @base-color;
              }
              .success-text {
                color: @normal-color-1;
              }
            }
            .el-dropdown-link {
              font-size:12px;
            }
            .el-dropdown-link:hover {
              color: @base-color;
              * {
                color: @base-color;
              }
            }
            .user-msg-dropdown {
              // height: 24px;
              .el-dropdown-link {
                height: 100%;
                display: inline-block;
                line-height: 24px;
                .el-icon-ksd-user {
                  float: left;
                  line-height: 24px;
                }
                .el-icon-caret-bottom {
                  float: right;
                  line-height: 24px;
                }
                .limit-user-name {
                  line-height: 24px;
                }
              }
            }
          }
        }
      }
    }
    .entry-admin.active {
      background-color: @ke-background-color-active;
      &:hover,
      &:focus {
        background-color: @ke-background-color-active;
        border-color: @ke-background-color-active;
      }

    }
    .global-mask {
      position: absolute;
      top: 0;
      right: 0;
      bottom: 0;
      left: 0;
      z-index: 999999;
      .background {
        width: 100%;
        height: 100%;
        opacity: 0.7;
        background-color: @000;
      }
      .notify-contect {
        position: absolute;
        top: 30%;
        left: 50%;
        transform: translateX(-50%);
        height: 50px;
        min-width: 300px;
        white-space: nowrap;
        padding: 0 40px;
        line-height: 50px;
        background: @base-color;
        color: @fff;
        text-align: center;
        font-size: 16px;
      }
    }
  }
  .limit-user-name {
    display: inline-block;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 120px;
    line-height: 29px;
  }
  .user-name {
    padding: 0 8px 8px 8px;
    line-height: 1;
    border-bottom: 1px solid @line-split-color;
    box-sizing: border-box;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .round-icon (@width, @height) {
    width:@width;
    height: @height;
    line-height: @height;
    text-align: center;
    background-color:@error-color-1;
    display:inline-block;
    border-radius: 50%;
    color:#fff;
    margin-left:3px;
  }
  .number-icon {
    .round-icon(16px, 16px);
    font-size:12px;
  }
  .dot-icon {
    .round-icon(6px, 6px);
    margin-top:-14px;
  }
  .el-menu--collapse {
    .dot-icon {
      margin-left:0;
    }
  }
  .el-menu--popup {
    margin-left: 1px;
    padding: 0px;
    &.el-menu {
      min-width: 100px;
    }
    .el-menu-item-group__title {
      display: none;
    }
    .el-menu-item {
      color: @text-placeholder-color;
      height: 32px;
      line-height: 32px;
    }
  }
  .diagnostic-model {
    width: calc(~'100% - 44px');
    height: 24px;
    border-radius: 12px;
    border: solid 1px @line-border-color;
    margin-left: 22px;
    box-sizing: border-box;
    position: absolute;
    bottom: 40px;
    color: @line-border-color;
    font-size: 12px;
    text-align: center;
    line-height: 22px;
    cursor: pointer;
    &.is-hold-menu {
      width: calc(~'100% - 31px');
      margin-left: 15px;
    }
    &:hover {
      color: @base-color;
      border: solid 1px @base-color;
    }
  }
  .diagnosis-tip {
    margin-left: 30px !important;
  }
</style>
