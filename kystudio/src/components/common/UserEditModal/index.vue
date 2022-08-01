<template>
  <el-dialog class="user-edit-modal" :width="modalWidth"
    :title="$t(modalTitle)"
    :visible="isShow"
    limited-area
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    :show-close="showCloseBtn"
    @close="isShow && closeHandler(false)">
    <p class="change-system-password-tip" v-if="isAdminRole && editType === 'password' && currentUser && 'defaultPassword' in currentUser && currentUser.defaultPassword">{{$t('kylinLang.common.useOldPasswordTip')}}</p>
    <el-form :model="form" label-position="top" :rules="rules" ref="form" v-if="isFormShow">
       <!-- 避免浏览器自动填充 -->
      <input name="username" type="text" style="display:none"/>
      <input name="password" type="password" style="width:1px;height:0;border:none;position:absolute"/>
      <!-- 表单：用户名 -->
      <el-form-item :label="$t('username')" prop="username" v-if="isFieldShow('username')" class="js_username">
        <el-input
          size="medium"
          :value="form.username"
          :placeholder="$t('usernamePld')"
          @input="value => inputHandler('username', value)"
          :disabled="editType !== 'new'">
          </el-input>
      </el-form-item>
      <!-- 表单：密码 -->
      <div class="needHideErrMsg">
        <el-form-item :label="$t('password')" prop="password" :show-message="false" v-if="isFieldShow('password')" class="js_password">
          <el-input
            :placeholder="$t('kylinLang.common.inputPld')"
            size="medium"
            type="password"
            :value="form.password"
            @input="value => inputHandler('password', value)"
            @blur="blurHandler('password')">
            </el-input>
        </el-form-item>
        <!-- 设置密码的时候，需要显示常规规则 begin -->
        <div class="pwdRuleList" v-if="isFieldShow('password')">
          <p :class="pwdRuleList.num" class="clearfix">
            <i v-if="!pwdRuleList.num" class="point ksd-fleft">•</i>
            <i v-else :class="{'el-icon-ksd-accept': pwdRuleList.num === 'ok', 'el-icon-ksd-close': pwdRuleList.num === 'error'}" class="ksd-fleft"></i>
            <span class="ksd-fleft msg">{{$t('passwordValid1')}}</span>
          </p>
          <p :class="pwdRuleList.letter" class="clearfix">
            <i v-if="!pwdRuleList.letter" class="point ksd-fleft">•</i>
            <i v-else :class="{'el-icon-ksd-accept': pwdRuleList.letter === 'ok', 'el-icon-ksd-close': pwdRuleList.letter === 'error'}" class="ksd-fleft"></i>
            <span class="ksd-fleft msg">{{$t('passwordValid2')}}</span>
          </p>
          <p :class="pwdRuleList.char" class="clearfix">
            <i v-if="!pwdRuleList.char" class="point ksd-fleft">•</i>
            <i v-else :class="{'el-icon-ksd-accept': pwdRuleList.char === 'ok', 'el-icon-ksd-close': pwdRuleList.char === 'error'}" class="ksd-fleft"></i>
            <span class="ksd-fleft msg">{{$t('passwordValid3')}}</span>
          </p>
          <p :class="pwdRuleList.len" class="clearfix">
            <i v-if="!pwdRuleList.len" class="point ksd-fleft">•</i>
            <i v-else :class="{'el-icon-ksd-accept': pwdRuleList.len === 'ok', 'el-icon-ksd-close': pwdRuleList.len === 'error'}" class="ksd-fleft"></i>
            <span class="ksd-fleft msg">{{$t('passwordValid4')}}</span>
          </p>
        </div>
        <!-- 设置密码的时候，需要显示常规规则 end -->
      </div>
      <!-- 表单：旧密码（ 面向非管理员 -->
      <el-form-item :label="$t('oldPassword')" prop="oldPassword" v-if="isFieldShow('oldPassword')" class="js_oldPassword">
        <el-input
          :placeholder="$t('kylinLang.common.inputPld')"
          size="medium"
          type="password"
          :value="form.oldPassword"
          @input="value => inputHandler('oldPassword', value)">
          </el-input>
      </el-form-item>
      <!-- 表单：新密码 -->
      <div class="needHideErrMsg">
        <el-form-item :label="$t('newPassword')" prop="newPassword" :show-message="false" v-if="isFieldShow('newPassword')" class="js_newPassword">
          <el-input
            :placeholder="$t('kylinLang.common.inputPld')"
            size="medium"
            type="password"
            :value="form.newPassword"
            @input="value => inputHandler('newPassword', value)"
            @blur="blurHandler('newPassword')">
          </el-input>
        </el-form-item>
        <!-- 设置密码的时候，需要显示常规规则 begin -->
        <div class="pwdRuleList" v-if="isFieldShow('newPassword')">
          <p :class="pwdRuleList.num" class="clearfix">
            <i v-if="!pwdRuleList.num" class="point ksd-fleft">•</i>
            <i v-else :class="{'el-icon-ksd-accept': pwdRuleList.num === 'ok', 'el-icon-ksd-close': pwdRuleList.num === 'error'}" class="ksd-fleft"></i>
            <span class="ksd-fleft msg">{{$t('passwordValid1')}}</span>
          </p>
          <p :class="pwdRuleList.letter" class="clearfix">
            <i v-if="!pwdRuleList.letter" class="point ksd-fleft">•</i>
            <i v-else :class="{'el-icon-ksd-accept': pwdRuleList.letter === 'ok', 'el-icon-ksd-close': pwdRuleList.letter === 'error'}" class="ksd-fleft"></i>
            <span class="ksd-fleft msg">{{$t('passwordValid2')}}</span>
          </p>
          <p :class="pwdRuleList.char" class="clearfix">
            <i v-if="!pwdRuleList.char" class="point ksd-fleft">•</i>
            <i v-else :class="{'el-icon-ksd-accept': pwdRuleList.char === 'ok', 'el-icon-ksd-close': pwdRuleList.char === 'error'}" class="ksd-fleft"></i>
            <span class="ksd-fleft msg">{{$t('passwordValid3')}}</span>
          </p>
          <p :class="pwdRuleList.len" class="clearfix">
            <i v-if="!pwdRuleList.len" class="point ksd-fleft">•</i>
            <i v-else :class="{'el-icon-ksd-accept': pwdRuleList.len === 'ok', 'el-icon-ksd-close': pwdRuleList.len === 'error'}" class="ksd-fleft"></i>
            <span class="ksd-fleft msg">{{$t('passwordValid4')}}</span>
          </p>
        </div>
        <!-- 设置密码的时候，需要显示常规规则 end -->
      </div>
      <!-- 表单：确认密码 -->
      <el-form-item :label="$t('confirmNewPassword')" prop="confirmPassword" v-if="isFieldShow('confirmPassword')" class="js_confirmPwd">
        <el-input
          :placeholder="$t('kylinLang.common.inputPld')"
          size="medium"
          type="password"
          :value="form.confirmPassword"
          @input="value => inputHandler('confirmPassword', value)">
          </el-input>
      </el-form-item>
      <!-- 表单：角色 -->
      <el-form-item :label="$t('role')" v-if="isFieldShow('admin')">
        <el-radio-group :value="form.admin" @input="value => inputHandler('admin', value)">
          <el-radio :label="true">{{$t('admin')}}</el-radio>
          <el-radio :label="false">{{$t('user')}}</el-radio>
        </el-radio-group>
      </el-form-item>
      <!-- 表单：分组 -->
      <el-form-item v-if="isFieldShow('group')">
        <el-transfer
          filterable
          :data="totalGroups"
          :value="form.authorities"
          :filter-placeholder="$t('userGroupFilter')"
          :titles="[$t('willCheckGroup'), $t('checkedGroup')]"
          :show-overflow-tip="true"
          @change="value => inputHandler('authorities', value)">
          </el-transfer>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button size="medium" v-if="showCancelBtn" @click="closeHandler(false)">{{$t('cancel')}}</el-button>
      <el-button type="primary" size="medium" @click="submit" :loading="isLoading">{{$t('ok')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { fieldVisiableMaps, titleMaps, getSubmitData } from './handler'
import { validate, validateTypes, handleError } from '../../../util'
import { Base64 } from 'js-base64'

const { USERNAME, PASSWORD, CONFIRM_PASSWORD } = validateTypes

vuex.registerModule(['modals', 'UserEditModal'], store)

@Component({
  computed: {
    // 全局getter注入
    ...mapGetters([
      'isAdminRole',
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('UserEditModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback,
      totalGroups: state => state.totalGroups,
      showCloseBtn: state => state.showCloseBtn,
      showCancelBtn: state => state.showCancelBtn
    }),
    ...mapState({
      currentUser: state => state.user.currentUser
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('UserEditModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      saveUser: 'SAVE_USER',
      editRole: 'EDIT_ROLE',
      resetPassword: 'RESET_PASSWORD',
      getGroupList: 'GET_GROUP_LIST',
      addGroupToUsers: 'ADD_GROUPS_TO_USER'
    })
  },
  locales
})
export default class UserEditModal extends Vue {
  // Data: 用来销毁el-form
  isFormShow = false
  isLoading = false
  // 密码规则出错信息
  pwdRuleList = {
    len: '',
    num: '',
    char: '',
    letter: ''
  }

  // Computed: Modal宽度
  get modalWidth () {
    return this.editType === 'group'
      ? '600px'
      : '440px'
  }

  // Computed: Modal标题
  get modalTitle () {
    return titleMaps[this.editType]
  }

  get rules () {
    return {
      // Data: el-form表单验证规则
      username: [{
        validator: this.validate(USERNAME), trigger: 'blur', required: true
      }],
      password: [{
        validator: this.validate(PASSWORD), trigger: 'blur', required: true
      }],
      oldPassword: [{
        message: this.$t('kylinLang.common.passwordEmpty'), trigger: 'blur', required: true
      }],
      newPassword: [{
        validator: this.validate(PASSWORD), trigger: 'blur', required: true
      }],
      confirmPassword: [{
        validator: this.validate(CONFIRM_PASSWORD), trigger: 'blur', required: true
      }]
    }
  }

  // Computed Method: 计算每个Form的field是否显示
  isFieldShow (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }

  // Watcher: 监视销毁上一次elForm
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      this.editType === 'group' && this.fetchUserGroups()
      document.addEventListener('keyup', this.handlerKeyEvent)
    } else {
      // 密码规则出错信息重置
      this.pwdRuleList = {
        len: '',
        num: '',
        char: '',
        letter: ''
      }
      document.removeEventListener('keyup', this.handlerKeyEvent)
      setTimeout(() => {
        this.isFormShow = false
      }, 300)
    }
  }

  handlerKeyEvent (e) {
    if (e.keyCode === 13) {
      this.submit()
    }
  }

  // Action: 模态框关闭函数
  closeHandler (isSubmit) {
    this.hideModal()

    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 200)
  }

  // Action: 修改Form函数
  inputHandler (key, value) {
    this.setModalForm({[key]: value})
    if (key === 'password' || key === 'newPassword') {
      if (value.length >= 8) {
        this.pwdRuleList.len = 'ok'
      } else {
        this.pwdRuleList.len = this.pwdRuleList.len ? 'error' : ''
      }
      if (/[a-zA-z]+/.test(value)) {
        this.pwdRuleList.letter = 'ok'
      } else {
        this.pwdRuleList.letter = this.pwdRuleList.letter ? 'error' : ''
      }
      if (/[0-9]+/.test(value)) {
        this.pwdRuleList.num = 'ok'
      } else {
        this.pwdRuleList.num = this.pwdRuleList.num ? 'error' : ''
      }
      if (/[~!@#$%^&*(){}|:"<>?\[\];',.\/`]+/.test(value)) { // eslint-disable-line
        this.pwdRuleList.char = 'ok'
      } else {
        this.pwdRuleList.char = this.pwdRuleList.char ? 'error' : ''
      }
    }
  }

  // Action: blur 事件
  blurHandler (key) {
    if (key === 'password' || key === 'newPassword') {
      let value = key === 'password' ? this.form.password : this.form.newPassword
      if (!value || value.length < 8) {
        this.pwdRuleList.len = 'error'
      }
      if (!/[a-zA-z]+/.test(value)) {
        this.pwdRuleList.letter = 'error'
      }
      if (!/[0-9]+/.test(value)) {
        this.pwdRuleList.num = 'error'
      }
      if (!/[~!@#$%^&*(){}|:"<>?\[\];',.\/`]+/.test(value)) { // eslint-disable-line
        this.pwdRuleList.char = 'error'
      }
    }
  }

  // Action: Form递交函数
  async submit () {
    this.isLoading = true
    try {
      // 获取Form格式化后的递交数据
      const data = getSubmitData(this)
      // Base64加密密码
      if (this.editType === 'password' || this.editType === 'resetUserPassword') {
        data.new_password = Base64.encode(data.new_password)
        data.password = Base64.encode(data.password)
      }
      if (this.editType === 'new') {
        data.detail.password = Base64.encode(data.detail.password)
      }
      // 验证表单
      await this.$refs['form'].validate()
      // 针对不同的模式，发送不同的请求
      this.editType === 'new' && await this.saveUser(data)
      this.editType === 'edit' && await this.editRole(data)
      this.editType === 'group' && await this.addGroupToUsers(data)
      if (this.editType === 'password' || this.editType === 'resetUserPassword') {
        await this.resetPassword(data)
      }
      // 成功提示
      this.$message({
        type: 'success',
        message: this.editType === 'new'
          ? this.$t('saveUserSuccess')
          : this.editType === 'group'
            ? this.$t('updateUserOrGroupSuccess')
            : this.$t('updateUserSuccess')
      })
      // 关闭模态框，通知父组件成功
      this.closeHandler(true)
    } catch (e) {
      // 异常处理
      e && handleError(e)
    }
    this.isLoading = false
  }

  // Helper: 给el-form用的验证函数
  validate (type) {
    // 第三方对接进来的用户名，可能规则不符合我们的用户名的格式限制，所以编辑时，不能走校验
    if (type === USERNAME && this.editType !== 'new') {
      return true
    } else {
      // TODO: 这里的this是vue的实例，而data却是class的实例
      return validate[type].bind(this)
    }
  }

  // Helper: 从后台获取用户组
  async fetchUserGroups () {
    // const project = this.currentSelectedProject // 处理资源组时，发现这个接口不用传 project 参数
    const { data: { data: totalGroups } } = await this.getGroupList()

    this.setModal({ totalGroups })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.user-edit-modal {
  .el-dialog {
    width: 650px\0 !important;
  }
  .el-dialog__body{
    overflow-y: overlay !important;
  }
  .el-form-item__content {
    line-height: 1;
  }
  .el-transfer-panel {
    width: 250px;
  }
  .change-system-password-tip {
    margin-bottom: 15px;
    font-size: 14px;
    color: @text-title-color;
  }
  .needHideErrMsg{
    .el-form-item{
      margin-bottom:5px;
    }
    .pwdRuleList{
      font-size: 12px;
      line-height: 14px;
      margin-bottom:15px;
      p{
        margin-top:5px;
        .msg{
          width: calc(~'100% - 20px');
          color: @text-normal-color;
        }
      }
      i{
        margin-right:5px;
        margin-top:1px;
        &.point{
          color: @text-normal-color;
          margin-top:0;
        }
      }
      .ok{
        color: @normal-color-1;
      }
      .error{
        color: @error-color-1;
      }
    }
  }
}
</style>
