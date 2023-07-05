<template>
    <el-dialog class="user-data-permission" width="400px"
      :title="$t('dataPermission')"
      :visible="isShow"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="isShow && closeHandler()">
      <span>{{$t('kylinLang.common.userName')}}</span>
      <el-input class="ksd-mt-8" v-model="userName" :disabled="true"></el-input>
      <div class="ksd-mt-8 flex">
        <span>{{$t('dataPermission')}}</span>
        <el-tooltip class="item" effect="dark" :content="$t('dataPermissionTips')" placement="bottom">
          <i class="el-icon-ksd-info ksd-fs-14 ksd-ml-5"></i>
        </el-tooltip>
        <el-switch
          :value="dataPermission"
          @change="handleChangePermission"
          class="ksd-ml-8"
          :active-text="$t('kylinLang.common.OFF')"
          :inactive-text="$t('kylinLang.common.ON')">
        </el-switch>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="closeHandler()">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" @click="submit" :loading="isLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
  </template>
  
  <script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  import { mapActions, mapMutations, mapState } from 'vuex'
  import vuex from '../../../../store'
  import locales from './locales'
  import store, { types } from './store'
  import { handleError } from 'util/business'
  vuex.registerModule(['modals', 'UserDataPermission'], store)
  @Component({
    computed: {
      // Store数据注入
      ...mapState('UserDataPermission', {
        isShow: state => state.isShow,
        dataPermission: state => state.dataPermission,
        userName: state => state.userName,
        callback: state => state.callback
      })
    },
    methods: {
      // Store方法注入
      ...mapMutations('UserDataPermission', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL
      }),
      // 后台接口请求
      ...mapActions({
        updataUserDataPermission: 'UPDATE_USER_DATA_PERMISSION'
      })
    },
    locales
  })
  export default class UserDataPermission extends Vue {
    isLoading = false
    closeHandler () {
      this.hideModal()
    }
    handleChangePermission (val) {
      this.setModal({ dataPermission: val })
    }
    async submit () {
      this.isLoading = true
      try {
        await this.updataUserDataPermission({ username: this.userName, enabled: this.dataPermission })
        this.isLoading = false
        this.callback(true)
      } catch (e) {
        handleError(e)
        this.isLoading = false
      }
      this.hideModal()
    }
  }
  </script>
  <style lang="less">
    @import '../../../../assets/styles/variables.less';
    .user-data-permission {
      .flex {
        display: flex;
        align-items: center;
      }
      .el-icon-ksd-info {
        color: @text-placeholder-color;
      }
    }
  </style>
  