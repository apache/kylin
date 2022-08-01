<template>
  <div class="setting" v-loading="isLoading">
    <header class="setting-header">
      <h1 class="ksd-title-label" v-if="projectSettings">{{projectSettings.alias || projectSettings.project}}</h1>
    </header>
    <section class="setting-body" v-if="projectSettings">
      <el-tabs v-model="viewType" :key="$lang">
        <el-tab-pane :label="$t('basic')" :name="viewTypes.BASIC">
          <SettingBasic :project="projectSettings" @reload-setting="getCurrentSettings" @form-changed="handleFormChanged"></SettingBasic>
        </el-tab-pane>
        <el-tab-pane :label="$t('advanced')" :name="viewTypes.ADVANCED">
          <SettingAdvanced :project="projectSettings" @reload-setting="getCurrentSettings" @form-changed="handleFormChanged"></SettingAdvanced>
        </el-tab-pane>
        <el-tab-pane :label="modelSetting" :name="viewTypes.MODEL">
          <SettingModel :project="currentProjectData"></SettingModel>
        </el-tab-pane>
      </el-tabs>
    </section>
    <kylin-empty-data v-else>
    </kylin-empty-data>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions, mapMutations } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { viewTypes } from './handler'
import { handleError, handleSuccessAsync, cacheSessionStorage } from '../../util'
import SettingBasic from './SettingBasic/SettingBasic.vue'
import SettingAdvanced from './SettingAdvanced/SettingAdvanced.vue'
import SettingModel from './SettingModel/SettingModel.vue'

@Component({
  computed: {
    ...mapGetters([
      'currentProjectData'
    ])
  },
  methods: {
    ...mapActions({
      fetchProjectSettings: 'FETCH_PROJECT_SETTINGS',
      getUserAccess: 'USER_ACCESS'
    }),
    ...mapMutations({
      setProject: 'SET_PROJECT'
    })
  },
  components: {
    SettingBasic,
    SettingAdvanced,
    SettingModel
  },
  locales
})
export default class Setting extends Vue {
  viewType = viewTypes.BASIC
  viewTypes = viewTypes
  isLoading = false
  projectSettings = null
  changedForm = {
    isBasicSettingChange: false,
    isAdvanceSettingChange: false
  }
  get modelSetting () {
    return this.$t('model')
  }
  _showLoading () {
    this.isLoading = true
  }
  _hideLoading () {
    this.isLoading = false
  }
  handleFormChanged (changedForm) {
    this.changedForm = { ...this.changedForm, ...changedForm }
  }
  /* async beforeRouteLeave (to, from, next) {
    const [ tabName ] = Object.entries(this.changedForm).find(([tabName, isFormChanged]) => isFormChanged) || []
    try {
      tabName && await this.leaveConfirm()
      next()
    } catch (e) {
      this.viewType = tabName
      next(false)
    }
  } */
  beforeRouteLeave (to, from, next) {
    const [ tabName ] = Object.entries(this.changedForm).find(([tabName, isFormChanged]) => isFormChanged) || []
    if (!to.params.ignoreIntercept && tabName) {
      next(false)
      setTimeout(() => {
        this.$confirm(window.kylinVm.$t('kylinLang.common.willGo'), window.kylinVm.$t('kylinLang.common.notice'), {
          confirmButtonText: window.kylinVm.$t('kylinLang.common.exit'),
          cancelButtonText: window.kylinVm.$t('kylinLang.common.cancel'),
          closeOnClickModal: false,
          closeOnPressEscape: false,
          type: 'warning',
          width: '400px'
        }).then(() => {
          if (to.name === 'refresh') { // 刷新逻辑下要手动重定向
            next()
            this.$nextTick(() => {
              this.$router.replace({name: 'Setting', params: { refresh: true }})
            })
            return
          }
          next()
        }).catch(() => {
          if (to.name === 'refresh') { // 取消刷新逻辑，所有上一个project相关的要撤回
            let preProject = cacheSessionStorage('preProjectName') // 恢复上一次的project
            this.setProject(preProject)
            this.getUserAccess({project: preProject})
          }
          this.viewType = tabName
          next(false)
        })
      })
    } else {
      next()
    }
  }
  async getCurrentSettings () {
    this._showLoading()
    try {
      const projectName = this.currentProjectData.name
      const response = await this.fetchProjectSettings({ projectName })
      const result = await handleSuccessAsync(response)
      const quotaSize = result.storage_quota_size / 1024 / 1024 / 1024 / 1024
      this.projectSettings = {...result, storage_quota_tb_size: quotaSize.toFixed(2)}
    } catch (e) {
      handleError(e)
    }
    this._hideLoading()
  }
  /* leaveConfirm () {
    const confirmMessage = this.$t('kylinLang.common.willGo')
    const confirmTitle = this.$t('kylinLang.common.notice')
    const confirmButtonText = this.$t('kylinLang.common.exit')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })
  } */
  mounted () {
    if (this.currentProjectData) {
      this.getCurrentSettings()
    }
  }
}
</script>

<style lang="less">
@import '../../assets/styles/variables.less';

.el-select-dropdown__item {
  .node-name {
    color: @text-normal-color;
  }
  .node-ip {
    color: @text-title-color;
  }
}

.setting {
  height: 100%;
  padding: 20px;
  .setting-header {
    margin-bottom: 20px;
  }
  .setting-header h1 {
    font-size: 16px;
  }
  .setting-body {
    height: 100%;
  }
  .project-setting,
  .quota-setting,
  .snapshot-setting {
    background-color: @aceditor-bg-color;
    border: 1px solid @line-split-color;
    padding: 15px;
  }
  .project-setting:not(:last-child) {
    margin-bottom: 15px;
  }
  // editable-block style
  .editable-block {
    margin-bottom: 24px;
  }
  // setting-item 通用style
  .setting-item {
    /* border-bottom: 1px solid @line-split-color; */
    padding: 8px 24px;
    margin: 0;
    .sec-nodes {
      font-size: 12px;
      line-height: 16px;
      margin-bottom: 8px;
      margin-top: 4px;
    }
    .node-name {
      color: @text-normal-color;
    }
    .node-ip {
      color: @text-title-color;
    }
  }
  .setting-item:last-child {
    border: none;
    padding-bottom:16px;
  }
  .setting-label {
    margin-right: 8px;
  }
  .setting-input {
    display: none;
    margin-bottom: 0;
  }
  .setting-label {
    display: inline-block;
    color: @text-normal-color;
    font-size: 12px;
  }
  .setting-value {
    display: inline-block;
    color: @text-title-color;
  }
  .is-edit .setting-input {
    display: inline-block;
  }
  .is-edit .setting-value {
    display: none;
    &.fixed {
      display: inline-block;
    }
  }
  .is-edit .setting-input {
    display: inline-block;
  }
  .is-edit .clearfix .setting-value {
    display: none;
  }
  .is-edit .clearfix .setting-input {
    display: block;
  }
  .clearfix {
    .setting-label,
    .setting-value {
      display: block;
      float: left;
    }
    .setting-input {
      display: none;
      float: left;
      margin-left: 3px;
    }
  }
  .setting-desc {
    margin-top: 8px;
    font-size: 12px;
    line-height: 16px;
    color: @text-normal-color;
    // &.large {
    //   font-size: 14px;
    //   color: @text-title-color;
    // }
    .warning{
      margin-top:5px;
      .el-icon-ksd-alert{
        margin-right:5px;
        color:@warning-color-1;
        font-size: 14px;
      }
    }
  }
  .setting-desc:last-child {
    margin-bottom: 0px;
  }
  .option-title {
    color: @text-title-color;
  }
  .field-item {
    margin-top: 10px;
    margin-left: 20px;
    .setting-label {
      position: relative;
      &:before {
        content: ' ';
        position: absolute;
        left: -10px;
        top: calc(~'50% + 1px');
        transform: translateY(-50%);
        width: 4px;
        height: 4px;
        border-radius: 50%;
        background: @text-normal-color;
      }
    }
  }
  .setting-input+.setting-input {
    margin-left: 10px;
  }
  .editable-block {
    .disabled,
    .disabled * {
      color: @text-disabled-color;
    }
    .disabled.field-item .setting-label:before {
      background: @text-disabled-color;
    }
  }
}
</style>
