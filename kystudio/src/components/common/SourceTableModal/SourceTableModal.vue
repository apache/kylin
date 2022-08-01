<template>
  <el-dialog class="source-table-modal" width="560px"
    :title="$t(modalTitle)"
    :visible="isShow"
    @open="handleOpen"
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @close="() => handleClose()"
    @closed="handleClosed">
    <el-form :model="form" :rules="rules" ref="form" size="medium" v-if="isFormShow" label-position="top">
      <!-- <el-form-item prop="isLoadExisted" v-if="_isFieldShow('isLoadExisted')">
        <el-radio class="font-medium" :value="form.isLoadExisted" :label="true" @input="value => handleInput('isLoadExisted', value)">
          {{$t('loadExistingData')}}
        </el-radio>
        <div class="item-desc">{{$t('loadExistingDataDesc')}}</div>
      </el-form-item> -->
      <el-form-item class="custom-load" prop="loadDataRange" v-if="_isFieldShow('loadDataRange')">
        <el-alert
          :title="$t('kylinLang.dataSource.rangeInfoTip')"
          type="info"
          class="ksd-pt-0"
          :show-background="false"
          :closable="false"
          show-icon>
        </el-alert>
        <div class="ky-no-br-space">
        <el-date-picker
          class="ksd-mr-5"
          type="datetime"
          :value="form.loadDataRange[0]"
          :is-auto-complete="true"
          :disabled="isDisabled || isLoadingNewRange"
          :placeholder="$t('kylinLang.common.startTime')"
          @change="resetError"
          value-format="timestamp"
          :format="format"
          @input="value => handleInputDate('loadDataRange.0', value)">
        </el-date-picker>
        <el-date-picker
          type="datetime"
          :value="form.loadDataRange[1]"
          :is-auto-complete="true"
          :disabled="isDisabled || isLoadingNewRange"
          :placeholder="$t('kylinLang.common.endTime')"
          @change="resetError"
          value-format="timestamp"
          :format="format"
          @input="value => handleInputDate('loadDataRange.1', value)">
        </el-date-picker>
        <el-tooltip effect="dark" :content="$t('detectAvailableRange')" placement="top">
          <el-button
            v-if="isFormShow&&$store.state.project.projectPushdownConfig"
            size="medium"
            style="line-height:1"
            class="ksd-ml-10"
            :disabled="isDisabled"
            :loading="isLoadingNewRange"
            icon="el-icon-ksd-data_range_search"
            @click="handleLoadNewestRange">
          </el-button>
        </el-tooltip>
        </div>
        <!-- for guide -->
        <span style="position:absolute;width:1px; height:0" @click="handleLoadNewestRange"></span>
        <span style="position:absolute;width:1px; height:0" v-if="form.loadDataRange[0] && form.loadDataRange[1]"></span>
      </el-form-item>
      <el-form-item class="custom-load" prop="freshDataRange" v-if="_isFieldShow('freshDataRange')">
        <div class="item-desc">{{$t('refreshRangeDesc')}}</div>
        <el-alert
          :title="$t('kylinLang.dataSource.rangeInfoTip')"
          type="info"
          class="ksd-pt-0"
          :show-background="false"
          :closable="false"
          show-icon>
        </el-alert>
        <div class="ky-no-br-space">
          <el-date-picker
            type="datetime"
            class="ksd-mr-5"
            :value="form.freshDataRange[0]"
            :is-auto-complete="true"
            :disabled="isDisabled"
            :placeholder="$t('kylinLang.common.startTime')"
            @change="resetError"
            value-format="timestamp"
            :format="format"
            @input="value => handleInputDate('freshDataRange.0', value)">
          </el-date-picker>
          <el-date-picker
            type="datetime"
            :value="form.freshDataRange[1]"
            :is-auto-complete="true"
            :disabled="isDisabled"
            :placeholder="$t('kylinLang.common.endTime')"
            @change="resetError"
            value-format="timestamp"
            :format="format"
            @input="value => handleInputDate('freshDataRange.1', value)">
          </el-date-picker>
        </div>
      </el-form-item>
    </el-form>
    <div class="error-msg" v-if="isShowRangeDateError">{{loadRangeDateError}}</div>
    <div slot="footer" class="dialog-footer ky-no-br-space" v-if="isShow">
      <el-button size="medium" @click="() => handleClose()">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" @click="handleSubmit" :loading="isLoading">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { set } from '../../../util/object'
import { handleError, handleSuccessAsync, getGmtDateFromUtcLike } from '../../../util'
import { fieldVisiableMaps, titleMaps, validate, fieldTypes, editTypes, _getLoadDataForm, _getRefreshDataForm, _getNewestTableRange } from './handler'

const { LOAD_DATA_RANGE, REFRESH_DATA_RANGE } = fieldTypes

vuex.registerModule(['modals', 'SourceTableModal'], store)

@Component({
  computed: {
    ...mapState('SourceTableModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback,
      project: state => state.project,
      table: state => state.table,
      model: state => state.model,
      format: state => state.format
    })
  },
  methods: {
    ...mapMutations('SourceTableModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      initForm: types.INIT_FORM
    }),
    ...mapActions({
      saveLoadRange: 'SAVE_DATA_RANGE',
      fetchFreshInfo: 'FETCH_RANGE_FRESH_INFO',
      freshDataRange: 'FRESH_RANGE_DATA',
      fetchNewestTableRange: 'FETCH_NEWEST_TABLE_RANGE'
    })
  },
  locales
})
export default class SourceTableModal extends Vue {
  isLoading = false
  isDisabled = false
  isFormShow = false
  rules = {
    [LOAD_DATA_RANGE]: [{ validator: this.validate(LOAD_DATA_RANGE), trigger: 'blur' }],
    [REFRESH_DATA_RANGE]: [{ validator: this.validate(LOAD_DATA_RANGE), trigger: 'blur' }]
  }
  isLoadingNewRange = false
  loadRangeDateError = ''
  isShowRangeDateError = false
  get modalTitle () {
    return titleMaps[this.editType]
  }
  handleOpen () {
    this._showForm()
  }
  handleClose (isSubmit = false) {
    this.isLoadingNewRange = false
    this.isLoading = false
    this.hideModal()
    this.callback && this.callback(isSubmit)
  }
  handleClosed () {
    this._hideForm()
    this.initForm()
    this.resetError()
  }
  handleInput (key, value) {
    this.setModalForm(set(this.form, key, value))
  }
  handleInputDate (key, value) {
    this.handleInput(key, value)
  }
  async handleLoadNewestRange () {
    this.isLoadingNewRange = true
    this.resetError()
    try {
      const submitData = _getNewestTableRange(this.project, this.table)
      const response = await this.fetchNewestTableRange(submitData)
      if (submitData.tableFullName !== this.table.fullName) {
        return
      }
      if (response.body.code === '000') {
        const result = await handleSuccessAsync(response)
        const startTime = +result.start_time
        const endTime = +result.end_time
        this.handleInputDate('loadDataRange', [ getGmtDateFromUtcLike(startTime), getGmtDateFromUtcLike(endTime) ])
      } else if (response.body.code === '999') {
        this.loadRangeDateError = response.body.msg
        this.isShowRangeDateError = true
      }
    } catch (e) {
      handleError(e)
    }
    this.isLoadingNewRange = false
  }
  resetError () {
    this.loadRangeDateError = ''
    this.isShowRangeDateError = false
  }
  async handleSubmit () {
    this._showLoading()
    try {
      await this.$refs['form'].validate()
      await this._submit()

      this._notifySuccess()
      this.handleClose(true)
    } catch (e) {
      handleError(e)
    }
    this._hideLoading()
  }
  async _submit () {
    switch (this.editType) {
      case editTypes.LOAD_DATA: {
        const submitData = _getLoadDataForm(this)
        return await this.saveLoadRange(submitData)
      }
      case editTypes.REFRESH_DATA: {
        const submitData = _getRefreshDataForm(this)
        const response = await this.fetchFreshInfo(submitData)
        const result = await handleSuccessAsync(response)
        const modelSize = result.byte_size
        submitData.affected_start = result.affected_start
        submitData.affected_end = result.affected_end
        if (modelSize) {
          await this._showRefreshConfirm({ modelSize })
        }
        return await this.freshDataRange(submitData)
      }
    }
  }
  _showRefreshConfirm ({ modelSize }) {
    const storageSize = Vue.filter('dataSize')(modelSize)
    const tableName = this.table.name
    const contentVal = { tableName, storageSize }
    const confirmTitle = this.$t('refreshTitle')
    const confirmMessage1 = this.$t('refreshContent1', contentVal)
    const confirmMessage2 = this.$t('refreshContent2', contentVal)
    const confirmMessage = _render(this.$createElement)
    const confirmButtonText = this.$t('kylinLang.common.submit')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })

    function _render (h) {
      return (
        <div>
          <p class="break-all">{confirmMessage1}</p>
          <p>{confirmMessage2}</p>
        </div>
      )
    }
  }
  _isFieldShow (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }
  validate (type) {
    return validate[type].bind(this)
  }
  _notifySuccess () {
    this.$message({ type: 'success', message: this.$t('kylinLang.common.saveSuccess') })
  }
  _hideForm () {
    this.isFormShow = false
  }
  _showForm () {
    this.isFormShow = true
  }
  _hideLoading () {
    this.isLoading = false
    this.isDisabled = false
  }
  _showLoading () {
    this.isLoading = true
    this.isDisabled = true
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.source-table-modal {
  .el-dialog__title {
    font-size: 16px;
    font-weight: @font-medium;
    color: #263238;
  }
  .el-form-item:last-child {
    margin-bottom: 0;
  }
  .el-form-item--medium .el-form-item__content, .el-form-item__content * {
    line-height: initial;
  }
  .item-desc:first-child {
    margin-bottom: 15px;
  }
  .item-desc:not(:first-child) {
    margin-top: 5px;
  }
  .custom-load {
    .el-radio {
      display: block;
      margin-bottom: 8px;
    }
  }
  .error-msg {
    color: @error-color-1;
    font-size: 12px;
    margin-top: 5px;
  }
}
</style>
