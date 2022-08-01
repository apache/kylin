<template>
  <el-dialog class="reload-modal" width="480px"
    :title="$t('dialogTitle')"
    :visible="isShow"
    limited-area
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @close="isShow && closeHandler(false)">
    <el-alert
      :type="tipType"
      :closable="false"
      show-icon>
      <div class="lh20" v-if="!hasColumnInfluence" v-html="$t('reloadNoEffectTip', {tableName: this.tableName})"></div>
      <div class="detail lh20" v-else-if="checkData && (checkData.broken_model_count || checkData.snapshot_deleted)">
        <p>{{brokenMsg}}
          <el-button type="primary" size="mini" class="ksd-fs-14" text v-if="!showDetail" @click="showDetail = true">{{$t('checkDetail')}}<i class="el-icon-arrow-down"></i></el-button>
          <el-button type="primary" size="mini" class="ksd-fs-14" text v-else @click="showDetail = false">{{$t('closeDetail')}}<i class="el-icon-arrow-up"></i></el-button>
        </p>
      </div>
      <div class="detail-text lh20" v-else>
        <div class="table-title">{{$t("sourceTable", {tableName: this.tableName})}}</div>
        <div class="detail-text-item" v-if="reduceMsg"><span class="dot">·</span>{{reduceMsg}}</div>
        <div class="detail-text-item" v-if="refreshChangeMsg"><span class="dot">·</span>{{refreshChangeMsg}}</div>
        <div class="detail-text-item" v-if="addMsg"><span class="dot">·</span>{{addMsg}}</div>
        <div class="detail-text-item" v-if="addLayout"><span class="dot">·</span>{{addLayout}}</div>
      </div>
    </el-alert>
    <div class="broken-detail lh20" v-if="checkData && (checkData.broken_model_count || checkData.snapshot_deleted) && showDetail">
      <div class="detail-text">
        <div class="table-title">{{$t("sourceTable", {tableName: this.tableName})}}</div>
        <div class="detail-text-item" v-if="reduceMsg"><span class="dot">·</span>{{reduceMsg}}</div>
        <div class="detail-text-item" v-if="refreshChangeMsg"><span class="dot">·</span>{{refreshChangeMsg}}</div>
        <div class="detail-text-item" v-if="addMsg"><span class="dot">·</span>{{addMsg}}</div>
        <div class="detail-text-item" v-if="addLayout"><span class="dot">·</span>{{addLayout}}</div>
      </div>
    </div>
    <div class="samping-box">
      <el-switch
        size="small"
        v-model="openSample">
      </el-switch><span class="lable-text ksd-ml-8">{{$t('tableSample')}}</span><el-tooltip placement="top" :content="!hasColumnInfluence ? $t('noEffectSampingTip') : $t('hasEffectSampingTip')">
        <span class="desc-icon el-ksd-icon-info_fill_16"></span>
      </el-tooltip>
      <!-- <p v-if="!hasColumnInfluence" class="sample-sub-top">{{$t('noEffectSampingTip', {tableName: this.tableName})}}</p>
      <p v-else class="sample-sub-top">{{$t('hasEffectSampingTip')}}</p> -->
      <el-form ref="sample-form" :rules="rules" :model="sampleOption">
        <el-form-item prop="sampleCount">
          <p>{{$t('sampleCount')}}<el-input v-model="sampleOption.sampleCount" :disabled="!openSample" size="mini" style="width:100px" class="ksd-mrl-5"></el-input>{{$t('rows')}}</p>
        </el-form-item>
      </el-form>
    </div> 
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button :type="showBuildIndex ? 'text' : ''" size="medium" @click="closeHandler(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button :type="!showBuildIndex ? 'primary' : ''" size="medium" :loading="reloadLoading" @click="submit">{{$t('reloadBtn')}}</el-button>
      <el-button type="primary" v-if="showBuildIndex" size="medium" :loading="reloadLoading1" @click="submit('refreshIndex')">{{$t('reloadAndRefresh')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import { handleError } from '../../../../util'
vuex.registerModule(['modals', 'ReloadTableModal'], store)

@Component({
  computed: {
    // Store数据注入
    ...mapState('ReloadTableModal', {
      isShow: state => state.isShow,
      tableName: state => state.tableName,
      checkData: state => state.checkData,
      callback: state => state.callback
    }),
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    // Store方法注入
    ...mapMutations('ReloadTableModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      resetModalForm: types.RESET_MODAL
    }),
    // 后台接口请求
    ...mapActions({
      reloadModel: 'RELOAD_DATASOURCE'
    })
  },
  locales
})
export default class ReloadTableModal extends Vue {
  isFormShow = false
  openSample = true
  reloadLoading = false
  reloadLoading1 = false
  maxSampleCount = 20000000
  minSampleCount = 10000
  sampleOption = {sampleCount: this.maxSampleCount}
  rules = {
    'sampleCount': [{ validator: this.validateSampleCount, trigger: 'blur' }]
  }
  showDetail = true
  validateSampleCount (rule, value, callback) {
    if (!this.openSample) {
      return callback()
    }
    if (!/^\d+$/.test(value)) {
      callback(new Error(this.$t('invalidType')))
    } else if (+value > this.maxSampleCount) {
      callback(new Error(this.$t('invalidLarger')))
    } else if (+value < this.minSampleCount) {
      callback(new Error(this.$t('invalidSmaller')))
    } else {
      value = +value
      callback()
    }
  }
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  clearFormValidate () {
    this.$refs['sample-form'].clearValidate()
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      this.showDetail = true
      this.openSample = true
      this.sampleOption.sampleCount = this.maxSampleCount
    } else {
      this.clearFormValidate()
      setTimeout(() => {
        this.isFormShow = false
      }, 300)
    }
  }
  @Watch('openSample')
  onOpenSample (val) {
    if (!val) {
      this.clearFormValidate()
    }
  }
  get showBuildIndex () {
    return this.checkData && Boolean((this.checkData.broken_model_count === 0) && (this.checkData.add_layouts_count || this.checkData.refresh_layouts_count || this.checkData.update_base_index_count))
  }
  // 有列发生了变化
  get hasColumnInfluence () {
    if (this.isShow) {
      return this.checkData.add_column_count + this.checkData.remove_column_count + this.checkData.data_type_change_column_count > 0
    }
  }
  // 列发生的变化导致其他model和dimension或者measure变化
  get hasDetailInfluence () {
    if (this.isShow) {
      let delMeasureCount = this.checkData.remove_measures_count
      let delDimensionCount = this.checkData.remove_dimensions_count
      let delIndexCount = this.checkData.remove_layouts_count
      let brokenModelCount = this.checkData.broken_model_count
      return delMeasureCount + delDimensionCount + delIndexCount + brokenModelCount > 0
    }
  }
  get tipType () {
    return this.checkData && (this.checkData.broken_model_count || this.checkData.snapshot_deleted) ? 'warning' : 'tip'
  }
  get brokenMsg () {
    let delModelCount = this.checkData.broken_model_count
    let msg = this.$t('reloadTips')
    if (this.checkData.broken_model_count > 0 && this.checkData.snapshot_deleted) {
      msg += this.$t('modelChangeAndSnapshotDel', { modelCount: delModelCount })
    } else {
      if (this.checkData.broken_model_count > 0) {
        msg += this.$t('modelchangeTip', { modelCount: delModelCount })
      }
      if (this.checkData.snapshot_deleted) {
        msg += this.$t('snapshotDelTip')
      }
    }
    return msg
  }
  get addMsg () {
    let addColumnCount = this.checkData.add_column_count
    return addColumnCount ? (this.$t('addColumnsTip', {addedColumnsCount: addColumnCount}) + this.$t('kylinLang.common.dot')) : ''
  }
  get reduceMsg () {
    let reduceColumnCount = this.checkData.remove_column_count
    let delMeasureCount = this.checkData.remove_measures_count
    let delDimensionCount = this.checkData.remove_dimensions_count
    let delIndexCount = this.checkData.remove_layouts_count
    let totalDelCount = delMeasureCount + delDimensionCount + delIndexCount
    let updateBaseIndexCount = this.checkData.update_base_index_count
    let msgArr = []
    let str = ''
    if (reduceColumnCount) {
      str = this.$t('reducedColumnsTip', {reducedColumnsCount: reduceColumnCount}) + this.$t('kylinLang.common.dot')
    }
    if (delDimensionCount) {
      msgArr.push(this.$t('dimensionTip', {delDimensionCount: delDimensionCount}))
    }
    if (delMeasureCount) {
      msgArr.push(this.$t('measureTip', {delMeasureCount: delMeasureCount}))
    }
    if (delIndexCount) {
      msgArr.push(this.$t('indexTip', {delIndexCount: delIndexCount}))
    }
    if (updateBaseIndexCount) {
      msgArr.push(this.$t('updateBaseIndexTip', {updateBaseIndexCount: updateBaseIndexCount}))
    }
    return totalDelCount === 0 ? str : (str + this.$t('afterLoaded') + msgArr.join(this.$t('kylinLang.common.comma')) + this.$t('willBeDelete'))
  }
  get refreshChangeMsg () { // 列发生变化会有索引被刷新
    let dataTypeChangeCount = this.checkData.data_type_change_column_count
    let refreshCount = this.checkData.refresh_layouts_count
    let totalCount = dataTypeChangeCount + refreshCount
    let msgArr = []
    if (dataTypeChangeCount) {
      msgArr.push(this.$t('dataTypeChange', {dataTypeChangeCount: dataTypeChangeCount}))
    }
    if (refreshCount) {
      msgArr.push(this.$t('refreshCountTip', { refreshCount: refreshCount }))
    }
    return totalCount ? msgArr.join('') : ''
  }
  get addLayout () {
    let addLayoutsCount = this.checkData.add_layouts_count
    return addLayoutsCount ? this.$t('addLayoutTips', {addLayoutsCount: addLayoutsCount}) + this.$t('kylinLang.common.comma') + this.$t('rebuildLayout') : ''
  }
  closeHandler (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.resetModalForm()
    }, 200)
  }
  async submit (type) {
    try {
      await this.$refs['sample-form'].validate()
      let message = ''
      type && type === 'refreshIndex' ? this.reloadLoading1 = true : this.reloadLoading = true
      await this.reloadModel({
        project: this.currentSelectedProject,
        table: this.tableName,
        need_sample: this.openSample,
        max_rows: this.openSample ? +this.sampleOption.sampleCount : 0,
        need_build: type && type === 'refreshIndex'
      })
      if (type && type === 'refreshIndex') {
        this.reloadLoading1 = false
        message = `${this.openSample ? this.$t('reloadSuccess', {tableName: this.tableName}) + (this.$lang === 'zh-cn' ? this.$t('kylinLang.common.dot') : '') + this.$t('sampleSuccess') + (this.$lang === 'zh-cn' ? this.$t('and') : '') + this.$t('structureSuccess') + this.$t('concludingRemarks') : this.$t('reloadSuccess', {tableName: this.tableName}) + (this.$lang === 'zh-cn' ? this.$t('kylinLang.common.dot') : '') + this.$t('structureSuccess') + this.$t('concludingRemarks') + this.$t('kylinLang.common.dot')}`
      } else {
        this.reloadLoading = false
        message = this.$t('reloadSuccess', {tableName: this.tableName}) + (this.openSample ? (this.$lang === 'zh-cn' ? this.$t('kylinLang.common.dot') : '') + this.$t('sampleSuccess') + this.$t('concludingRemarks') : '') + this.$t('kylinLang.common.dot')
      }
      this.$message({
        message,
        type: 'success'
      })
      this.closeHandler(true)
    } catch (e) {
      type && type === 'refreshIndex' ? this.reloadLoading1 = false : this.reloadLoading = false
      // 异常处理
      e && handleError(e)
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.reload-modal {
  .detail-box {
    background: @background-disabled-color;
    padding:10px;
    max-height:94px;
    border: solid 1px @border-color-base;
    margin-top:10px;
    font-size:12px;
  }
  .el-alert {
    .lh20 {
      line-height: 20px;
    }
    .el-alert__content {
      width:100%;
    }
    .mutiple-color-icon {
      margin-top: 2px;
    }
    .detail-text {
      .table-title {
        padding-bottom: 5px;
      }
      &-item {
        position: relative;
        padding-left: 12px;
        line-height: 20px;
        .dot {
          position: absolute;
          left: 0;
          top: 2px;
        }
      }
    }
  }
  .broken-detail {
    .detail-text {
      padding: 10px 14px;
      padding-bottom: 0px;
      padding-left: 29px;
    }
  }
  .samping-box {
    margin-top:18px;
    .desc-icon {
      color: @ke-color-info-secondary;
      margin-left: 5px;
    }
    .lable-text {
      font-weight: @font-medium;
      vertical-align: middle;
    }
    .sample-sub-top{
      color:@text-normal-color;
      margin-top:5px;
    }
  }
}

</style>
