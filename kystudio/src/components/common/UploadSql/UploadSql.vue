<template>
  <div>
    <el-dialog
      :visible="isShow"
      top="5vh"
      width="960px"
      limited-area
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="handleClose"
      :class="['importSqlDialog', {'is-step3': uploadFlag==='step3'}]">
      <span slot="title" class="ksd-title-label">{{uploadTitle}}</span>
      <div class="upload-block" v-if="uploadFlag==='step1'">
        <img src="../../../assets/img/license.png" alt="" v-show="!uploadItems.length">
        <div class="ksd-mt-10 text" v-show="!uploadItems.length">{{$t('pleImport')}}</div>
        <!-- <span class="upload-size-tip" v-show="!uploadItems.length">{{$t('uploadSizeTip')}}</span> -->
        <el-upload
          ref="sqlUpload"
          class="sql-upload"
          :headers="uploadHeader"
          action=""
          :on-remove="handleRemove"
          :on-change="fileItemChange"
          :file-list="uploadItems"
          multiple
          :auto-upload="false">
          <el-button type="primary" size="medium">{{$t('sqlFiles')}}
          </el-button>
          <div :class="['upload-rules', {'check-rules': showUploadRules}]">
            <ul class="rule-list">
              <li :class="['rule-item', item.status === 'success' ? 'is-success' : item.status === 'error' ? 'is-error' : null]" v-for="item in uploadRules" :key="item.type">
                <span v-if="item.status === 'default'">•</span><i class="el-icon-ksd-accept" v-else-if="item.status === 'success'"></i><i v-else class="el-icon-ksd-close"></i>
                {{item.query ? $t(item.text, item.query()): $t(item.text)}}
              </li>
            </ul>
            <p class="rule-info"><span class="icon el-icon-ksd-info ksd-mr-5"></span><span class="tip-text">{{$t('uploadSizeTip')}}</span></p>
          </div>
        </el-upload>
      </div>
      <el-row :gutter="15" v-if="uploadFlag==='step2'">
        <el-col :span="16">
          <div class="clearfix ksd-mb-10">
            <div class="ksd-fleft">
              <div v-if="pagerTableData.length&&whiteSqlData.capable_sql_num" class="ksd-fleft ksd-mr-10">
                <el-button type="primary" size="medium" plain @click="selectAll" v-if="selectSqls.length!==whiteSqlData.capable_sql_num">{{$t('checkAll')}}</el-button><el-button
                type="primary" size="medium" plain @click="cancelSelectAll" v-else>{{$t('cancelAll')}}</el-button>
              </div>

            </div>
            <div class="ksd-fright ksd-inline searchInput" v-if="whiteSqlData.size">
              <el-input v-model="whiteSqlFilter" @input="onWhiteSqlFilterChange" prefix-icon="el-icon-search" :placeholder="$t('kylinLang.common.search')" size="medium"></el-input>
            </div>
          </div>
          <el-table
            :data="pagerTableData"
            ref="multipleTable"
            :empty-text="emptyText"
            @row-click="activeSql"
            @select="handleSelectionChange"
            @select-all="handleSelectAllChange"
            :row-class-name="tableRowClassName"
            class="import-table"
            style="width: 100%">
            <el-table-column type="selection" width="44" align="center" :selectable="selectable"></el-table-column>
            <el-table-column prop="sql" label="SQL" :resizable="false">
              <template slot-scope="props">
                <span class="ksd-nobr-text">{{props.row.sql}}</span>
              </template>
            </el-table-column>
            <el-table-column prop="capable" :label="$t('kylinLang.common.status')" width="80">
              <template slot-scope="props">
                <i :class="{'el-icon-ksd-good_health': props.row.capable, 'el-icon-ksd-error_01': !props.row.capable}"></i>
              </template>
            </el-table-column>
            <el-table-column :label="$t('kylinLang.common.action')" width="80">
              <template slot-scope="props">
                <common-tip :content="$t('kylinLang.common.edit')">
                  <i class="el-icon-ksd-table_edit" :class="{'is-disabled': activeSqlObj && props.row.id === activeSqlObj.id && isEditSql}" @click.stop="editWhiteSql(props.row)"></i>
                </common-tip>
                <common-tip :content="$t('kylinLang.common.drop')">
                  <i class="el-icon-ksd-table_delete ksd-ml-10" @click.stop="delWhiteComfirm(props.row.id)"></i>
                </common-tip>
                </template>
            </el-table-column>
          </el-table>
          <kylin-pager ref="sqlListsPager" :refTag="pageRefTags.sqlListsPager" class="ksd-center ksd-mt-10" :totalSize="filteredDataSize" :curPage="whiteCurrentPage+1" layout="total, prev, pager, next, jumper" v-on:handleCurrentChange='whiteSqlDatasPageChange' :perPageSize="whitePageSize" v-if="filteredDataSize > 0"></kylin-pager>
        </el-col>
        <el-col :span="8">
          <div class="ky-list-title ksd-mt-10 ksd-fs-14">{{$t('sqlBox')}}</div>
          <div element-loading-spinner="el-icon-loading">
            <div v-loading="sqlLoading" class="query_panel_box ksd-mt-10">
              <kylin-editor ref="whiteInputBox" :height="inputHeight" :dragable="false" :readOnly="this.isReadOnly" lang="sql" theme="chrome" v-model="whiteSql" v-if="isShowEditor">
              </kylin-editor>
              <div class="operatorBox" v-if="isEditSql">
                <div class="btn-group ksd-fright ky-no-br-space">
                  <el-button size="small" plain @click="cancelEdit(isWhiteErrorMessage)">{{$t('kylinLang.common.cancel')}}</el-button>
                  <el-button size="small" :loading="validateLoading" @click="validateWhiteSql">{{$t('kylinLang.common.save')}}</el-button>
                </div>
              </div>
            </div>
            <div class="error_messages" v-if="isWhiteErrorMessage">
              <div v-for="(mes, index) in whiteMessages" :key="index">
                <div class="label">{{$t('messages')}}</div>
                <p>{{mes.incapable_reason}}</p>
                <div class="label ksd-mt-10">{{$t('suggestion')}}</div>
                <p>{{mes.suggestion}}</p>
              </div>
            </div>
          </div>
        </el-col>
      </el-row>
      <div :class="['review-sql-model', {'new-model': !isShowTabModels && isShowSuggestModels, 'origin-model': !isShowTabModels && isShowOriginModels}]" v-if="uploadFlag==='step3'">
        <el-alert class="recommendation-alert" :title="$t('recommendationTip')" type="warning" :closable="false" show-icon v-if="isShowOriginModels || isShowTabModels"></el-alert>
        <div class="ky-list-title" v-if="isShowSuggestModels">
          {{$t('newModelList')}} ({{selectModels.length}}/{{suggestModels.length}})
        </div>
        <el-alert v-if="isShowSuggestModels" :title="$t('renameTips')" show-icon :closable="false" type="tip"></el-alert>
        <div class="ky-list-title" v-if="isShowOriginModels">
          {{$t('recommendations')}} ({{selectRecommendsLength}}/{{getRecommendLength}})
        </div>
        <SuggestModel
          v-if="isShowSuggestModels"
          tableRef="modelsTable"
          :suggestModels="suggestModels"
          :maxHeight="430"
          @isValidated="isValidated"
          @getSelectModels="getSelectModels" />
        <SuggestModel
          v-if="isShowOriginModels"
          tableRef="originModelsTable"
          :suggestModels="originModels"
          :maxHeight="365"
          @getSelectRecommends="getSelectRecommends"
          @changeSelectRecommendsLength="changeSelectRecommendsLength"
          :isOriginModelsTable="true" />
        <el-tabs class="upload-tabs" v-model="modelType" v-if="isShowTabModels" @tab-click="changeTabs">
          <el-tab-pane :label="$t('model') + ` (${selectModels.length}/${suggestModels.length})`" name="suggest">
            <SuggestModel
              tableRef="modelsTable"
              ref="suggestModelRef"
              :suggestModels="suggestModels"
              :maxHeight="365"
              @isValidated="isValidated"
              @getSelectModels="getSelectModels" />
          </el-tab-pane>
          <el-tab-pane :label="`${$t('recommendations')} (${selectRecommendsLength}/${getRecommendLength})`" name="origin">
            <SuggestModel
              tableRef="originModelsTable"
              ref="originModelRef"
              :suggestModels="originModels"
              :maxHeight="365"
              @getSelectRecommends="getSelectRecommends"
              @changeSelectRecommendsLength="changeSelectRecommendsLength"
              :isOriginModelsTable="true" />
          </el-tab-pane>
        </el-tabs>
      </div>
      <span slot="footer" class="dialog-footer">
        <div class="ksd-fleft" style="display: flex;" v-if="uploadFlag==='step3' && (isShowSuggestModels || isShowTabModels && modelType === 'suggest')">
          <el-checkbox v-model="addBaseIndex">
            <span>{{$t('addBaseIndexCheckBox')}}</span>
          </el-checkbox>
          <el-tooltip effect="dark" :content="$t('baseIndexTips')" placement="top">
            <i class="el-ksd-icon-more_info_22 ksd-fs-22"></i>
          </el-tooltip>
        </div>
        <div class="ksd-fleft query-count">
          <span v-if="uploadFlag==='step2'">
            <span><i class="el-icon-ksd-good_health"></i>{{whiteSqlData.capable_sql_num}}</span><span class="ksd-ml-10">
            <i class="el-icon-ksd-error_01"></i>{{whiteSqlData.size-whiteSqlData.capable_sql_num}}</span>
            <span class="merge-sql-tip"><span class="divide">|</span><i class="el-icon-ksd-alert"></i>{{$t('mergeSqlTip')}}</span>
          </span>
          <!-- <span class="selected-item" v-if="uploadFlag==='step3'"><i class="el-icon-ksd-alert"></i>{{isShowTabModels ? $t('selectModelsAndRecommends', {models: selectModels.length, recommends: selectRecommendsLength}) : isShowSuggestModels ? $t('selectModelTips', {models: selectModels.length}) : $t('selectRecommendTips', {recommends: selectRecommendsLength})}}</span> -->
          <!-- <span v-if="uploadFlag==='step1'" class="tips">
            <i class="el-icon-ksd-info ksd-fs-14"></i><span class="ksd-fs-12">{{$t('uploadFileTips')}}</span>
          </span> -->
        </div>
        <div class="ky-no-br-space">
          <el-button size="medium" @click="handleCancel" v-if="!isShowOriginModels">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" size="medium" v-if="uploadFlag==='step1'" :loading="importLoading" :disabled="!uploadItems.length||checkedUploadRules"  @click="submitFiles">{{$t('kylinLang.common.next')}}</el-button>
          <el-button type="primary" size="medium" v-if="uploadFlag==='step2'&&!isGenerateModel" :disabled="!finalSelectSqls.length" :loading="submitSqlLoading" @click="submitSqls">{{$t('addTofavorite')}}</el-button>
          <el-button type="primary" size="medium" v-if="uploadFlag==='step2'&&isGenerateModel" :loading="generateLoading" :disabled="!finalSelectSqls.length"  @click="submitSqls">{{$t('kylinLang.common.next')}}</el-button>
          <el-button type="primary" size="medium" v-if="uploadFlag==='step3'&&isGenerateModel" :loading="submitModelLoading" :disabled="!getFinalSelectModels.length || isNameErrorModelExisted" @click="submitModels">{{$t('kylinLang.common.ok')}}</el-button>
          <!-- <el-button type="primary" size="medium" v-if="uploadFlag==='step3'&&isGenerateModel&&isShowOriginModels" @click="handleCloseAcceptModal">{{$t('kylinLang.common.ok')}}</el-button> -->
        </div>
      </span>
    </el-dialog>
    <el-dialog
      :visible="isConvertShow"
      width="480px"
      limited-area
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="handleConvertClose"
      class="convertDialog">
      <span slot="title" class="ky-list-title">{{$t('kylinLang.common.tip')}}</span>
      <div>{{$t('existedAnsweredModels')}}</div>
      <span slot="footer" class="dialog-footer ky-no-br-space">
        <el-button @click="convertSqlsSubmit(false)" :loading="cancelConvertLoading" size="medium">{{$t('noConvert')}}</el-button>
        <el-button type="primary" @click="convertSqlsSubmit(true)" :loading="convertLoading" size="medium">{{$t('convert')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { handleSuccessAsync, handleError, ArrayFlat } from '../../../util/index'
import { handleSuccess, kylinConfirm, kylinWarn } from '../../../util/business'
import SuggestModel from './SuggestModel.vue'
import { pageRefTags, pageCount } from '../../../config'

vuex.registerModule(['modals', 'UploadSqlModel'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('UploadSqlModel', {
      isShow: state => state.isShow,
      isGenerateModel: state => state.isGenerateModel,
      title: state => state.title,
      callback: state => state.callback
    }),
    ...mapState({
      favoriteImportSqlMaxSize: state => +state.system.favoriteImportSqlMaxSize
    })
  },
  methods: {
    ...mapActions({
      importSqlFiles: 'IMPORT_SQL_FILES',
      formatSql: 'FORMAT_SQL',
      addToFavoriteList: 'ADD_TO_FAVORITE_LIST',
      validateWhite: 'VALIDATE_WHITE_SQL',
      suggestModel: 'SUGGEST_MODEL',
      saveSuggestModels: 'SAVE_SUGGEST_MODELS',
      validateModelName: 'VALIDATE_MODEL_NAME',
      suggestIsByAnswered: 'SUGGEST_IS_BY_ANSWERED'
    }),
    // Store方法注入
    ...mapMutations('UploadSqlModel', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  components: {
    SuggestModel
  },
  locales
})
export default class UploadSqlModel extends Vue {
  pageRefTags = pageRefTags
  ArrayFlat = ArrayFlat
  uploadFlag = 'step1'
  importLoading = false
  messageInstance = null
  uploadItems = []
  pagerTableData = []
  isWhiteErrorMessage = false
  whiteMessages = []
  validateLoading = false
  isReadOnly = true
  whiteSql = ''
  isEditSql = false
  submitSqlLoading = false
  whiteSqlFilter = ''
  activeSqlObj = null
  whiteSqlData = null
  sqlLoading = false
  inputHeight = 479
  selectSqls = []
  filteredDataSize = 0
  whiteCurrentPage = 0
  timer = null
  whitePageSize = pageCount
  isShowEditor = true
  sqlFormatterObj = {}
  generateLoading = false
  submitModelLoading = false
  isNameErrorModelExisted = false
  suggestModels = []
  selectModels = []
  originModels = []
  selectRecommends = []
  selectOriginModels = []
  selectRecommendsLength = 0
  modelType = 'suggest'
  isConvertShow = false
  convertLoading = false
  cancelConvertLoading = false
  convertSqls = []
  uploadRules = {
    fileFormat: { type: 'fileFormat', text: 'uploadRule1', status: 'default' },
    // unValidSqls: {type: 'unValidSqls', text: 'uploadRule2', status: 'default'},
    totalSize: { type: 'totalSize', text: 'uploadRule3', status: 'default' }
    // sqlSizes: {type: 'sqlSizes', text: 'uploadRule4', status: 'default', query: this.queryHandler}
  }

  showUploadRules = false
  wrongFormatFile = []
  addBaseIndex = true
  errMsgInstance = null
  handleClose () {
    this.hideModal()
    this.resetModalForm()
    this.resetImport()
  }

  // 点击取消按钮
  handleCancel () {
    if (this.uploadFlag === 'step3') {
      this.$confirm(this.$t('closeSqlDialogTip'), this.$t('kylinLang.common.tip'), {
        confirmButtonText: this.$t('confirmCancel'),
        cancelButtonText: this.$t('backToEdit'),
        centerButton: true,
        type: 'warning'
      }).then(() => {
        this.handleClose()
      }).catch(() => {
      })
    } else {
      this.handleClose()
    }
  }

  resetImport () {
    this.uploadFlag = 'step1'
    this.uploadItems = []
    this.activeSqlObj = null
    this.isEditSql = false
    this.pagerTableData = []
    this.whiteSqlFilter = ''
    this.importLoading = false
    this.sqlFormatterObj = {}
    this.generateLoading = false
    this.suggestModels = []
    this.selectModels = []
    this.selectOriginModels = []
    this.selectRecommends = []
    this.originModels = []
    this.submitModelLoading = false
    this.isNameErrorModelExisted = false
    this.modelType = 'suggest'
    this.selectRecommendsLength = 0
    this.messageInstance && this.messageInstance.close()
    this.wrongFormatFile = []
    this.showUploadRules = false
    for (const item in this.uploadRules) {
      this.uploadRules[item].status = 'default'
    }
  }

  get emptyText () {
    return this.whiteSqlFilter ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  get uploadTitle () {
    if (this.isGenerateModel) {
      if (this.uploadFlag === 'step1') {
        return this.title || this.$t('modelFromSql')
      } else if (this.uploadFlag === 'step2') {
        return this.$t('generateModel')
      } else {
        return this.$t('preview')
      }
    } else {
      return this.$t('importSql')
    }
  }

  get isShowSuggestModels () {
    return this.suggestModels.length > 0 && this.originModels.length === 0
  }

  get isShowOriginModels () {
    return this.suggestModels.length === 0 && this.originModels.length > 0
  }

  get isShowTabModels () {
    return this.suggestModels.length > 0 && this.originModels.length > 0
  }

  get getRecommendLength () {
    return this.ArrayFlat(this.originModels.map(it => it.rec_items)).length
  }

  @Watch('inputHeight')
  onHeightChange (val) {
    if (val) {
      this.isShowEditor = false
      this.$nextTick(() => {
        this.isShowEditor = true
      })
    }
  }

  queryHandler () {
    return { size: this.favoriteImportSqlMaxSize }
  }

  // 切换预览界面tabs
  changeTabs (e) {
    if (e.name === 'suggest') {
      this.$nextTick(() => {
        this.$refs.suggestModelRef.initClickItem()
      })
    } else {
      this.$nextTick(() => {
        this.$refs.originModelRef.initClickItem()
      })
    }
  }

  changeSelectRecommendsLength (len) {
    this.selectRecommendsLength = len
  }

  tableRowClassName ({ row, rowIndex }) {
    if (this.activeSqlObj && row.id === this.activeSqlObj.id) {
      return 'active-row'
    }
    return ''
  }

  showLoading () {
    this.sqlLoading = true
  }

  hideLoading () {
    this.sqlLoading = false
  }

  onWhiteSqlFilterChange () {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.whiteSqlDatasPageChange(0)
    }, 500)
  }

  delWhiteComfirm (id) {
    if (this.isEditSql) {
      kylinWarn(this.$t('editSqlTips'))
      return
    }
    kylinConfirm(this.$t('delSql'), { centerButton: true }, this.$t('delSqlTitle')).then(() => {
      this.delWhite(id)
    })
  }

  getSelectModels (models) {
    this.selectModels = models
  }

  getSelectOriginModels (models) {
    this.selectOriginModels = models
  }

  getSelectRecommends (rec) {
    this.selectRecommends = rec
  }

  get getFinalSelectModels () {
    return [...this.selectModels, ...this.selectRecommends]
  }

  isValidated (isNameErrorModelExisted) {
    this.isNameErrorModelExisted = isNameErrorModelExisted
  }

  delWhite (id) {
    for (const key in this.whiteSqlData.data) {
      if (this.whiteSqlData.data[key].id === id) {
        if (this.whiteSqlData.data[key].capable) {
          this.whiteSqlData.capable_sql_num--
        }
        this.whiteSqlData.size--
        this.whiteSqlData.data.splice(key, 1)
        this.$nextTick(() => {
          this.whiteSqlDatasPageChange(this.whiteCurrentPage)
        })
        break
      }
    }
    for (const index in this.selectSqls) {
      if (this.selectSqls[index].id === id) {
        this.selectSqls.splice(index, 1)
        break
      }
    }
  }

  whiteSqlDatasPageChange (currentPage, pageSize) {
    const size = pageSize || +localStorage.getItem(this.pageRefTags.sqlListsPager) || pageCount
    this.whiteCurrentPage = currentPage
    this.whitePageSize = size
    const filteredData = this.whiteFilter(this.whiteSqlData.data)
    this.filteredDataSize = filteredData.length
    this.pagerTableData = filteredData.slice(currentPage * size, (currentPage + 1) * size)
    if (this.filteredDataSize) {
      this.$nextTick(() => {
        this.activeSql(this.pagerTableData[0])
      })
      const targetSelectSqls = []
      this.pagerTableData.forEach((item) => {
        let index = -1
        for (const key in this.selectSqls) {
          if (this.selectSqls[key].id === item.id) {
            index = key
            break
          }
        }
        if (index !== -1) {
          targetSelectSqls.push(item)
        }
      })
      this.$nextTick(() => {
        this.toggleSelection(targetSelectSqls)
      })
    } else {
      this.whiteSql = ''
      this.$refs.whiteInputBox && this.$refs.whiteInputBox.$refs.kylinEditor && this.$refs.whiteInputBox.$refs.kylinEditor.editor.setValue('')
      this.activeSqlObj = null
      this.isEditSql = false
      this.whiteMessages = []
      this.isWhiteErrorMessage = false
      this.inputHeight = 479
    }
    this.$nextTick(() => {
      this.$refs.multipleTable && this.$refs.multipleTable.doLayout()
    })
  }

  selectAll () {
    this.selectSqls = this.whiteSqlData.data.filter((item) => {
      return item.capable
    })
    this.selectPagerSqls(true)
  }

  cancelSelectAll () {
    this.selectSqls = []
    this.selectPagerSqls(false)
  }

  handleSelectionChange (val, row) {
    this.mergeSelectSqls(row)
  }

  handleSelectAllChange (val) {
    if (val.length) {
      val.forEach((item) => {
        this.mergeSelectSqls(item, 'batchAdd')
      })
    } else {
      this.pagerTableData.forEach((item) => {
        this.mergeSelectSqls(item, 'batchRemove')
      })
    }
  }

  // 单选一条时：toggle row; batchFlag有值时：批量添加rows或者批量去除rows
  mergeSelectSqls (row, batchFlag) {
    let index = -1
    for (const key in this.selectSqls) {
      if (this.selectSqls[key].id === row.id) {
        index = key
        break
      }
    }
    if (index === -1) {
      if (batchFlag !== 'batchRemove') {
        this.selectSqls.push(row)
      }
    } else {
      if (batchFlag !== 'batchAdd') {
        this.selectSqls.splice(index, 1)
      }
    }
  }

  handleCloseAcceptModal () {
    this.hideModal()
    this.resetModalForm()
    this.resetImport()
    this.$emit('reloadModelList')
  }

  submitModels () {
    this.submitModelLoading = true
    const models = [...this.selectModels.map(it => ({ ...it, with_base_index: this.addBaseIndex }))]
    models.forEach(obj => {
      delete obj.isChecked
      delete obj.isNameError
    })
    const recommends = [...this.selectRecommends]
    recommends.forEach(obj => {
      delete obj.isChecked
    })
    const data = {
      new_models: models,
      reused_models: recommends
    }
    this.saveSuggestModels({ project: this.currentSelectedProject, ...data }).then((res) => {
      handleSuccess(res, (data) => {
        // this.$message.success(this.$t('kylinLang.common.actionSuccess'))
        this.submitModelLoading = false
        this.hideModal()
        this.$emit('reloadModelList')
        if (this.selectModels.length && this.selectRecommendsLength) {
          this.$confirm(this.$t('successCreateModelsAndRecommends', { models: this.selectModels.length, recommends: this.selectRecommendsLength }), this.$t('importSuccess'), {
            confirmButtonText: this.$t('kylinLang.common.ok'),
            showCancelButton: false,
            type: 'success'
          })
        } else if (this.selectModels.length) {
          this.$message.success(this.$t('successCreateModels', { models: this.selectModels.length }))
        } else {
          this.$confirm(this.$t('successCreateRecommends', { recommends: this.selectRecommendsLength }), this.$t('importSuccess'), {
            confirmButtonText: this.$t('kylinLang.common.ok'),
            showCancelButton: false,
            type: 'success'
          })
        }
      })
    }, (res) => {
      handleError(res)
      this.submitModelLoading = false
      this.hideModal()
    })
  }

  async submitSqls () {
    const unCheckedSQL = this.whiteSqlData.capable_sql_num - this.finalSelectSqls.length
    if (this.isEditSql) {
      await kylinWarn(this.$t('editSqlTips'), {
        type: 'warning',
        closeOnClickModal: false,
        confirmButtonText: this.$t('confirmEditSqlText')
      })
      try {
        await this.validateWhiteSql()
      } catch (e) {
        setTimeout(() => {
          this.$el.querySelector('.error_messages')?.scrollIntoView()
        }, 50)
        return
      }
    }
    if (unCheckedSQL) {
      kylinConfirm(this.$t('submitConfirm', { unCheckedSQL }), { cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('kylinLang.common.submit'), type: 'warning', centerButton: true }).then(() => {
        this.submit()
      })
    } else {
      this.submit()
    }
  }

  submit () {
    const sqlsData = this.finalSelectSqls
    const sqls = sqlsData.map((item) => {
      return item.sql
    })
    if (!this.isGenerateModel) {
      this.submitSqlLoading = true
      this.addToFavoriteList({ project: this.currentSelectedProject, sqls }).then((res) => {
        handleSuccess(res, (data) => {
          this.submitSqlLoading = false
          const importedMsg = this.$t('addSuccess', { sqlsNum: sqls.length, importedNum: data.imported })
          const existedMsg1 = data.imported < sqls.length ? this.$t('existedMsg1', { existedNum: sqls.length - data.imported }) : ''
          const existedMsg2 = data.blacklist ? this.$t('existedMsg2', { blackNum: data.blacklist }) : ''
          this.$alert(importedMsg + existedMsg1 + existedMsg2 + this.$t('end'), this.$t('kylinLang.common.notice'), {
            confirmButtonText: this.$t('kylinLang.common.ok'),
            iconClass: 'el-icon-info primary'
          })
          sqlsData.forEach((item) => {
            this.delWhite(item.id)
          })
          this.$emit('reloadListAndSize')
          this.hideModal()
        })
      }, (res) => {
        handleError(res)
        this.submitSqlLoading = false
        this.hideModal()
      })
    } else {
      this.generateLoading = true
      this.suggestIsByAnswered({ project: this.currentSelectedProject, sqls }).then((res) => {
        handleSuccess(res, (data) => {
          if (!data) {
            this.getSuggestModels(sqls, data)
          } else {
            this.isConvertShow = true
            this.convertSqls = sqls
          }
        })
      }, (res) => {
        handleError(res)
        this.generateLoading = false
      })
    }
  }

  handleConvertClose () {
    this.isConvertShow = false
    this.convertSqls = []
    this.generateLoading = false
  }

  convertSqlsSubmit (reuseExistedModel) {
    if (reuseExistedModel) {
      this.convertLoading = true
    } else {
      this.cancelConvertLoading = true
    }
    this.getSuggestModels(this.convertSqls, reuseExistedModel)
  }

  getSuggestModels (sqls, reuseExistedModel) {
    this.suggestModel({ project: this.currentSelectedProject, sqls, reuse_existed_model: reuseExistedModel }).then((res) => {
      handleSuccess(res, (data) => {
        if (!this.isShow) return
        // 优化建议超过 1000 让其重新上传sql
        if (data.reused_models.length > 1000) {
          this.$confirm(this.$t('recommendsOverSizeTip'), this.$t('recommendsOverSizeTitle'), {
            confirmButtonText: this.$t('kylinLang.common.ok'),
            showCancelButton: false,
            showClose: false,
            type: 'warning'
          }).then(() => {
            this.generateLoading = false
            this.convertLoading = false
            this.cancelConvertLoading = false
            this.isConvertShow = false
          })
          return
        }
        this.suggestModels = data.new_models.map((d) => {
          d.isChecked = true
          d.isNameError = false
          return d
        })
        this.originModels = data.reused_models.map((d) => {
          d.isChecked = true
          return d
        })
        this.generateLoading = false
        this.convertLoading = false
        this.cancelConvertLoading = false
        this.isConvertShow = false
        this.uploadFlag = 'step3'
      })
    }, (res) => {
      handleError(res)
      this.generateLoading = false
      this.convertLoading = false
      this.cancelConvertLoading = false
      this.isConvertShow = false
    })
  }

  get finalSelectSqls () {
    let finalSqls = []
    finalSqls = this.selectSqls.filter((item) => {
      return item.sql.indexOf(this.whiteSqlFilter) !== -1
    })
    return finalSqls
  }

  selectPagerSqls (isSelectAll) {
    const selectedRows = isSelectAll
      ? this.pagerTableData.filter((item) => {
        return item.capable
      })
      : []
    this.$nextTick(() => {
      this.toggleSelection(selectedRows)
    })
  }

  whiteFilter (data) {
    return data.filter((sqlObj) => {
      return sqlObj.sql.toLowerCase().indexOf(this.whiteSqlFilter.toLowerCase()) !== -1
    })
  }

  toggleSelection (rows) {
    if (rows && rows.length) {
      this.$refs.multipleTable.clearSelection()
      rows.forEach(row => {
        this.$refs.multipleTable.toggleRowSelection(row)
      })
    } else {
      this.$refs.multipleTable && this.$refs.multipleTable.clearSelection()
    }
  }

  get uploadHeader () {
    if (this.$store.state.system.lang === 'en') {
      return { 'Accept-Language': 'en' }
    } else {
      return { 'Accept-Language': 'cn' }
    }
  }

  get checkedUploadRules () {
    return Object.keys(this.uploadRules).filter(item => this.uploadRules[item].status === 'error').length > 0
  }

  async editWhiteSql (sqlObj) {
    // 编辑模式下，不需要再进入当前SQL的编辑
    if (this.isEditSql && sqlObj.id === this.activeSqlObj.id) {
      return
    }
    if (this.isEditSql) {
      await kylinWarn(this.$t('editSqlTips'), {
        type: 'warning',
        closeOnClickModal: false,
        confirmButtonText: this.$t('confirmEditSqlText')
      })
      await this.validateWhiteSql()
    }
    this.isEditSql = true
    this.inputHeight = 382
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 382
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 382 - 140
    }
    let formatterSql
    if (this.sqlFormatterObj[sqlObj.id]) {
      formatterSql = this.sqlFormatterObj[sqlObj.id]
      this.whiteSql = formatterSql
      this.$refs.whiteInputBox && this.$refs.whiteInputBox.$emit('input', formatterSql)
    } else {
      this.showLoading()
      const res = await this.formatSql({ sqls: [sqlObj.sql] })
      const data = await handleSuccessAsync(res)
      formatterSql = data[0]
      this.sqlFormatterObj[sqlObj.id] = formatterSql
      this.$refs.whiteInputBox && this.$refs.whiteInputBox.$emit('input', formatterSql)
      this.hideLoading()
    }
    this.activeSqlObj = sqlObj
    this.isReadOnly = false
  }

  async activeSql (sqlObj) {
    if (this.isEditSql) {
      await kylinWarn(this.$t('editSqlTips'), {
        type: 'warning',
        closeOnClickModal: false,
        confirmButtonText: this.$t('confirmEditSqlText')
      })
      await this.validateWhiteSql()
    }
    this.activeSqlObj = sqlObj
    this.isEditSql = false
    this.isReadOnly = true
    if (sqlObj.capable) {
      this.isWhiteErrorMessage = false
      this.inputHeight = 479
    } else {
      this.isWhiteErrorMessage = true
      this.inputHeight = 479 - 140
      this.whiteMessages = sqlObj.sql_advices
    }
    let formatterSql
    if (this.sqlFormatterObj[sqlObj.id]) {
      formatterSql = this.sqlFormatterObj[sqlObj.id]
      this.$refs.whiteInputBox && this.$refs.whiteInputBox.$emit('input', formatterSql)
      this.whiteSql = formatterSql
    } else {
      this.showLoading()
      const res = await this.formatSql({ sqls: [sqlObj.sql] })
      const data = await handleSuccessAsync(res)
      formatterSql = data[0]
      this.sqlFormatterObj[sqlObj.id] = formatterSql
      this.$refs.whiteInputBox && this.$refs.whiteInputBox.$emit('input', formatterSql)
      this.hideLoading()
    }
  }

  fileItemChange (file, fileList) {
    if (this.errMsgInstance) {
      this.errMsgInstance.close()
    }
    this.wrongFormatFile = []
    for (const item in this.uploadRules) {
      this.uploadRules[item].status = 'default'
    }
    this.checkUploadFiles(fileList)
    this.flagFiles()
  }

  handleRemove (file, fileList) {
    if (this.errMsgInstance) {
      this.errMsgInstance.close()
    }
    this.messageInstance && this.messageInstance.close()
    this.uploadItems = fileList
    // let totalSize = 0
    // this.uploadItems.forEach((item) => {
    //   totalSize = totalSize + item.size
    // })
    // if (totalSize > 5 * 1024 * 1024) { // 后端限制不能大于5M
    //   this.messageInstance = this.$message.warning(this.$t('filesSizeError'))
    // }
    for (const item in this.uploadRules) {
      this.uploadRules[item].status = 'default'
    }
    this.wrongFormatFile = []
    if (fileList.length) {
      this.checkUploadFiles(fileList)
      this.flagFiles()
    }
  }

  selectable (row) {
    return row.capable
  }

  cancelEdit (isErrorMes) {
    this.isEditSql = false
    this.inputHeight = isErrorMes ? 479 - 140 : 479
    this.whiteSql = this.sqlFormatterObj[this.activeSqlObj.id]
    this.activeSqlObj = null
    this.isReadOnly = true
  }

  validateWhiteSql () {
    return new Promise((resolve, reject) => {
      this.validateLoading = true
      this.validateWhite({ sql: this.whiteSql, project: this.currentSelectedProject }).then((res) => {
        handleSuccess(res, (data, code) => {
          if (code !== '000') {
            return reject(new Error('error requset'))
          }
          this.validateLoading = false
          if (data.capable) {
            this.$message.success(this.$t('kylinLang.common.actionSuccess'))
            this.whiteMessages = []
            this.inputHeight = 479
            this.isWhiteErrorMessage = false
            this.isEditSql = false
            this.isReadOnly = true
            for (const key in this.whiteSqlData.data) {
              if (this.whiteSqlData.data[key].id === this.activeSqlObj.id) {
                this.whiteSqlData.data[key].sql = this.whiteSql
                this.sqlFormatterObj[this.activeSqlObj.id] = this.whiteSql
                if (!this.whiteSqlData.data[key].capable) {
                  this.whiteSqlData.data[key].capable = true
                  this.whiteSqlData.data[key].sql_advices = []
                  this.whiteSqlData.capable_sql_num++
                }
                break
              }
            }
            resolve()
          } else {
            this.whiteMessages = data.sql_advices
            this.inputHeight = 479 - 140
            this.isWhiteErrorMessage = true
            reject(new Error('uncapable data'))
          }
        })
      }, (res) => {
        this.validateLoading = false
        handleError(res)
        reject(res)
      })
    })
  }

  checkUploadFiles (fileList) {
    let totalSize = 0
    this.uploadItems = fileList.map((item) => {
      totalSize += item.size
      item.raw && (item.raw.status = item.status)
      return item.raw ? item.raw : item
    })
    const fileNames = fileList.map(it => it.name)
    if (totalSize > 5 * 1024 * 1024) { // 后端限制不能大于5M
      this.uploadRules.totalSize.status = 'error'
    } else {
      this.uploadRules.totalSize.status = 'success'
    }
    if (fileNames.filter(name => name.toLowerCase().indexOf('.txt') === -1 && name.toLowerCase().indexOf('.sql') === -1).length > 0) {
      this.uploadRules.fileFormat.status = 'error'
    } else {
      this.uploadRules.fileFormat.status = 'success'
    }
  }

  uploadErrorMsg () {
    this.errMsgInstance = this.$message({
      message: this.$t('uploadErrorMsg', { maxCount: this.favoriteImportSqlMaxSize }),
      type: 'error'
    })
  }

  submitFiles () {
    const formData = new FormData() // 利用H5 FORMDATA 同时传输多文件和数据
    this.uploadItems.filter((item) => {
      return item.name.toLowerCase().indexOf('.txt') !== -1 || item.name.toLowerCase().indexOf('.sql') !== -1
    }).forEach(file => {
      formData.append('files', file)
    })
    this.importLoading = true
    this.importSqlFiles({ project: this.currentSelectedProject, formData }).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        if (!this.isShow) return
        this.checkUploadFiles(this.uploadItems)
        this.showUploadRules = true
        this.importLoading = false
        // this.uploadRules.sqlSizes.status = data.size > this.favoriteImportSqlMaxSize ? 'error' : 'success'
        // this.uploadRules.unValidSqls.status = data.wrong_format_file && data.wrong_format_file.length ? 'error' : 'success'
        if (data.size > this.favoriteImportSqlMaxSize) {
          return this.uploadErrorMsg()
        }
        this.wrongFormatFile = data.wrong_format_file || []
        if (Object.keys(this.uploadRules).filter(item => this.uploadRules[item].status === 'success').length === Object.keys(this.uploadRules).length) {
          this.uploadFlag = 'step2'
          this.whiteSqlData = data
          this.selectAll()
          this.whiteSqlDatasPageChange(0)
          if (msg) {
            this.$message.warning(msg)
          }
        }
        this.flagFiles()
      })
    }, (res) => {
      handleError(res)
      this.importLoading = false
    })
  }

  // 标记有问题的文件
  flagFiles () {
    this.$nextTick(() => {
      const doms = this.$refs.sqlUpload ? this.$refs.sqlUpload.$el.querySelectorAll('.el-upload-list__item') : []
      const indexs = []
      for (const i in this.uploadItems) {
        const target = this.uploadItems[i]
        if (target.name.toLowerCase().indexOf('.txt') === -1 && target.name.toLowerCase().indexOf('.sql') === -1) {
          indexs.push(+i)
        }
        if (this.wrongFormatFile.includes(target.name)) {
          indexs.push(+i)
        }
      }
      doms.forEach((item, index) => {
        indexs.includes(index) && (doms[index].style.cssText = 'border: 1px solid #E73371;')
        !indexs.includes(index) && (doms[index].style.cssText = '')
      })
    })
  }

  mounted () {
  }
}
</script>
<style lang="less">
  @import '../../../assets/styles/variables.less';
  .sql-upload {
    .el-upload-list__item {
      line-height: 2;
      background: @regular-background-color;
      .el-icon-document {
        color: @text-disabled-color;
      }
      .el-icon-close {
        color: @text-title-color;
        top: 8px;
      }
    }
  }
  .importSqlDialog .el-dialog__body .upload-block .sql-upload .el-upload-list .el-upload-list__item:hover {
    background-color: @base-background-color-1;
  }
  .importSqlDialog {
    &.is-step3 {
      .el-dialog__body {
        padding: 20px 20px 0;
      }
    }
    .query_panel_box {
      border: none;
    }
    .ksd-null-pic-text {
      margin: 122.5px 0;
    }
    .query-count {
      color: @text-title-color;
      line-height: 30px;
      height: 30px;
      display: inline-block;
      .merge-sql-tip {
        font-size: 12px;
        color: @text-normal-color;
        .divide {
          margin-right: 10px;
          margin-left: 5px;
          color: @line-border-color3;
        }
        i {
          margin-right: 5px;
          color: @text-disabled-color;
        }
      }
      .selected-item {
        color: @text-normal-color;
        font-size: 12px;
        .el-icon-ksd-alert {
          margin-right: 5px;
          color: @text-disabled-color;
        }
      }
    }
    .tips {
      span {
        margin-left: 5px;
        color: @text-normal-color;
      }
      i {
        color: @text-disabled-color;
      }
    }
    .el-icon-ksd-good_health {
      color: @normal-color-1;
      margin-right: 2px;
      font-size: 14px;
    }
    .el-icon-ksd-error_01 {
      color: @error-color-1;
      margin-right: 2px;
      font-size: 14px;
    }
    .el-dialog__body {
      // min-height: 460px;
      height: 460px;
      .model-table {
        .rename-error {
          color: @error-color-1;
          font-size: 12px;
          line-height: 1.2;
        }
        .name-error {
          .el-input__inner {
            border-color: @error-color-1;
          }
        }
      }
      .import-table {
        .cell {
          height: 23px;
        }
        .active-row {
          background-color: @base-color-9;
        }
        .el-table__row {
          cursor: pointer;
        }
        .el-icon-ksd-table_edit,
        .el-icon-ksd-table_delete {
          &:hover {
            color: @base-color;
          }
          &.is-disabled {
            color: @color-text-disabled;
          }
        }
      }
      .new_sql_status {
        border-color: @base-color;
        .ace-chrome {
          border-color: @base-color;
          border-bottom-color: @text-secondary-color;
        }
      }
      .operatorBox{
        margin-top: 0;
        padding: 10px;
        display: block;
        overflow: hidden;
      }
      .error_messages {
        height: 130px;
        border: 1px solid @ke-border-secondary;
        border-radius: 2px;
        font-size: 12px;
        margin-top: 10px;
        padding: 10px;
        box-sizing: border-box;
        overflow-y: auto;
        .label {
          color: @error-color-1;
        }
      }
      .smyles_editor_wrap .smyles_dragbar {
        height: 0;
      }
      .upload-block {
        text-align: center;
        margin: 0 auto;
        img {
          margin-top: 100px;
        }
        .text {
          font-size: 14px;
          color: @text-title-color;
          line-height: 24px;
          font-weight: bold;
        }
        .upload-size-tip {
          width: 440px;
          display: inline-block;
          color: @text-normal-color;
          font-size: 12px;
          margin-top: 5px;
        }
        .el-upload {
          margin-top: 15px;
        }
        .el-upload-list {
          width: 300px;
          margin: 0 auto;
          text-align: left;
          .el-upload-list__item {
            .el-upload-list__item-name {
              margin-right: 20px;
              &:hover {
                text-decoration: none;
                color: inherit;
              }
            }
            &:hover {
              background-color: inherit;
            }
          }
        }
        .upload-rules {
          max-width: 300px;
          margin: auto;
          background-color: @ke-background-color-base-1;
          font-size: 12px;
          margin-top: 10px;
          padding: 5px 20px;
          color: @text-normal-color;
          line-height: 20px;
          &.check-rules {
            background-color: transparent;
          }
          .rule-list {
            display: inline-block;
            text-align: left;
          }
          .rule-item {
            &.is-success {
              color: @ke-color-success;
            }
            &.is-error {
              color: @ke-color-danger;
            }
          }
          .rule-info {
            margin-top: 10px;
            margin-left: -7px;
            text-align: left;
            .icon {
              display: inline-block;
            }
            .tip-text {
              display: inline-block;
              width: calc(~'100% - 20px');
              vertical-align: top;
            }
          }
        }
      }
    }
    .filter-tags {
      margin-bottom: 10px;
      padding: 6px 10px;
      box-sizing: border-box;
      position: relative;
      background: @background-disabled-color;
      .filter-tags-layout {
        width: calc(~'100% - 100px');
        display: inline-block;
        line-height: 34px;
      }
      .el-tag {
        margin-left: 5px;
        &:first-child {
          margin-left: 0;
        }
      }
      .clear-all-filters {
        position: absolute;
        top: 8px;
        right: 10px;
        font-size: 14px;
        color: @base-color;
        cursor: pointer;
      }
    }
    .recommendation-alert {
      margin-bottom: 10px;
    }
    .review-sql-model {
      height: 100%;
      .upload-tabs {
        height: calc(~'100% - 47px');
        .el-tabs__header {
          margin-bottom: 0;
        }
        .el-tabs__content {
          height: calc(~'100% - 30px');
          #pane-suggest {
            height: 100%
          }
          #pane-origin {
            height: 100%
          }
        }
      }
    }
    .review-sql-model.new-model {
      .ky-list-title {
        padding: 4px 0;
        box-sizing: border-box;
        // border-bottom: 1px solid #dddddd;
      }
      .model-layout {
        height: calc(~'100% - 30px');
        // .model-table {
        //   height: 100%;
        //   .el-table__body-wrapper {
        //     max-height: calc(~'100% - 36px');
        //     overflow-y: auto;
        //   }
        // }
      }
    }
    .review-sql-model.origin-model {
      .ky-list-title {
        padding: 4px 0;
        box-sizing: border-box;
        border-bottom: 1px solid @ke-border-secondary;
      }
      .model-layout {
        height: calc(~'100% - 75px');
      }
    }
  }
</style>
