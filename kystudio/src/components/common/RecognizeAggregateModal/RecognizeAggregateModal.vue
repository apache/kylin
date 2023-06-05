<template>
    <el-dialog class="recognize-aggregate-modal" width="960px"
      append-to-body
      :title="$t('title')"
      :visible="isShow"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      :before-close="handleCancel"
      @closed="handleClosed"
    >
      <div class="dialog-content">
        <div class="recognize-area">
          <div class="recognize-header" v-if="errorLines.length">
            <div class="result-counter">
              <span class="error">{{$tc('errorCount', errorCount, { count: errorCount })}}</span>
              <el-tooltip :content="$t('repeatTip')" placement="top">
                <span class="warning">{{$tc('repeatCount', repeatCount, { count: repeatCount })}}</span>
              </el-tooltip>
            </div>
            <div class="result-actions">
              <el-button icon-button text type="primary" size="mini" icon="el-ksd-n-icon-arrow-up-outlined" @click="handlePrevious" /><!--
           --><el-button icon-button text type="primary" size="mini" icon="el-ksd-n-icon-arrow-down-outlined" @click="handleNext" />
            </div>
          </div>
          <AceEditor :key="isShow" :placeholder="'123'" class="text-input" ref="editorRef" :value="form.text" @input="handleInputText" />
          <div class="actions">
            <el-tooltip :content="$t('dexecute')" placement="left">
              <el-button icon-button class="recognize" size="small" type="primary" icon="el-icon-caret-right" :disabled="!form.text" @click="handleRecognize" />
            </el-tooltip>
          </div>
        </div>
        <div class="recognize-results">
          <template v-if="form.dimensions.length">
            <div class="results-header">
              {{$tc('selectedDimensionCount', selectedDimensionCount, { count: selectedDimensionCount })}}
            </div>
            <div class="list-actions">
              <el-checkbox :key="isSelectAll" :indeterminate="isIndeterminate" :checked="isSelectAll" @change="handleSelectAll" />
              <div class="header-dimension-name">{{$t('dimensionName')}}</div>
              <div class="header-data-type">{{$t('dataType')}}</div>
            </div>
            <RecycleScroller
              class="dimension-list"
              :items="form.dimensions"
              :item-size="37"
              key-field="value"
            >
              <template slot-scope="{ item }">
                <div class="dimension" @click="handleCheckDimension(item)">
                  <el-checkbox :key="item.isChecked" :checked="item.isChecked" @change="handleCheckDimension(item)" />
                  <span class="name">{{ item.label }}</span>
                  <span class="data-type">{{ item.dataType }}</span>
                  <div v-if="item.isDisabled" class="current-used-mask" />
                </div>
              </template>
            </RecycleScroller>
          </template>
          <div class="all-dimension-error" v-else-if="isAllDimensionError">
            <i class="el-ksd-n-icon-error-circle-filled" />
            <span>{{$t('recognizeFailed')}}</span>
          </div>
          <EmptyData v-else :showImage="false" :content="$t('emptyText')" />
        </div>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="handleCancel">
          {{$t('kylinLang.common.cancel')}}
        </el-button>
        <el-button type="primary" size="medium" :disabled="!selectedDimensionCount" @click="handleSubmit">
          {{$t('kylinLang.common.submit')}}
        </el-button>
      </div>
    </el-dialog>
  </template>
  
  <script>
  import Vue from 'vue'
  import AceEditor from 'vue2-ace-editor'
  import { Component, Watch } from 'vue-property-decorator'
  import { RecycleScroller } from 'vue-virtual-scroller'
  import { mapState, mapMutations, mapGetters } from 'vuex'
  import locales from './locales'
  import vuex from '../../../store'
  import EmptyData from '../../common/EmptyData/EmptyData'
  import store, { types, getInitialErrors, ALERT_STATUS } from './store'
  import { collectErrorsInEditor, refreshEditor, scrollToLineAndHighlight, updatePlaceHolder, ERROR_TYPE } from './handler'
  import { AGGREGATE_TYPE } from '../../../config'
  vuex.registerModule(['modals', 'RecognizeAggregateModal'], store)
  @Component({
    components: {
      AceEditor,
      EmptyData,
      RecycleScroller
    },
    computed: {
      ...mapState('RecognizeAggregateModal', {
        isShow: state => state.isShow,
        type: state => state.type,
        status: state => state.status,
        form: state => state.form,
        errors: state => state.errors,
        errorLines: state => state.errorLines,
        errorInEditor: state => state.errorInEditor,
        errorCursor: state => state.errorCursor,
        callback: state => state.callback
      }),
      ...mapGetters('RecognizeAggregateModal', [
        'modelDimensions',
        'includes',
        'mandatories',
        'hierarchies',
        'hierarchyItems',
        'joints',
        'jointItems'
      ])
    },
    methods: {
      ...mapMutations('RecognizeAggregateModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        resetModal: types.RESET_MODAL,
        setModalForm: types.SET_MODAL_FORM
      })
    },
    locales
  })
  export default class RecognizeAggregateModal extends Vue {
    ALERT_STATUS = ALERT_STATUS
    @Watch('$lang')
    onLocaleChanged () {
      this.updatePlaceHolder()
    }
    @Watch('isShow')
    onIsShowChanged (newVal, oldVal) {
      this.updatePlaceHolder()
      this.updateRecognizeShortcut(newVal, oldVal)
    }
    get errorCount () {
      const { errorInEditor } = this
      return errorInEditor.filter(line => [ERROR_TYPE.COLUMN_NOT_IN_MODEL, ERROR_TYPE.COLUMN_NOT_IN_INCLUDES].includes(line.type)).length
    }
    get repeatCount () {
      const { errorInEditor } = this
      return errorInEditor.filter(line => [ERROR_TYPE.COLUMN_DUPLICATE].includes(line.type)).length
    }
    get selectedDimensionCount () {
      const { form: { dimensions } } = this
      return dimensions.filter(dimension => dimension.isChecked).length
    }
    get isSelectAll () {
      const { form: { dimensions } } = this
      return !dimensions.some(dimension => !dimension.isChecked && !dimension.isDisabled)
    }
    get isAllDimensionError () {
      const { selectedDimensionCount, errorCount, repeatCount } = this
      return !selectedDimensionCount && !!(errorCount + repeatCount)
    }
    get isIndeterminate () {
      const { selectedDimensionCount, isSelectAll } = this
      return selectedDimensionCount && !isSelectAll
    }
    isColumnUsedInCurrent (column) {
      const { type, includes, mandatories, hierarchyItems, jointItems } = this
      switch (type) {
        case AGGREGATE_TYPE.INCLUDE: return includes.includes(column)
        case AGGREGATE_TYPE.MANDATORY: return mandatories.includes(column)
        case AGGREGATE_TYPE.HIERARCHY: return hierarchyItems.includes(column)
        case AGGREGATE_TYPE.JOINT: return jointItems.includes(column)
        default: return false
      }
    }
    isColumnUsedInOther (column) {
      const { type, groupIdx, mandatories, hierarchies, joints } = this
      switch (type) {
        // 层级和联合中有此维度
        case AGGREGATE_TYPE.MANDATORY:
          return hierarchies.some(hierarchy => hierarchy.items.some(item => item === column)) ||
            joints.some(joint => joint.items.some(item => item === column))
        // 必需、其他层级和联合中有此维度
        case AGGREGATE_TYPE.HIERARCHY:
          return mandatories.some(mandatory => mandatory === column) ||
            hierarchies.some(hierarchy => hierarchy.items.some((item, idx) => idx !== groupIdx && item === column)) ||
            joints.some(joint => joint.items.some(item => item === column))
        // 必需、层级和其他联合中有此维度
        case AGGREGATE_TYPE.JOINT:
          return mandatories.some(mandatory => mandatory === column) ||
            hierarchies.some(hierarchy => hierarchy.items.some(item => item === column)) ||
            joints.some(joint => joint.items.some((item, idx) => idx !== groupIdx && item === column))
        // 包含维度不做判断
        case AGGREGATE_TYPE.INCLUDE:
        default: return false
      }
    }
    isColumnInModel (column) {
      const { modelDimensions } = this
      return modelDimensions.some(d => d.column === column)
    }
    isColumnInIncludes (column) {
      const { includes } = this
      return includes.includes(column)
    }
    getColumnErrorMessage (errorType, column) {
      switch (errorType) {
        case ERROR_TYPE.COLUMN_NOT_IN_MODEL:
          return this.$t('columnNotInModel', { column })
        case ERROR_TYPE.COLUMN_NOT_IN_INCLUDES:
          return this.$t('columnNotInIncludes', { column })
        case ERROR_TYPE.COLUMN_DUPLICATE:
          return this.$t('columnDuplicate', { column })
        default: return 'Unknow Error'
      }
    }
    setNotInModelError (column) {
      const { errors } = this
      if (!errors.notInModel.includes(column)) this.setModal({ errors: { ...errors, notInModel: [...errors.notInModel, column] } })
    }
    setNotInIncludesError (column) {
      const { errors } = this
      if (!errors.notInIncludes.includes(column)) this.setModal({ errors: { ...errors, notInIncludes: [...errors.notInIncludes, column] } })
    }
    setDuplicateError (column) {
      const { errors } = this
      if (!errors.duplicate.includes(column)) this.setModal({ errors: { ...errors, duplicate: [...errors.duplicate, column] } })
    }
    setUsedInOthersError (column) {
      const { errors } = this
      if (!errors.usedInOthers.includes(column)) this.setModal({ errors: { ...errors, usedInOthers: [...errors.usedInOthers, column] } })
    }
    clearupErrors () {
      this.setModal({ errors: getInitialErrors() })
    }
    updateRecognizeShortcut (newVal, oldVal) {
      if (!oldVal && newVal) {
        document.addEventListener('keydown', this.handleDexecute)
      } else if (!newVal && oldVal) {
        document.removeEventListener('keydown', this.handleDexecute)
      }
    }
    updatePlaceHolder () {
      this.$nextTick(() => {
        const { editorRef } = this.$refs
        const { editor } = editorRef || {}
        updatePlaceHolder(editor, (h) => (
          <div class="ace_placeholder">
            <div>
              {this.$t('inputPlaceholder1')}
              <el-tooltip
                popperClass="recognize-aggregate-placeholder-tooltip"
                content={(
                  <ul>
                    <li>{this.$t('inputPlaceholderTooltip1')}</li>
                    <li>{this.$t('inputPlaceholderTooltip2')}</li>
                  </ul>
                )}
                placement="top"
              >
                <span class="how-to-use">{this.$t('inputPlaceholderTooltipTrigger')}</span>
              </el-tooltip>
            </div>
            <div>
              {this.$t('inputPlaceholder2')}
            </div>
          </div>
        ))
      })
    }
    showErrors () {
      const { errors } = this
      const { editorRef: { editor } } = this.$refs
      const session = editor.getSession()
      const { errorInEditor, errorLines } = collectErrorsInEditor(errors, editor)
      session.setAnnotations(errorInEditor.map(error => ({
        row: error.row,
        column: 0,
        text: this.getColumnErrorMessage(error.type, error.column),
        type: [ERROR_TYPE.COLUMN_DUPLICATE].includes(error.type) ? 'warning' : 'error'
      })))
      this.setModal({ errorLines, errorInEditor })
      this.$nextTick(() => refreshEditor(editor))
    }
    handleDexecute (event) {
      const { metaKey, key, keyCode } = event
      if (metaKey && (keyCode === 13 || key === 'Enter')) {
        this.handleRecognize()
      }
    }
    handleInputText (text) {
      this.setModalForm({ text })
    }
    handleCheckDimension (dimension) {
      const { form } = this
      if (!dimension.isDisabled) {
        const dimensions = form.dimensions.map((d) => (
          d.value === dimension.value ? { ...d, isChecked: !d.isChecked } : d
        ))
        this.setModalForm({ dimensions: [...dimensions] })
      }
    }
    handleRecognize () {
      const { type, form, modelDimensions } = this
      const dimensions = []
      this.clearupErrors()
      let formattedText = ''
      for (const text of form.text.replace(/\n*/g, '').split(/,\n*/g)) {
        const columnText = text.trim().toLocaleUpperCase()
        if (columnText) {
          const dimension = modelDimensions.find(d => d.column === columnText)
          if (dimension) {
            if (type !== AGGREGATE_TYPE.INCLUDE && !this.isColumnInIncludes(dimension.column)) {
              this.setNotInIncludesError(dimension.column)
            } else if (!this.isColumnUsedInOther(dimension.column)) {
              const duplicate = dimensions.some(d => d.value === dimension.column)
              if (!duplicate) {
                const isFormChecked = form.dimensions.find(d => d.value === columnText)?.isChecked
                const isChecked = isFormChecked ?? true
                const isDisabled = this.isColumnUsedInCurrent(dimension.column)
                const dataType = dimension.type
                dimensions.push({ value: dimension.column, label: dimension.column, isChecked, isDisabled, dataType })
              } else {
                this.setDuplicateError(dimension.column)
              }
            } else {
              this.setUsedInOthersError(dimension.column)
            }
          } else {
            this.setNotInModelError(columnText)
          }
          formattedText += `${columnText},\n`
        }
      }
      this.setModalForm({ text: formattedText, dimensions })
      this.$nextTick(() => {
        this.showErrors()
      })
    }
    handlePrevious () {
      const { editorRef: { editor } } = this.$refs
      const { errorLines, errorCursor } = this
      const lineIdx = errorLines.indexOf(errorCursor)
      const nextCursor = errorLines[lineIdx - 1] ?? errorLines[errorLines.length - 1]
      this.setModal({ errorCursor: nextCursor })
      scrollToLineAndHighlight(editor, nextCursor)
    }
    handleNext () {
      const { editorRef: { editor } } = this.$refs
      const { errorLines, errorCursor } = this
      const lineIdx = errorLines.indexOf(errorCursor)
      const nextCursor = errorLines[lineIdx + 1] ?? errorLines[0]
      this.setModal({ errorCursor: nextCursor })
      scrollToLineAndHighlight(editor, nextCursor)
    }
    handleSelectAll () {
      const { form, isSelectAll } = this
      const dimensions = isSelectAll
        ? form.dimensions.map((d) => (
          !d.isDisabled ? { ...d, isChecked: false } : d
        ))
        : form.dimensions.map((d) => (
          !d.isDisabled ? { ...d, isChecked: true } : d
        ))
      this.setModalForm({ dimensions })
    }
    handleClosed () {
      this.resetModal()
    }
    handleCancel (done) {
      if (typeof done === 'function') done()
      this.hideModal()
    }
    handleSubmit () {
      const { form, callback } = this
      callback(form.dimensions.filter(d => !d.isDisabled && d.isChecked).map(d => d.value))
      this.hideModal()
    }
  }
  </script>
  <style lang="less">
  @import '../../../assets/styles/variables.less';
  .recognize-aggregate-modal {
    .el-dialog {
      width: 800px !important;
    }
    .el-dialog__header {
      padding: 24px 24px 16px 24px;
    }
    .el-dialog__body {
      padding: 0;
      border-top: 1px solid @ke-border-divider-color;
      border-bottom: 1px solid @ke-border-divider-color;
    }
    .dialog-content {
      display: flex;
      height: 424px;
    }
    .recognize-area {
      display: flex;
      flex-direction: column;
      width: 30%;
      position: relative;
      border-right: 1px solid @ke-border-divider-color;
    }
    .recognize-header {
      height: 38px;
      flex: none;
      display: flex;
      align-items: center;
      position: relative;
      padding: 0px 8px;
      border-bottom: 1px solid @ke-border-divider-color;
    }
    .result-counter {
      font-weight: 400;
      font-size: 12px;
      line-height: 16px;
      .error {
        color: @ke-color-danger-hover;
      }
      .warning {
        color: @ke-color-warning-hover;
      }
      * + * {
        margin-left: 7px;
      }
    }
    .result-actions {
      position: absolute;
      top: 50%;
      right: 8px;
      transform: translateY(-50%);
      .el-button + .el-button {
        margin-left: 4px;
      }
      .is-text:focus:not(:hover) {
        background: transparent;
        border-color: transparent;
      }
    }
    .text-input {
      background-color: @ke-background-color-secondary;
      .ace_gutter {
        background-color: @ke-background-color-secondary;
      }
      .ace_placeholder {
        font-weight: 400;
        font-size: 12px;
        line-height: 16px;
        color: @text-normal-color;
        padding: 0 6px;
        white-space: pre-wrap;
      }
    }
    .recognize-results {
      width: 70%;
      display: flex;
      flex-direction: column;
      position: relative;
      overflow-x: hidden;
      overflow-y: auto;
    }
    .results-header {
      height: 38px;
      flex: none;
      display: flex;
      align-items: center;
      position: relative;
      padding: 0px 8px;
      border-bottom: 1px solid @ke-border-divider-color;
      font-weight: 400;
      font-size: 12px;
      line-height: 16px;
    }
    .actions {
      position: absolute;
      right: 16px;
      bottom: 16px;
      z-index: 1;
    }
    .empty-data {
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
    }
    .list-actions {
      display: flex;
      border-bottom: 1px solid @ke-border-divider-color;
      align-items: center;
      font-weight: 500;
      font-size: 12px;
      line-height: 16px;
      padding: 10px;
      .el-checkbox__inner {
        display: block;
      }
      .el-checkbox {
        margin-right: 16px;
      }
    }
    .dimension-list {
      border-radius: 3px;
      height: 100%;
      box-sizing: border-box;
    }
    .dimension {
      position: relative;
      padding: 8px 10px;
      cursor: pointer;
      display: flex;
      align-items: center;
      border-bottom: 1px solid @ke-border-divider-color;
      user-select: none;
      .el-checkbox {
        margin-right: 16px;
      }
      &:hover {
        background-color: @ke-color-info-secondary-bg;
      }
    }
    .dimension > * {
      vertical-align: middle;
    }
    .header-data-type,
    .data-type {
      position: absolute;
      right: 10px;
      width: 100px;
    }
    .current-used-mask {
      cursor: not-allowed;
      background: white;
      opacity: 0.5;
      position: absolute;
      top: 0;
      right: 0;
      bottom: 0;
      left: 0;
      z-index: 1;
    }
    .ace_gutter-cell.ace_error {
      background-image: url('./error.svg');
      background-repeat: no-repeat;
      background-position: 4px center;
    }
    .ace_gutter-cell.ace_warning {
      background-image: url('./warning.svg');
      background-repeat: no-repeat;
      background-position: 4px center;
    }
    .all-dimension-error {
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      color: @ke-color-danger-hover;
    }
    .how-to-use {
      color: @ke-color-primary;
      cursor: pointer;
      &:hover {
        color: @ke-color-primary-hover;
      }
    }
  }
  .recognize-aggregate-placeholder-tooltip {
    font-weight: 400;
    font-size: 12px;
    line-height: 16px;
    ul, li {
      list-style: disc;
    }
    ul {
      margin-left: 15px;
    }
  }
  </style>
  