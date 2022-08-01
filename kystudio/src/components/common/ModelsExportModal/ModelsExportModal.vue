<template>
  <el-dialog class="models-export-modal"
    width="480px"
    :title="$t('exportModel')"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @open="handleOpen"
    @close="handleClose"
    @closed="handleClosed">
    <div v-if="isBodyShow" v-loading="isLoading || isSubmiting">
      <p class="all-tips">
        <i class="el-icon-ksd-info"></i>
        <span>{{$t('exportAllTip')}}</span>
      </p>
       <!-- <el-alert :title="$t('exportAllTip')" icon="el-icon-ksd-info" :show-background="false" type="info" :closable="false"></el-alert> -->
      <div class="header clearfix" v-if="type === 'all'">
        <div class="ksd-fleft">
          <span class="title">{{$t('chooseModels')}}</span>
        </div>
        <div class="ksd-fright">
          <el-input class="filter" prefix-icon="el-icon-search" size="small" :placeholder="$t('placeholder')" @input="handleFilter" />
        </div>
      </div>
      <p class="export-tips">{{$t('exportOneModelTip')}}</p>
      <!-- <p v-if="choosedModelsArr.length > 0" class="choosed-block">{{choosedModelsArr.join(', ')}}</p> -->
      <!-- <el-tree
        highlight-current
        v-if="type === 'all'"
        check-strictly
        class="model-tree"
        ref="tree"
        node-key="id"
        v-show="!isTreeEmpty"
        :data="models"
        :props="{ label: 'name', isLeaf: true }"
        :show-checkbox="getIsNodeShowCheckbox"
        :render-content="renderContent"
        :filter-node-method="handleFilterNode"
        @check="handleSelectModels"
      /> -->
      <div class="export-model-list" v-if="type === 'all'">
        <el-checkbox-group v-model="selectedModals" @change="handleSelectModels">
          <el-checkbox v-for="item in exportModal.list" :disabled="item.status === 'BROKEN'" :label="item.id" :key="item.id">
            <el-tooltip :content="$t('exportBrokenModelCheckboxTip')" effect="dark" placement="top" :disabled="item.status !== 'BROKEN'">
              <span>{{item.name}}</span>
            </el-tooltip>
          </el-checkbox>
        </el-checkbox-group>
        <p class="loadingMore" v-if="showLoadingMore" @click="loadMoreModals"><i class="el-icon-loading" v-if="isLoadingModals"></i>{{isLoadingModals ? $t('loading') : $t('kylinLang.common.loadMore')}}</p>
        <div class="model-tree" v-show="isTreeEmpty">
          <div class="no-data">{{$t('kylinLang.common.noData')}}</div>
        </div>
      </div>
      <div class="export-other">
        <p class="title">{{$t('exportOther')}}</p>
        <p class="export-tips">{{$t('exportOtherTips')}}</p>
        <p class="other-content mrgb15">
          <el-tooltip :content="$t('disabledOverrideTip')" effect="dark" placement="top" :disabled="selectedModals.length === 0 || changeCheckboxType('ops') !== selectedModals.length">
            <el-checkbox v-model="form.exportOverProps" :disabled="changeCheckboxType('ops') === selectedModals.length">{{$t('override')}}</el-checkbox>
          </el-tooltip>
        </p>
        <p class="other-content">
          <el-tooltip :content="$t('disabledMultPartitionTip')" effect="dark" placement="top" :disabled="selectedModals.length === 0 || changeCheckboxType('mult-partition') !== selectedModals.length">
            <el-checkbox v-model="form.exportMultiplePartitionValues" :disabled="changeCheckboxType('mult-partition') === selectedModals.length">{{$t('subPartitionValues')}}</el-checkbox>
          </el-tooltip>
        </p>
      </div>
    </div>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button size="medium" :disabled="isSubmiting" @click="handleCancel">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" :disabled="!selectedModals.length" @click="handleSubmit" :loading="isSubmiting">{{$t('export')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions } from 'vuex'
import vuex, { actionTypes } from '../../../store'
import locales from './locales'
import store from './store'
import OverflowTextTooltip from '../OverflowTextTooltip/OverflowTextTooltip.vue'

vuex.registerModule(['modals', 'ModelsExportModal'], store)

@Component({
  components: {
    OverflowTextTooltip
  },
  computed: {
    ...mapState('ModelsExportModal', {
      project: state => state.project,
      type: state => state.type,
      models: state => state.models,
      form: state => state.form,
      isShow: state => state.isShow,
      callback: state => state.callback
    })
    // choosedModelsArr () {
    //   let arr = []
    //   for (let i in this.form.ids) {
    //     for (let j in this.models) {
    //       if (this.models[j].id === this.form.ids[i]) {
    //         arr.push(this.models[j].name)
    //       }
    //     }
    //   }
    //   return arr
    // }
  },
  methods: {
    ...mapMutations('ModelsExportModal', {
      setModalForm: actionTypes.SET_MODAL_FORM,
      hideModal: actionTypes.HIDE_MODAL,
      initModal: actionTypes.INIT_MODAL,
      resetState: actionTypes.RESET_MODAL_STATE
    }),
    ...mapActions('ModelsExportModal', {
      getModelsMetadataStructure: actionTypes.GET_MODELS_METADATA_STRUCTURE
    }),
    ...mapActions({
      downloadModelsMetadata: actionTypes.DOWNLOAD_MODELS_METADATA,
      downloadModelsMetadataBlob: actionTypes.DOWNLOAD_MODELS_METADATA_BLOB
    })
  },
  locales
})
export default class ModelsExportModal extends Vue {
  isLoading = false
  isBodyShow = false
  isSubmiting = false
  isTreeEmpty = false
  isLoadingModals = false
  showLoadingMore = false
  filterName = ''

  selectedModals = []
  exportModal = {
    list: [],
    pageOffset: 1,
    pageSize: 200
  }

  @Watch('isShow')
  changeExportModalVisibled (newVal, oldVal) {
    newVal && !oldVal && (this.exportModalList = this.models)
  }

  getIsNodeShowCheckbox (data) {
    return data.nodeType === 'model'
  }

  changeCheckboxType (type) {
    if (type === 'ops') {
      return this.models.filter(it => this.selectedModals.includes(it.id) && !it.has_override_props).length
    } else if (type === 'mult-partition') {
      return this.models.filter(it => this.selectedModals.includes(it.id) && !it.has_multiple_partition_values).length
    }
  }

  async handleOpen () {
    try {
      const { project } = this
      let data = {}
      this.isLoading = true
      this.type !== 'all' && (data.model_ids = this.form.ids.join(','))
      await this.getModelsMetadataStructure({ project, ...data })
      this.isLoading = false
      this.isBodyShow = true
      if (this.type !== 'all') {
        this.selectedModals = this.form.ids
        return
      }
      this.models.length > this.exportModal.pageSize && (this.showLoadingMore = true)
      this.pageExportModals()
    } catch (e) {
      this.handleClose()
      this.$message.error(this.$t('fetchModelsFailed'))
    }
  }

  handleClose (isSubmit = false) {
    this.hideModal()
    this.resetState()
    this.exportModal.list = []
    this.exportModal.pageOffset = 1
    this.showLoadingMore = false
    this.selectedModals = []
    this.filterName = ''
    this.callback && this.callback(isSubmit)
  }

  // changeCheck (item) {
  //   if (item.choosed) {
  //     this.choosedModelsArr.push(item.name)
  //   } else {
  //     var index = this.choosedModelsArr.indexOf(item.name)
  //     this.choosedModelsArr.splice(index, 1)
  //   }
  // }

  handleClosed () {
    this.isBodyShow = false
  }

  handleSelectModels (data) {
    const hasOverrideProps = this.models.filter(it => data.includes(it.id) && it.has_override_props)
    const hasMultPartitions = this.models.filter(it => data.includes(it.id) && it.has_multiple_partition_values)
    this.setModalForm({
      ids: data,
      exportOverProps: !hasOverrideProps.length && this.form.exportOverProps ? false : this.form.exportOverProps,
      exportMultiplePartitionValues: !hasMultPartitions.length && this.form.exportMultiplePartitionValues ? false : this.form.exportMultiplePartitionValues
    })
  }

  handleCancel () {
    this.handleClose()
  }

  loadMoreModals () {
    this.isLoadingModals = true
    this.exportModal.pageOffset += 1
    this.pageExportModals()
  }

  pageExportModals () {
    const { pageOffset, pageSize } = this.exportModal
    this.exportModal.list = this.models.filter(it => it.name.toLocaleLowerCase().indexOf(this.filterName.toLocaleLowerCase()) >= 0).slice(0, pageOffset * pageSize)
    this.isLoadingModals = false
    this.isTreeEmpty = !this.exportModal.list.length
  }

  handleFilter (value) {
    this.filterName = value
    setTimeout(() => {
      this.exportModal.pageOffset = 1
      this.pageExportModals()
    }, 200)
  }

  handleFilterNode (inputValue, data) {
    if (!inputValue) return true

    const value = inputValue.toLowerCase()

    return data.search.some(search => search.toLowerCase().includes(value))
  }

  async handleSubmit () {
    const { project, form } = this
    this.isSubmiting = true
    try {
      // if (this.type !== 'all') {
      await this.downloadModelsMetadata({ project, form })
      // } else {}
      this.handleClose(true)
      this.$message.success(this.$t('exportSuccess'))
    } catch (e) {
      this.$message.error(this.$t('exportFailed'))
    }
    this.isSubmiting = false
  }
  downloadResouceData (project, form) {
    const params = {}
    for (const [key, value] of Object.entries(form)) {
      if (value instanceof Array) {
        value.forEach((item, index) => {
          params[`${key}[${index}]`] = item
        })
      } else if (typeof value === 'object') {
        params[key] = JSON.stringify(value)
      } else {
        params[key] = value
      }
    }
    try {
      this.downloadModelsMetadataBlob({project, params}).then(res => {
        this.isSubmiting = false
        this.handleClose(true)
        let str = res && res.headers.map['content-disposition'][0]
        let fileName1 = str.split('filename=')[1]
        let fileName = fileName1.includes('"') ? JSON.parse(fileName1) : fileName1
        if (res && res.body) {
          let data = res.body
          const blob = new Blob([data], {type: 'application/json;charset=utf-8'})
          if (window.navigator.msSaveOrOpenBlob) {
            navigator.msSaveBlob(data, fileName)
          } else {
            let link = document.createElement('a')
            link.href = window.URL.createObjectURL(blob)
            link.download = fileName
            link.click()
            window.URL.revokeObjectURL(link.href)
          }
        }
        this.$message.success(this.$t('exportSuccess'))
      })
    } catch (e) {
      this.isSubmiting = false
      this.handleClose(true)
      this.$message.error(this.$t('exportFailed'))
    }
  }

  // renderNodeIcon (h, { node, data }) {
  //   switch (data.nodeType) {
  //     case 'table': return data.type === 'FACT'
  //       ? <i class="tree-icon el-icon-ksd-fact_table" />
  //       : <i class="tree-icon el-icon-ksd-lookup_table" />
  //     case 'model':
  //     default: return null
  //   }
  // }

  // renderNodeText (h, { node, data }) {
  //   switch (data.nodeType) {
  //     case 'model': return <span v-custom-tooltip={{ text: node.label, w: 50 }}>{node.label}</span>
  //     case 'table': return <span v-custom-tooltip={{ text: node.label, w: 80 }}>{node.label}</span>
  //     default: return null
  //   }
  // }

  // renderContent (h, { node, data }) {
  //   return (
  //     <span class={['tree-item', data.nodeType]}>
  //       {this.renderNodeIcon(h, { node, data })}
  //       {this.renderNodeText(h, { node, data })}
  //     </span>
  //   )
  // }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.models-export-modal {
  .export-tips {
    font-size: 12px;
    color: @text-normal-color;
    margin-bottom: 10px;
  }
  .export-other {
    margin-top: 15px;
    font-size: 0;
    .other-content {
      .el-checkbox__input {
        margin-top: -5px;
      }
      .el-checkbox__inner {
        vertical-align: middle;
        margin-top: 5px;
      }
    }
  }
  .mrgb15 {
    margin-bottom: 10px;
    margin-top: 5px;
  }
  .choosed-block {
    margin-bottom: 10px;
    color: @text-normal-color;
    font-size: 12px;
  }
  .all-tips {
    margin-bottom: 10px;
    font-size: 14px;
    i {
      color: @text-disabled-color
    }
    span {
      color: @text-title-color;
    }
  }
  .one-tip {
    margin-bottom: 10px;
  }
  .el-alert__title {
    font-size: 14px;
  }
  .filter {
    width: 240px;
  }
  .header {
    margin-bottom: 10px;
  }
  .title {
    line-height: 24px;
    font-weight: bold;
    color: @text-title-color;
  }
  .model-tree {
    // border: 1px solid @line-border-color;
    width: 100%;
    height: 100%;
    position: relative;
  }
  .no-data {
    position: absolute;
    top: 30%;
    left: 50%;
    transform: translate(-50%, -30%);
    color: @text-disabled-color;
  }
  .tree-icon {
    margin-right: 5px;
  }
  .tree-item.table {
    display: flex;
    align-items: center;
    width: 100%;
  }
  .tree-item {
    width: 100%;
    .custom-tooltip-layout {
      display: block;
      .el-tooltip {
        display: block;
      }
    }
  }
  .export-model-list {
    width: 100%;
    height: 200px;
    overflow: auto;
    border: 1px solid #dddddd;
    padding: 10px;
    box-sizing: border-box;
    .loadingMore {
      cursor: pointer;
      color: @text-disabled-color;
      font-size: 12px;
    }
    .el-checkbox {
      display: flex;
      margin-bottom: 10px;
      // .el-checkbox__input {
      //   margin-top: -5px;
      // }
      .el-checkbox__inner {
        vertical-align: middle;
      }
    }
    .el-checkbox+.el-checkbox {
      margin-left: 0;
    }
  }
}
</style>
