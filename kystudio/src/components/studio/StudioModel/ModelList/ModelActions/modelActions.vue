<template>
  <div class="action-items" @click.stop v-if="currentModel && Object.keys(currentModel).length">
    <template v-if="'visible' in currentModel && !currentModel.visible">
      <common-tip :content="$t('authorityDetails')">
        <i class="icon-item el-ksd-icon-lock_old" @click="showNoAuthorityContent(currentModel)"></i>
      </common-tip>
    </template>
    <template v-else>
      <common-tip :content="isStreamModelEnableEdit ? $t('kylinLang.common.edit'): $t('disableEditModel')" class="ksd-mr-8" v-if="currentModel.status !== 'BROKEN' && datasourceActions.includes('modelActions')" :disabled="!!editText&&isStreamModelEnableEdit">
        <span v-if="!editText" class="item" @click="(e) => handleEditModel(currentModel.alias, e)"><i :class="['icon-item', 'edit-icon', 'el-ksd-icon-edit_22', isStreamModelEnableEdit ? '' : 'is-disable']"></i></span>
        <el-button v-else class="item" :disabled="!isStreamModelEnableEdit" @click="(e) => handleEditModel(currentModel.alias, e)" icon="el-ksd-icon-edit_22" type="primary" text>{{editText}}</el-button>
      </common-tip>
      <common-tip :content="$t('kylinLang.common.repair')" class="ksd-mr-8" v-if="currentModel.broken_reason === 'SCHEMA' && datasourceActions.includes('modelActions')" :disabled="!!editText">
        <span v-if="!editText" class="item" @click="(e) => handleEditModel(currentModel.alias, e)"><i :class="['icon-item', 'el-ksd-icon-repair_22']"></i></span>
        <el-button v-else class="item" @click="(e) => handleEditModel(currentModel.alias, e)" icon="el-ksd-icon-repair_22" type="primary" text>{{editText}}</el-button>
      </common-tip>
      <common-tip
        class="ksd-mr-8"
        :disabled="buildText ? !disableLineBuildBtn(currentModel) : false"
        :content="getDisableBuildTips(currentModel)"
        v-if="currentModel.status !== 'BROKEN' && datasourceActions.includes('buildIndex')">
        <el-popover
          ref="popoverBuild"
          placement="bottom-end"
          width="280"
          trigger="manual"
          v-model="buildVisible[currentModel.uuid]">
          <div>{{$t('buildTips')}}</div>
          <div style="text-align: right; margin: 0">
            <el-button type="primary" size="mini" class="ksd-ptb-0" text @click="closeBuildTips(currentModel.uuid)">{{$t('iKnow')}}</el-button>
          </div>
        </el-popover>
        <span
          v-if="!buildText"
          :class="['item', {'build-disabled':disableLineBuildBtn(currentModel)}]"
          v-popover:popoverBuild
          @click="setModelBuldRange(currentModel)">
          <i :class="['icon-item', 'el-ksd-icon-build_index_22', {'ksd-mr-5': buildText}]"></i>
        </span>
        <el-button
          v-else
          class="item"
          :disabled="disableLineBuildBtn(currentModel)"
          v-popover:popoverBuild
          @click="setModelBuldRange(currentModel)"
          icon="el-ksd-icon-build_index_22"
          type="primary"
          text>{{buildText}}</el-button>
      </common-tip>
      <common-tip :content="$t('kylinLang.common.moreActions')" :disabled="!!moreText" v-if="datasourceActions.includes('modelActions') || modelActions.includes('purge') || modelActions.includes('exportTDS')">
        <el-dropdown @command="(command) => {handleCommand(command, currentModel)}" :id="currentModel.name" trigger="click" >
          <span class="el-dropdown-link" >
            <span v-if="!moreText" class="item ksd-fs-14"><i :class="['icon-item', otherIcon]"></i></span>
            <el-button v-else class="item" :icon="otherIcon" type="primary" text>{{moreText}}</el-button>
          </span>
          <el-dropdown-menu slot="dropdown"  :uuid='currentModel.uuid' :append-to-body="appendToBody" :popper-container="'modelListPage'" class="specialDropdown">
            <el-dropdown-item
              command="dataLoad"
              :class="{'disabled-action': currentModel.model_type !== 'BATCH'&&isHavePartitionColumn}"
              v-if="currentModel.status !== 'BROKEN' && modelActions.includes('dataLoad')">
              <common-tip
                :content="$t('disableActionTips4')"
                :disabled="!(currentModel.model_type !== 'BATCH'&&isHavePartitionColumn)">
                {{$t('modelPartitionSet')}}
              </common-tip>
            </el-dropdown-item>
            <el-dropdown-item
              :class="{'disabled-action': currentModel.model_type !== 'BATCH'}"
              @click.native="subParValMana(currentModel)"
              v-if="currentModel.status !== 'BROKEN' && $store.state.project.multi_partition_enabled && currentModel.multi_partition_desc && modelActions.includes('manageSubPartitionValues')">
              <common-tip
                :content="$t('disableActionTips2')"
                :disabled="currentModel.model_type === 'BATCH'">
                {{$t('subPartitionValuesManage')}}
              </common-tip>
            </el-dropdown-item>
            <el-dropdown-item
              command="exportMetadata"
              :class="{'disabled-action': currentModel.status === 'BROKEN' || currentModel.model_type !== 'BATCH'}"
              v-if="metadataActions.includes('executeModelMetadata')">
              <common-tip
                :content="currentModel.model_type !== 'BATCH' ? $t('disableActionTips2') : $t('bokenModelExportMetadatasTip')"
                :disabled="currentModel.status !== 'BROKEN' && currentModel.model_type === 'BATCH'">
                {{$t('exportMetadatas')}}
              </common-tip>
            </el-dropdown-item>
            <el-dropdown-item
              command="exportTDS"
              :class="{'disabled-action': currentModel.status === 'BROKEN' || currentModel.model_type !== 'BATCH'}"
              v-if="modelActions.includes('exportTDS')">
              <common-tip
                :content="currentModel.model_type !== 'BATCH' ? $t('disableActionTips2') : $t('bokenModelExportTDSTip')"
                :disabled="currentModel.status !== 'BROKEN' && currentModel.model_type === 'BATCH'">
                <span>{{$t('exportTds')}}</span>
              </common-tip>
            </el-dropdown-item>
            <el-dropdown-item
              command="rename"
              divided
              v-if="currentModel.status !== 'BROKEN' && modelActions.includes('exportMDX')">
              <common-tip
                :content="$t('disableActionTips')"
                disabled>
                {{$t('rename')}}
              </common-tip>
            </el-dropdown-item>
            <el-dropdown-item
              command="clone"
              :class="{'disabled-action': currentModel.model_type !== 'BATCH'}"
              v-if="currentModel.status !== 'BROKEN' && modelActions.includes('clone')">
              <common-tip
                :content="$t('disableActionTips2')"
                :disabled="currentModel.model_type === 'BATCH'">
                {{$t('kylinLang.common.clone')}}
              </common-tip>
            </el-dropdown-item>
            <el-dropdown-item
              v-if="currentModel.status !== 'BROKEN' && modelActions.includes('changeModelOwner')"
              @click.native="openChangeModelOwner(currentModel, currentModel.uuid)">
              <common-tip
                :content="$t('disableActionTips')"
                disabled>
                {{$t('changeModelOwner')}}
              </common-tip>
            </el-dropdown-item>
            <el-dropdown-item
              command="delete"
              v-if="modelActions.includes('delete')">
              {{$t('delete')}}
            </el-dropdown-item>
            <el-dropdown-item
              command="purge"
              :class="{'disabled-action': currentModel.model_type === 'HYBRID'}"
              v-if="currentModel.status !== 'BROKEN' && modelActions.includes('purge')">
              <common-tip
                :content="$t('disableActionTips3')"
                :disabled="currentModel.model_type !== 'HYBRID'">
                {{$t('purge')}}
              </common-tip>
            </el-dropdown-item>
            <el-dropdown-item
              command="offline"
              v-if="currentModel.status !== 'OFFLINE' && currentModel.status !== 'BROKEN' && modelActions.includes('offline')">
              <common-tip
                :content="$t('disableActionTips')"
                disabled>
                {{$t('offLine')}}
              </common-tip>
            </el-dropdown-item>
            <el-dropdown-item
              command="online"
              :class="{'disabled-action': currentModel.forbidden_online || !currentModel.has_segments || (!$store.state.project.multi_partition_enabled && currentModel.multi_partition_desc)}"
              v-if="currentModel.status !== 'ONLINE' && currentModel.status !== 'BROKEN' && currentModel.status !== 'WARNING' && modelActions.includes('online')">
              <common-tip
                :content="getDisabledOnlineTips(currentModel)"
                v-if="currentModel.forbidden_online || !currentModel.has_segments || (!$store.state.project.multi_partition_enabled && currentModel.multi_partition_desc)">
                <span>{{$t('onLine')}}</span>
              </common-tip>
              <span v-else>{{$t('onLine')}}</span>
            </el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </common-tip>
    </template>
    <el-dialog width="480px" :title="$t('changeModelOwner')" class="change_owner_dialog" :visible.sync="changeOwnerVisible" :append-to-body="true" @close="resetModelOwner" :close-on-click-modal="false">
      <el-alert
        :title="$t('changeDesc')"
        type="info"
        :show-background="false"
        :closable="false"
        class="ksd-pt-0"
        show-icon>
      </el-alert>
      <el-form :model="modelOwner" @submit.native.prevent ref="projectOwnerForm" label-width="130px" label-position="top">
        <el-form-item :label="$t('modelName')" prop="model">
          <el-input disabled name="project" v-model="modelOwner.modelName" size="medium"></el-input>
        </el-form-item>
        <el-form-item :label="$t('changeTo')" prop="owner">
         <el-select
          :placeholder="$t('pleaseChangeOwner')"
          filterable
          remote
          :remote-method="loadAvailableModelOwners"
          @blur="(e) => loadAvailableModelOwners(e.target.value)"
          v-model="modelOwner.owner"
          size="medium"
          class="owner-select"
          style="width:100%">
          <el-option :label="user" :value="user" v-for="user in userOptions" :key="user"></el-option>
        </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="changeOwnerVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" :disabled="!(modelOwner.model&&modelOwner.owner)" @click="changeModelOwner" :loading="changeLoading">{{$t('change')}}</el-button>
      </div>
    </el-dialog>

    <!-- 导出 TDS -->
    <el-dialog width="480px" :title="$t('exportTDSTitle')" class="export_tds_dialog" v-if="showExportTDSDialog" :append-to-body="true" :visible="true" @close="closeExportTDSDialog" :close-on-click-modal="false">
      <p class="export-tds-alert">{{$t('step1')}}</p>
      <el-radio-group v-model="exportTDSType">
        <el-radio v-for="it in exportTDSOtions" :key="it.value" :label="it.value">{{$t(it.text)}}</el-radio>
      </el-radio-group>
      <p class="export-tds-alert">{{$t('step2')}}</p>
      <el-radio-group v-model="exportTDSConnectionType">
        <el-radio v-for="it in tdsConnectionOptions" :key="it.value" :label="it.value">{{$t(it.text)}}</el-radio>
      </el-radio-group>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="closeExportTDSDialog">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" @click="handlerExportTDS">{{$t('kylinLang.query.export')}}</el-button>
      </div>
    </el-dialog>

  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync, objectClone } from 'util'
import { apiUrl } from '../../../../../config'
import { handleError, kylinConfirm, kylinMessage, handleSuccess } from 'util/business'
import locales from './locales'

@Component({
  props: {
    currentModel: {
      type: Object,
      default () {
        return {}
      }
    },
    editText: {
      type: String,
      default: ''
    },
    buildText: {
      type: String,
      default: ''
    },
    moreText: {
      type: String,
      default: ''
    },
    otherIcon: {
      type: String,
      default: 'el-ksd-icon-more_16'
    },
    appendToBody: {
      type: Boolean,
      default: false
    }
  },
  computed: {
    ...mapGetters([
      'datasourceActions',
      'metadataActions',
      'modelActions',
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      fetchSegments: 'FETCH_SEGMENTS',
      getModelByModelName: 'LOAD_MODEL_INFO',
      getAvailableModelOwners: 'GET_AVAILABLE_MODEL_OWNERS',
      updateModelOwner: 'UPDATE_MODEL_OWNER',
      purgeModel: 'PURGE_MODEL',
      disableModel: 'DISABLE_MODEL',
      enableModel: 'ENABLE_MODEL',
      delModel: 'DELETE_MODEL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelBuildModal', {
      callModelBuildDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelCheckDataModal', {
      checkModelData: 'CALL_MODAL'
    }),
    ...mapActions('ModelPartition', {
      callModelPartitionDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelRenameModal', {
      callRenameModelDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelCloneModal', {
      callCloneModelDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelsExportModal', {
      callModelsExportModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class ModelActions extends Vue {
  buildVisible = {}
  changeOwnerVisible = false
  userOptions = []
  changeLoading = false
  modelOwner = {
    modelName: '',
    model: '',
    owner: ''
  }
  ownerFilter = {
    page_size: 100,
    page_offset: 0,
    project: '',
    model: '',
    name: ''
  }
  currentExportTDSModel = null
  showExportTDSDialog = false
  exportTDSOtions = [
    {value: 'AGG_INDEX_COL', text: 'exportTDSOptions1'},
    {value: 'AGG_INDEX_AND_TABLE_INDEX_COL', text: 'exportTDSOptions2'},
    {value: 'ALL_COLS', text: 'exportTDSOptions3'}
  ]
  exportTDSType = 'AGG_INDEX_COL'
  exportTDSConnectionType = 'TABLEAU_ODBC_TDS'
  tdsConnectionOptions = [
    {value: 'TABLEAU_ODBC_TDS', text: 'connectODBC'},
    {value: 'TABLEAU_CONNECTOR_TDS', text: 'connectTableau'}
  ]

  // 处理每行的构建按钮的 disable 状态
  disableLineBuildBtn (row) {
    if (row.model_type === 'STREAMING') {
      return true
    } else {
      if (!row.total_indexes || (!this.$store.state.project.multi_partition_enabled && row.multi_partition_desc)) {
        return true
      } else {
        return false
      }
    }
  }

  // 处理构建按钮上的文案提示
  getDisableBuildTips (row) {
    let tips = this.$t('build')
    if (row.model_type === 'STREAMING') {
      tips = this.$t('disableActionTips')
    } else {
      if (!this.$store.state.project.multi_partition_enabled && row.multi_partition_desc) {
        tips = this.$t('multilParTip')
      } else {
        if (row.total_indexes) {
          tips = this.$t('build')
        } else {
          tips = this.$t('noIndexTips')
        }
      }
    }
    return tips
  }

  getDisabledOnlineTips (row) {
    let tips = this.$t('cannotOnlineTips')
    if (row.forbidden_online) {
      tips += '<br/>'
      tips += this.$t('closeSCD2ModalOnlineTip')
    }
    if (!row.has_segments) {
      tips += '<br/>'
      tips += this.$t('noSegmentOnlineTip')
    }
    if (!this.$store.state.project.multi_partition_enabled && row.multi_partition_desc) {
      tips += '<br/>'
      tips += this.$t('multilParTip')
    }
    return tips
  }

  // 编辑model
  handleEditModel (modelName, event) {
    if (!this.isStreamModelEnableEdit) return
    event && event.target.parentElement.className.split(' ').includes('icon') && event.target.parentElement.blur()
    this.$router.push({name: 'ModelEdit', params: { modelName: modelName, action: 'edit' }})
  }

  // 展示model无权限的相关table和columns信息
  showNoAuthorityContent (row) {
    const { unauthorized_tables, unauthorized_columns } = row
    let details = []
    if (unauthorized_tables && unauthorized_tables.length) {
      details.push({title: `Table (${unauthorized_tables.length})`, list: unauthorized_tables})
    }
    if (unauthorized_columns && unauthorized_columns.length) {
      details.push({title: `Columns (${unauthorized_columns.length})`, list: unauthorized_columns})
    }
    this.callGlobalDetailDialog({
      theme: 'plain-mult',
      title: this.$t('kylinLang.model.authorityDetail'),
      msg: this.$t('kylinLang.model.authorityMsg', {modelName: row.name}),
      showCopyBtn: true,
      showIcon: false,
      showDetailDirect: true,
      details,
      showDetailBtn: false,
      dialogType: 'error',
      customClass: 'no-acl-model',
      showCopyTextLeftBtn: true
    })
  }

  async setModelBuldRange (modelDesc, isNeedBuildGuild) {
    // 纯流模型中，构建按钮禁用
    if (modelDesc.model_type === 'STREAMING') {
      return false
    }
    if (!modelDesc.total_indexes && !isNeedBuildGuild || (!this.$store.state.project.multi_partition_enabled && modelDesc.multi_partition_desc)) return
    const projectName = this.currentSelectedProject
    // 如果是融合数据模型，这里的构建入口是针对批数据的，所以要传batch id
    const modelName = modelDesc.model_type === 'HYBRID' ? modelDesc.batch_id : modelDesc.uuid
    const res = await this.fetchSegments({ projectName, modelName })
    const { total_size, value } = await handleSuccessAsync(res)
    let type = 'incremental'
    if (!(modelDesc.partition_desc && modelDesc.partition_desc.partition_date_column)) {
      type = 'fullLoad'
    }
    this.$nextTick(async () => {
      await this.callModelBuildDialog({
        modelDesc: modelDesc,
        type: type,
        title: this.$t('build'),
        isHaveSegment: !!total_size,
        disableFullLoad: type === 'fullLoad' && value.length > 0 && value[0].status_to_display !== 'ONLINE' // 已存在全量加载任务时，屏蔽
      })
      this.$emit('loadModelsList')
    })
  }

  async openChangeModelOwner (model, modelId) {
    this.modelOwner.modelName = model.alias
    this.modelOwner.model = modelId
    this.ownerFilter.project = this.currentSelectedProject
    this.ownerFilter.model = modelId
    await this.loadAvailableModelOwners()
    this.changeOwnerVisible = true
  }

  async loadAvailableModelOwners (filterName) {
    this.ownerFilter.name = filterName || ''
    try {
      const res = await this.getAvailableModelOwners(this.ownerFilter)
      const data = await handleSuccessAsync(res)
      this.userOptions = data.value
    } catch (e) {
      this.$message({ closeOtherMessages: true, message: e.body.msg, type: 'error' })
    }
  }

  async changeModelOwner () {
    if (!(this.modelOwner.model && this.modelOwner.owner)) {
      return
    }
    this.modelOwner.project = this.currentSelectedProject
    this.changeLoading = true
    try {
      await this.updateModelOwner(this.modelOwner)
      this.changeLoading = false
      this.changeOwnerVisible = false
      this.$message.success(this.$t('changeModelSuccess', this.modelOwner))
      this.$emit('loadModelsList')
    } catch (e) {
      this.$message({ message: e.body.msg, type: 'error' })
      this.changeLoading = false
      this.changeOwnerVisible = false
    }
  }

  checkActionCanDo (command, modelDesc) {
    let flag = true
    switch (command) {
      case 'exportMetadata':
        flag = modelDesc.model_type === 'BATCH'
        break
      case 'exportTDS':
        flag = modelDesc.model_type === 'BATCH'
        break
      case 'clone':
        flag = modelDesc.model_type === 'BATCH'
        break
      case 'purge':
        flag = modelDesc.model_type !== 'HYBRID'
        break
      default:
        flag = true
        break
    }
    return flag
  }

  async handleCommand (command, modelDesc) {
    const canDoAction = this.checkActionCanDo(command, modelDesc)
    // 如果是不可操作的情况，直接 return 掉
    if (!canDoAction) {
      return false
    }
    if (command === 'dataCheck') {
      this.checkModelData({
        modelDesc: modelDesc
      }).then((isSubmit) => {
        if (isSubmit) {
          this.$emit('loadModelsList')
        }
      })
    } else if (command === 'dataLoad') {
      if (modelDesc.model_type !== 'BATCH' && this.isHavePartitionColumn) {
        return false
      }
      this.getModelByModelName({model_name: modelDesc.alias, project: this.currentSelectedProject}).then((response) => {
        handleSuccess(response, (data) => {
          if (data && data.value && data.value.length) {
            this.modelData = data.value[0]
            this.modelData.project = this.currentSelectedProject
            let cloneModelDesc = objectClone(this.modelData)
            this.callModelPartitionDialog({
              modelDesc: cloneModelDesc
            }).then((res) => {
              if (res.isSubmit) {
                this.$emit('loadModelsList')
              }
            })
          }
        })
      }, (res) => {
        handleError(res)
      })
    } else if (command === 'rename') {
      const {isSubmit, newName} = await this.callRenameModelDialog(objectClone(modelDesc))
      if (isSubmit) {
        this.$emit('loadModelsList')
        this.$emit('rename', newName)
      }
    } else if (command === 'delete') {
      kylinConfirm(this.$t('delModelTip', {modelName: modelDesc.alias}), null, this.$t('delModelTitle')).then(() => {
        this.handleDrop(modelDesc)
      })
    } else if (command === 'purge') {
      return kylinConfirm(this.$t('pergeModelTip', {modelName: modelDesc.alias}), {type: 'warning'}, this.$t('pergeModelTitle')).then(() => {
        this.handlePurge(modelDesc).then(() => {
          this.refreshSegment(modelDesc.alias)
        })
      })
    } else if (command === 'clone') {
      if (modelDesc.model_type !== 'BATCH') {
        return false
      }
      const isSubmit = await this.callCloneModelDialog(objectClone(modelDesc))
      isSubmit && this.$emit('loadModelsList')
    } else if (command === 'offline') {
      kylinConfirm(this.$t('disableModelTip', {modelName: modelDesc.alias}), {confirmButtonText: this.$t('disableModelTitle')}, this.$t('disableModelTitle')).then(() => {
        this.handleDisableModel(objectClone(modelDesc))
      })
    } else if (command === 'online') {
      if (modelDesc.forbidden_online || !modelDesc.has_segments || !this.$store.state.project.multi_partition_enabled && modelDesc.multi_partition_desc) return
      this.handleEnableModel(objectClone(modelDesc))
    } else if (command === 'exportMetadata') {
      if (modelDesc.status === 'BROKEN') return
      const form = { ids: [modelDesc.uuid] }
      const project = this.currentSelectedProject
      const type = 'one'
      await this.callModelsExportModal({ project, form, type })
    } else if (command === 'exportTDS') {
      if (modelDesc.status === 'BROKEN') return
      if (modelDesc.status === 'OFFLINE') {
        this.$confirm(this.$t('exportTDSOfflineTips'), this.$t('kylinLang.common.tip'), {
          confirmButtonText: this.$t('exportTDSContinueBtn'),
          closeOnClickModal: false,
          closeOnPressEscape: false
        }).then(() => {
          this.showExportTDSDialog = true
          this.currentExportTDSModel = modelDesc
        })
      } else {
        this.showExportTDSDialog = true
        this.currentExportTDSModel = modelDesc
      }
    }
  }

  // 子分区值管理
  subParValMana (model) {
    if (model.model_type !== 'BATCH') {
      return false
    }
    this.$router.push({name: 'ModelSubPartitionValues', params: { modelName: model.alias, modelId: model.uuid }})
  }

  get isHaveNoDimMeas () {
    return this.currentModel.simplified_dimensions.length === 0 && this.currentModel.simplified_measures.length === 1 && this.currentModel.simplified_measures[0].name === 'COUNT_ALL' // 没有设置维度，只有默认度量
  }

  get isHavePartitionColumn () {
    return this.currentModel.partition_desc && !!this.currentModel.partition_desc.partition_date_column
  }

  get isStreamModelEnableEdit () {
    return this.currentModel.model_update_enabled && this.currentModel.model_type !== 'BATCH' || this.currentModel.model_type === 'BATCH'
  }

  handleModel (action, modelDesc, successTip) {
    return this[action]({modelId: modelDesc.uuid, project: this.currentSelectedProject}).then(() => {
      kylinMessage(successTip)
      this.$emit('loadModelsList')
      this.$emit('loadModels')
    }, (res) => {
      handleError(res)
    })
  }
  // 禁用model
  handleDisableModel (modelDesc) {
    this.handleModel('disableModel', modelDesc, this.$t('disableModelSuccessTip'))
  }
  // 启用model
  handleEnableModel (modelDesc) {
    this.handleModel('enableModel', modelDesc, this.$t('enabledModelSuccessTip'))
  }
  // 删除model
  handleDrop (modelDesc) {
    this.handleModel('delModel', modelDesc, this.$t('deleteModelSuccessTip'))
  }
  // 清理model
  async handlePurge (modelDesc) {
    return this.handleModel('purgeModel', modelDesc, this.$t('purgeModelSuccessTip'))
  }

  resetModelOwner () {
    this.modelOwner = {
      modelName: '',
      model: '',
      owner: ''
    }
  }

  handlerExportTDS () {
    const { uuid, model_id = uuid } = this.currentExportTDSModel
    const data = {
      project: this.currentSelectedProject,
      export_as: this.exportTDSConnectionType,
      element: this.exportTDSType,
      server_host: window.location.hostname,
      server_port: window.location.port
    }
    let params = ''
    Object.keys(data).forEach(item => {
      params += `${item}=${data[item]}&`
    })
    const dom = document.createElement('a')
    dom.href = `${location.protocol}//${location.host}${apiUrl}models/${model_id}/export?${params}`
    dom.download = true
    document.body.appendChild(dom)
    dom.click()
    document.body.removeChild(dom)
    this.closeExportTDSDialog()
  }

  // 关闭导出 TDS 弹窗
  closeExportTDSDialog () {
    this.showExportTDSDialog = false
    this.exportTDSType = 'AGG_INDEX_COL'
    this.exportTDSConnectionType = 'TABLEAU_ODBC_TDS'
  }

  closeBuildTips (uuid) {
    this.buildVisible[uuid] = false
    localStorage.setItem('hideBuildTips', true)
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.action-items {
  font-size: 0;
  .tip_box {
    line-height: inherit !important;
    vertical-align: inherit !important;
  }
  .item {
    cursor: pointer;
  }
  .icon-item {
    font-size: 22px;
    vertical-align: middle;
    margin-top: -2px;
    cursor: pointer;
  }
  .edit-icon {
    margin-top: -3px;
    &.is-disable {
      color: @color-text-disabled;
      cursor: not-allowed;
    }
  }
}
.disabled-action {
  color: @text-disabled-color;
  cursor: not-allowed;
  &:hover {
    background: none;
    color: #bbbbbb;
  }
}
.export_tds_dialog {
  .export-tds-alert {
    margin-bottom: 5px;
    margin-top: 20px;
    &:first-child {
      margin-top: 0;
    }
  }
  .el-radio-group {
    display: flex;
    flex-direction: column;
    .el-radio {
      margin-left: 0;
      margin-top: 15px;
      display: block;
    }
    .el-radio__label {
      word-break: break-all;
      white-space: break-spaces;
      white-space: normal;
    }
  }
}
</style>
