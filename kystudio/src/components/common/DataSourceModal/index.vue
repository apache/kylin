<template>
  <el-dialog :class="['data-srouce-modal', {'source-modal-limit': sourceHive}]" :width="modelWidth"
    :title="$t(modalTitle)"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @open="handleOpen"
    @close="() => handleClose()"
    @closed="handleClosed">
    <template v-if="isFormShow">
      <SourceSelect
        v-if="editType === editTypes.SELECT_SOURCE"
        :source-type="sourceType"
        @input="handleInputDatasource">
      </SourceSelect>
      <SourceHiveSetting
        ref="source-hive-setting-form"
        v-if="[editTypes.CONFIG_SOURCE, editTypes.VIEW_SOURCE].includes(editType) && [editTypes.HIVE].includes(sourceType)"
        :form="form.settings"
        :edit-type="editType"
        :is-editable="editType !== editTypes.VIEW_SOURCE"
        @input="(key, value) => handleInput(`settings.${key}`, value)">
      </SourceHiveSetting>
      <SourceHive
        v-if="sourceHive"
        ref="source-hive-form"
        :databases-size="databaseSizeObj"
        :source-type="sourceType"
        :selected-tables="form.selectedTables"
        :selected-databases="form.selectedDatabases"
        :need-sampling="form.needSampling"
        :sampling-rows="form.samplingRows"
        @input="handleInputTableOrDatabase">
      </SourceHive>
      <SourceKafka
        v-if="sourceKafka"
        ref="kafka-form"
        :source-type="sourceType"
        :convert-data-store="form.convertData"
        :sample-data-store="form.sampleData"
        :tree-data-store="form.treeData"
        @input="handleInputKafkaData">
      </SourceKafka>
      <SourceKafkaStep2
        v-if="sourceKafkaStep2"
        ref="kafka-form2"
        :source-type="sourceType"
        :convert-data="form.convertData"
        :column-data="form.columnData"
        @input="handleInputKafkaData">
      </SourceKafkaStep2>
      <!-- <SourceGbase
        v-if="sourceGbase"
        ref="gbase-datasource"
        :source-type="sourceType"
      >
      </SourceGbase> -->
      <SourceCSVConnect
        ref="source-csv-connection-form"
        :form="form.csvSettings"
        v-if="[editTypes.CSV].includes(editType)">
      </SourceCSVConnect>
      <SourceCSVSetting
        @lockStep="lockStep"
        ref="source-csv-setting-form"
       :form="form.csvSettings"
        v-if="[editTypes.CONFIG_CSV_SETTING].includes(editType) && [editTypes.CSV].includes(sourceType)">
      </SourceCSVSetting>
      <SourceCSVStructure
        @lockStep="lockStep"
        ref="source-csv-structure-form"
        :form="form.csvSettings"
        v-if="[editTypes.CONFIG_CSV_STRUCTURE].includes(editType) && [editTypes.CSV].includes(sourceType)">
      </SourceCSVStructure>
      <SourceCSVSql
        ref="source-csv-sql-form"
        :form="form.csvSettings"
        v-if="[editTypes.CONFIG_CSV_SQL].includes(editType) && [editTypes.CSV].includes(sourceType)">
      </SourceCSVSql>
    </template>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button size="medium" :disabled="stepLocked || isLoading" @click="handleCancel" v-if="cancelText">{{cancelText}}</el-button>
      <el-button type="primary" size="medium" :disabled="stepLocked" :key="editType" @click="handleSubmit" v-if="confirmText" :loading="isLoading">{{confirmText}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { titleMaps, cancelMaps, confirmMaps, getSubmitData, editTypes } from './handler'
import { handleSuccessAsync, handleError, indexOfObjWithSomeKey } from '../../../util'
import { set } from '../../../util/object'

import SourceSelect from './SourceSelect/SourceSelect.vue'
import SourceHiveSetting from './SourceHiveSetting/SourceHiveSetting.vue'
import SourceHive from './SourceHive/SourceHive.vue'
import SourceKafka from './SourceKafka/SourceKafka.vue'
import SourceKafkaStep2 from './SourceKafkaStep2/SourceKafkaStep2.vue'
import SourceGbase from './SourceJDBC/SourceGbase/SourceGbase.vue'
vuex.registerModule(['modals', 'DataSourceModal'], store)

@Component({
  components: {
    SourceSelect,
    SourceHiveSetting,
    SourceHive,
    SourceKafka,
    SourceKafkaStep2,
    SourceGbase
  },
  computed: {
    ...mapState('DataSourceModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback,
      firstEditType: state => state.firstEditType,
      databaseSizeObj: state => state.databaseSizeObj
    }),
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapMutations('DataSourceModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      initForm: types.INIT_FORM,
      setModalForm: types.SET_MODAL_FORM
    }),
    ...mapActions({
      loadAllProject: 'LOAD_ALL_PROJECT',
      updateProject: 'UPDATE_PROJECT',
      importTable: 'LOAD_HIVE_IN_PROJECT',
      saveCsvDataSourceInfo: 'SAVE_CSV_INFO',
      saveSourceConfig: 'SAVE_SOURCE_CONFIG',
      updateProjectDatasource: 'UPDATE_PROJECT_DATASOURCE',
      convertTopicJson: 'CONVERT_TOPIC_JSON',
      saveKafka: 'SAVE_KAFKA',
      checkConnectByGbase: 'CHECK_BASE_CONFIG'
    })
  },
  locales
})
export default class DataSourceModal extends Vue {
  isLoading = false
  isDisabled = false
  isFormShow = false
  editTypes = editTypes
  prevSteps = []
  stepLocked = false
  lockStep (status) {
    this.stepLocked = status
  }
  get modalTitle () {
    return titleMaps[this.editType]
  }

  get modelWidth () { return this.editType === editTypes.HIVE || this.editType === editTypes.GBASE ? '960px' : (this.editType === editTypes.SELECT_SOURCE || this.editType === editTypes.GBASE ? '600px' : '780px') }
  get confirmText () { return this.$t(confirmMaps[this.editType]) }
  get cancelText () {
    return this.firstEditType === this.editType ? this.$t('kylinLang.common.cancel') : this.$t(cancelMaps[this.editType])
  }
  get sourceType () { return this.form.project.override_kylin_properties['kylin.source.default'] }
  get sourceHive () { return [this.editTypes.HIVE, this.editTypes.RDBMS, this.editTypes.RDBMS2, this.editTypes.GBASE].includes(this.editType) }
  get sourceKafka () { return this.editType === this.editTypes.KAFKA }
  get sourceKafkaStep2 () { return this.editType === this.editTypes.KAFKA2 }
  // get sourceGbase () { return this.editType === this.editTypes.GBASE }

  handleInput (key, value) {
    this.setModalForm(set(this.form, key, value))
  }
  handleInputTableOrDatabase (payload) {
    this.setModalForm(payload)
  }
  handleInputKafkaData (payload) {
    this.setModalForm(payload)
  }
  handleInputDatasource (value) {
    const properties = { ...this.form.project.override_kylin_properties }
    properties['kylin.source.default'] = value || 8
    this.handleInput('project.override_kylin_properties', properties)
  }
  handleOpen () {
    this._showForm()
  }
  handleClose (isSubmit) {
    this._hideLoading()
    this.hideModal()
    this.prevSteps = []
    this.callback && this.callback(isSubmit)
  }
  handleClosed () {
    this._hideForm()
    this.initForm()
  }
  handleCancel () {
    // for datasource config
    // if (this.firstEditType !== editTypes.SELECT_SOURCE || this.editType === editTypes.SELECT_SOURCE) {
    //   this.handleClose(false)
    // } else if (this.editType === editTypes.CONFIG_SOURCE) {
    //   this.setModal({ editType: editTypes.SELECT_SOURCE })
    // } else {
    //   this.setModal({ editType: editTypes.CONFIG_SOURCE })
    // }

    if (this.prevSteps.length === 0) {
      this.handleClose(false)
    } else {
      this.setModal({ editType: this.prevSteps.pop() })
    }
  }
  async handleSubmit () {
    this._showLoading()
    try {
      if (await this._validate()) {
        try {
          const results = await this._submit()
          if (results) {
            if (this.editTypes.HIVE === this.editType) {
              results.sourceType = 9
            } else { // 目前只有Hive和Kafka数据源
              results.sourceType = 1
            }
            this.handleClose(results)
          }
        } catch (e) {
          handleError(e)
          this.prevSteps.pop()
        }
      }
    } catch (e) {
      handleError(e)
    }
    this._hideLoading()
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
  async _submit () {
    const submitData = getSubmitData(this.form, this.editType)
    this.prevSteps.push(this.editType)
    switch (this.editType) {
      // for datasource config
      // case editTypes.SELECT_SOURCE: {
      //   await this.updateProject(submitData)
      //   return this.setModal({ editType: editTypes.CONFIG_SOURCE })
      // }
      // case editTypes.CONFIG_SOURCE: {
      //   await this.saveSourceConfig(submitData)
      //   return this.setModal({ editType: this.form.project.override_kylin_properties['kylin.source.default'] })
      // }
      case editTypes.SELECT_SOURCE: {
        await this.updateProjectDatasource(submitData)
        // TODO HA 模式时 post 等接口需要等待同步完去刷新列表
        // await handleWaiting()
        await this.loadAllProject()
        return this.setModal({ editType: this.form.project.override_kylin_properties['kylin.source.default'] })
      }
      case editTypes.VIEW_SOURCE: {
        return this.handleClose(false)
      }
      case editTypes.CSV: {
        if (this.form.csvSettings.addTableType === 0) {
          return this.setModal({ editType: editTypes.CONFIG_CSV_SETTING })
        } else {
          return this.setModal({ editType: editTypes.CONFIG_CSV_SQL })
        }
      }
      case editTypes.CONFIG_CSV_SETTING: {
        return this.setModal({ editType: editTypes.CONFIG_CSV_STRUCTURE })
      }
      case editTypes.CONFIG_CSV_STRUCTURE: {
        const response = await this.saveCsvDataSourceInfo({type: 'guide', data: submitData})
        return await handleSuccessAsync(response)
      }
      case editTypes.CONFIG_CSV_SQL: {
        const response = await this.saveCsvDataSourceInfo({type: 'expert', data: submitData})
        return await handleSuccessAsync(response)
      }
      case editTypes.HIVE:
      case editTypes.GBASE:
      case editTypes.RDBMS:
      case editTypes.RDBMS2: {
        const response = await this.importTable(submitData)
        // TODO HA 模式时 post 等接口需要等待同步完去刷新列表
        // await handleWaiting()
        return await handleSuccessAsync(response)
      }
      case editTypes.KAFKA: {
        const response = await this.convertTopicJson(submitData)
        this.setModalForm({ columnData: response.body.data })
        return this.setModal({ editType: editTypes.KAFKA2 })
      }
      case editTypes.KAFKA2: {
        if (!submitData.isShowHiveTree) { // 如果选择不关联，确保关联的Hive去掉
          submitData.kafka_config.batch_table_identity = ''
          submitData.kafka_config.has_shadow_table = false
        }
        const response = await this.saveKafka(submitData)
        return await handleSuccessAsync(response)
      }
    }
  }
  async _validate () {
    switch (this.editType) {
      case editTypes.SELECT_SOURCE: {
        const isValid = this.form.project.override_kylin_properties['kylin.source.default']
        !isValid && this.$message(this.$t('pleaseSelectSource'))
        return isValid
      }
      case editTypes.CONFIG_SOURCE: {
        return await this.$refs['source-hive-setting-form'].$refs.form.validate()
      }
      case editTypes.HIVE:
      case editTypes.RDBMS:
      case editTypes.GBASE:
      case editTypes.RDBMS2: {
        const isValid = this.form.selectedTables.length || this.form.selectedDatabases.length
        !isValid && this.$message(this.$t('pleaseSelectTableOrDatabase'))
        let isSamplingValid = true
        if (this.form.needSampling) {
          isSamplingValid = !!this.form.samplingRows && this.form.samplingRows >= 10000 && this.form.samplingRows <= 20000000
        }
        if (!isSamplingValid) {
          this.$refs['source-hive-form'].$emit('samplingFormValid')
        }
        return isValid && isSamplingValid
      }
      case editTypes.CSV: {
        return await this.$refs['source-csv-connection-form'].$refs.form.validate()
      }
      case editTypes.CONFIG_CSV_SETTING: {
        return await this.$refs['source-csv-setting-form'].$refs.form.validate()
      }
      case editTypes.CONFIG_CSV_STRUCTURE: {
        return await this.$refs['source-csv-structure-form'].$refs.form.validate()
      }
      case editTypes.CONFIG_CSV_SQL: {
        return await this.$refs['source-csv-sql-form'].$refs.form.validate()
      }
      case editTypes.KAFKA: {
        const isValid = this.form.convertData
        !isValid && this.$message(this.$t('pleaseGetClusterInfoAndTopicInfo'))
        return isValid
      }
      case editTypes.KAFKA2: {
        const submitData = getSubmitData(this.form, this.editType)
        let isValid = true
        if (!await this.$refs['kafka-form2'].$refs.kafkaForm.validate()) {
          isValid = false
          return
        }
        if (submitData.isShowHiveTree && !submitData.kafka_config.has_shadow_table) {
          isValid = false
          this.$message(this.$t('needAttachedHiveTableInfo'))
          return
        }
        const columnDateTypes = submitData.table_desc.columns.map(c => {
          return c.datatype
        })
        if (!submitData.isShowHiveTree && columnDateTypes.indexOf('timestamp') === -1) {
          isValid = false
          this.$message(this.$t('needTimestampColumnInfo'))
          return
        }
        if (submitData.isShowHiveTree && submitData.table_desc.columns.length !== submitData.batchTableColumns.length) {
          isValid = false
          this.$message(this.$t('hiveTableErrorTips'))
          return
        }
        if (submitData.isShowHiveTree && submitData.table_desc.columns.length === submitData.batchTableColumns.length) {
          for (let i = 0; i <= submitData.table_desc.columns.length - 1; i++) {
            const index = indexOfObjWithSomeKey(submitData.batchTableColumns, 'name', submitData.table_desc.columns[i].name)
            if (index === -1) {
              isValid = false
              this.$message(this.$t('hiveTableErrorTips'))
              return
            }
          }
          const batchTableColumnDateTypes = submitData.batchTableColumns.map(c => {
            return c.datatype
          })
          if (batchTableColumnDateTypes.indexOf('timestamp') === -1) {
            isValid = false
            this.$message(this.$t('needTimestampColumnInfo'))
            return
          }
        }
        return isValid
      }
      default:
        return true
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.data-srouce-modal {
  .el-dialog {
    margin-top:5vh!important;
    max-width: none !important;
  }
  .el-dialog__body {
    padding: 0;
  }
  .create-kafka {
    padding: 24px 24px 0 24px;
  }
  .source-csv {
    padding: 24px 24px 0;
  }
  &.source-modal-limit {
    .el-dialog {
      margin-top: auto !important;
      position: absolute;
      top: 0;
      bottom: 0;
      left: 0;
      right: 0;
      margin: auto;
      height: 647px;
    }
    .el-dialog__body {
      height: 524px;
      max-height: inherit !important;
      overflow: hidden !important;
    }
  }
}
</style>
