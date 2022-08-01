<template>
  <el-dialog append-to-body limited-area :title="$t('adddimension')" @close="isShow && handleClose(false)"  width="480px" :visible="isShow" class="add-dimension-dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <div>
      <!-- <div class="ky-tips-box">
        <div class="tips-title">Reminder!</div>
        <p class="tips-content">
          你可以於 Dimension Candidate 新增或選擇已建立的 Computer Column,按下此按鈕       可新增 Computer Column，利用拖曳加入則無此功能
        </p>
      </div> -->
      <el-form v-if="isFormShow" :model="dimensionInfo" :rules="rules"  ref="dimensionForm" label-width="100px" label-position="top" class="demo-ruleForm">
        <el-form-item :label="$t('dimensionName')" prop="name">
          <el-input v-model.trim="dimensionInfo.name" ></el-input>
        </el-form-item>
        <el-form-item :label="$t('dimensionCandidate')" prop="column">
          <div class="measure-flex-row">
            <div class="flex-item">
              <el-tooltip :content="disableDelDimTips" placement="bottom" :disabled="!isSecondStorageCannotEdit">
                <el-select filterable style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="showCC||isSecondStorageCannotEdit" v-model="dimensionInfo.column">
                  <el-option-group
                    v-for="(columns, key) in allColumnsGroup"
                    :key="key"
                    :label="$t(key)">
                    <el-option v-for="(item, index) in columns"
                    :key="index"
                    :value="item.table_alias + '.' + item.name">
                      <span>{{item.table_alias + '.' + item.name}}</span>
                      <span class="ky-option-sub-info">{{item.datatype}}</span>
                    </el-option>
                  </el-option-group>
                </el-select>
              </el-tooltip>
            </div>
            <common-tip :content="$t('addCCTip')" ><el-button size="medium" @click="showCCForm" :disabled="showCC||isSecondStorageCannotEdit" icon="el-ksd-icon-auto_computed_column_old" class="ksd-ml-10" type="primary" plain></el-button></common-tip>
          </div>
          <CCEditForm v-if="showCC" @saveSuccess="saveCC" @delSuccess="delCC" :ccDesc="ccDesc" :modelInstance="modelInstance"></CCEditForm>
        </el-form-item>
        <!-- <el-form-item :label="$t('dimensionComment')" prop="comment">
          <el-input type="textarea" v-model="dimensionInfo.comment"></el-input>
        </el-form-item> -->
      </el-form>
    </div>
    <span slot="footer" class="dialog-footer ky-no-br-space">
      <el-button plain @click="isShow && handleClose(false)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" @click="submit">{{$t('kylinLang.common.ok')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../store'
import locales from './locales'
import CCEditForm from '../ComputedColumnForm/ccform.vue'
import store, { types } from './store'
import { NamedRegex1 } from 'config'
import { objectClone } from 'util/index'
vuex.registerModule(['modals', 'SingleDimensionModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'dimMeasNameMaxLength'
    ]),
    // Store数据注入
    ...mapState('SingleDimensionModal', {
      callback: state => state.callback,
      isShow: state => state.isShow,
      dimension: state => state.form.dimension,
      modelInstance: state => state.form.modelInstance
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('SingleDimensionModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
    })
  },
  components: {
    CCEditForm
  },
  locales
})
export default class SingleDimensionModal extends Vue {
  isLoading = false
  isFormShow = false
  showCC = false
  ccDesc = null
  dimensionStr = JSON.stringify({
    name: '',
    column: '',
    comment: '',
    status: 'DIMENSION'
  })
  dimensionInfo = JSON.parse(this.dimensionStr)
  selectColumns = []
  allBaseColumns = []
  rules = {
    name: [
      {required: true, validator: this.checkName, trigger: 'blur'}
    ],
    column: [
      { required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change' },
      { validator: this.checkColumn }
    ]
  }
  checkName (rule, value, callback) {
    if (!NamedRegex1.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip2')))
    } else if (!this.modelInstance.checkSameEditDimensionName(this.dimensionInfo)) {
      callback(new Error(this.$t('sameName')))
    } else if (value.length > this.dimMeasNameMaxLength) {
      callback(new Error(this.$t('kylinLang.common.nameMaxLen', {len: this.dimMeasNameMaxLength})))
    } else {
      callback()
    }
  }
  checkColumn (rule, value, callback) {
    if (!this.modelInstance.checkSameEditDimensionColumn(this.dimensionInfo)) {
      callback(new Error(this.$t('sameColumn')))
    } else {
      callback()
    }
  }
  showCCForm () {
    this.showCC = true
    this.dimensionInfo.column = ''
  }
  saveCC (cc) {
    this.dimensionInfo.datatype = cc.datatype
    this.dimensionInfo.column = cc.tableAlias + '.' + cc.columnName
    this.dimensionInfo.cc = cc
  }
  delCC (cc) {
    this.ccDesc = null
    this.showCC = false
    this.dimensionInfo.column = ''
    this.dimensionInfo.cc = null
    this.dimensionInfo.isCC = false
  }
  get isHybridModel () {
    const factTable = this.modelInstance.getFactTable()
    return factTable.source_type === 1 || ['HYBRID'].includes(this.modelInstance.model_type)
  }
  get disableDelDimTips () {
    if (this.isHybridModel) {
      return this.$t('streamTips')
    } else {
      return this.$t('disableDelDimTips')
    }
  }
  get isSecondStorageCannotEdit () {
    return (this.modelInstance.second_storage_enabled || this.isHybridModel) && this.modelInstance.partition_desc && this.modelInstance.partition_desc.partition_date_column === this.dimensionInfo.column
  }
  get allColumnsGroup () {
    if (this.modelInstance) {
      let ccColumns = this.modelInstance.getComputedColumns()
      let cloneCCList = objectClone(ccColumns)
      cloneCCList = cloneCCList.map((x) => {
        x.name = x.columnName
        x.table_alias = x.tableAlias
        return x
      })
      return {
        columns: this.allBaseColumns,
        ccColumns: cloneCCList
      }
    }
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      if (this.dimension && this.dimension.table_guid) {
        if (this.dimensionInfo.cc) {
          this.showCC = true
          this.ccDesc = this.dimensionInfo.cc
        } else {
          this.showCC = false
          this.ccDesc = null
        }
      } else {
        this.dimensionInfo = JSON.parse(this.dimensionStr)
        this.showCC = false
        this.ccDesc = null
      }
      Object.assign(this.dimensionInfo, this.dimension)
      const columnsList = []
      Object.values(this.modelInstance.tables).forEach(item => {
        item.columns && item.columns.forEach(it => {
          columnsList.push(it.name)
        })
      })
      if (columnsList.indexOf(this.dimensionInfo.name) !== columnsList.lastIndexOf(this.dimensionInfo.name)) {
        this.dimensionInfo.name = this.dimension.column.split('.').reverse().join('_')
      }
      this.allBaseColumns = this.modelInstance.getTableColumns()
      this.isFormShow = true
    } else {
      this.dimensionInfo = JSON.parse(this.dimensionStr)
      setTimeout(() => {
        this.isFormShow = false
      }, 200)
    }
  }
  async submit () {
    this.$refs.dimensionForm.validate((valid) => {
      if (!valid) { return }
      this.handleClose(true)
    })
  }
  @Watch('dimensionInfo.column')
  getColumnInfo (fullName) {
    if (fullName) {
      let dimensionNamed = fullName.split('.')
      let alias = dimensionNamed[0]
      let column = dimensionNamed[1]
      let ccObj = this.modelInstance.getCCObj(alias, column)
      if (ccObj) {
        this.dimensionInfo.cc = ccObj
        this.dimensionInfo.isCC = true
        this.showCC = true
      } else {
        this.dimensionInfo.cc = null
        this.dimensionInfo.isCC = false
        this.showCC = false
      }
      this.ccDesc = ccObj
      this.dimensionInfo.isCC = !!ccObj
    }
  }
  _handleCloseFunc (isSubmit) {
    let dimension = objectClone(this.dimensionInfo)
    this.hideModal()
    setTimeout(() => {
      this.callback && this.callback({
        isSubmit: isSubmit,
        data: {
          dimension: dimension
        }
      })
      this.resetModalForm()
    }, 300)
  }
  handleClose (isSubmit) {
    if (!isSubmit) {
      this._handleCloseFunc(isSubmit)
      return
    }
    let dimensionNamed = this.dimensionInfo.column.split('.')
    let ntable = this.modelInstance.getTableByAlias(dimensionNamed[0])
    this.dimensionInfo.table_guid = ntable.guid
    if (this.dimensionInfo.isCC) {
      this.dimensionInfo.datatype = this.dimensionInfo.datatype
    } else {
      let datatype = ntable.getColumnType(dimensionNamed[1])
      this.dimensionInfo.datatype = datatype
    }
    if (this.dimension && this.dimension.table_guid) {
      this.modelInstance.editDimension(this.dimensionInfo, this.dimensionInfo._id).then(() => {
        this._handleCloseFunc(isSubmit)
      }, () => {
        console.log('dimsnion命名重复')
      })
    } else {
      this.modelInstance.addDimension(this.dimensionInfo).then(() => {
        this._handleCloseFunc(isSubmit)
      }, () => {
        console.log('dimsnion命名重复')
      })
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.add-dimension-dialog{
  .measure-flex-row {
    display: flex;
    align-items: center;
    .flex-item {
      flex-shrink: 1;
      width: 100%;
    }
  }
  .cc-area{
    background-color: @table-stripe-color;
    border:solid 1px @line-split-color;
    padding:24px 20px 20px 20px;
    margin-top: 10px;
  }
}

</style>
