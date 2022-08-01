<template>
  <el-dialog append-to-body limited-area :title="$t('addJoinCondition')" @close="isShow && handleClose(false)" width="720px" :visible="isShow" class="links-dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <!-- <el-alert
      :title="$t('tableRelationTips', {tableName: pTableName})"
      type="warning"
      class="ksd-mb-15"
      show-icon
      :closable="false"
      v-if="showTRelationTips">
    </el-alert> -->
    <p class="join-notices"><i :class="[!isErrorValue.length ? 'el-icon-ksd-alert' : 'el-icon-ksd-error_01 is-error']"></i>{{$t(!isErrorValue.length ? 'joinNotice' : 'joinErrorNotice')}}<span class="review-details" @click="showDetails = !showDetails">{{$t('details')}}<i :class="[showDetails ? 'el-icon-ksd-more_01-copy' : 'el-icon-ksd-more_02', 'arrow']"></i></span></p>
    <div class="detail-content" v-if="showDetails">
      <p :class="[item.isError && 'is-error']" v-for="item in getDetails" :key="item.value"><i class="point">•</i>{{item.text}}</p>
    </div>
    <p class="title-label ksd-mt-15">{{$t('tableJoin')}}</p>
    <el-row :gutter="10">
      <el-col :span="10">
        <el-select :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" @change="changeFTable" style="width:100%" filterable v-model="selectF">
          <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!selectF"></i>
          <el-option  v-for="key in selectedFTables" :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
      <el-col :span="4">
        <el-select
          :placeholder="$t('kylinLang.common.pleaseSelect')"
          style="width:100%"
          class="link-type"
          popper-class="js_link-type"
          v-model="joinType">
          <el-option :value="key" v-for="(key, i) in linkKind" :key="i">{{key}}</el-option>
        </el-select>
      </el-col>
      <el-col :span="10">
        <el-select :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"  @change="changePTable" size="medium" style="width:100%" filterable v-model="selectP">
          <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!selectP"></i>
          <el-option v-for="key in selectedPTables"  :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
    </el-row>
    <p class="title-label ksd-mt-15">{{$t('tableRelation')}}</p>
    <el-row :gutter="10" class="ksd-mt-10">
      <el-col :span="10">
        <el-select style="width:100%" v-model="selectTableRelation">
          <el-option  v-for="key in selectedTRelations" :value="key.value" :key="key.value" :label="$t(key.label)"></el-option>
        </el-select>
      </el-col>
      <span class="precompute-check ksd-ml-5"><el-checkbox v-model="isPrecompute">{{$t('precomputeJoin')}}</el-checkbox><el-tooltip effect="dark" placement="top"><span slot="content" v-html="$t('precomputeJoinTip')"></span><i class="el-ksd-icon-more_info_22 icon ksd-ml-5"></i></el-tooltip></span>
    </el-row>
    <!-- <div class="ky-line ksd-mt-15"></div> -->
    <!-- 列的关联 -->
    <p class="title-label ksd-mt-20 ksd-mb-5">{{$t('columnsJoin')}}</p>
    <el-form class="join-form clearfix" ref="conditionForm" :model="joinColumns">
      <el-form-item v-for="(key, val) in joinColumns.foreign_key" :key="val" class="ksd-mb-6">
        <el-form-item :prop="'foreign_key.' + val" :rules="[{validator: checkIsBrokenForeignKey, trigger: 'change'}]">
          <el-select
            size="small"
            :class="['foreign-select', {'is-error': errorFlag.includes(val)}]"
            filterable
            v-model="joinColumns.foreign_key[val]"
            popper-class="js_foreign-select"
            :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')">
              <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!joinColumns.foreign_key[val]"></i>
            <el-option :disabled="true" v-if="checkIsBroken(brokenForeignKeys, joinColumns.foreign_key[val])" :value="joinColumns.foreign_key[val]" :label="joinColumns.foreign_key[val].split('.')[1]"></el-option>
            <el-option v-for="f in fColumns" :value="fTable.alias+'.'+f.name" :key="f.name" :label="f.name">
            </el-option>
          </el-select>
        </el-form-item>
        <el-select
          size="small"
          :class="['join-type', {'is-error': errorFlag.includes(val)}]"
          :placeholder="$t('kylinLang.common.pleaseSelect')"
          popper-class="js_join-type"
          v-model="joinColumns.op[val]">
          <el-option :value="item.value" :label="item.label" :disabled="['GREATER_THAN_OR_EQUAL', 'LESS_THAN'].includes(item.value) && !scd2_enabled" v-for="item in columnsLinkKind" :key="'joinColumnstype' + item.value"></el-option>
        </el-select>
        <el-form-item :prop="'primary_key.' + val" :rules="[{validator: checkIsBrokenPrimaryKey, trigger: 'change'}]">
          <el-select
            size="small"
            :class="['primary-select', {'is-error': errorFlag.includes(val)}]"
            filterable
            v-model="joinColumns.primary_key[val]"
            popper-class="js_primary-select"
            :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')">
            <i slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!joinColumns.primary_key[val]"></i>
            <el-option :disabled="true" v-if="checkIsBroken(brokenPrimaryKeys, joinColumns.primary_key[val])" :value="joinColumns.primary_key[val]" :label="joinColumns.primary_key[val].split('.')[1]"></el-option>
            <el-option v-for="p in pColumns" :value="pTable.alias+'.'+p.name" :key="p.name" :label="p.name">
            </el-option>
          </el-select>
        </el-form-item>
        <el-button  type="primary" class="ksd-ml-10" plain icon="el-ksd-icon-add_22" size="mini" @click="addJoinConditionColumns" circle></el-button><el-button  icon="el-ksd-icon-minus_22" size="mini" @click="removeJoinConditionColumn(val)" circle></el-button>
      </el-form-item>
    </el-form>
    <span slot="footer" class="dialog-footer">
      <!-- <el-button @click="delConn" v-if="currentConnObj" size="medium" class="ksd-fleft">{{$t('delConn')}}</el-button> -->
      <el-button plain @click="isShow && handleClose(false)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" :disabled="disableSaveBtn" @click="saveJoinCondition">{{$t('kylinLang.common.ok')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'
import { modelRenderConfig } from '../ModelEdit/config'
import vuex from '../../../../store'
import { handleError, handleSuccessAsync, objectClone } from '../../../../util'
import locales from './locales'
import store, { types } from './store'
import { kylinMessage, kylinConfirm } from 'util/business'
vuex.registerModule(['modals', 'TableJoinModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('TableJoinModal', {
      isShow: state => state.isShow,
      form: state => state.form,
      callback: state => state.callback
    }),
    ...mapState({
      scd2_enabled: state => state.project.scd2_enabled
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('TableJoinModal', {
      hideModal: types.HIDE_MODAL,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      invalidIndexes: 'INVALID_INDEXES'
    })
  },
  locales
})

export default class TableJoinModal extends Vue {
  isLoading = false
  isFormShow = false
  linkKind = modelRenderConfig.joinKind // 默认可选的连接类型
  joinType = '' // 选择连接的类型
  selectF = '' // 选择的外键表的alias名
  selectP = '' // 选择的主键表的alias名
  selectTableRelation = 'MANY_TO_ONE'
  joinColumns = {
    foreign_key: [''],
    op: [''], // 新加的列的关联关系的字段
    primary_key: ['']
  } // join信息
  columnsLinkKind = [
    {label: '=', value: 'EQUAL'},
    {label: '>=', value: 'GREATER_THAN_OR_EQUAL'},
    {label: '<', value: 'LESS_THAN'}
  ]
  columnJoinType = 0
  isErrorValue = []
  showDetails = false
  disableSaveBtn = false
  errorFlag = []
  selectedTRelations = [
    {value: 'MANY_TO_ONE', label: 'manyToOne'},
    // {value: 'ONE_TO_ONE', label: 'oneToOne'},
    // {value: 'ONE_TO_MANY', label: 'oneToMany'},
    {value: 'MANY_TO_MANY', label: 'manyToMany'}
  ]
  isPrecompute = true
  joinConditionBackUp = null
  // get showTRelationTips () {
  //   if (this.selectTableRelation === 'ONE_TO_MANY' || this.selectTableRelation === 'MANY_TO_MANY') {
  //     return true
  //   } else {
  //     return false
  //   }
  // }

  // 是否 disabled 预计算关联关系 checkbox
  // get disabledCheckPrecomputed () {
  //   const status = this.selectTableRelation === 'MANY_TO_ONE'
  //   this.isPrecompute = !status
  //   return status
  // }

  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
      this.$nextTick(() => {
        this.$refs.conditionForm && this.$refs.conditionForm.validate()
      })
      this.selectP = this.form.pid || ''
      this.selectF = this.form.fid || ''
      this.joinType = this.form.joinType || 'INNER'
      this.selectTableRelation = this.form.selectTableRelation || 'MANY_TO_ONE'
      let ptable = this.form.tables[this.selectP]
      // let ftable = this.form.tables[this.selectF]
      let joinData = ptable && ptable.getJoinInfoByFGuid(this.selectF) || null
      if (joinData) { // 有join数据的情况
        var joinInfo = joinData.join
        this.joinColumns.foreign_key = objectClone(joinInfo.foreign_key)
        this.joinColumns.primary_key = objectClone(joinInfo.primary_key)
        this.joinColumns.op = objectClone(joinInfo.op)
        this.joinType = joinInfo.type
        this.isPrecompute = typeof joinData.flattenable !== 'undefined' ? joinData.flattenable === 'flatten' : true
      } else { // 无join数据的情况,设置默认值
        this.$set(this.joinColumns, 'foreign_key', [''])
        this.$set(this.joinColumns, 'primary_key', [''])
        this.$set(this.joinColumns, 'op', [''])
        this.isPrecompute = true
      }
      // 拖动添加默认填充
      // 如果添加的是重复的关联关系不做处理
      if (this.form.fColumnName && this.form.pColumnName) {
        let findex = this.joinColumns.foreign_key.indexOf(this.form.fColumnName)
        let pindex = this.joinColumns.primary_key.indexOf(this.form.pColumnName)
        if (findex === pindex && findex >= 0) {
          return
        }
      }
      if (this.form.fColumnName) {
        if (this.joinColumns.foreign_key[0]) {
          this.joinColumns.foreign_key.push(this.form.fColumnName)
        } else {
          this.joinColumns.foreign_key[0] = this.form.fColumnName
        }
      }
      if (this.form.pColumnName) {
        if (this.joinColumns.primary_key[0]) {
          this.joinColumns.primary_key.push(this.form.pColumnName)
        } else {
          this.joinColumns.primary_key[0] = this.form.pColumnName
        }
      }
      if (this.form.fColumnName && this.form.pColumnName) {
        if (this.joinColumns.op[0]) {
          this.joinColumns.op.push('EQUAL')
        } else {
          this.joinColumns.op[0] = 'EQUAL'
        }
      }
    } else {
      this.showDetails = false
      this.isErrorValue = []
    }
    newVal && this.backUpJoinCondition()
  }
  @Watch('joinType')
  @Watch('joinColumns', {deep: true})
  changeJoinCondition (oldVal, newVal) {
    let type = this.checkNoEqualRelation(this.joinColumns) && this.checkLinkCompelete()
    this.disableSaveBtn = !type
  }
  get getDetails () {
    let notices = [
      { text: this.$t('notice1'), value: 1, isError: false },
      { text: this.$t('notice2'), value: 2, isError: false },
      { text: this.$t('notice3'), value: 3, isError: false },
      { text: this.$t('notice4'), value: 4, isError: false }
    ]
    return notices.map(item => {
      return {...item, isError: this.isErrorValue.includes(item.value)}
    })
  }

  // 格式化一开始的 join 关系
  backUpJoinCondition () {
    let {join_tables: joinInfo, tables} = this.form.modelInstance
    if (!joinInfo) return
    joinInfo = joinInfo.map(item => {
      return {
        ...item,
        selectP: Object.keys(tables).filter(it => tables[it].alias === item.alias).length > 0 ? Object.keys(tables).filter(it => tables[it].alias === item.alias)[0] : '',
        selectF: Object.keys(tables).filter(it => tables[it].alias === (item.join.foreign_key[0] ? item.join.foreign_key[0].split('.')[0] : '')).length > 0 ? Object.keys(tables).filter(it => tables[it].alias === (item.join.foreign_key[0] ? item.join.foreign_key[0].split('.')[0] : ''))[0] : ''
      }
    })
    const joinCondition = joinInfo.filter(it => it.selectP === this.selectP && it.selectF === this.selectF)[0]
    if (!joinCondition) return
    this.joinConditionBackUp = Object.freeze({
      selectF: joinCondition.selectF,
      selectP: joinCondition.selectP,
      joinData: {
        foreign_key: joinCondition.join.foreign_key,
        op: joinCondition.join.op,
        primary_key: joinCondition.join.primary_key
      },
      selectTableRelation: joinCondition.join_relation_type,
      joinType: joinCondition.join.type,
      isPrecompute: joinCondition.flattenable
    })
  }

  changeFTable () {
    this.joinColumns.foreign_key = ['']
  }
  changePTable (key) {
    this.joinColumns.primary_key = ['']
    this.pTableName = this.selectedPTables[key].alias
  }
  get pTableName () {
    let tableName = ''
    for (let key in this.selectedPTables) {
      if (this.selectedPTables[key].guid === this.selectP) {
        tableName = this.selectedPTables[key].alias
      }
    }
    return tableName
  }
  get selectedFTables () {
    return this.form.tables && Object.values(this.form.tables).filter((t) => {
      if (t.guid !== this.selectP) {
        return t
      }
    }) || []
  }
  get selectedPTables () {
    return this.form.tables && Object.values(this.form.tables).filter((t) => {
      if (t.guid !== this.selectF) {
        return t
      }
    }) || []
  }
  get fColumns () {
    let ntable = this.fTable
    if (ntable) {
      return ntable.columns
    }
    return []
  }
  get pColumns () {
    let ntable = this.pTable
    if (ntable) {
      return ntable.columns
    }
    return []
  }
  get fTable () {
    return this.form.tables && this.form.tables[this.selectF] || []
  }
  get pTable () {
    return this.form.tables && this.form.tables[this.selectP] || []
  }
  checkIsBrokenPrimaryKey (rule, value, callback) {
    if (value) {
      if (this.checkIsBroken(this.brokenPrimaryKeys, value)) {
        return callback(new Error(this.$t('noColumnFund')))
      }
    }
    callback()
  }
  checkIsBrokenForeignKey (rule, value, callback) {
    if (value) {
      if (this.checkIsBroken(this.brokenForeignKeys, value)) {
        return callback(new Error(this.$t('noColumnFund')))
      }
    }
    callback()
  }
  checkIsBroken (brokenList, key) {
    if (key) {
      return ~brokenList.indexOf(key)
    }
    return false
  }
  get brokenPrimaryKeys () {
    let ntable = this.pTable
    if (ntable && this.isShow) {
      return this.form.modelInstance.getBrokenModelLinksKeys(ntable.guid, this.joinColumns.primary_key)
    }
    return []
  }
  get brokenForeignKeys () {
    let ntable = this.fTable
    if (ntable && this.isShow) {
      return this.form.modelInstance.getBrokenModelLinksKeys(ntable.guid, this.joinColumns.foreign_key)
    }
    return []
  }
  // 添加condition关联列的框
  addJoinConditionColumns () {
    this.$refs.conditionForm.clearValidate()
    this.joinColumns.foreign_key.unshift('')
    this.joinColumns.primary_key.unshift('')
    this.joinColumns.op.unshift('EQUAL')
    this.$nextTick(() => {
      this.$refs.conditionForm.validate()
    })
  }
  // 删除condition关联列的框
  removeJoinConditionColumn (i) {
    this.$refs.conditionForm.clearValidate()
    if (this.joinColumns.foreign_key.length === 1) {
      this.joinColumns.foreign_key.splice(0, 1, '')
      this.joinColumns.op.splice(0, 1, 'EQUAL')
      this.joinColumns.primary_key.splice(0, 1, '')
      this.$nextTick(() => {
        this.$refs.conditionForm.validate()
      })
      return
    }
    this.joinColumns.foreign_key.splice(i, 1)
    this.joinColumns.primary_key.splice(i, 1)
    this.joinColumns.op.splice(i, 1)
    this.$nextTick(() => {
      this.$refs.conditionForm.validate()
    })
  }
  removeDuplicateCondition () {
    let obj = {}
    let { foreign_key, primary_key, op } = this.joinColumns
    for (let i = foreign_key.length - 1; i >= 0; i--) {
      if (obj[foreign_key[i] + primary_key[i] + op[i]]) {
        foreign_key.splice(i, 1)
        op.splice(i, 1)
        primary_key.splice(i, 1)
      } else {
        obj[foreign_key[i] + primary_key[i] + op[i]] = true
      }
    }
  }
  checkLinkCompelete () {
    if (!this.selectF || !this.selectP || this.joinColumns.foreign_key.indexOf('') >= 0 || this.joinColumns.primary_key.indexOf('') >= 0) {
      return false
    }
    return true
  }
  get currentConnObj () {
    if (this.form.modelInstance) {
      return this.form.modelInstance.getConn(this.selectP, this.selectF)
    }
  }
  delConn () {
    kylinConfirm(this.$t('delConnTip'), null, this.$t('delConnTitle')).then(() => {
      if (this.form.modelInstance) {
        this.form.modelInstance
        if (this.currentConnObj) {
          this.form.modelInstance.removeRenderLink(this.currentConnObj)
          this.handleClose(false)
        }
      }
    })
  }

  // 检测关联关系
  checkNoEqualRelation (joinColumns) {
    let flag = true
    this.isErrorValue = []
    this.errorFlag = []
    let joins = joinColumns.foreign_key.map((item, index) => ({fk: item, op: joinColumns.op[index], pk: joinColumns.primary_key[index], index, joinExpression: `${item}&${joinColumns.primary_key[index]}`}))
    let greater_than = joins.filter(it => it.op === 'GREATER_THAN_OR_EQUAL' && it.fk && it.pk)
    let less_than = joins.filter(it => it.op === 'LESS_THAN' && it.fk && it.pk)
    // let equals = joins.filter(it => it.op === 'EQUAL' && it.fk && it.pk)
    // 两张表仅可使用同一关联关系一次
    const pAlias = this.form.tables[this.form.pid].name.split('.')[1]
    const currentJoin = {
      foreign_key: joinColumns.foreign_key,
      op: joinColumns.op,
      primary_key: joinColumns.primary_key.map(it => it.replace(/^(\w+)\./, `${pAlias}.`)),
      type: this.joinType
    }

    if (this.form.modelInstance.compareModelLink(this.form, currentJoin)) {
      // this.errorFlag = [...this.errorFlag, ...cludesIndex]
      this.isErrorValue.push(4)
      flag = false
    }
    // let correctTableName = (pid, fid) => {
    //   return {
    //     pTable: this.form.tables[pid].name.replace(/^(\w+)\./, ''),
    //     tTable: this.form.tables[fid].name.replace(/^(\w+)\./, '')
    //   }
    // }
    // let fk = joinColumns.foreign_key.map(it => it.replace(/(\w+)\./, `${correctTableName(this.form.pid, this.form.fid).tTable}.`))
    // let pk = joinColumns.primary_key.map(it => it.replace(/(\w+)\./, `${correctTableName(this.form.pid, this.form.fid).pTable}.`))
    // let combination = fk.map((item, index) => `${item}&${pk[index]}`)
    // let joinInfoHistory = Object.keys(this.form.modelInstance.linkUsedColumns).filter(it => it.indexOf(this.form.fid) >= 0 && it !== `${this.form.pid}${this.form.fid}`)
    // if (joinInfoHistory.length) {
    //   let tables = []
    //   joinInfoHistory.map(item => this.form.modelInstance.linkUsedColumns[item]).forEach((list, idx) => {
    //     let joinId = joinInfoHistory[idx]
    //     let start = joinId.indexOf(this.form.fid)
    //     let uid = []
    //     if (start === 0) {
    //       uid = [joinId.slice(0, this.form.fid.length), joinId.slice(this.form.fid.length, joinId.length)]
    //     } else {
    //       uid = [joinId.slice(joinId.length - this.form.fid.length, joinId.length), joinId.slice(0, joinId.length - this.form.fid.length)]
    //     }
    //     for (let i = 0; i <= list.length / 2 - 1; i++) {
    //       tables.push(`${list[i + list.length / 2].replace(/(\w+)\./, `${correctTableName(uid[1], uid[0]).tTable}.`)}&${list[i].replace(/(\w+)\./, `${correctTableName(uid[1], uid[0]).pTable}.`)}`)
    //     }
    //   })
    //   if (tables.length) {
    //     let cludesIndex = []
    //     combination.forEach((v, index) => {
    //       tables.includes(v) && cludesIndex.push(index)
    //     })
    //     if (cludesIndex.length) {
    //       this.errorFlag = [...this.errorFlag, ...cludesIndex]
    //       this.isErrorValue.push(4)
    //       flag = false
    //     }
    //   }
    // }
    // 至少有一个equal连接关系
    if (!joinColumns.op.includes('EQUAL')) {
      this.isErrorValue.push(3)
      flag = false
    }
    // 列的关联关系不能重复定义
    joins.forEach((item, index, self) => {
      this.errorFlag = [...this.errorFlag, ...self.map(it => item.index !== it.index && it.joinExpression === item.joinExpression && item.index).filter(it => typeof it === 'number')]
    })
    if (this.errorFlag.length) {
      this.isErrorValue.push(1)
      flag = false
    }
    // 连接关系 >= 和 < 成对出现, 且位于中间的列必须一致(此处判断放在最后处理)
    let checkFun = () => {
      let fl = true
      let firstV = greater_than.shift(0)
      while (firstV) {
        let idx = less_than.findIndex(it => it.fk === firstV.fk)
        if (idx >= 0) {
          less_than.splice(idx, 1)
        } else {
          this.errorFlag.push(firstV.index)
          fl = false
        }
        firstV = greater_than.shift(0)
      }
      if (less_than.length) {
        this.errorFlag = [...this.errorFlag, ...less_than.map(it => it.index)]
        fl = false
      }
      return fl
    }
    if (!checkFun()) {
      this.isErrorValue.push(2)
      flag = false
    }
    !flag && (this.showDetails = true)
    return flag
  }

  // 保存关联关系
  async saveJoinCondition () {
    await this.$refs.conditionForm.validate()
    var joinData = this.joinColumns // 修改后的连接关系
    var selectF = this.selectF // 外键表名
    var selectP = this.selectP // 主键表名
    var selectTableRelation = this.selectTableRelation // 表关系
    let antiFlattenLookups = []
    let computedColumns = []
    // if (this.checkLinkCompelete() && this.checkNoEqualRelation()) {
    // 校验是否链接层环状
    if (this.form && this.form.modelInstance.checkLinkCircle(selectF, selectP)) {
      kylinMessage(this.$t('kylinLang.model.cycleLinkTip'), {type: 'warning'})
      return
    }

    try {
      const res = await this.form.modelInstance.generateMetadata(true)
      const factTable = this.form.modelInstance.getFactTable()
      if (res.join_tables.length === 0 && factTable.name !== this.fTable.name) { // 维度表未空，且连接关系中没有事实表，提示应该先连事实表
        this.$message({
          type: 'error',
          message: this.$t('noFactTableTips')
        })
        return
      }
      // if (res.uuid) {
      const currentJoinIndex = res.join_tables.findIndex(it => it.alias === this.pTableName)
      if (currentJoinIndex > -1) {
        res.join_tables[currentJoinIndex] = {
          ...res.join_tables[currentJoinIndex],
          join: {
            ...joinData,
            type: this.joinType
          },
          join_relation_type: selectTableRelation,
          flattenable: this.isPrecompute ? 'flatten' : 'normalized',
          table: this.form.modelInstance.tables[selectP].name
        }
        res.all_named_columns = this.form.modelInstance.all_named_columns || []
      } else {
        res.join_tables.push({
          alias: this.pTableName,
          join: {
            ...joinData,
            type: this.joinType
          },
          join_relation_type: selectTableRelation,
          flattenable: this.isPrecompute ? 'flatten' : 'normalized',
          table: this.form.modelInstance.tables[selectP].name
        })
        res.all_named_columns = this.form.modelInstance.all_named_columns || []
      }
      const response = await this.invalidIndexes(res)
      const result = await handleSuccessAsync(response)
      const {computed_columns, dimensions, measures, agg_index_count, table_index_count, anti_flatten_lookups} = result
      antiFlattenLookups = anti_flatten_lookups
      computedColumns = computed_columns

      if ((computed_columns.length + dimensions.length + measures.length + agg_index_count + table_index_count) > 0) {
        await this.$msgbox({
          title: this.$t('kylinLang.common.tip'),
          message: <div>
            <span>{this.$t('deletePrecomputeJoinDialogTips')}</span>
            <div class="deleted-precompute-join-content">
              <el-collapse>
                {dimensions.length ? <el-collapse-item title={this.$t('dimensionCollapse', {num: dimensions.length})}>
                  {dimensions.map(it => (<p>{it}</p>))}
                </el-collapse-item> : null}
                {measures.length ? <el-collapse-item title={this.$t('measureCollapse', {num: measures.length})}>
                  {measures.map(it => (<p>{it}</p>))}
                </el-collapse-item> : null}
                {computed_columns.length ? <el-collapse-item title={this.$t('computedColumnCollapse', {num: computed_columns.length})}>
                  {computed_columns.map(it => (<p>{it.columnName}</p>))}
                </el-collapse-item> : null}
                {(agg_index_count || table_index_count) ? <el-collapse-item title={this.$t('indexesCollapse', {num: agg_index_count + table_index_count})}>
                  {agg_index_count ? <div>{this.$t('aggIndexes', {len: agg_index_count})}</div> : null}
                  {table_index_count ? <div>{this.$t('tableIndexes', {len: table_index_count})}</div> : null}
                </el-collapse-item> : null}
              </el-collapse>
            </div>
          </div>,
          type: 'warning',
          cancelButtonText: this.$t('backEdit'),
          showCancelButton: true,
          closeOnClickModal: false,
          closeOnPressEscape: false
        })
      }
    } catch (e) {
      if ('errorKey' in e) {
        e.errorKey === 'noFact' && this.$message({
          type: 'error',
          message: this.$t('noFactTableTips')
        })
        return
      } else {
        handleError(e)
      }
    }

    // 删除重复的条件
    this.removeDuplicateCondition()
    // 传出处理后的结果
    this.handleClose(true, {
      selectF: selectF,
      selectP: selectP,
      joinData: joinData,
      selectTableRelation: selectTableRelation,
      joinType: this.joinType,
      isPrecompute: this.isPrecompute ? 'flatten' : 'normalized',
      anti_flatten_lookups: antiFlattenLookups,
      anti_flatten_cc: computedColumns
    })
  }
  handleClose (isSubmit, data) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback({
        isSubmit: isSubmit,
        data: {...data, isNew: !this.joinConditionBackUp},
        isChange: this.compareJoinCondition(data)
      })
      this.joinConditionBackUp = null
    }, 300)
  }
  compareJoinCondition (data) {
    // if (!this.joinConditionBackUp && data.joinType === 'LEFT') return false
    let newData = JSON.parse(JSON.stringify(data))
    delete newData.anti_flatten_lookups
    delete newData.anti_flatten_cc
    return JSON.stringify(this.joinConditionBackUp) !== JSON.stringify(newData)
  }
  mounted () {
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.links-dialog {
  min-height: 720px;
  .join-notices {
    margin-bottom: 5px;
    i {
      color: @text-disabled-color;
      margin-right: 5px;
    }
    color: @text-title-color;
    font-size: 12px;
    .review-details {
      color: @base-color;
      cursor: pointer;
      position: relative;
    }
    .arrow {
      transform: rotate(90deg);
      margin-left: 3px;
      font-size: 7px;
      color: @base-color;
      position: absolute;
      top: 4px;
    }
    .is-error {
      color: @error-color-1;
    }
  }
  .is-error {
    color: @error-color-1;
  }
  .detail-content {
    background-color: @base-background-color-1;
    padding: 10px 15px;
    box-sizing: border-box;
    font-size: 12px;
    color: @text-normal-color;
    .point {
      // color: @text-normal-color;
      margin-right: 5px;
    }
  }
  .title-label {
    color: @text-title-color;
    font-size: 14px;
    font-weight: bold;
    margin-bottom: 10px;
  }
  .precompute-check {
    line-height: 16px;
    .el-checkbox {
      margin-top: 5px;
      .el-checkbox__inner {
        vertical-align: middle;
      }
    }
    .icon {
      font-size: 22px;
      vertical-align: bottom;
    }
  }
  .el-button+.el-button {
    margin-left:5px;
  }
  .error-msg {display:none}
  .is-broken {
    .el-input__inner{
      border:solid 1px @color-danger;
    }
    .error-msg {
      color:@color-danger;
      display:block;
    }
  }
  .join-form {
    .el-form-item {
      display: block;
      margin-bottom: 0;
      float: left;
    }
    .foreign-select, .primary-select {
      width: 270px;
    }
    .foreign-select.is-error {
      .el-input__inner {
        border:solid 1px @color-danger;
      }
    }
    .primary-select.is-error {
      .el-input__inner {
        border:solid 1px @color-danger;
      }
    }
    .join-type {
      width: 60px;
      margin: 0 5px;
      float: left;
      &.is-error {
        .el-input__inner {
          border:solid 1px @color-danger;
        }
      }
    }
  }
}
.deleted-precompute-join-content {
  background-color: #FAFAFA;
  padding: 5px 10px;
  box-sizing: border-box;
  max-height: 200px;
  margin-top: 8px;
  overflow: auto;
  .el-collapse {
    border: 0;
  }
  .el-collapse-item {
    .el-collapse-item__header {
      background-color: #FAFAFA;
      line-height: 30px;
      height: 30px;
      border: 0;
    }
    .el-collapse-item__wrap {
      border: 0;
      background-color: initial;
      .el-collapse-item__content {
        padding-bottom: 0;
      }
    }
    .el-collapse-item__arrow {
      line-height: 30px;
      float: left;
    }
  }
}
</style>
