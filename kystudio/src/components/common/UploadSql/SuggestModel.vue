<template>
  <div class="model-layout">
    <el-row :gutter="15" v-if="!isOriginModelsTable">
      <el-col :span="15">
        <el-table
          :data="suggestModels"
          class="model-table"
          v-scroll-shadow
          :ref="tableRef"
          style="width: 100%"
          @sort-change="onSortChangeModels"
          @select="handleSelectionModel"
          @selection-change="handleSelectionModelChange"
          @select-all="handleSelectionAllModel"
          @row-click="modelRowClick"
          :row-class-name="setRowClass"
        >
          <el-table-column type="selection" width="44"></el-table-column>
          <el-table-column :label="$t('kylinLang.model.modelNameGrid')">
            <template slot-scope="scope">
              <el-input v-model="scope.row.alias" :class="{'name-error': scope.row.isNameError}" size="small" @change="handleRename(scope.row)"></el-input>
              <div class="rename-error" v-if="scope.row.isNameError">{{modelNameError}}</div>
            </template>
          </el-table-column>
          <el-table-column :label="$t('kylinLang.common.fact')" prop="fact_table" show-overflow-tooltip width="140"></el-table-column>
          <el-table-column label="SQL" prop="sqls" width="100" align="right" sortable="custom" :render-header="renderHeaderSql">
            <template slot-scope="scope">{{scope.row.rec_items[0].sqls.length || 0}}</template>
          </el-table-column>
        </el-table>
      </el-col>
      <el-col class="details" :span="9">
        <el-table
          v-if="activeRowId"
          :data="modelDetails"
          class="model-table"
          v-scroll-shadow
          ref="newModelDetails"
          style="width: 100%">
          <el-table-column type="expand" width="44">
            <template slot-scope="scope">
              <template v-if="scope.row.type === 'cc'">
                <p><span class="label">{{$t('th_expression')}}：</span>{{scope.row.cc.expression}}</p>
              </template>
              <template v-if="scope.row.type === 'dimension'">
                <p><span class="label">{{$t('th_column')}}：</span>{{scope.row.dimension.name}}</p>
              </template>
              <template v-if="scope.row.type === 'measure'">
                <p><span class="label">{{$t('th_column')}}：</span>{{scope.row.measure.name}}</p>
                <p><span class="label">{{$t('th_function')}}：</span>{{scope.row.measure.function.expression}}</p>
                <p><span class="label">{{$t('th_parameter')}}：</span>{{scope.row.measure.function.parameters}}</p>
              </template>
            </template>
          </el-table-column>
          <el-table-column :label="$t('th_name')" show-overflow-tooltip>
            <template slot-scope="scope">
              {{scope.row.name || scope.row.columnName}}
            </template>
          </el-table-column>
          <el-table-column :label="$t('th_type')" prop="type" width="80">
            <template slot-scope="scope">
              {{ $t(scope.row.type) }}
            </template>
          </el-table-column>
        </el-table>
        <div class="no-model-data" v-else>
          <span class="icon el-icon-ksd-select"></span>
          <p>{{$t('noModelDetailsTip')}}</p>
        </div>
      </el-col>
    </el-row>
    <el-row :gutter="15" v-else>
      <div class="recommend-layout" v-show="hasRecommendation">
        <el-col :span="15">
          <el-table
            :data="recommendDatas"
            class="model-table"
            border
            v-scroll-shadow
            :ref="tableRef"
            @sort-change="onSortChange"
            @select="handleSelectionRecommends"
            @selection-change="handleSelectionRecommendationChange"
            @row-click="recommendRowClick"
            :row-class-name="setRowClass">
            <el-table-column type="selection" width="44"></el-table-column>
            <el-table-column :label="$t('kylinLang.model.modelNameGrid')" show-overflow-tooltip>
              <template slot-scope="scope">
                <span>{{scope.row.alias}}</span>
              </template>
            </el-table-column>
            <el-table-column :label="$t('kylinLang.model.index')" show-overflow-tooltip prop="index_id">
            </el-table-column>
            <el-table-column label="SQL" prop="sqls" width="100" align="right" sortable="custom" :render-header="renderHeaderSql">
              <template slot-scope="scope">{{scope.row.sqls.length || 0}}</template>
            </el-table-column>
          </el-table>
        </el-col>
        <el-col class="details" :span="9">
          <el-table
            v-if="activeRowId"
            :data="recommendDetails"
            class="model-table"
            border
            ref="originModelDetails"
            v-scroll-shadow
            style="width: 100%">
            <el-table-column type="expand" width="44">
              <template slot-scope="scope">
                <template v-if="scope.row.type === 'cc'">
                  <p><span class="label">{{$t('th_expression')}}：</span>{{scope.row.cc.expression}}</p>
                </template>
                <template v-if="scope.row.type === 'dimension'">
                  <p><span class="label">{{$t('th_column')}}：</span>{{scope.row.name}}</p>
                </template>
                <template v-if="scope.row.type === 'measure'">
                  <p><span class="label">{{$t('th_column')}}：</span><span class="break-word">{{scope.row.measure.name}}</span></p>
                  <p><span class="label">{{$t('th_function')}}：</span>{{scope.row.measure.function.expression}}</p>
                  <p><span class="label">{{$t('th_parameter')}}：</span>{{scope.row.measure.function.parameters}}</p>
                </template>
              </template>
            </el-table-column>
            <el-table-column :label="$t('th_name')" prop="name" show-overflow-tooltip>
            </el-table-column>
            <el-table-column :label="$t('th_type')" prop="type" width="80">
              <template slot-scope="scope">
                {{ $t(scope.row.type) }}
              </template>
            </el-table-column>
          </el-table>
          <div class="no-origin-data" v-else>
            <span class="icon el-icon-ksd-select"></span>
            <p>{{$t('noOriginDetailsTip')}}</p>
          </div>
        </el-col>
      </div>
      <template v-if="!hasRecommendation">
        <div class="no-recommends">
          <span class="icon el-icon-ksd-empty-box"></span>
          <p>{{$t('noRecommendsTip')}}</p>
        </div>
      </template>
    </el-row>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { NamedRegex } from 'config'
import { handleSuccess } from '../../../util/business'
import { handleError, ArrayFlat } from '../../../util/index'
import { mapActions, mapGetters } from 'vuex'
import locales from './locales'
@Component({
  props: ['suggestModels', 'tableRef', 'isOriginModelsTable', 'maxHeight'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      validateModelName: 'VALIDATE_MODEL_NAME'
    })
  },
  locales
})
export default class SuggestModel extends Vue {
  ArrayFlat = ArrayFlat
  modelNameError = ''
  isNameErrorModelExisted = ''
  selectModels = []
  modelDetails = []
  selectRecommends = []
  recommendDetails = []
  activeRowId = ''
  recommendDatas = []
  mounted () {
    this.recommendDatas = [...this.ArrayFlat(this.suggestModels.map(item => item.rec_items.map(it => ({ ...it, alias: item.alias }))))]
    this.$nextTick(() => {
      if (this.isOriginModelsTable) {
        this.recommendDatas.forEach(it => {
          this.$refs[this.tableRef] && this.$refs[this.tableRef].toggleRowSelection(it)
        })
      } else {
        this.suggestModels.forEach((model) => {
          this.$refs[this.tableRef] && this.$refs[this.tableRef].toggleRowSelection(model)
        })
      }
      this.initClickItem()
      document.addEventListener('click', this.handleClickEvent)
      // 解决table初次渲染有大部分空白问题
      this.$refs[this.tableRef] && this.$refs[this.tableRef].doLayout()
    })
  }

  get modelTips () {
    if (this.isOriginModelsTable) {
      return this.$t('originModelTips')
    } else {
      return this.$t('newModelTips')
    }
  }

  get hasRecommendation () {
    let flag = false
    const layouts = [...this.ArrayFlat(this.suggestModels.map(it => it.rec_items))]
    for (let i = 0; i <= layouts.length - 1; i++) {
      const { computed_columns, dimensions, measures } = layouts[i]
      if (computed_columns.length + dimensions.length + measures.length > 0) {
        flag = true
        break
      }
    }
    return flag
  }

  onSortChangeModels ({ column, prop, order }) {
    if (prop !== 'sqls') return
    if (order === 'ascending') {
      this.suggestModels.sort((a, b) => {
        return a.rec_items[0].sqls.length - b.rec_items[0].sqls.length
      })
    } else {
      this.suggestModels.sort((a, b) => {
        return b.rec_items[0].sqls.length - a.rec_items[0].sqls.length
      })
    }
  }

  onSortChange ({ column, prop, order }) {
    if (prop !== 'sqls') return
    if (order === 'ascending') {
      this.recommendDatas.sort((a, b) => {
        return a.sqls.length - b.sqls.length
      })
    } else {
      this.recommendDatas.sort((a, b) => {
        return b.sqls.length - a.sqls.length
      })
    }
  }

  initClickItem () {
    if (this.isOriginModelsTable) {
      this.recommendDatas.length && this.recommendRowClick(this.recommendDatas[0])
    } else {
      this.suggestModels.length && this.modelRowClick(this.suggestModels[0])
    }
  }

  handleClickEvent (e) {
    if (!e.target.closest('.row-click') && !e.target.closest('.active-row') && !e.target.closest('.details')) {
      this.activeRowId = ''
    }
  }

  renderHeaderSql (h, { column, index }) {
    return <span class="sql-header">
      SQL
      <el-tooltip content={ this.$t('sqlInfo') } effect="dark" placement="top">
        <span class="icon el-icon-ksd-what ksd-ml-5"></span>
      </el-tooltip>
    </span>
  }

  setRowClass ({ row }) {
    return row.uuid === this.activeRowId ? 'active-row' : 'row-click'
  }

  modelRowClick (row) {
    const computed_columns = this.ArrayFlat(row.rec_items.map(it => it.computed_columns))
    const dimensions = this.ArrayFlat(row.rec_items.map(it => it.dimensions))
    const measures = this.ArrayFlat(row.rec_items.map(it => it.measures))
    this.activeRowId = row.uuid
    this.modelDetails = [...measures.map(it => ({ ...it, name: it.measure.name, type: 'measure' })), ...dimensions.map(it => ({ ...it, name: it.dimension.name, type: 'dimension' })), ...computed_columns.map(it => ({ ...it, name: it.cc.columnName, type: 'cc' }))]
    this.$nextTick(() => {
      this.$refs.newModelDetails && this.$refs.newModelDetails.doLayout()
    })
  }

  recommendRowClick (row) {
    this.activeRowId = row.index_id
    this.recommendDetails = [...row.computed_columns.map(it => ({ ...it, name: it.cc.columnName, type: 'cc' })), ...row.dimensions.map(it => ({ ...it.dimension, type: 'dimension' })), ...row.measures.map(it => ({ ...it, name: it.measure.name, type: 'measure' }))]
    this.$nextTick(() => {
      this.$refs.originModelDetails && this.$refs.originModelDetails.doLayout && this.$refs.originModelDetails.doLayout()
    })
  }

  sqlsTable (sqls) {
    return sqls.map((s) => {
      return { sql: s }
    })
  }

  handleRename (model) {
    let suggestListRename = false
    model.isNameError = false
    this.modelNameError = ''
    if (model.isChecked) {
      if (model.alias.trim().length > 127) {
        model.isNameError = true
        suggestListRename = true
        this.modelNameError = this.$t('kylinLang.common.overLength127Tip')
        this.checkRenameModelExisted()
      }
      if (!NamedRegex.test(model.alias.trim())) {
        model.isNameError = true
        suggestListRename = true
        this.modelNameError = this.$t('kylinLang.common.nameFormatValidTip')
        this.checkRenameModelExisted()
      }
      if (!suggestListRename) {
        for (let m = 0; m < this.suggestModels.length; m++) {
          if (this.suggestModels[m].uuid !== model.uuid && this.suggestModels[m].alias === model.alias.trim()) {
            model.isNameError = true
            suggestListRename = true
            this.modelNameError = this.$t('renameError')
            this.checkRenameModelExisted()
            break
          }
        }
      }
      if (!suggestListRename) {
        this.validateModelName({ alias: model.alias.trim(), uuid: model.uuid, project: this.currentSelectedProject }).then((res) => {
          handleSuccess(res, (data) => {
            if (data) {
              model.isNameError = true
              this.modelNameError = this.$t('renameError')
            }
            this.checkRenameModelExisted()
          })
        }, (res) => {
          handleError(res)
        })
      }
    }
  }

  handleSelectionModel (selection, row) {
    row.isChecked = !row.isChecked
    if (!this.isOriginModelsTable) {
      this.handleRename(row)
    }
  }

  handleSelectionModelChange (selection) {
    this.selectModels = selection
    this.$emit('getSelectModels', this.selectModels)
  }

  handleSelectionAllModel (selection) {
    if (selection.length) {
      selection.forEach((m) => {
        m.isChecked = true
        if (!this.isOriginModelsTable) {
          this.handleRename(m)
        }
      })
    } else {
      this.suggestModels.forEach((m) => {
        m.isChecked = false
        if (!this.isOriginModelsTable) {
          this.handleRename(m)
        }
      })
    }
  }

  handleSelectionRecommends (selection, row) {
    row.isChecked = !row.isChecked
  }

  handleSelectionRecommendationChange (val) {
    this.selectRecommends = val
    const suggestList = [...JSON.parse(JSON.stringify(this.suggestModels))]
    const layoutIds = val.map(it => it.index_id)
    const data = [...suggestList.filter(it => {
      return it.rec_items.some(item => layoutIds.includes(item.index_id))
    })]
    data.forEach(it => {
      it.rec_items = it.rec_items.filter(item => layoutIds.includes(item.index_id))
    })
    this.$emit('getSelectRecommends', data)
    this.$emit('changeSelectRecommendsLength', val.length)
  }

  checkRenameModelExisted () {
    this.isNameErrorModelExisted = false
    for (let i = 0; i < this.suggestModels.length; i++) {
      if (this.suggestModels[i].isChecked && this.suggestModels[i].isNameError) {
        this.isNameErrorModelExisted = true
        break
      }
    }
    this.$emit('isValidated', this.isNameErrorModelExisted)
  }

  beforeDestroy () {
    document.removeEventListener('click', this.handleClickEvent)
  }
}
</script>
<style lang="less">
  @import '../../../assets/styles/variables.less';
  .rename-error {
    color: @error-color-1;
    font-size: 12px;
    line-height: 1.2;
  }
  .break-word {
    word-break: break-all;
  }
  .model-table {
    .el-table__row {
      cursor: pointer;
    }
    .active-row {
      background-color: @base-color-9;
    }
    .el-table__expanded-cell {
      padding: 24px;
      font-size: 12px;
      color: @text-title-color;
      p {
        margin-bottom: 5px;
        word-break: break-all;
        &:last-child {
          margin-bottom: 0;
        }
      }
      .label {
        color: @text-normal-color;
      }
    }
    .sql-header {
      .icon {
        cursor: pointer;
      }
    }
  }
  .model-layout {
    height: 100%;
    .el-row {
      height: 100%;
      .el-col {
        height: 100%;
        padding: 20px 0;
        overflow: auto;
      }
      .el-col.details {
        border-left: 1px solid @ke-border-divider-color;
      }
    }
    .model-table {
      .el-table__body-wrapper {
        max-height: calc(~'100% - 40px');
      }
    }
    .recommend-layout {
      height: 100%;
    }
  }
  .no-model-data, .no-origin-data {
    text-align: center;
    margin: 0 60px;
    font-size: 12px;
    position: absolute;
    top: 50%;
    transform: translate(0, -50%);
    color: @text-normal-color;
    .icon {
      font-size: 37px;
      color: @text-placeholder-color;
      margin-bottom: 5px;
    }
  }
  .no-recommends {
    width: 300px;
    font-size: 12px;
    position: absolute;
    margin: auto;
    left: 0;
    right: 0;
    top: 150px;
    text-align: center;
    color: @text-normal-color;
    .icon {
      color: @text-placeholder-color;
      font-size: 36px;
      margin-bottom: 5px;
      // border: 1px dashed @text-placeholder-color;
    }
  }
</style>
