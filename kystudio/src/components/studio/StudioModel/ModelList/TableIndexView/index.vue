<template>
  <div class="table-index-view" v-loading="isLoading">
    <div class="title-list">
      <div class="title-header">
        <el-select v-model="indexType" size="small" disabled class="index-types">
          <el-option
            v-for="item in options"
            :key="item"
            :label="$t(item)"
            :value="item">
          </el-option>
        </el-select>
        <div class="icon-group ksd-fright" v-if="isShowTableIndexActions&&!isHideEdit">
          <common-tip :content="!indexUpdateEnabled && model.model_type === 'STREAMING' ? $t('refuseAddIndexTip') : $t('addTableIndex')">
            <i :class="['el-ksd-icon-project_add_old', 'ksd-fs-16', {'is-disabled': !indexUpdateEnabled && model.model_type === 'STREAMING'}]" @click="(e) => confrimEditTableIndex(undefined, e)"></i>
          </common-tip>
        </div>
      </div>
      <ul v-if="indexDatas.length">
        <li class="table-index-title" @click="scrollToMatched(indexIdx)" v-for="(index, indexIdx) in indexDatas" :key="indexIdx">
          {{$t('tableIndexTitle', { indexId: index.id })}}
        </li>
      </ul>
      <kylin-nodata v-else>
      </kylin-nodata>
    </div>
    <div class="table-index-detail-block" v-if="indexDatas.length">
      <div class="table-index-detail ksd-mb-15" v-for="index in indexDatas" :key="index.id">
        <div class="table-index-content-title">
          <span class="ksd-fs-16">
            {{$t('tableIndexTitle', { indexId: index.id })}}
          </span><span class="index-type ksd-ml-10">
            {{$t('custom')}}</span><span class="index-time ksd-ml-15">
            <i class="el-ksd-icon-time_16"></i>
            {{index.last_modified | toServerGMTDate}}
          </span>
          <span class="ksd-fright icon-group">
            <common-tip :content="!handleEditOrDelIndex(index) ? $t('refuseEditIndexTip') : $t('kylinLang.common.edit')">
              <i :class="['el-ksd-icon-edit_16', 'ksd-fs-16', {'is-disabled': !handleEditOrDelIndex(index)}]" @click="(e) => confrimEditTableIndex(index, e)"></i>
            </common-tip><common-tip :content="!handleEditOrDelIndex(index) ? $t('refuseRemoveIndexTip') : $t('kylinLang.common.delete')">
              <i :class="['el-ksd-icon-table_delete_16', 'ksd-fs-16', 'ksd-ml-10', {'is-disabled': !handleEditOrDelIndex(index)}]" @click="removeIndex(index)"></i>
            </common-tip>
          </span>
        </div>
        <p class="ksd-mb-10" v-if="model.model_type === 'HYBRID'">
          <span>{{$t('indexTimeRange')}}</span>
          <span v-if="index.index_range">{{$t('kylinLang.common.' + index.index_range)}}</span>
        </p>
        <p class="ksd-mb-10" v-if="model.model_type === 'HYBRID'">{{$t('includeColumns')}}</p>
        <div class="table-index-content">
          <el-table
          size="medium"
          :data="getIndexDetail(index)"
          border class="index-details">
          <el-table-column
            :label="$t('ID')"
            prop="id"
            width="64">
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('column')"
            prop="column">
          </el-table-column>
          <el-table-column
            :label="$t('cardinality')"
            prop="cardinality"
            width="100"
            align="center">
            <span slot-scope="scope">
              <template v-if="scope.row.cardinality === null"><i class="no-data_placeholder">NULL</i></template>
              <template v-else>{{ scope.row.cardinality }}</template>
            </span>
          </el-table-column>
          <el-table-column
          label="Shard"
          align="center"
          width="70">
            <template slot-scope="scope">
                <i class="el-ksd-icon-good_health_old ksd-fs-16 ky-success" v-show="scope.row.shared"></i>
            </template>
            </el-table-column>
          </el-table>
          <!-- <kylin-pager layout="prev, pager, next" :background="false" class="ksd-mt-10 ksd-center" ref="pager" :perpage_size="index.detailCurrentCount" :curPage="index.detailCurrentPage+1" :totalSize="index.col_order.length"  v-on:handleCurrentChange='(index) => currentChange(size, count, index)'></kylin-pager> -->
        </div>
      </div>
    </div>
    <div class="table-index-detail-block" v-else>
      <div class="empty-block">
        <div>{{$t('aggTableIndexTips')}}</div>
        <common-tip :content="$t('refuseAddIndexTip')" :disabled="indexUpdateEnabled || model.model_type !== 'STREAMING'">
          <el-button type="primary" text :disabled="!indexUpdateEnabled && model.model_type === 'STREAMING'" icon="el-ksd-icon-table_add_old" @click="confrimEditTableIndex()" v-if="isShowTableIndexActions && !isHideEdit">{{$t('tableIndex')}}</el-button>
        </common-tip>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync, handleError, kylinConfirm } from 'util'
import NModel from '../../ModelEdit/model.js'
@Component({
  props: {
    model: {
      type: Object
    },
    projectName: {
      type: String
    },
    isShowTableIndexActions: {
      type: Boolean,
      default: true
    },
    isHideEdit: {
      type: Boolean,
      default: false
    }
  },
  computed: {
    ...mapGetters([
      'currentProjectData',
      'isOnlyQueryNode'
    ]),
    modelInstance () {
      this.model.project = this.currentProjectData.name
      return new NModel(this.model)
    }
  },
  methods: {
    ...mapActions({
      loadAllIndex: 'LOAD_ALL_INDEX',
      deleteIndex: 'DELETE_INDEX'
    }),
    ...mapActions('TableIndexEditModal', {
      showTableIndexEditModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class TableIndexView extends Vue {
  indexType = 'custom'
  options = ['custom']
  filterArgs = {
    page_offset: 0,
    page_size: 999,
    key: '',
    reverse: '',
    sources: ['CUSTOM_TABLE_INDEX'],
    status: []
  }
  indexDatas = []
  isLoading = false
  indexUpdateEnabled = true

  handleEditOrDelIndex (index) {
    return !(!this.indexUpdateEnabled && (this.model.model_type === 'STREAMING' || ['HYBRID', 'STREAMING'].includes(index.index_range)))
  }

  confrimEditTableIndex (indexDesc, event) {
    if (indexDesc && !this.indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(indexDesc.index_range)) return
    if (!this.indexUpdateEnabled && this.model.model_type === 'STREAMING') return
    event && event.target.parentElement.className.split(' ').includes('icon') && event.target.parentElement.blur()
    this.editTableIndex(indexDesc)
  }
  async editTableIndex (indexDesc) {
    const { isSubmit } = await this.showTableIndexEditModal({
      modelInstance: this.modelInstance,
      tableIndexDesc: indexDesc || {name: 'TableIndex_1'},
      indexUpdateEnabled: this.indexUpdateEnabled
    })
    isSubmit && this.loadTableIndices()
    isSubmit && this.$emit('refreshModel')
  }
  async removeIndex (index) {
    if (!this.handleEditOrDelIndex(index)) return
    try {
      await kylinConfirm(this.$t('delIndexTip'), {confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('delIndex'))
      const params = {
        project: this.projectName,
        model: this.model.uuid,
        id: index.id,
        index_range: index.index_range || 'EMPTY'
      }
      await this.deleteIndex(params)
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
      this.loadTableIndices()
      this.$emit('refreshModel')
    } catch (e) {
      handleError(e)
    }
  }
  async loadTableIndices () {
    this.isLoading = true
    try {
      const res = await this.loadAllIndex(Object.assign({
        project: this.projectName,
        model: this.model.uuid
      }, this.filterArgs))
      const data = await handleSuccessAsync(res)
      this.indexUpdateEnabled = data.index_update_enabled
      this.indexDatas = data.value.map((d) => {
        d.detailCurrentPage = 0
        d.detailCurrentCount = 10
        return d
      })
      this.isLoading = false
    } catch (e) {
      handleError(e)
      this.isLoading = false
    }
  }
  scrollToMatched (index) {
    this.$nextTick(() => {
      const detailContents = this.$el.querySelectorAll('.table-index-detail-block .table-index-detail')
      this.$el.querySelector('.table-index-detail-block').scrollTop = detailContents[index].offsetTop - 15
    })
  }
  mounted () {
    this.loadTableIndices()
  }
  getIndexDetail (index) {
    // let tableIndexList = index.col_order.slice(index.detailCurrentCount * index.detailCurrentPage, index.detailCurrentCount * (index.detailCurrentPage + 1))
    let renderData = []
    renderData = index.col_order.map((item, i) => {
      let newitem = {
        id: index.detailCurrentCount * index.detailCurrentPage + i + 1,
        column: item.key,
        cardinality: item.cardinality,
        shared: index.shard_by_columns.includes(item.key)
      }
      return newitem
    })
    return renderData
  }
  currentChange (size, count, index) {
    index.detailCurrentPage = size
    index.detailCurrentCount = count
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.table-index-view {
  height: calc(~'100% - 40px');
  overflow: hidden;
  width: 100%;
  .title-list {
    width: 220px;
    height: 100%;
    box-shadow:2px 2px 4px 0px rgba(229,229,229,1);
    background-color: @fff;
    float:left;
    overflow-y: auto;
    padding-top: 15px;
    position: relative;
    .title-header {
      padding: 0 10px 10px 10px;
      line-height: 24px;
      .index-types {
        width: 140px;
      }
    }
    .table-index-title {
      height: 40px;
      line-height: 40px;
      cursor: pointer;
      padding-left: 10px;
      font-size: 14px;
      &:hover {
        background-color: @base-color-9;
      }
    }
  }
  .table-index-detail-block {
    height: 100%;
    margin: 15px 15px 15px 235px;
    padding-bottom: 15px;
    overflow-y: auto;
    position: relative;
    .icon-group i {
      cursor: pointer;
    }
    .icon-group i:hover {
      color: @base-color;
    }
    .icon-group .is-disabled {
      cursor: not-allowed;
      color: @text-disabled-color;
      &:hover {
        color: @text-disabled-color;
      }
    }
    .table-index-detail {
      padding: 15px;
      background-color: @fff;
      border: 1px solid @line-border-color4;
      .table-index-content-title {
        height: 30px;
        line-height: 30px;
        margin-bottom: 10px;
        .index-type {
          font-size: 12px;
          border: 1px solid @text-disabled-color;
          color: @text-disabled-color;
          border-radius: 2px;
          padding: 0 2px;
        }
        .index-time {
          font-size: 14px;
          color: @text-disabled-color;
        }
      }
    }
    .table-index-content {
      .no-data_placeholder {
        color: @text-placeholder-color;
        font-size: 12px;
      }
    }
    .empty-block {
      text-align: center;
      color:@text-disabled-color;
      position: absolute;
      top: 50%;
      text-align: center;
      width: 100%;
    }
  }
}
</style>
