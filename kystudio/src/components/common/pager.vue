<template>
  <div class="kylin-pager" v-show="totalSize">
    <el-pagination
      :background="background"
      :layout="layout"
      :page-size="pageSize"
      :page-sizes="pageSizes"
      :total="totalSize"
      :current-page="curPage"
      @current-change="pageChange"
      @size-change="sizeChange"
      >
    </el-pagination>
  </div>
</template>
<script>
import { bigPageCount, pageSizes } from '../../config'
import { cacheLocalStorage } from '../../util'
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  props: {
    perPageSize: {
      type: Number,
      default: 0
    },
    totalSize: {
      type: Number,
      default: 0
    },
    curPage: {
      type: Number,
      default: 1
    },
    layout: {
      type: String,
      default: 'total, sizes, prev, pager, next, jumper'
    },
    background: {
      type: Boolean,
      default: false
    },
    refTag: {
      type: String,
      default: ''
    }
  },
  methods: {
    pageChange (value) {
      this.currentPage = value
      this.$emit('handleCurrentChange', value - 1, this.pageSize)
    },
    sizeChange (size) {
      this.pageSize = size
      this.currentPage = 0
      cacheLocalStorage(this.refTag, size)
      this.$emit('handleCurrentChange', this.currentPage, size)
    }
  }
})
export default class Pager extends Vue {
  data () {
    return {
      pageSize: +localStorage.getItem(this.refTag) || this.perPageSize || bigPageCount,
      pageSizes: pageSizes,
      currentPage: this.curPage
    }
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
.pager {
  font-size: 13px;
  .el-pagination {
    display: inline-block;
  }
  .total_size {
    color: #fff;
    display: inline-block;
    font-size: 13px;
    min-width: 28px;
    padding: 2px 5px 2px 5px;
    height: 32px;
    line-height: 28px;
    vertical-align: top;
    box-sizing: border-box;
  }
}
</style>
