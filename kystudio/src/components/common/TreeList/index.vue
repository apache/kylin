<template>
  <div class="tree-list" :style="{ width: isResizable ? `${treeStyle.width}px` : null }">
    <div class="filter-box" v-if="isShowFilter">
      <el-input
        size="medium"
        prefix-icon="el-icon-search"
        v-model.trim="filterText"
        :placeholder="placeholder"
        @clear="handleInput"
        @keyup.native="handleInput">
      </el-input>
    </div>
    <el-tree
      :key="treeKey || filterText"
      v-loading="isLoading"
      ref="tree"
      class="filter-tree"
      :class="{'ignore-column-tree': ignoreColumnTree}"
      node-key="id"
      :show-checkbox="isShowCheckbox"
      :empty-text="emptyText"
      :data="data"
      :props="props"
      :show-overflow-tooltip="showOverflowTooltip"
      :render-content="renderNode"
      :expand-on-click-node="isExpandOnClickNode"
      :default-expand-all="isExpandAll"
      :default-expanded-keys="defaultExpandedKeys"
      :filter-node-method="handleNodeFilter"
      @check-change="handleNodeClick"
      @node-click="handleNodeClick"
      @node-expand="handleNodeExpand"
      @node-collapse="handleNodeCollapse">
    </el-tree>
    <slot :renderData="data"></slot>
    <div class="resize-bar" v-show="isShowResizeBar" ref="resize-bar">
      <i></i>
      <i></i>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { isIE } from '../../../util'

const filterDefaultWhiteList = ['isMore', 'isLoading']

@Component({
  props: {
    data: {
      type: Array,
      default: () => []
    },
    defaultExpandedKeys: {
      type: Array,
      default: () => []
    },
    draggableNodeTypes: {
      type: Array,
      default: () => []
    },
    isModelHaveFact: {
      type: Boolean,
      default: false
    },
    isSecondStorageEnabled: {
      type: Boolean,
      default: false
    },
    filterWhiteListTypes: {
      type: Array,
      default: () => []
    },
    ignoreColumnTree: {
      type: Boolean,
      default: false
    },
    isGroupTrees: {
      type: Boolean,
      default: false
    },
    isShowFilter: {
      type: Boolean,
      default: false
    },
    isShowCheckbox: {
      type: Boolean,
      default: false
    },
    isShowResizeBar: {
      type: Boolean,
      default: false
    },
    isExpandOnClickNode: {
      type: Boolean,
      default: true
    },
    isExpandAll: {
      type: Boolean,
      default: false
    },
    isResizable: {
      type: Boolean,
      default: false
    },
    emptyText: {
      type: String,
      default: ''
    },
    placeholder: {
      type: String,
      default: ''
    },
    onFilter: {
      type: Function
    },
    minWidth: {
      type: Number,
      default: 218
    },
    maxWidth: {
      type: Number,
      default: 440
    },
    showOverflowTooltip: {
      type: Boolean,
      default: false
    },
    props: {
      type: Object,
      default: () => ({
        children: 'children',
        label: 'label'
      })
    },
    isRemote: {
      type: Boolean,
      default: false
    },
    treeKey: {
      type: String,
      default: ''
    }
  },
  locales: {
    'en': {
      loadMore: 'Load More ...'
    }
  }
})
export default class TreeList extends Vue {
  filterText = ''
  isResizing = false
  isLoading = false
  resizeFrom = 0
  movement = 0
  timer = 0
  treeStyle = {
    width: null
  }
  fixTreeWidth () {
    this.treeStyle.width = this.$el.clientWidth
  }
  mounted () {
    this.$on('filter', (event) => {
      let val = event && event.target.value || ''
      this.filterText = val
      this.handleInput(event)
    })
    const resizeBarEl = this.$refs['resize-bar']
    this.fixTreeWidth()
    resizeBarEl.addEventListener('mousedown', this.handleResizeStart)
    document.addEventListener('mousemove', this.handleResize)
    document.addEventListener('mouseup', this.handleResizeEnd)
  }
  beforeDestroy () {
    const resizeBarEl = this.$refs['resize-bar']

    resizeBarEl.removeEventListener('mousedown', this.handleResizeStart)
    document.removeEventListener('mousemove', this.handleResize)
    document.removeEventListener('mouseup', this.handleResizeEnd)
  }
  showLoading () {
    this.isLoading = true
  }
  hideLoading () {
    this.isLoading = false
  }
  handleInput (event) {
    clearTimeout(this.timer)
    if (event && event.which === 13) {
      this.handleFilter()
    } else {
      this.timer = setTimeout(() => {
        this.handleFilter()
      }, 1000)
    }
  }
  async handleFilter () {
    if (this.onFilter) {
      this.showLoading()
      await this.onFilter(this.filterText)
      this.hideLoading()
    }
    if (!this.isRemote) {
      this.$refs['tree'].filter(this.filterText)
    }
  }
  getTreeItemStyle (data, node) {
    const treeItemStyle = {}
    // 18: this.$refs['tree'].indent
    const paddingLeft = (node.level - 1) * 18

    if (data.type === 'isMore') {
      treeItemStyle.width = `calc(100% + ${paddingLeft}px)`
      treeItemStyle.transform = `translateX(${-24 - paddingLeft}px)`
      treeItemStyle.paddingLeft = `calc(${paddingLeft + 24}px)`
    }

    return treeItemStyle
  }
  renderNode (h, { node, data, store }) {
    const { render, draggable, handleClick, handleDbClick, type } = data
    const isNodeDraggable = this.draggableNodeTypes.includes(type) || draggable
    const treeItemStyle = this.getTreeItemStyle(data, node)
    const className = 'tree-item guide-' + (data.database !== undefined ? data.database.toLowerCase() + node.label.toLowerCase() : node.label.toLowerCase())
    const props = {
      directives: [
        ...(type === 'isLoading' ? [{ name: 'loading', value: true }] : [])
      ],
      class: [
        ...(type === 'isMore' ? ['load-more', data.parent.label.toLowerCase() + '-more'] : []),
        ...(data.isSelected ? ['selected'] : [])
      ]
    }

    if (type === 'isMore') {
      node.visible = data.parent.children.length !== 1 || data.parent.children.length === 0
    }
    if (data.isHidden !== undefined) {
      node.visible = !data.isHidden
    }
    if (data.children) {
      this.patchNodeLoading(data, node)
      this.patchNodeMore(data, node)
    }
    return (
      <div class={ (this.isModelHaveFact || this.isSecondStorageEnabled) && data.datasource === 1 ? 'tree-item nodraggable' : className }
        style={treeItemStyle}
        draggable={isNodeDraggable}
        onMousedown={event => this.handleMouseDown(event, data, node)}
        onDragstart={event => this.handleDragstart(event, data, node)}
        onClick={event => handleClick && handleClick(event, data, node)}
        onDbClick={event => handleDbClick && handleDbClick(event, data, node)}
        {...props}>
        { render ? (
          render(h, { node, data, store })
        ) : node.label }
        { type === 'isMore' ? (
          <span>{this.$t('loadMore')}</span>
        ) : null}
      </div>
    )
  }
  handleNodeClick (data, node) {
    switch (data.type) {
      case 'isMore':
        if (data.text === 'columns') {
          data.parent.child_options.page_offset += 1
          'childContent' in data.parent && (data.parent.children = data.parent.childContent.slice(0, data.parent.child_options.page_offset * data.parent.child_options.page_size - 1))
          data.parent.isMore = data.parent.child_options.page_offset * data.parent.child_options.page_size < data.parent.childContent.length
        } else {
          this.$emit('load-more', data, node)
        }
        break
      default: this.$emit('click', data, node); break
    }
  }
  handleNodeExpand (data, node) {
    this.$emit('node-expand', data, node)
  }
  handleNodeCollapse (data, node) {
    this.$emit('node-collapse', data, node)
  }
  handleMouseDown (event, data, node) {
    event.stopPropagation()
  }
  handleResizeStart (event) {
    const isLeftKey = event.which === 1
    if (isLeftKey) {
      this.isResizing = true
      this.resizeFrom = event.pageX
    }
  }
  handleResize (event) {
    if (this.isResizing) {
      const tempWidth = this.treeStyle.width + event.pageX - this.resizeFrom

      if (tempWidth > this.minWidth && tempWidth < this.maxWidth) {
        this.movement = event.pageX - this.resizeFrom
        this.treeStyle.width = this.treeStyle.width + this.movement
        this.resizeFrom = event.pageX
        this.$emit('resize', this.treeStyle.width)
      }
    }
  }
  handleResizeEnd (event) {
    this.isResizing = false
    this.resizeFrom = 0
    this.movement = 0
  }
  handleDragstart (event, data, node) {
    event = event || window.event
    event.dataTransfer && (event.dataTransfer.effectAllowed = 'move')
    event.stopPropagation()
    if (!isIE()) {
      event.dataTransfer && event.dataTransfer.setData && event.dataTransfer.setData('text', '')
    }
    this.$emit('drag', data, event.srcElement ? event.srcElement : event.target)
  }
  handleNodeFilter (value, data, node) {
    const NodeWhiteList = [...this.filterWhiteListTypes, ...filterDefaultWhiteList]

    if (node.isLeaf) {
      if (NodeWhiteList.includes(data.type)) {
        return true
      } else if (value) {
        return data.label.toUpperCase().includes(value.toUpperCase())
      } else if (!value) {
        return !data.children
      }
    } else {
      return true
    }
  }
  patchNodeLoading (data) {
    const hasLoadingNode = data.children.some(child => child.type === 'isLoading')
    if (data.isLoading) {
      if (!hasLoadingNode) {
        data.children.push({ id: 'isLoading', label: '', type: 'isLoading', parent: data })
      }
    } else {
      if (hasLoadingNode) {
        data.children = data.children.filter(child => child.type !== 'isLoading')
      }
    }
  }
  patchNodeMore (data, node) {
    const hasMoreNode = data.children.some(child => child.type === 'isMore')
    const lastChild = data.children[data.children.length - 1]
    if (data.isMore) {
      if (!hasMoreNode) {
        data.children.push({ id: 'isMore', label: '', type: 'isMore', isSelected: false, parent: data })
      }
      if (lastChild && lastChild.type !== 'isMore') {
        data.children = data.children.filter(child => child.type !== 'isMore')
        data.children.push({ id: 'isMore', label: '', type: 'isMore', isSelected: false, parent: data, text: 'childContent' in data ? 'columns' : '' })
      }
    } else {
      if (hasMoreNode) {
        data.children = data.children.filter(child => child.type !== 'isMore')
      }
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.tree-list {
  position: relative;
  width: 100%;
  .filter-box {
    margin-bottom: 10px;
    .el-input-group__append .el-icon-search:hover {
      color: inherit;
    }
  }
  .tree-item.el-loading-parent--relative {
    position: relative !important;
    height: 100%;
    // margin:0 auto;
    width: auto;
    background: white;
    flex: none;
    cursor: default;
    margin-left: calc(50% - 16px)!important;
    .el-loading-spinner {
      width: 16px;
    }
    .circular {
      width: 16px;
      width: 16px;
    }
  }
  .load-more {
    flex: none;
    height: 100%;
    line-height: 30px!important;
    position: relative;
    box-sizing: border-box;
    span {
      font-size:12px;
      cursor:pointer;
      &:hover{
        color:@base-color;
      }
    }
  }
  .resize-bar {
    position: absolute;
    top: 50%;
    right: 0;
    transform: translate(10px, -50%);
    width: 10px;
    height: 84px;
    line-height: 84px;
    background-color: #cfd8dc;
    border: 1px solid #b0bec5;
    text-align: center;
    cursor: col-resize;
    user-select: none;
  }
  .resize-bar i {
    border-left: 1px solid #fff;
    height: 20px;
    & + i {
      margin-left: 2px;
    }
  }
}
</style>
