<template>
  <div :class="['model-er-diagram', {'is-full-screen': isFullScreen}]" v-drag="{sizeChangeCb:dragBox}" v-loading="loadingER">
    <el-alert class="alertChangeER" :title="$t('changeERTips')" type="warning" show-icon :closable="false" v-if="changeER && showChangeAlert"></el-alert>
    <div class="er-layout" ref="el-draw-layout" v-if="currentModel" :style="getErLayoutStyle">
      <div :class="['table-box', {'is-lookup': t.type !== 'FACT'}]" :id="t.guid" v-for="t in currentModel.tables" :key="t.guid" :style="getTableStyles(t)">
        <div :class="['table-title', {'table-spread-out': !t.spreadOut}]" @dblclick="handleDBClick(t)">
          <span class="table-sign">
            <el-tooltip :content="$t(t.type)" placement="top">
              <i class="el-ksd-n-icon-symbol-f-filled kind" v-if="t.type === 'FACT'"></i>
              <i v-else class="el-ksd-n-icon-dimention-table-filled kind"></i>
            </el-tooltip>
          </span>
          <span class="table-alias">
            <span v-custom-tooltip="{text: t.alias, w: 10, effect: 'dark', 'popper-class': 'popper--small model-alias-tooltip', 'visible-arrow': false, position: 'bottom-start'}">{{t.alias}}</span>
          </span>
          <el-tooltip :content="`${t.columns.length}`" placement="top" :disabled="typeof getColumnNums(t) === 'number'">
            <span class="table-column-nums">{{getColumnNums(t)}}</span>
          </el-tooltip>
        </div>
        <div class="column-list-box">
          <ul>
            <li
              class="column-li"
              v-for="col in getColumns(t)"
              :key="col.id"
              :id="`${t.guid}_${col.name}`"
              :class="{'is-pfk': isPFK(col.name, t).isPK || isPFK(col.name, t).isFK}"
              @mouseleave="(e) => handleMouseLeave(e, t, col)"
              @mouseenter="(e) => handleMouseEnterColumn(e, t, col)"
            >
              <span class="ksd-nobr-text">
                <span class="col-type-icon">
                  <span class="pfk-sign">{{isPFK(col.name, t).isPK ? 'PK' : isPFK(col.name, t).isFK ? 'FK' : ''}}</span><i :class="columnTypeIconMap(col.datatype)"></i>
                </span>
                <span :class="['col-name', {'is-link': col.isPFK}]" v-custom-tooltip="{text: col.name, w: 30, effect: 'dark', 'popper-class': 'popper--small', 'visible-arrow': false, position: 'bottom-start', observerId: t.guid}">{{col.name}}</span>
              </span>
            </li>
          </ul>
        </div>
      </div>
    </div>
    <ModelNavigationTools v-if="showShortcutsGroup && currentModel && currentModel.canvas" :showReset="changeER" :zoom="currentModel.canvas.zoom" @command="handleActionsCommand" @addZoom="handleZoom('up')" @reduceZoom="handleZoom('down')" @autoLayout="autoLayout" @reset="resetERDiagram" />
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { handleSuccessAsync, objectClone } from 'util'
import { columnTypeIcon } from '../../../config'
import { mapActions, mapGetters, mapState, mapMutations } from 'vuex'
import { initPlumb, drawLines, customCanvasPosition, createAndUpdateSvgGroup } from './handler'
import { modelRenderConfig } from '../../studio/StudioModel/ModelEdit/config'
import ModelNavigationTools from '../ModelTools/ModelNavigationTools'
import locales from './locales'
@Component({
  props: {
    model: {
      type: Object,
      default () {
        return {}
      }
    },
    showShortcutsGroup: {
      type: Boolean,
      default: true
    },
    showChangeAlert: {
      type: Boolean,
      default: true
    },
    source: {
      type: String,
      default: 'overview'
    }
  },
  computed: {
    ...mapState({
      modelList: state => state.model.modelsList
    }),
    ...mapGetters([
      'currentProjectData',
      'isFullScreen'
    ])
  },
  methods: {
    ...mapActions({
      loadColumnOfModel: 'LOAD_DATASOURCE_OF_MODEL'
    }),
    ...mapMutations({
      toggleFullScreen: 'TOGGLE_SCREEN'
    })
  },
  locales,
  components: {
    ModelNavigationTools
  }
})
export default class ModelERDiagram extends Vue {
  currentModel = null
  columnTypeIconMap = columnTypeIcon
  plumbTool = null
  plumbInstance = null
  loadingER = false
  defaultZoom = 9
  defaultCanvasBackup = null
  defaultTableBackup = null
  foreignKeys = []
  primaryKeys = []
  linkFocus = []
  showOnlyConnectedColumn = false
  changeER = false

  get getErLayoutStyle () {
    return {'transform': `scale(${(this.currentModel?.canvas?.zoom ?? 9) / 10})`}
  }
  // 判断是否为主外键
  isPFK (column, table) {
    return {
      isPK: this.primaryKeys.includes(`${table.alias}.${column}`),
      isFK: this.foreignKeys.includes(`${table.alias}.${column}`)
    }
  }
  // 获取列的数量
  getColumnNums (t) {
    return t.columns.length > 999 ? '999+' : t.columns.length
  }
  // 获取在可视区域内列的个数
  getColumnNumInView (table) {
    if (!this.currentModel.canvas) return ''
    const { canvas } = this.currentModel
    const { height } = canvas.coordinate[`${table.alias}`]
    const num = Math.floor((height - 38) / 34)
    return table.spreadOut ? table.columns.length > num ? `${num}` :`${table.columns.length}` : ''
  }
  getColumns (t) {
    return this.showOnlyConnectedColumn ? t.columns.filter(it => this.isPFK(it.name, t).isPK || this.isPFK(it.name, t).isFK) : t.columns
  }
  async created () {
    this.loadingER = true
    await this.getTableColumns()
    this.$nextTick(() => {
      const { plumbInstance, plumbTool } = initPlumb(this.$el.querySelector('.er-layout'), this.currentModel.canvas ?? this.defaultZoom)
      this.plumbInstance = plumbInstance
      this.plumbTool = plumbTool
      drawLines(this, this.plumbTool, this.currentModel.joints)
      this.handleSortTables()
      if (!this.currentModel.canvas) {
        const cv = customCanvasPosition(this, this.querySelector('.er-layout'), this.currentModel, this.defaultZoom)
        this.$set(this.currentModel, 'canvas', cv)
        this.$nextTick(() => {
          this.plumbTool.refreshPlumbInstance()
        })
      }
      this.loadingER = false
      this.exchangeTableData()
      this.defaultCanvasBackup = objectClone(this.currentModel.canvas)
      this.defaultTableBackup = objectClone(this.currentModel.tables)
      this.exchangePosition()

    })
  }
  // 获取 table 位置信息
  getTableStyles (table) {
    const { canvas } = this.currentModel
    if (!canvas) return {}
    const { height, width, x, y } = canvas.coordinate[`${table.alias}`]
    return { top: `${y}px`, left: `${x}px`, height: `${height}px`, width: `${width}px` }
  }
  // 双击表头 - 展开或收起
  handleDBClick (t) {
    this.$set(t, 'spreadOut', !t.spreadOut)
    !t.spreadOut && this.$set(t, 'spreadHeight', this.currentModel.canvas.coordinate[`${t.alias}`].height)
    const boxH = !t.spreadOut ? this.$el.querySelector('.table-title').offsetHeight + 4 : t.spreadHeight
    this.$set(this.currentModel.canvas.coordinate[`${t.alias}`], 'height', boxH)
    this.$nextTick(() => {
      this.plumbTool.refreshPlumbInstance()
    })
  }
  getTableColumns () {
    return new Promise((resolve, reject) => {
      const { name } = this.currentProjectData
      const [{ canvas }] = this.modelList.filter(item => item.alias === this.model.name)
      this.loadColumnOfModel({project: name, model_name: this.model.name}).then(async (result) => {
        const values = await handleSuccessAsync(result)
        this.currentModel = {
          ...this.model,
          tables: this.model.tables.map(table => {
            const [{ columns }] = values.filter(v => table.name === `${v.database}.${v.name}`)
            return {
              ...table,
              columns
            }
          }),
          canvas: canvas
        }
        resolve()
      }).catch(e => {
        this.loadingER = false
      })
    })
  }
  handleSortTables () {
    const tables = this.currentModel.tables
    tables.forEach(t => {
      const pfkLinkColumns = t.columns.filter(it => this.isPFK(it.name, t).isPK && this.isPFK(it.name, t).isFK).sort((a, b) => a - b)
      const pkLinkColumns = t.columns.filter(it => this.isPFK(it.name, t).isPK && !this.isPFK(it.name, t).isFK).sort((a, b) => a - b)
      const fkLinkColumns = t.columns.filter(it => this.isPFK(it.name, t).isFK && !this.isPFK(it.name, t).isPK).sort((a, b) => a - b)
      const unlinkColumns = t.columns.filter(it => !this.isPFK(it.name, t).isPK && !this.isPFK(it.name, t).isFK)
      t.columns = [...pfkLinkColumns, ...pkLinkColumns, ...fkLinkColumns, ...unlinkColumns]
    })
  }
  // 拖动画布
  dragBox (x, y) {
    const drawBoard = this.$el.querySelector('.er-layout')
    const mL = drawBoard.offsetLeft ?? 0
    const mT = drawBoard.offsetTop ?? 0
    drawBoard.style.cssText += `margin-left: ${mL + x}px; margin-top: ${mT + y}px`
    this.changeER = true
  }
  // 放大缩小
  handleZoom (type) {
    let {zoom} = this.currentModel.canvas
    if (type === 'up') {
      this.$set(this.currentModel.canvas, 'zoom', zoom + 1 > 10 ? 10 : zoom + 1)
    } else {
      this.$set(this.currentModel.canvas, 'zoom', zoom - 1 < 4 ? 4 : zoom - 1)
    }
    this.changeER = true
  }
  // 自动布局
  autoLayout () {
    const cv = customCanvasPosition(this, this.$el.querySelector('.er-layout'), this.currentModel, this.currentModel.canvas.zoom)
    this.$set(this.currentModel, 'canvas', cv)
    this.changeER = true
    this.$nextTick(() => {
      this.plumbTool.refreshPlumbInstance()
      createAndUpdateSvgGroup(null, { type: 'update' })
    })
  }
  handleMouseEnterColumn (e, t, col) {
    const linkList  = []
    this.currentModel.joints.forEach(item => {
      if (item.guid === t.guid) {
        linkList.push(...item.joins.filter(it => it.primaryKey === `${t.alias}.${col.name}`).map(it => ({table_guid: it.guid, linked_column: it.foreignKey})))
      } else if (item.joins.filter(it => it.guid === t.guid).length > 0) {
        const linkJoin = item.joins.filter(it => it.guid === t.guid && it.foreignKey === `${t.alias}.${col.name}`)
        if (!linkJoin.length) return
        linkJoin.forEach(it => {
          linkList.push({table_guid: item.guid, linked_column: it.primaryKey})
        })
      }
    })
    linkList.forEach(lk => {
      const tDom = document.getElementById(`${lk.table_guid}`)
      const linkColumn = document.getElementById(`${lk.table_guid}_${lk.linked_column.split('.')[1]}`)
      const linkLine = document.getElementsByClassName(`${t.guid}&${lk.table_guid}`).length > 0 ? document.getElementsByClassName(`${t.guid}&${lk.table_guid}`) : document.getElementsByClassName(`${lk.table_guid}&${t.guid}`)
      linkColumn && (linkColumn.className += ' is-hover')
      linkLine[0].nextElementSibling && (linkLine[0].nextElementSibling.className += ' is-focus')
      tDom && (tDom.className += ' is-hover');
      (Array.prototype.slice.call(linkLine)).forEach(item => {
        if (item.nodeName === 'svg') {
          const cls = item.getAttribute('class')
          item.setAttribute('class', `${cls} is-focus`)
        } else {
          item.className += ' is-focus'
        }
      })
    })
  }
  // 移除 linked column focus 状态
  handleMouseLeave () {
    const svgLine = this.$el.querySelectorAll('.jtk-connector.is-focus')
    const linkColumn = this.$el.querySelectorAll('.is-pfk.is-hover')
    const linkLabels = this.$el.querySelectorAll('.jtk-overlay.is-focus')
    if (linkLabels.length > 0) {
      Array.prototype.slice.call(linkLabels).forEach(item => {
        item.classList.remove('is-focus')
      })
    }
    if (linkColumn.length > 0) {
      Array.prototype.slice.call(linkColumn).forEach(item => {
        item.classList.remove('is-hover')
      })
    }
    if (svgLine.length > 0) {
      Array.prototype.slice.call(svgLine).forEach(item => {
        item.setAttribute('class', `${item.className.baseVal.replace(/is-focus|is-broken/g, '')}`)
      })
    }
  }
  // 额外功能
  handleActionsCommand (command, showOnlyConnectedColumn) {
    this.changeER = true
    this.showOnlyConnectedColumn = showOnlyConnectedColumn
    if (command === 'collapseAllTables') {
      const tableTitleHeight = document.querySelector('.table-title').offsetHeight
      this.currentModel.tables.forEach((item) => {
        this.$set(item, 'spreadOut', false)
        this.$set(this.currentModel.canvas.coordinate[`${item.alias}`], 'height', tableTitleHeight + 4)
      })
    } else if (command === 'expandAllTables') {
      this.currentModel.tables.forEach((item) => {
        this.$set(item, 'spreadOut', true)
        this.$set(this.currentModel.canvas.coordinate[`${item.alias}`], 'height', item.spreadHeight)
      })
    } else if (command === 'showOnlyConnectedColumn') {
      this.currentModel.tables.forEach(item => {
        const { columns } = item
        const len = columns.filter(it => this.isPFK(it.name, item).isFK || this.isPFK(it.name, item).isPK).length
        const columnHeight = document.querySelector('.column-li').offsetHeight
        const tableTitleHeight = document.querySelector('.table-title').offsetHeight
        const sumHeight = columnHeight * len
        if (sumHeight !== 0) {
          this.$set(item, 'spreadOut', true)
          this.$set(this.currentModel.canvas.coordinate[`${item.alias}`], 'height', tableTitleHeight + sumHeight + 5)
        } else {
          this.$set(item, 'spreadOut', false)
          this.$set(this.currentModel.canvas.coordinate[`${item.alias}`], 'height', tableTitleHeight + 4)
        }
      })
    }
    this.$nextTick(() => {
      this.plumbTool.refreshPlumbInstance()
      createAndUpdateSvgGroup(null, { type: 'update' })
    })
  }
  exchangeTableData () {
    const currentTableTitle = this.$el.querySelector('.table-title')
    const modelTableBoxBorder = +window.getComputedStyle(currentTableTitle)['borderWidth'].replace(/px/, '')
    for (let item in this.currentModel.tables) {
      const t = this.currentModel.tables[item]
      const canvasHeight = this.currentModel.canvas.coordinate[`${t.alias}`].height
      if (canvasHeight === currentTableTitle.offsetHeight + modelTableBoxBorder * 2 + 4) {
        this.$set(t, 'spreadOut', false)
        this.$set(t, 'spreadHeight', modelRenderConfig.tableBoxHeight)
      } else {
        this.$set(t, 'spreadHeight', canvasHeight || modelRenderConfig.tableBoxHeight)
      }
    }
  }
  // 重置 ER 图
  resetERDiagram () {
    this.changeER = false
    this.showOnlyConnectedColumn = false
    this.currentModel.canvas.zoom = 9
    if (this.defaultTableBackup) {
      this.currentModel.tables.forEach((item, index) => {
        this.$set(item, 'spreadOut', this.defaultTableBackup[index].spreadOut)
        this.$set(item, 'spreadHeight', this.defaultTableBackup[index].spreadHeight)
      })
    }
    this.toggleFullScreen(false)
    this.$set(this.currentModel, 'canvas', objectClone(this.defaultCanvasBackup))
    const drawBoard = this.$el.querySelector('.er-layout')
    drawBoard.style.cssText += `margin-left: 0; margin-top: 0`
    this.$nextTick(() => {
      this.plumbTool.refreshPlumbInstance()
      createAndUpdateSvgGroup(null, { type: 'update' })
    })
  }
  fullScreen () {
    this.changeER = true
    this.toggleFullScreen(!this.isFullScreen)
  }
  exchangePosition () {
    if (this.source === 'modelList') {
      if (!(this.currentModel && this.currentModel.tables)) return
      const [factTable] = this.currentModel.tables.filter(it => it.type === 'FACT')
      const factGuid = factTable.guid
      document.getElementById(factGuid).scrollIntoView()
    }
  }
}
</script>
<style lang="less">
@import '../../../assets/styles/variables.less';
.model-er-diagram {
  width: 100%;
  height: 100%;
  cursor: grab;
  .alertChangeER {
    z-index: 10;
  }
  &.is-full-screen {
    position: fixed;
    top: 0;
    left: 0;
    background: @ke-background-color-secondary;
    z-index: 10;
  }
  .er-layout {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    user-select: none;
    .jtk-connector.is-focus {
      path {
        stroke: @ke-color-primary;
        stroke-width: 2;
      }
    }
    .jtk-overlay {
      background: @ke-background-color-secondary;
      .join-type {
        color: @ke-border-secondary-active;
        font-size: 30px;
        cursor: grab;
      }
      &.jtk-hover {
        .join-type {
          color: #9DCEFB;
          &:hover {
            color: @ke-color-primary;
          }
        }
      }
      .close-icon {
        display: none;
      }
      &.is-focus {
        .join-type {
          color: @ke-color-primary;
        }
      }
    }
  }
  .table-box {
    width: 200px;
    height: 230px;
    background-color: @fff;
    position: absolute;
    border-radius: 6px;
    border: 2px solid @ke-color-secondary-hover;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    z-index: 10;
    &.is-focus {
      border: 2px solid @ke-color-primary-hover;
    }
    &.is-broken.is-focus {
      border: 2px solid @ke-color-danger;
    }
    &.link-hover {
      border: 2px solid #9DCEFB;
    }
    &:hover {
      border: 2px solid #9DCEFB;
      .scrollbar-track-y{
        opacity: 1;
      }
    }
    &:focus {
      border: 2px solid @line-border-focus;
    }
    &.is-lookup {
      .table-title {
        background-color: @background-gray;
        color: @fff;
      }
    }
    .no-data {
      position: initial;
      margin-top: 32px;
    }
    .table-title {
      background-color: @base-color;
      color: @fff;
      line-height: 38px;
      border-radius: 5px 5px 0 0;
      height: 38px; // 高度改变需要更改 getColumnNumInView 方法里相应值
      padding: 0 8px 0 12px;
      display: flex;
      align-items: center;
      &.table-spread-out {
        border-radius: 5px;
      }
      .table-sign {
        display: inline-block;
        font-size: 0;
        cursor: default;
      }
      .table-column-nums {
        font-size: 12px;
        cursor: default;
        i {
          margin-top: -2px;
        }
      }
      .table-alias {
        line-height: 29px\0;
        width: calc(~"100% - 50px");
        height: 100%;
        display: inline-block;
        margin-left: 4px;
        font-weight: bold;
        font-size: 14px;
      }
      .kind {
        font-size: 16px;
        margin: 0;
      }
      .kind:hover {
        color:@grey-3;
      }
      i {
        color:@fff;
      }
    }
    .column-list-box {
      overflow: hidden;
      flex: 1;
      ul {
        li {       
          padding-left: 5px;
          border-top: solid 2px transparent;
          border-bottom: 2px solid transparent;
          height: 32px;
          line-height: 32px;
          font-size: 14px;
          position: relative;
          &.is-hover {
            background-color: @base-color-8;
            border: 2px solid @ke-color-primary;
          }
          &.is-pfk {
            .col-name {
              color: @ke-color-primary;
            }
            background-color: #F0F8FE;
            &:hover {
              background: @base-color-8;
              border: 2px solid @ke-color-primary
            }
          }
          .ksd-nobr-text {
            width: calc(~'100% - 25px');
            display: flex;
            align-items: center;
            .tip_box {
              height: 32px;
              line-height: 32px;
            }
          }
          .col-type-icon {
            color: @text-disabled-color;
            font-size: 12px;
            margin-right: 5px;
            .pfk-sign{
              color: @text-placeholder-color;
              position: absolute;
              right: 8px;
            }
          }
          .col-name {
            color: @text-title-color;
            &.is-link {
              color: @ke-color-primary;
              font-weight: @font-medium;
            }
          }
          &.column-li-cc {
            background-color: @warning-color-2;
            &:hover {
              background-color: @warning-color-3;
            }
          }
        }
      }
    }
  }
  .diagram-actions {
    position: absolute;
    right: 20px;
    z-index: 50;
  }
  .shortcuts-group {
    position: absolute;
    right: 20px;
    bottom: 20px;
  }
}
#use-group:hover {
  > path:not(#use) {
    stroke: #9DCEFB;
    stroke-width: 2;
  }
}
.model-action-tools {
  margin-left: 50px;
  .is-active {
    color: @ke-color-primary;
  }
}
.model-alias-tooltip {
  margin-top: -10px !important;
}
</style>