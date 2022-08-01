<template>
  <div class="model-er-diagram">
    <Stage
      ref="$canvas"
      draggable
      is-drag-layer
      :stage-x="canvas.stage.x"
      :stage-y="canvas.stage.y"
      :zoom="canvas.stage.zoom"
      @zoom="handleZoom"
      @dragmove="handleDragStage">
      <ModelTable
        v-for="canvasTable in canvas.tables"
        :key="canvasTable.guid"
        :id="canvasTable.guid"
        :table="canvasTable"
        :x="canvasTable.X"
        :y="canvasTable.Y"
        :width="configs.TABLE_WIDTH"
      />
      <TableJoint
        v-for="canvasJoint in canvas.joints"
        :key="canvasJoint.guid"
        :id="canvasJoint.guid"
        :sourceId="getGuid(canvasJoint.foreign)"
        :targetId="getGuid(canvasJoint.primary)"
        :joint-type="canvasJoint.type"
      />
    </Stage>
    <div class="diagram-header clearfix">
      <div class="diagram-labels clearfix">
        <div class="diagram-label">
          <i class="label-icon fact" />
          <span class="label-text">{{$t('factTable')}}</span>
        </div>
        <div class="diagram-label">
          <i class="label-icon lookup" />
          <span class="label-text">{{$t('lookupTable')}}</span>
        </div>
        <div class="diagram-label" v-if="isShowFullScreen">
          <i class="label-icon el-icon-ksd-full_screen_1" @click="handleFullScreen" />
        </div>
      </div>
      <el-button-group class="diagram-actions" v-if="isShowActions">
        <el-button plain icon="el-ksd-icon-zoom_in_old" :disabled="canvas.stage.zoom >= 2" @click="handleZoom(canvas.stage.zoom * 0.2)" />
        <el-button plain icon="el-ksd-icon-zoom_out_old" :disabled="canvas.stage.zoom <= 0.25" @click="handleZoom(canvas.stage.zoom * -0.2)" />
        <el-button plain icon="el-ksd-icon-zoom_to_default_old" @click="handleReset" />
      </el-button-group>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'

import { autoLayout } from '../../../util'
import { configs } from './handler'
import locales from './locales'
import { Stage, ModelTable, TableJoint } from '../../../canvas'

@Component({
  props: {
    model: {
      type: Object,
      required: true
    },
    // 是否展示操作按钮
    isShowActions: {
      type: Boolean,
      default: false
    },
    // 是否展示全屏弹框按钮
    isShowFullScreen: {
      type: Boolean,
      default: false
    }
  },
  components: {
    Stage,
    ModelTable,
    TableJoint
  },
  methods: {
    ...mapActions('ModelERDiagramModal', {
      callModelERDiagramModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class ModelERDiagram extends Vue {
  configs = configs
  canvas = {
    tree: null,
    stage: { x: 0, y: 0, zoom: 1 },
    tables: [],
    joints: []
  }

  mounted () {
    this.handleAutoLayout()
    this.handleSetFactTableToCenter()
  }

  get tableMap () {
    return this.model.tables.reduce((map, table) => {
      const textCountInLine = 24
      const padding = 6.5
      const fontHeight = 14 * 1.5
      const lineCount = table.alias.length / textCountInLine
      const height = padding * 2 + fontHeight * lineCount
      const tableWithHeight = { ...table, height, width: configs.TABLE_WIDTH }
      return {
        ...map,
        [table.alias]: tableWithHeight,
        [table.guid]: tableWithHeight
      }
    }, {})
  }

  getGuid (name) {
    return this.canvas.tables.filter(it => it.alias === name)[0].guid
  }

  setZoom (zoom) {
    const { canvas: { stage } } = this
    if (zoom >= 2) {
      stage.zoom = 2
    } else if (zoom <= 0.25) {
      stage.zoom = 0.25
    } else {
      stage.zoom = zoom
    }
  }

  handleAutoLayout () {
    const { tables, joints } = this.model
    const { Tree } = autoLayout
    const factTable = tables.find(table => table.type === 'FACT')

    this.canvas.tree = new Tree({
      boxML: 70,
      boxMT: 70,
      rootNode: factTable,
      connections: joints,
      getNodeWidth: () => configs.TABLE_WIDTH,
      getNodeHeight: guid => (this.tableMap && this.tableMap[guid].height) || configs.TABLE_HEIGHT,
      getPrevNodeGuid: joint => this.tableMap[joint.foreign].guid,
      getNextNodeGuid: joint => this.tableMap[joint.primary].guid
    }).positionTree()

    this.canvas.tables = this.canvas.tree.getNodeDb().db.map((node) => ({ ...this.tableMap[node.guid], ...node }))
    this.canvas.joints = joints
  }

  handleSetFactTableToCenter () {
    const PADDING = 20
    const canvasWidth = this.$refs.$canvas.stageWidth
    const canvasHeight = this.$refs.$canvas.stageHeight
    const viewPortHeight = canvasHeight - PADDING * 2
    const viewPortWidth = canvasWidth - PADDING * 2
    // 获取ER图的真实尺寸(无缩放的px)
    const diagramSize = this.canvas.tables.reduce((size, canvasTable) => {
      // 获取当前table的右边值和底部值
      const tableBottom = canvasTable.Y + canvasTable.height
      const tableRight = canvasTable.X + canvasTable.width
      // 如果大于size，则保存当前table的尺寸
      const width = size.width < tableRight ? tableRight : size.width
      const height = size.height < tableBottom ? tableBottom : size.height
      return { width, height }
    }, { width: 0, height: 0 })
    // 计算缩放，有缩小的，使用缩小
    const scaleX = viewPortWidth / diagramSize.width
    const scaleY = viewPortHeight / diagramSize.height
    const zoom = Math.min(1, scaleX, scaleY)
    this.setZoom(zoom)
    // 超过canvas视口尺寸，居中缩放；不超过则单纯居中
    this.canvas.stage.y = scaleY < 1 ? PADDING : (viewPortHeight - diagramSize.height) / 2 + PADDING
    this.canvas.stage.x = scaleX < 1 ? PADDING : (viewPortWidth - diagramSize.width) / 2 + PADDING
  }

  handleZoom (offsetZoom) {
    const { canvas: { stage } } = this
    this.setZoom(stage.zoom + offsetZoom)
  }

  handleFullScreen () {
    const { model } = this
    this.callModelERDiagramModal({ model })
  }

  handleDragStage (position) {
    this.canvas.stage.x = position.x
    this.canvas.stage.y = position.y
  }

  handleReset () {
    this.$refs.$canvas.handleResize()

    this.canvas.stage.x = 0
    this.canvas.stage.y = 0
    this.setZoom(1)
    this.$nextTick(() => {
      this.handleAutoLayout()
      this.handleSetFactTableToCenter()
    })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.model-er-diagram {
  height: 100%;
  width: 100%;
  position: relative;
  .stage {
    height: 100%;
  }
  .diagram-header {
    position: absolute;
    top: 0px;
    right: 0px;
  }
  .diagram-labels {
    float: left;
    background-color: rgba(255, 255, 255, 0.7);
    padding: 0 10px;
  }
  .diagram-label {
    line-height: 30px;
    height: 30px;
    margin-right: 10px;
    float: left;
    &:last-child {
      margin-right: 0;
    }
  }
  .label-icon {
    width: 14px;
    height: 14px;
    display: inline-block;
    &.fact {
      background-color: @base-color;
    }
    &.lookup {
      background-color: @base-color-12;
    }
    &.el-icon-ksd-full_screen_1 {
      cursor: pointer;
      &:hover {
        color: @base-color;
      }
    }
  }
  .diagram-actions {
    float: left;
    margin-left: 10px;
  }
  .label-icon,
  .label-text {
    vertical-align: middle;
  }
}
</style>
