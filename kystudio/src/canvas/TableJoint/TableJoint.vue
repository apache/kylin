<template>
  <v-group ref="$group">
    <v-arrow ref="$arrow" :config="arrowStyle" />
    <v-label ref="$label" :config="labelStyle">
      <v-tag :config="tagStyle" />
      <v-text :config="textStyle" />
    </v-label>
  </v-group>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import { getConnectorPoints, getCenterPoint } from './handler'

@Component({
  props: {
    id: {
      type: String,
      required: true
    },
    sourceId: {
      type: String,
      required: true
    },
    targetId: {
      type: String,
      required: true
    },
    jointType: {
      type: String,
      required: true
    }
  }
})
export default class TableJoint extends Vue {
  position = {
    center: { x: 0, y: 0 },
    source: { x: 0, y: 0 },
    target: { x: 0, y: 0 }
  }

  mounted () {
    if (!this.$refs.$arrow.getNode().getLayer()) return
    this.addEventListener()
    this.handleRefreshJoint()
  }

  beforeDestroy () {
    if (!this.$refs.$arrow.getNode().getLayer()) return
    this.removeEventListener()
  }

  get arrowStyle () {
    const { position: { source, target } } = this
    return {
      points: [source.x, source.y, target.x, target.y],
      pointerLength: 5,
      pointerWidth: 5,
      fill: '#0A88DE',
      stroke: '#0A88DE',
      strokeWidth: 1
    }
  }

  get labelStyle () {
    const { position: { center } } = this
    return { x: center.x - 25, y: center.y - 8, visible: true, listening: false }
  }

  get tagStyle () {
    return {
      fill: '#0A88DE',
      cornerRadius: 2,
      pointerDirection: 'none',
      pointerWidth: 10,
      pointerHeight: 10,
      lineJoin: 'round'
    }
  }

  get textStyle () {
    const { jointType } = this
    return {
      text: jointType,
      fontSize: 12,
      width: 50,
      height: 16,
      align: 'center',
      verticalAlign: 'middle',
      fill: 'white'
    }
  }

  get sourceNode () {
    const layer = this.$refs.$arrow.getNode().getLayer()
    return layer.findOne(`#${this.sourceId}`)
  }

  get targetNode () {
    const layer = this.$refs.$arrow.getNode().getLayer()
    return layer.findOne(`#${this.targetId}`)
  }

  addEventListener () {
    this.sourceNode.on('dragmove text-redraw', this.handleRefreshJoint)
    this.targetNode.on('dragmove text-redraw', this.handleRefreshJoint)
  }

  removeEventListener () {
    this.sourceNode && this.sourceNode.off('dragmove text-redraw', this.handleRefreshJoint)
    this.targetNode && this.targetNode.off('dragmove text-redraw', this.handleRefreshJoint)
  }

  handleRefreshJoint () {
    const { sourceNode, targetNode } = this
    const $layer = this.$refs.$group.getNode().getLayer()
    const scale = $layer.scale()
    const sourceRect = sourceNode.getClientRect()
    const targetRect = targetNode.getClientRect()
    const sourceSize = { width: sourceRect.width / scale.x, height: sourceRect.height / scale.y, ...sourceNode.position() }
    const targetSize = { width: targetRect.width / scale.x, height: targetRect.height / scale.y, ...targetNode.position() }
    const { source, target } = getConnectorPoints(sourceSize, targetSize)
    const centerPoint = getCenterPoint(source, target)

    this.position.source.x = source.x
    this.position.source.y = source.y
    this.position.target.x = target.x
    this.position.target.y = target.y
    this.position.center.x = centerPoint.x
    this.position.center.y = centerPoint.y
  }
}
</script>
