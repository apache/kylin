<template>
  <div class="segment-chart">
    <div class="container" ref="container" @scroll="handleScroll">
      <div class="stage" :style="stageStyle">
        <div class="segment"
          v-for="(segment, index) in inviewSegments"
          :style="segment.style"
          :key="`s-${index}`"
          :class="segment.classNames"
          @mouseout="event => handleMouseOut(event, segment)"
          @mousemove="event => handleMouseMove(event, segment)"
          @click="event => handleClick(event, segment)">
        </div>
        <div class="tick"
          v-for="(tick, index) in inviewTicks"
          :key="`t-${index}`"
          :style="tick.style">
          <div class="tick-label">{{tick.name}}</div>
        </div>
        <slot></slot>
      </div>
    </div>
    <div class="el-popover" v-if="tip" :style="tip.style">
      <div class="popper__arrow"></div>
      <p>Segment ID: {{tip.id}}</p>
      <p>Storage Size: {{tip.storage | dataSize}}</p>
      <p>Start: {{tip.startDate}}</p>
      <p>End: {{tip.endDate}}</p>
    </div>
  </div>
</template>

<script>
import * as d3 from 'd3'
import Vue from 'vue'
import dayjs from 'dayjs'
import { Component, Watch } from 'vue-property-decorator'
import locales from './locales'
import { getGmtDateFromUtcLike } from '../../../../../../util'
import { scaleTypes, formatTypes, isFilteredSegmentsContinue } from './handler'
const { MINUTE, HOUR, DAY, MONTH, YEAR } = scaleTypes

@Component({
  props: {
    segments: {
      type: Array,
      default: () => []
    },
    scaleType: {
      type: String,
      default: DAY,
      validator: value => [MINUTE, HOUR, DAY, MONTH, YEAR].includes(value)
    },
    gridWidth: {
      type: Number,
      default: 152
    }
  },
  locales
})
export default class SegmentChart extends Vue {
  scrollLeft = 0
  viewPort = [0, 0]
  tip = null
  hoveredSegmentId = null
  selectedSegmentIds = []
  timer = null
  isFullInitilized = true
  containerWidth = 0
  colorGenerator = d3.interpolate(d3.rgb('#ffcd58'), d3.rgb('#ff0000'))
  get selectedSegments () {
    return this.segments.filter(segment => this.selectedSegmentIds.includes(segment.id))
  }
  get formatType () {
    return formatTypes[this.scaleType]
  }
  get totalTime () {
    const { startTick, endTick } = this
    return endTick.timestamp - startTick.timestamp
  }
  get startTick () {
    return this.xTicks[0]
  }
  get endTick () {
    return this.xTicks[this.xTicks.length - 1]
  }
  get xTicks () {
    const { formatType, scaleType, segments, gridWidth, containerWidth } = this
    const segmentCount = segments.length - 1
    const startTime = segments[0] && segments[0].startTime
    const endTime = segments[segmentCount] && segments[segmentCount].endTime
    const startDate = startTime && getGmtDateFromUtcLike(startTime)
    const endDate = endTime && getGmtDateFromUtcLike(endTime)
    const startTick = dayjs(startDate).startOf(scaleType)
    const endTick = dayjs(endDate).add(1, DAY).startOf(scaleType)
    const diff = endTick.diff(startTick, scaleType)
    const isTickFull = diff * gridWidth > containerWidth
    const xTicks = []
    const tickCount = isTickFull ? diff : Math.ceil(containerWidth / gridWidth)
    const offsetGMT = new Date().getTimezoneOffset()

    let currentTick = startTick
    for (let i = 0; i < tickCount; i++) {
      const isStartOfYear = currentTick.valueOf() === currentTick.startOf(YEAR).valueOf()
      const data = currentTick
      const timestamp = currentTick.valueOf() - offsetGMT * 60 * 1000
      const name = !isStartOfYear
        ? currentTick.format(formatType)
        : currentTick.startOf(YEAR).format(formatTypes[YEAR])

      xTicks.push({ name, data, timestamp, offsetGMT })
      currentTick = currentTick.add(1, scaleType)
    }
    return xTicks
  }
  get inviewTicks () {
    return this.xTicks.map(xTick => {
      const style = this.getTickStyle(xTick)
      const isShow = this.isTickShow(xTick)
      return { ...xTick, style, isShow }
    }).filter(xTick => {
      return xTick.isShow
    })
  }
  get inviewSegments () {
    return this.segments.map(item => {
      const { id, startTime, endTime } = item
      const storage = item.bytes_size
      const hitCount = item.hit_count
      return { id, startTime, endTime, storage, hitCount }
    }).map(segment => {
      const style = this.getSegmentStyle(segment)
      const isShow = this.isSegmentShow(segment)
      const classNames = this.getSegmentClass(segment)
      return { ...segment, style, isShow, classNames }
    }).filter(segment => {
      return segment.isShow
    })
  }
  get stageStyle () {
    return {
      width: `${this.xTicks.length * this.gridWidth}px`
    }
  }
  @Watch('segments')
  onSegmentChange () {
    const { segments } = this
    const containerWidth = this.$refs['container'] ? this.$refs['container'].clientWidth : 0
    const lastSegment = segments[segments.length - 1]

    if (lastSegment) {
      const segmentRight = this.getSegmentRight(lastSegment)
      const isSegmentFull = containerWidth < segmentRight

      setTimeout(() => {
        !isSegmentFull && this.$emit('load-more')
      }, 100)
    }

    this.clearSelectedSegments()
  }
  isTickShow (xTick) {
    const { totalTime, startTick, gridWidth, xTicks, viewPort } = this
    const curTimestamp = xTick.timestamp
    const startTimestamp = startTick.timestamp
    const tickLeft = (curTimestamp - startTimestamp) / totalTime * xTicks.length * gridWidth
    return viewPort[0] <= tickLeft && tickLeft <= viewPort[1]
  }
  isSegmentShow (segment) {
    const { totalTime, startTick, gridWidth, xTicks, viewPort } = this
    const { endTime, startTime } = segment
    const startTimestamp = startTick.timestamp
    const segmentLeft = (startTime - startTimestamp) / totalTime * xTicks.length * gridWidth
    const segmentRight = segmentLeft + (endTime - startTime) / totalTime * xTicks.length * gridWidth
    return (viewPort[0] <= segmentLeft && segmentLeft <= viewPort[1]) ||
      (viewPort[0] <= segmentRight && segmentRight <= viewPort[1]) ||
      (viewPort[0] >= segmentLeft && viewPort[1] <= segmentRight)
  }
  getTickStyle (xTick) {
    const { totalTime, startTick } = this
    const curTimestamp = xTick.timestamp
    const startTimestamp = startTick.timestamp
    return {
      left: `${(curTimestamp - startTimestamp) / totalTime * 100}%`
    }
  }
  getSegmentStyle (segment) {
    const { totalTime, startTick, hoveredSegmentId } = this
    const { id, endTime, startTime, hitCount } = segment
    const startTimestamp = startTick.timestamp
    return {
      left: `${(startTime - startTimestamp) / totalTime * 100}%`,
      width: `${(endTime - startTime) / totalTime * 100}%`,
      background: this.colorGenerator(hitCount / 100),
      zIndex: hoveredSegmentId === id ? 3 : (this.selectedSegmentIds.includes(segment.id) ? 2 : 1)
    }
  }
  getSegmentRight (segment) {
    const { totalTime, startTick, gridWidth, xTicks } = this
    const { endTime, startTime } = segment
    const startTimestamp = startTick.timestamp
    const segmentLeft = (startTime - startTimestamp) / totalTime * xTicks.length * gridWidth
    const segmentRight = segmentLeft + (endTime - startTime) / totalTime * xTicks.length * gridWidth

    return segmentRight
  }
  getSegmentClass (segment) {
    return {
      selected: this.selectedSegmentIds.includes(segment.id)
    }
  }
  clearSelectedSegments () {
    this.selectedSegmentIds = []
    this.$emit('input', this.selectedSegmentIds, true)
  }
  initContainer () {
    this.containerWidth = this.$refs['container'] ? this.$refs['container'].clientWidth : 0
  }
  handleScroll () {
    clearTimeout(this.timer)

    this.timer = setTimeout(() => {
      const containerEl = this.$refs['container']
      const { clientWidth, scrollLeft } = containerEl
      // this.viewPort = [ scrollLeft, scrollLeft + clientWidth ]
      this.viewPort = [ -clientWidth + scrollLeft, scrollLeft + clientWidth * 2 ]
      this.scrollLeft = scrollLeft
    }, 100)
  }
  handleMouseOut () {
    this.hoveredSegmentId = null
    this.tip = null
  }
  handleMouseMove (event, segment) {
    const { id, startTime, endTime, storage } = segment
    const isFullLoading = endTime === 7258089600000
    const startDate = !isFullLoading ? dayjs(getGmtDateFromUtcLike(startTime)).format('YYYY-MM-DD HH:mm:ss') : this.$t('fullLoad')
    const endDate = !isFullLoading ? dayjs(getGmtDateFromUtcLike(endTime)).format('YYYY-MM-DD HH:mm:ss') : this.$t('fullLoad')
    const style = {
      left: `${event.offsetX + event.target.offsetLeft - this.scrollLeft}px`
    }
    this.hoveredSegmentId = id
    this.tip = { id, startDate, endDate, storage, style }
  }
  handleClick (event, segment) {
    const isSelected = this.selectedSegmentIds.includes(segment.id)
    const isSelectable = isFilteredSegmentsContinue(segment, this.selectedSegments)
    if (isSelectable) {
      if (isSelected) {
        this.selectedSegmentIds = this.selectedSegmentIds.filter(segmentId => segmentId !== segment.id)
      } else {
        this.selectedSegmentIds.push(segment.id)
      }
    }
    this.$emit('input', this.selectedSegmentIds, isSelectable)
  }
  mounted () {
    this.handleScroll()
    this.initContainer()
  }
}
</script>

<style lang="less">
@import '../../../../../../assets/styles/variables.less';

.segment-chart {
  width: 100%;
  background: white;
  .container {
    width: 100%;
    overflow-x: auto;
    overflow-y: hidden;
    white-space: nowrap;
    position: relative;
  }
  .stage {
    height: 152px;
    margin: 20px 0 110px 0;
    border-top: 1px solid #CFD8DC;
    border-bottom: 1px solid #CFD8DC;
  }
  .segment {
    box-sizing: border-box;
    position: absolute;
    height: 150px;
    cursor: pointer;
    &:hover {
      box-shadow: 0 0 8px 0 #455A64;
    }
    &.selected {
      border: 2px solid #0988DE;
      border-radius: 5px;
    }
  }
  .tick {
    position: absolute;
    width: 1px;
    background: #CFD8DC;
    height: 170px;
  }
  .tick-label {
    position: absolute;
    bottom: -10px;
    transform: translate(-50%, 100%);
  }
  .el-popover {
    position: absolute;
    top: 0;
    transform: translate(-25px, -100%);
    white-space: nowrap;
  }
  .el-popover .popper__arrow {
    position: absolute;
    display: block;
    width: 0;
    height: 0;
    bottom: -7px;
    left: 10%;
    margin-right: 2px;
    filter: drop-shadow(0 2px 12px rgba(0, 0, 0, 0.03));
    border: 6px solid transparent;
    border-top-color: @line-border-color1;
    border-bottom-width: 0;
    &::after {
      position: absolute;
      display: block;
      width: 0;
      height: 0;
      border: 6px solid transparent;
      content: '';
      border-top-color: #fff;
      border-bottom-width: 0;
      bottom: 1px;
      margin-left: -6px;
    }
  }
}
</style>
